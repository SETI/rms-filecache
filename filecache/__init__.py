import atexit
import contextlib
import os
from pathlib import Path
import requests
import tempfile
import uuid

import filelock

import boto3
import botocore

from google.cloud import storage as gs_storage
import google.api_core.exceptions

try:
    from ._version import __version__
except ImportError:  # pragma: no cover
    __version__ = 'Version unspecified'


_GLOBAL_LOGGER = False


def set_global_logger(logger):
    """Set the global logger for all FileCache instances created in the future."""
    global _GLOBAL_LOGGER
    logger = logger if logger else False  # Turn None into False
    _GLOBAL_LOGGER = logger


class FileCache:
    _FILE_CACHE_PREFIX = '.file_cache_'

    def __init__(self, temp_dir=None, shared=False, cache_owner=False, mp_safe=None,
                 atexit_cleanup=True, logger=None):
        r"""Initialization for the FileCache class.

        Parameters:
            temp_dir (str or Path, optional): The directory in which to cache files.
                In None, the system temporary directory is used, which involves checking
                the environment variables ``TMPDIR``, ``TEMP``, and ``TMP``, and if none
                of those are set then using ``C:\TEMP``, ``C:\TMP``, ``\TEMP``, or
                ``\TMP`` on Windows and ``/tmp``, ``/var/tmp``, or ``/usr/tmp`` on other
                platforms. The file cache will be stored in a sub-directory within this
                temporary directory. To prevent security issues, the temporary directory
                must already exist and be writeable.
            shared (bool or str, optional): If False, the file cache will be
                stored in a uniquely-named subdirectory of `temp_dir` with the prefix
                ``.file_cache_``. If True, the file cache will be stored in a subdirectory
                of `temp_dir` called ``.file_cache___global__``. If a string is specified,
                the file cache will be stored in a subdirectory of `temp_dir` called
                ``.file_cache_<shared>``.
            cache_owner (bool, optional): This option is only relevant if `shared` is not
                False. If `cache_owner` is True, this FileCache is considered the owner of
                the shared cache, and if it is created as a context manager then on exit
                the shared cache will be deleted. If it is not created as a context
                manager then the :meth:`clean_up` method needs to be called. This option
                should only be set to True if this FileCache is going to be the sole user
                of the cache, or if the process creating this file cache owns the cache
                contents and has control over all other processes that might be accessing
                it.
            mp_safe (bool or None, optional): If False, never create new prefixes without
                multiprocessor-safety locking. If True, always create new prefixes with
                multiprocessor-safety locking. If None, safety locking is used if shared
                is not False, as it is assumed that multiple programs will be using the
                shared cache simultaneously.
            atexit_cleanup (bool, optional): If True, at program exit automatically call
                the clean_up method. Note that using this option will keep this class
                instance around for the entire duration of the program, potentially
                wasting memory. To be more memory conscious, but also be solely
                responsible for calling clean_up, set this parameter to False.
            logger (logger, optional): If False, do not do any logging. If None, use the
                global logger set with :func:`set_global_logger`. Otherwise use the
                specified logger.

        Notes:
            FileCache can be used as a context, such as::

                with FileCache() as fc:
                    ...

            In this case, the cache directory is created on entry to the context and
            deleted on exit. However, if the cached is marked as shared, the directory
            will not be deleted on exit, and if all shared FileCache objects are created
            in this manner, the shared cache directories will never be deleted unless
            :meth:`clean_up` is explicitly called.
        """

        # We try very hard here to make sure that no possible passed-in argument for
        # temp_dir or shared could result in a directory name that is anything other
        # than a new cache directory. In particular, since we may be deleting this
        # directory later, we want to make sure it's impossible for a bad actor to inject
        # a directory or filename that could result in the deletion of system or user
        # files. One key aspect of this is we do not allow the user to specify the
        # specific subdirectory name without the unique prefix, and we do not allow
        # the shared directory name to have additional directory components like "..".

        if temp_dir is None:
            temp_dir = tempfile.gettempdir()
        temp_dir = Path(temp_dir).resolve()

        self._is_shared = True

        if shared is False:
            sub_dir = Path(f'{self._FILE_CACHE_PREFIX}{uuid.uuid4()}')
            self._is_shared = False
        elif shared is True:
            sub_dir = Path(f'{self._FILE_CACHE_PREFIX}__global__')
        elif isinstance(shared, str):
            if '/' in shared or '\\' in shared:
                raise ValueError('shared argument has directory elements')
            sub_dir = Path(f'{self._FILE_CACHE_PREFIX}{shared}')
        else:
            raise TypeError('shared argument is of improper type')

        if str(sub_dir.parent) != '.':  # pragma: no cover - shouldn't be possible
            raise ValueError('shared argument has directory elements')

        self._logger = _GLOBAL_LOGGER if logger is None else logger

        self._cache_dir = temp_dir / sub_dir
        if self._logger:
            if self._is_shared:
                self._logger.debug(f'Creating shared cache {self._cache_dir}')
            else:
                self._logger.debug(f'Creating cache {self._cache_dir}')
        self._cache_dir.mkdir(exist_ok=self._is_shared)

        self._is_cache_owner = cache_owner
        self._is_mp_safe = mp_safe if mp_safe is not None else self._is_shared

        if atexit_cleanup:
            atexit.register(self.clean_up)

    @property
    def cache_dir(self):
        """Return the top-level directory of the cache."""
        return self._cache_dir

    @property
    def is_shared(self):
        """Return a bool indicating whether or not this cache is shared."""
        return self._is_shared

    @property
    def is_cache_owner(self):
        """Return a bool indicating whether or not this FileCache owns a shared cache."""
        return self._is_cache_owner

    @property
    def is_mp_safe(self):
        """Return a bool indicating whether or not this FileCache is multi-processor safe.
        """
        return self._is_mp_safe

    def new_prefix(self, prefix, anonymous=False, lock_timeout=60, **kwargs):
        """Create a new FileCachePrefix with the given prefix.

        Parameters:
            prefix (Path or str): The prefix for the storage location. If the prefix
                starts with ``gs://bucket-name`` it is from Google Storage. If the prefix
                starts with ``s3://bucket-name`` it is from Amazon S3. If the prefix
                starts with ``http://`` or ``https://`` it is from a website download.
                Anything else is considered to be in the local filesystem and can be a str
                or Path object.
            anonymous (bool, optional): If True, access cloud resources (GS and S3)
                without specifying credentials. Otherwise, credentials must be initialized
                in the program's environment.
            lock_timeout(int, optional): How long to wait if another process is marked
                as retrieving the file before raising an exception.

        Notes:
            Depending on the given prefix type, there may be additional keyword arguments
            available.
        """

        return FileCachePrefix(prefix, self, anonymous=anonymous,
                               lock_timeout=lock_timeout, logger=self._logger,
                               **kwargs)

    def clean_up(self, final=False):
        """Delete all files stored in the cache including the cache directory.

        Parameters:
            final (bool, optional): If False and this FileCache is not marked as the
                `cache_owner` of the cache, a shared cache is left alone. If False and
                this FileCache is marked as the `cache_owner` of the cache, or if True, a
                shared cache is deleted. Beware that this could affect other processes
                using the same cache!
        """

        if self._logger:
            self._logger.debug(f'Cleaning up cache {self._cache_dir}')

        # Verify this is really a cache directory before walking it and deleting
        # every file. We are just being paranoid to make sure this never does a
        # "rm -rf" on a real directory like "/".
        if not Path(self._cache_dir).name.startswith(self._FILE_CACHE_PREFIX):
            raise ValueError(
                f'Cache directory does not start with {self._FILE_CACHE_PREFIX}')

        if not self._is_shared or (self._is_shared and (final or self.is_cache_owner)):
            # Delete all of the files and subdirectories we left behind, including the
            # file cache directory itself.
            # We would like to use Path.walk() but that was only added in Python 3.12.
            # We allow remove and rmdir to fail with FileNotFoundError because we could
            # have two programs cleaning up a shared cache at the same time fighting
            # each other, or someone could have asked for the local path to a file and
            # then never written anything there.
            for root, dirs, files in os.walk(self._cache_dir, topdown=False):
                for name in files:
                    if self._logger:
                        self._logger.debug(f'  Removing {name}')
                    try:
                        os.remove(os.path.join(root, name))
                    except FileNotFoundError:  # pragma: no cover
                        pass
                for name in dirs:
                    if self._logger:
                        self._logger.debug(f'  Removing {name}')
                    try:
                        os.rmdir(os.path.join(root, name))
                    except FileNotFoundError:  # pragma: no cover
                        pass

            if self._logger:
                self._logger.debug(f'  Removing {self._cache_dir}')
            try:
                os.rmdir(self._cache_dir)
            except FileNotFoundError:  # pragma: no cover
                pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.clean_up()


class FileCachePrefix:
    """Class for retrieving and uploading files in a FileCache."""

    def __init__(self, prefix, filecache, anonymous=False, lock_timeout=60, logger=None):
        """Initialization for the FileCachePrefix class.

        Parameters:
            prefix (str or Path): The prefix for the storage location. If the prefix
                starts with ``gs://bucket-name`` it is from Google Storage. If the prefix
                starts with ``s3://bucket-name`` it is from Amazon S3. If the prefix
                starts with ``http://`` or ``https://`` it is from a website download.
                Anything else is considered to be in the local filesystem and can be a str
                or Path object.
            file_cache (FileCache): The FileCache in which to store files retrieved
                from this prefix.
            anonymous (bool, optional): If True, access cloud resources (GS and S3)
                without specifying credentials.
            lock_timeout(int, optional): How long to wait if another process is marked
                as retrieving the file before raising an exception.
            logger (logger, optional): If False, do not do any logging. If None, use the
                global logger set with :func:`set_global_logger`. Otherwise use the
                specified logger.

        Notes:
            If the specified FileCache is marked as being multiprocessor-safe, then file
            locking will be used to protect against multiple instances of FileCachePrefix
            downloading the same file into the same cache. Note that this will likely only
            work properly on a local filesystem.
        """

        self._filecache = filecache
        self._lock_timeout = lock_timeout
        self._logger = _GLOBAL_LOGGER if logger is None else logger

        if not isinstance(prefix, (str, Path)):
            raise TypeError('prefix is not a str or Path')

        prefix = str(prefix).rstrip('/') + '/'

        if self._logger:
            self._logger.debug(f'Initializing prefix {prefix}')

        if prefix.startswith('gs://'):
            self._init_gs_prefix(prefix, anonymous)
        elif prefix.startswith('s3://'):
            self._init_s3_prefix(prefix, anonymous)
        elif prefix.startswith(('http://', 'https://')):
            self._init_web_prefix(prefix)
        else:
            self._init_local_prefix(prefix)

    def _init_gs_prefix(self, prefix, anonymous):
        self._prefix_type = 'gs'
        self._gs_client = (gs_storage.Client().create_anonymous_client()
                           if anonymous else gs_storage.Client())
        self._gs_bucket_name, _, self._prefix = prefix.lstrip('gs://').partition('/')
        self._gs_bucket = self._gs_client.bucket(self._gs_bucket_name)
        self._cache_root = self._filecache._cache_dir / prefix.replace('gs://', 'gs_')

    def _init_s3_prefix(self, prefix, anonymous):
        self._prefix_type = 's3'
        self._s3_client = (boto3.client('s3',
                                        config=botocore.client.Config(
                                           signature_version=botocore.UNSIGNED))
                           if anonymous else boto3.client('s3'))
        self._s3_bucket_name, _, self._prefix = prefix.lstrip('s3://').partition('/')
        self._cache_root = self._filecache._cache_dir / prefix.replace('s3://', 's3_')

    def _init_web_prefix(self, prefix):
        self._prefix_type = 'web'
        self._prefix = prefix
        self._cache_root = (self._filecache._cache_dir /
                            prefix.replace('http://', 'http_')
                                  .replace('https://', 'http_'))

    def _init_local_prefix(self, prefix):
        self._prefix_type = 'local'
        # This can raise FileNotFoundError if the prefix path doesn't exist
        prefix = str(Path(prefix).resolve(strict=True)).replace('\\', '/')
        self._prefix = prefix
        self._cache_root = Path(prefix)

    def get_local_path(self, filename):
        """Return the local path for the given filename using this prefix.

        Parameters:
            filename (str): The name of the file to use relative to this prefix.

        Returns:
            Path: The Path of the filename in the temporary directory. The file does not
            have to exist because this path could be used for writing a file to upload.
            Because of this, the complete parent directory structure will have been
            created as necessary.
        """

        filename = filename.replace('\\', '/').lstrip('/')
        local_path = self._cache_root / filename
        local_path.parent.mkdir(parents=True, exist_ok=True)

        if self._logger:
            self._logger.debug(f'Returning local path for {filename} as {local_path}')

        return local_path

    def _lock_path(self, path):
        path = Path(path)
        return path.parent / f'.__lock__{path.name}'

    def retrieve(self, filename):
        """Retrieve a file from the storage location and store it in the file cache.

        Parameters:
            filename (str): The name of the file to retrieve relative to the prefix.

        Returns:
            Path: The Path of the filename in the temporary directory.

        Raises:
            FileNotFoundError: If the file does not exist.
            TimeoutError: If we could not acquire the lock to allow downloading of the
                file within the given timeout.
        """

        filename = filename.replace('\\', '/').lstrip('/')

        if self._prefix_type == 'local':
            local_path = Path(self._cache_root) / filename
            if self._logger:
                self._logger.debug(f'Accessing local file {filename}')
            if '/..' in filename:
                raise ValueError(f'Invalid filename {filename}')
            if not local_path.exists():
                raise FileNotFoundError(f'File does not exist: {local_path}')
            # Don't tell the file cache about local files
            return local_path

        local_path = self._cache_root / filename

        if self._filecache.is_mp_safe:
            lock_path = self._lock_path(local_path)
            lock = filelock.FileLock(lock_path, timeout=self._lock_timeout)
            try:
                lock.acquire()
            except filelock._error.Timeout:
                raise TimeoutError(f'Could not acquire lock on {lock_path}')
            try:
                return self._unprotected_retrieve(filename, local_path)
            finally:
                # Technically there is a potential race condition here, because after
                # we release the lock, someone else could lock this file, and then we
                # could delete it (because on Linux locks are only advisory). Then the
                # next process to come along to try to lock this file would also succeed
                # because it would really be a different lock file! However, we have
                # to do it in this order because otherwise it won't work on Windows,
                # where locks are not just advisory.
                lock.release()
                lock_path.unlink(missing_ok=True)

        return self._unprotected_retrieve(filename, local_path)

    def _unprotected_retrieve(self, filename, local_path):
        if local_path.exists():
            if self._logger:
                self._logger.debug(
                    f'Accessing existing {self._cache_root}/{filename} at {local_path}')
            return local_path

        local_path.parent.mkdir(parents=True, exist_ok=True)

        if self._logger:
            self._logger.debug(
                f'Downloading {self._cache_root}/{filename} to {local_path}')

        if self._prefix_type == 'web':
            return self._retrieve_from_web(filename, local_path)

        if self._prefix_type == 'gs':
            return self._retrieve_from_gs(filename, local_path)

        if self._prefix_type == 's3':
            return self._retrieve_from_s3(filename, local_path)

        raise AssertionError(
            f'Internal error unknown prefix type {self._prefix_type}')  # pragma: no cover

    def _retrieve_from_web(self, filename, local_path):
        url = f'{self._prefix}{filename}'
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise FileNotFoundError(f'Failed to download file from: {url}') from e

        temp_local_path = local_path.with_suffix(local_path.suffix + '.dltemp')
        try:
            with open(temp_local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=1024*1024):
                    f.write(chunk)
            temp_local_path.rename(local_path)
        except Exception:  # pragma: no cover
            temp_local_path.unlink(missing_ok=True)
            raise

        return local_path

    def _retrieve_from_gs(self, filename, local_path):
        blob_name = f'{self._prefix}{filename}'
        blob = self._gs_bucket.blob(blob_name)

        temp_local_path = local_path.with_suffix(local_path.suffix + '.dltemp')
        try:
            blob.download_to_filename(str(temp_local_path))
            temp_local_path.rename(local_path)
        except (google.api_core.exceptions.BadRequest,  # bad bucket name
                google.resumable_media.common.InvalidResponse,  # bad bucket name
                google.cloud.exceptions.NotFound):  # bad filename
            # The google API library will still create the file before noticing
            # that it can't be downloaded, so we have to remove it here
            temp_local_path.unlink(missing_ok=True)
            raise FileNotFoundError(
                f'Failed to download file from: gs://{self._gs_bucket_name}/'
                f'{blob_name}')
        except Exception:  # pragma: no cover
            temp_local_path.unlink(missing_ok=True)
            raise

        return local_path

    def _retrieve_from_s3(self, filename, local_path):
        s3_key = f'{self._prefix}{filename}'

        temp_local_path = local_path.with_suffix(local_path.suffix + '.dltemp')
        try:
            self._s3_client.download_file(self._s3_bucket_name, s3_key,
                                          str(temp_local_path))
            temp_local_path.rename(local_path)
        except botocore.exceptions.ClientError:
            temp_local_path.unlink(missing_ok=True)
            raise FileNotFoundError(
                f'Failed to download file from: s3://{self._s3_bucket_name}/'
                f'{s3_key}')
        except Exception:  # pragma: no cover
            temp_local_path.unlink(missing_ok=True)
            raise

        return local_path


    def upload(self, filename):
        """Send a file from the file cache to the storage location.

        Parameters:
            filename (str): The name of the file to upload relative to the prefix.

        Raises:
            FileNotFoundError: If the file does not exist.
        """

        filename = filename.replace('\\', '/').lstrip('/')
        local_path = self._cache_root / filename

        if not local_path.exists():
            raise FileNotFoundError(f'File {local_path} does not exist')

        if self._prefix_type == 'local':
            # We don't do anything for local paths since the file is already in the
            # correct location.
            return

        if self._logger:
            self._logger.debug(f'Uploading {local_path} to {self._prefix}/{filename}')

        if self._prefix_type == 'gs':
            return self._upload_to_gs(filename, local_path)

        if self._prefix_type == 's3':
            return self._upload_to_s3(filename, local_path)

        raise AssertionError(
            f'Internal error unknown prefix type {self._prefix_type}')  # pragma: no cover

    def _upload_to_gs(self, filename, local_path):
        blob_name = f'{self._prefix}{filename}'
        blob = self._gs_bucket.blob(blob_name)

        blob.upload_from_filename(str(local_path))

    def _upload_to_s3(self, filename, local_path):
        s3_key = f'{self._prefix}{filename}'

        self._s3_client.upload_file(str(local_path), self._s3_bucket_name, s3_key)

    @contextlib.contextmanager
    def open(self, filename, mode='r', *args, **kwargs):
        """Retrieve/open or open/upload a file as a context manager.

        If `mode` is a read mode (like ``r`` or ``rb``) then the file will be first
        retrieved by calling :meth:`retrieve` and then opened. If the `mode` is a write
        mode (like ``w`` or ``wb``) then the file will be first opened for write, and when
        this context manager is exited the file will be uploaded.

        Parameters:
            filename (str or Path): The filename to open.

        Returns:
            file-like object: The same object as would be returned by the normal `open()`
            function.
        """

        if mode[0] == 'r':
            local_path = self.retrieve(filename)
            with open(local_path, mode, *args, **kwargs) as fp:
                yield fp
        else:
            local_path = self.get_local_path(filename)
            with open(local_path, mode, *args, **kwargs) as fp:
                yield fp
            self.upload(filename)

    @property
    def filecache(self):
        """Return the FileCache object associated with this instance."""
        return self._filecache
