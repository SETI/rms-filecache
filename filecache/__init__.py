import os
from pathlib import Path
import requests
import tempfile
import uuid

import boto3
import botocore

from google.cloud import storage as gs_storage
import google.api_core.exceptions


try:
    from ._version import __version__
except ImportError:  # pragma: no cover
    __version__ = 'Version unspecified'


class FileCache:
    def __init__(self, temp_dir=None, shared=False):
        r"""Initialization for the FileCache class.

        Parameters:
            temp_dir (str or Path, optional): The directory in which to cache files.
                In None, the system temporary directory is used, which involves checking
                the environment variables TMPDIR, TEMP, and TMP, and if none of those are
                set then using C:\TEMP, C:\TMP, \TEMP, or \TMP on Windows and /tmp,
                /var/tmp, or /usr/tmp on other platforms. The file cache will be stored in
                a sub-directory within this temporary directory. To prevent security
                issues, the temporary directory must already exist and be writeable.

            shared (bool or str, optional): If False, the file cache will be
                stored in a unique subdirectory of temp_dir with the prefix
                ".file_cache_". If True, the file cache will be stored in a subdirectory
                of temp_dir called ".file_cache___global__". If a string is specified, the
                file cache will be stored in a subdirectory of temp_dir called
                ".file_cache_<shared>".

        Notes:
            FileCache can be used as a context, such as::

                with FileCache() as fc:
                    ...

            In this case, the cache directory is created on entry to the context, and
            delete on exit. However, if the cached is marked as shared, the directory
            will not be deleted on exit, and if all shared FileCache objects are created
            in this manner, the shared cache directory will never be deleted.
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
            sub_dir = Path(f'.file_cache_{uuid.uuid4()}')
            self._is_shared = False
        elif shared is True:
            sub_dir = Path('.file_cache___global__')
        elif isinstance(shared, str):
            if '/' in shared or '\\' in shared:
                raise ValueError('shared argument has directory elements')
            sub_dir = Path(f'.file_cache_{shared}')
        else:
            raise TypeError('shared argument is of improper type')

        if str(sub_dir.parent) != '.':  # pragma: no cover - shouldn't be possible
            raise ValueError('shared argument has directory elements')

        self._cache_dir = temp_dir / sub_dir

        try:
            self._cache_dir.mkdir(exist_ok=self._is_shared)
        except (FileNotFoundError, FileExistsError, ValueError):  # pragma: no cover
            raise

        self._file_cache = []

    @property
    def cache_dir(self):
        return self._cache_dir

    @property
    def is_shared(self):
        return self._is_shared

    def new_source(self, prefix, anonymous=False, **kwargs):
        """Create a new FileCacheSource with the given prefix.

        Parameters:
            prefix (Path or str): The prefix for the storage location. If the prefix
                starts with "gs://bucket-name" it is from Google Storage. If the prefix
                starts with "s3://bucket-name" it is from Amazon S3. If the prefix starts
                with "http://" or "https://" it is from a website download. Anything else
                is considered to be in the local filesystem and can be a str or Path
                object.
            anonymous (bool, optional): If True, access cloud resources (GS and S3)
                without specifying credentials.

        Notes:
            Depending on the given source type, there may be additional keyword
            arguments available.
        """

        return FileCacheSource(prefix, self, **kwargs)

    def clean_up(self, final=False):
        """Delete all files stored in the cache including the cache directory.

        Parameters:
            final (bool, optional): If False, a shared cache is left alone.
                If True, a shared cache is deleted. Beware that this could affect other
                processes using the same cache!
        """

        if self._is_shared:
            if not final:
                # Don't delete files from shared caches unless specifically asked to
                return

            # Delete all of the files and subdirectories we left behind, including the
            # file cache directory itself.
            # We would like to use Path.walk() but that was only added in Python 3.12
            for root, dirs, files in os.walk(self._cache_dir, topdown=False):
                for name in files:
                    os.remove(os.path.join(root, name))
                for name in dirs:
                    os.rmdir(os.path.join(root, name))

        else:
            # Delete all of the files that we know we put into the cache.
            # We don't delete every file in the cache like we do for the shared cache
            # simply because doing a full "rm -rf" can be dangerous and at least in
            # this case we should know what to delete. If something else is in the
            # cache that we didn't put there, perhaps we shouldn't delete it!
            for file_path in self._file_cache:
                print('Removing', str(file_path))
                try:
                    file_path.unlink()
                except FileNotFoundError:  # pragma: no cover
                    pass

            # Delete all of the subdirectories we left behind, including the file
            # cache directory itself. If there are any files left in these directories
            # that we didn't put there, this will raise an exception.
            # We would like to use Path.walk() but that was only added in Python 3.12
            for root, dirs, files in os.walk(self._cache_dir, topdown=False):
                for name in dirs:
                    os.rmdir(os.path.join(root, name))

        self._cache_dir.rmdir()

    def _record_cached_file(self, filename, local_path):
        print('Recording', filename, local_path)
        self._file_cache.append(local_path)

    def _is_cached(self, local_path):
        return local_path in self._file_cache

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.clean_up()


class FileCacheSource:
    """Class for retrieving files to store in a FileCache.
    """

    def __init__(self, prefix, filecache, anonymous=False):
        """Initialization for the FileCacheSource class.

        Parameters:
            prefix (str or Path): The prefix for the storage location. If the prefix
                starts with "gs://bucket-name" it is from Google Storage. If the prefix
                starts with "s3://bucket-name" it is from Amazon S3. If the prefix starts
                with "http://" or "https://" it is from a website download. Anything else
                is considered to be in the local filesystem and can be a str or Path
                object.
            file_cache (FileCache): The FileCache in which to store files retrieved
                from this source.
            anonymous (bool, optional): If True, access cloud resources (GS and S3)
                without specifying credentials.

        Raises:
            FileNotFoundError: If a local path does not exist.
        """

        self._filecache = filecache

        if not isinstance(prefix, (str, Path)):
            raise TypeError('prefix is not a str or Path')

        prefix = str(prefix).rstrip('/') + '/'

        if prefix.startswith('gs://'):
            self._init_gs_source(prefix, anonymous)
        elif prefix.startswith('s3://'):
            self._init_s3_source(prefix, anonymous)
        elif prefix.startswith(('http://', 'https://')):
            self._init_web_source(prefix)
        else:
            self._init_local_source(prefix)

    def _init_gs_source(self, prefix, anonymous):
        self._source_type = 'gs'
        self._gs_client = (gs_storage.Client().create_anonymous_client()
                           if anonymous else gs_storage.Client())
        self._gs_bucket_name, _, self._prefix = prefix.lstrip('gs://').partition('/')
        self._gs_bucket = self._gs_client.bucket(self._gs_bucket_name)
        self._cache_root = self._filecache._cache_dir / prefix.replace('gs://', 'gs_')

    def _init_s3_source(self, prefix, anonymous):
        self._source_type = 's3'
        self._s3_client = (boto3.client('s3',
                                        config=botocore.client.Config(
                                           signature_version=botocore.UNSIGNED))
                           if anonymous else boto3.client('s3'))
        self._s3_bucket_name, _, self._prefix = prefix.lstrip('s3://').partition('/')
        self._cache_root = self._filecache._cache_dir / prefix.replace('s3://', 's3_')

    def _init_web_source(self, prefix):
        self._source_type = 'web'
        self._prefix = prefix
        self._cache_root = (self._filecache._cache_dir /
                            prefix.replace('http://', 'http_')
                                  .replace('https://', 'http_'))

    def _init_local_source(self, prefix):
        self._source_type = 'local'
        # This can raise FileNotFoundError if the prefix path doesn't exist
        prefix = str(Path(prefix).resolve(strict=True)).replace('\\', '/')
        self._prefix = prefix
        self._cache_root = self._filecache._cache_dir / 'local'

    def is_cached(self, filename):
        filename = filename.replace('\\', '/').lstrip('/')

        if self._source_type == 'local':
            if '/..' in filename:
                raise ValueError(f'Invalid filename {filename}')
            local_path = Path(self._prefix) / filename
            if not local_path.exists():
                raise FileNotFoundError(f'File does not exist: {local_path}')
            return True

        local_path = self._cache_root / filename

        return self._filecache._is_cached(local_path)

    def retrieve(self, filename):
        """Retrieve a file(s) from the storage location and store it in the file cache.

        Parameters:
            filename (str): The name of the file to retrieve relative to the prefix.

        Returns:
            Path: The Path of the filename in the temporary directory.

        Raises:
            FileNotFoundError: If the file does not exist.
        """

        filename = filename.replace('\\', '/').lstrip('/')

        if self._source_type == 'local':
            if '/..' in filename:
                raise ValueError(f'Invalid filename {filename}')
            local_path = Path(self._prefix) / filename
            if not local_path.exists():
                raise FileNotFoundError(f'File does not exist: {local_path}')
            # Don't tell the file cache about local files
            return local_path

        local_path = self._cache_root / filename

        if self._filecache.is_shared and local_path.exists():
            # Another FileCache already downloaded it
            return local_path

        if self._filecache._is_cached(local_path):
            return local_path

        if local_path.exists():  # pragma: no cover
            # Not shared or local or cached and the file exists
            # This shouldn't be possible
            raise FileExistsError(f'Internal error - File already exists: {filename}')

        local_path.parent.mkdir(parents=True, exist_ok=True)

        if self._source_type == 'web':
            return self._retrieve_from_web(filename, local_path)

        if self._source_type == 'gs':
            return self._retrieve_from_gs(filename, local_path)

        if self._source_type == 's3':
            return self._retrieve_from_s3(filename, local_path)

        raise AssertionError(
            f'Internal error unknown source type {self._source_type}')  # pragma: no cover

    def _retrieve_from_web(self, filename, local_path):
        url = f'{self._prefix}{filename}'
        try:
            response = requests.get(url)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise FileNotFoundError(f'Failed to download file from: {url}') from e

        with open(local_path, 'wb') as f:
            f.write(response.content)

        self._filecache._record_cached_file(filename, local_path)
        return local_path

    def _retrieve_from_gs(self, filename, local_path):
        blob_name = f'{self._prefix}{filename}'
        blob = self._gs_bucket.blob(blob_name)
        try:
            blob.download_to_filename(str(local_path))
        except (google.api_core.exceptions.BadRequest,  # bad bucket name
                google.resumable_media.common.InvalidResponse,  # bad bucket name
                google.cloud.exceptions.NotFound):  # bad filename
            # The google API library will still create the file before noticing
            # that it can't be downloaded, so we have to remove it here
            local_path.unlink()
            raise FileNotFoundError(
                f'Failed to download file from: gs://{self._gs_bucket_name}/'
                f'{blob_name}')
        self._filecache._record_cached_file(filename, local_path)
        return local_path

    def _retrieve_from_s3(self, filename, local_path):
        s3_key = f'{self._prefix}{filename}'
        try:
            self._s3_client.download_file(self._s3_bucket_name, s3_key, str(local_path))
        except botocore.exceptions.ClientError:
            raise FileNotFoundError(
                f'Failed to download file from: s3://{self._s3_bucket_name}/'
                f'{s3_key}')

        self._filecache._record_cached_file(filename, local_path)
        return local_path
