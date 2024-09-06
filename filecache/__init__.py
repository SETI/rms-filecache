from pathlib import Path
import requests
import tempfile
import uuid

# import boto3
# from botocore.exceptions import NoCredentialsError

from google.cloud import storage as gs_storage
from google.auth.exceptions import DefaultCredentialsError
import google.api_core.exceptions


try:
    from ._version import __version__
except ImportError:  # pragma: no cover
    __version__ = 'Version unspecified'


class FileCache:
    def __init__(self, temp_dir=None, shared=False):
        r"""

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
            if '/' in shared or '\\':
                raise ValueError('shared argument has directory elements')
            sub_dir = Path(f'.file_cache_{shared}')
        else:
            raise ValueError('shared argument is of improper type')

        if str(sub_dir.parent) != '.':
            raise ValueError('shared argument has directory elements')

        self._cache_dir = temp_dir / sub_dir

        try:
            self._cache_dir.mkdir(exist_ok=False)
        except (FileNotFoundError, FileExistsError, ValueError):
            raise

        self._file_cache = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.clean_up()

    def new_source(self, prefix):
        """_summary_

        Args:
            prefix (_type_): _description_
        """

        return FileCacheSource(prefix, self)

    def _record_cached_file(self, filename, local_path):
        print('Recording', filename, local_path)
        self._file_cache.append(local_path)

    def _is_cached(self, local_path):
        return local_path in self._file_cache

    def clean_up(self, force_shared=False):
        """Delete all files stored in the cache including the cache directory.

        Parameters:
            force_shared (bool, optional): If False, a shared cache is left alone.
                If True, a shared cache is deleted. Beware that this could affect
                other processes using the same cache!
        """

        if self._is_shared and not force_shared:
            # Don't delete files from shared caches unless specifically asked to
            return

        print('Cleaning up', self._cache_dir)
        # Delete all of the files that we know we put into the cache
        for file_path in self._file_cache:
            print('Removing', str(file_path))
            try:
                file_path.unlink()
            except FileNotFoundError:
                pass

        # Delete all of the subdirectories we left behind, including the file
        # cache directory itself. If there are any files left in these directories
        # that we didn't put there, this will raise an exception.
        for root, dirs, files in self._cache_dir.walk(top_down=False):
            for name in dirs:
                (root / name).rmdir()

        self._cache_dir.rmdir()


class FileCacheSource:
    """Base class for retrieving files to store in a FileCache.
    """

    def __init__(self, prefix, filecache):
        """

        Parameters:
            prefix (str): The prefix for the storage location. If the prefix starts
                with "gs://bucket-name" it is from Google Storage. If the prefix starts
                with "s3://bucket-name" it is from Amazon S3. If the prefix starts with
                "http://" or "https://" it is from a website download. Anything else
                is considered to be in the local filesystem.
            file_cache (FileCache): The FileCache in which to store files retrieved
                from this source.
        """

        self._filecache = filecache

        prefix = prefix.rstrip('/')
        get_prefix = prefix

        if prefix.startswith('gs://'):
            self._source_type = 'gs'
            try:
                self._gs_client = gs_storage.Client()
            except DefaultCredentialsError:
                # See https://cloud.google.com/docs/authentication/
                # provide-credentials-adc#how-to
                raise
            self._gs_bucket_name, _, get_prefix = prefix.lstrip('gs://').partition('/')
            self._gs_bucket = self._gs_client.bucket(self._gs_bucket_name)

        elif prefix.startswith('s3://'):
            self._source_type = 's3'

        elif prefix.startswith(('http://', 'https://')):
            self._source_type = 'web'

        else:
            self._source_type = 'local'
            prefix = str(Path(prefix).resolve()).replace('\\', '/')

        # What to add to a filename when retrieving data
        self._prefix = get_prefix

        # What to add to a filename when storing in the file cache
        if self._source_type == 'local':
            self._cache_root = self._filecache._cache_dir / 'local'
        else:
            self._cache_root = (self._filecache._cache_dir /
                                prefix.replace('gs://', 'gs_')
                                      .replace('s3://', 's3_')
                                      .replace('http://', 'http_')
                                      .replace('https://', 'http_'))

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
            # Don't don't tell the file cache about local files
            return local_path

        local_path = self._cache_root / filename

        if self._filecache._is_cached(local_path):
            return local_path

        local_path.parent.mkdir(parents=True, exist_ok=True)

        if self._source_type == 'web':
            url = f'{self._prefix}/{filename}'
            response = requests.get(url)
            if response.status_code == 200:
                with open(local_path, 'wb') as f:
                    f.write(response.content)
                self._filecache._record_cached_file(filename, local_path)
                return local_path
            else:
                raise FileNotFoundError(f'Failed to download file from: {url}')

        if self._source_type == 'gs':
            blob_name = f'{self._prefix}/{filename}'
            blob = self._gs_bucket.blob(blob_name)
            try:
                blob.download_to_filename(str(local_path))
            except google.cloud.exceptions.NotFound:
                raise FileNotFoundError(
                    f'Failed to download file from: gs://{self._gs_bucket_name}/'
                    f'{blob_name}')
            self._filecache._record_cached_file(filename, local_path)
            return local_path

    # @contextmanager
    # def retrieve_context(self, filename):
    #     """Context manager for retrieving a file.

    #     Args:
    #         filename (str): The name of the file to retrieve.

    #     Yields:
    #         Path: The path to the retrieved file.
    #     """
    #     file_path = self.retrieve(filename)
    #     try:
    #         yield file_path
    #     finally:
    #         if file_path in self.retrieved_files:
    #             self.clean_up()

# class S3StorageManager(StorageManager):
#     """Storage manager for Amazon S3."""

#     def __init__(self, prefix, temp_dir=None, aws_access_key_id=None,
# aws_secret_access_key=None):
#         super().__init__(prefix, temp_dir)
#         self.s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id,
# aws_secret_access_key=aws_secret_access_key)

#     def retrieve(self, filename):
#         bucket_name, s3_key = self._parse_s3_path(filename)
#         local_path = self.temp_dir / filename
#         local_path.parent.mkdir(parents=True, exist_ok=True)

#         try:
#             self.s3_client.download_file(bucket_name, s3_key, str(local_path))
#             self.retrieved_files.append(local_path)
#             return local_path
#         except NoCredentialsError:
#             raise Exception("AWS credentials not found")

#     def _parse_s3_path(self, filename):
#         bucket_name = self.prefix.split("//")[1]
#         return bucket_name, filename
