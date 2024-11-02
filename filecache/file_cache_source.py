from concurrent import futures
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import requests
import uuid

import boto3
import botocore

from google.cloud import storage as gs_storage  # type: ignore
import google.api_core.exceptions


class FileCacheSource:
    """Superclass for all remote file source classes. Do not use directly.

    The :class:`FileCacheSource` subclasses (:class:`FileCacheSourceLocal`,
    :class:`FileCacheSourceHTTP`, :class:`FileCacheSourceGS`, and
    :class:`FileCacheSourceS3`) provide direct access to local and remote sources,
    bypassing the caching mechanism of :class:`FileCache`.
    """

    def __init__(self, src_prefix=None):
        """Initialization for the FileCacheSource superclass.

        Note:
            Do not instantiate :class:`FileCacheSource` directly. Instead use one of the
            subclasses (:class:`FileCacheSourceLocal`, :class:`FileCacheSourceHTTP`,
            :class:`FileCacheSourceGS`, and :class:`FileCacheSourceS3`).

        Parameters:
            src_prefix (str, optional): The prefix for the source, which can be one of
                ``http://<host>``, ``https://<host>``, ``gs://<bucket>``, or
                ``s3://<bucket>``. For a local source, this parameter should not be
                specified.
        """

        self._src_type = None
        self._src_prefix_ = (src_prefix if src_prefix else '').rstrip('/') + '/'

        # The _cache_subdir attribute is only used by the FileCache class
        self._cache_subdir = None

    def exists(self, sub_path):
        raise NotImplementedError

    def retrieve(self, sub_path, local_path):
        raise NotImplementedError

    def retrieve_multi(self, sub_paths, local_paths, nthreads=8):
        """Retrieve multiple files from the storage location using threads.

        Parameters:
            sub_paths (list or tuple): The path of the files to retrieve relative to the
                source prefix.
            local_paths (list or tuple): The paths to the destinations where the
                downloaded files will be stored.
            nthreads (int, optional): The maximum number of threads to use.

        Returns:
            list[Path or Exception]: A list containing the local paths of the retrieved
            files. If a file failed to download, the entry is the Exception that caused
            the failure. This list is in the same order and has the same length as
            `local_paths`.

        Notes:
            All parent directories in all `local_paths` are created even if a file
            download fails.

            The download of each file is an atomic operation. However, even if some files
            have download failures, all other files will be downloaded.
        """

        if not isinstance(nthreads, int) or nthreads <= 0:
            raise ValueError(f'nthreads must be a positive integer, got {nthreads}')

        results = {}
        for sub_path, result in self._download_object_parallel(sub_paths, local_paths,
                                                               nthreads):
            results[sub_path] = result

        ret = []
        for sub_path in sub_paths:
            ret.append(results[sub_path])

        return ret

    def _download_object(self, sub_path, local_path):
        self.retrieve(sub_path, local_path)
        return local_path

    def _download_object_parallel(self, sub_paths, local_paths, nthreads):
        with ThreadPoolExecutor(max_workers=nthreads) as executor:
            future_to_paths = {executor.submit(self._download_object, x[0], x[1]): x[0]
                               for x in zip(sub_paths, local_paths)}
            for future in futures.as_completed(future_to_paths):
                sub_path = future_to_paths[future]
                exception = future.exception()
                if not exception:
                    yield sub_path, future.result()
                else:
                    yield sub_path, exception

    def upload(self, sub_path, local_path):
        raise NotImplementedError

    def upload_multi(self, sub_paths, local_paths, nthreads=8):
        """Upload multiple files to a storage location.

        Parameters:
            sub_paths (list or tuple): The path of the destination files relative to the
                source prefix.
            local_paths (list or tuple): The paths of the files to upload.
            nthreads (int, optional): The maximum number of threads to use.

        Returns:
            list[Path or Exception]: A list containing the local paths of the uploaded
            files. If a file failed to upload, the entry is the Exception that caused the
            failure. This list is in the same order and has the same length as
            `local_paths`.
        """

        if not isinstance(nthreads, int) or nthreads <= 0:
            raise ValueError(f'nthreads must be a positive integer, got {nthreads}')

        results = {}
        for sub_path, result in self._upload_object_parallel(sub_paths, local_paths,
                                                             nthreads):
            results[sub_path] = result

        ret = []
        for sub_path in sub_paths:
            ret.append(results[sub_path])

        return ret

    def _upload_object(self, sub_path, local_path):
        self.upload(sub_path, local_path)
        return local_path

    def _upload_object_parallel(self, sub_paths, local_paths, nthreads):
        with ThreadPoolExecutor(max_workers=nthreads) as executor:
            future_to_paths = {executor.submit(self._upload_object, x[0], x[1]): x[0]
                               for x in zip(sub_paths, local_paths)}
            for future in futures.as_completed(future_to_paths):
                sub_path = future_to_paths[future]
                exception = future.exception()
                if not exception:
                    yield sub_path, future.result()
                else:
                    yield sub_path, exception


class FileCacheSourceLocal(FileCacheSource):
    """Class that provides direct access to local files.

    This class is unlikely to be directly useful to an external program, as it provides
    essentially no functionality on top of the standard Python filesystem functions.
    """

    def __init__(self, src_prefix=None, anonymous=False, **kwargs):
        """Initialization for the FileCacheLocal class.

        Parameters:
            src_prefix (str, optional): This parameter is only provided to mirror the
                signature of the other source classes. It should not be used.
            anonymous (bool, optional): This parameter is only provided to mirror the
                signature of the other source classes. It should not be used.
        """

        if src_prefix is not None and src_prefix != '':
            raise ValueError(f'Invalid prefix: {src_prefix}')
        super().__init__(**kwargs)

        self._src_type = 'local'
        self._cache_subdir = ''

    def exists(self, sub_path):
        """Check if a file exists without downloading it.

        Parameters:
            sub_path (str): The full path of the local file.

        Returns:
            bool: True if the file exists. Note that it is possible that a file could
            exist and still not be accessible due to permissions.
        """

        return Path(sub_path).is_file()

    def retrieve(self, sub_path, local_path):
        """Retrieve a file from the storage location.

        Parameters:
            sub_path (str or Path): The full path of the local file to retrieve.
            local_path (str or Path): The path to the desination where the file will
                be stored.

        Returns:
            Path: The Path of the filename, which is the same as the `sub_path`
            parameter.

        Raises:
            ValueError: If `sub_path` and `local_path` are not identical.
            FileNotFoundError: If the file does not exist.

        Notes:
            This method essentially does nothing except check for the existence of the
            file.
        """

        local_path = Path(local_path).expanduser().resolve()
        sub_path = Path(sub_path).expanduser().resolve()

        if local_path != sub_path:
            raise ValueError(
                f'Paths differ for local retrieve: {local_path} and {sub_path}')

        if not sub_path.is_file():
            raise FileNotFoundError(f'File does not exist: {sub_path}')

        # We don't actually do anything for local paths since the file is already in the
        # correct location.
        return local_path

    def upload(self, sub_path, local_path):
        """Upload a file from the local filesystem to the storage location.

        Parameters:
            sub_path (str or Path): The full path of the destination.
            local_path (str or Path): The full path of the local file to upload.

        Returns:
            Path: The Path of the filename, which is the same as the `local_path`
            parameter.

        Raises:
            ValueError: If `sub_path` and `local_path` are not identical.
            FileNotFoundError: If the file does not exist.
        """

        local_path = Path(local_path).expanduser().resolve()
        sub_path = Path(sub_path).expanduser().resolve()

        if local_path != sub_path:
            raise ValueError(
                f'Paths differ for local upload: {local_path} and {sub_path}')

        if not local_path.is_file():
            raise FileNotFoundError(f'File does not exist: {local_path}')

        # We don't actually do anything for local paths since the file is already in the
        # correct location.
        return local_path


class FileCacheSourceHTTP(FileCacheSource):
    """Class that provides access to files stored on a webserver."""

    def __init__(self, src_prefix, anonymous=False, **kwargs):
        """Initialization for the FileCacheHTTP class.

        Parameters:
            src_prefix (str): The prefix to all URL accesses, of the form
                ``http://<hostname>`` or ``https://<hostname>``.
            anonymous (bool, optional): This parameter is only provided to mirror the
                signature of the other source classes. It should not be used.
        """

        src_prefix = src_prefix.rstrip('/')
        if (not src_prefix.startswith(('http://', 'https://')) or
                src_prefix.count('/') != 2):
            raise ValueError(f'Invalid prefix: {src_prefix}')

        super().__init__(src_prefix)

        self._prefix_type = 'web'
        self._cache_subdir = (src_prefix
                              .replace('http://', 'http_')
                              .replace('https://', 'http_'))

    def exists(self, sub_path):
        """Check if a file exists without downloading it.

        Parameters:
            sub_path (str): The path of the file on the webserver given by the source
                prefix.

        Returns:
            bool: True if the file (including the webserver) exists. Note that it is
            possible that a file could exist and still not be downloadable due to
            permissions.
        """

        ret = True
        try:
            response = requests.head(f'{self._src_prefix_}{sub_path}')
            response.raise_for_status()
        except requests.exceptions.RequestException:
            ret = False

        return ret

    def retrieve(self, sub_path, local_path):
        """Retrieve a file from the webserver.

        Parameters:
            sub_path (str): The path of the file to retrieve relative to the source
                prefix.
            local_path (str or Path): The path to the destination where the downloaded
                file will be stored.

        Returns:
            Path: The Path where the file was stored (same as `local_path`).

        Raises:
            FileNotFoundError: If the remote file does not exist or the download fails for
                another reason.

        Notes:
            All parent directories in `local_path` are created even if the file download
            fails.

            The download is an atomic operation.
        """

        local_path = Path(local_path).expanduser().resolve()

        url = f'{self._src_prefix_}{sub_path}'

        local_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise FileNotFoundError(f'Failed to download file: {url}') from e

        temp_local_path = local_path.with_suffix(f'{local_path.suffix}.{uuid.uuid4()}')
        try:
            with open(temp_local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=1024*1024):
                    f.write(chunk)
            temp_local_path.rename(local_path)
        except Exception:
            temp_local_path.unlink(missing_ok=True)
            raise

        return local_path

    def upload(self, sub_path, local_path):
        """Upload a local file to a webserver. Not implemented."""

        raise NotImplementedError


class FileCacheSourceGS(FileCacheSource):
    """Class that provides access to files stored in Google Storage."""

    def __init__(self, src_prefix, anonymous=False, **kwargs):
        """Initialization for the FileCacheGS class.

        Parameters:
            src_prefix (str): The prefix for all Google Storage accesses, of the form
                ``gs://<bucket>``.
            anonymous (bool, optional): If True, access Google Storage without specifying
                credentials. Otherwise, credentials must be initialized in the program's
                environment.
        """

        src_prefix = src_prefix.rstrip('/')
        if (not src_prefix.startswith('gs://') or
                src_prefix.count('/') != 2):
            raise ValueError(f'Invalid prefix: {src_prefix}')

        super().__init__(src_prefix)

        self._src_type = 'gs'
        self._client = (gs_storage.Client.create_anonymous_client()
                        if anonymous else gs_storage.Client())
        self._bucket_name = src_prefix.lstrip('gs://')
        self._bucket = self._client.bucket(self._bucket_name)
        self._cache_subdir = src_prefix.replace('gs://', 'gs_')

    def exists(self, sub_path, logger=None):
        """Check if a file exists without downloading it.

        Parameters:
            sub_path (str): The path of the file in the Google Storage bucket given by the
                source prefix.

        Returns:
            bool: True if the file (including the bucket) exists. Note that it is possible
            that a file could exist and still not be downloadable due to permissions.
            False will also be returned if the bucket itself does not exist or is not
            accessible.
        """

        blob = self._bucket.blob(sub_path)
        try:
            return blob.exists()
        except Exception:
            return False

    def retrieve(self, sub_path, local_path):
        """Retrieve a file from a Google Storage bucket.

        Parameters:
            sub_path (str): The path of the file in the Google Storage bucket given by the
                source prefix.
            local_path (str or Path): The path to the destination where the downloaded
                file will be stored.

        Returns:
            Path: The Path where the file was stored (same as `local_path`).

        Raises:
            FileNotFoundError: If the remote file does not exist or the download fails for
                another reason.

        Notes:
            All parent directories in `local_path` are created even if the file download
            fails.

            The download is an atomic operation.
        """

        local_path = Path(local_path).expanduser().resolve()

        local_path.parent.mkdir(parents=True, exist_ok=True)

        blob = self._bucket.blob(sub_path)

        temp_local_path = local_path.with_suffix(f'{local_path.suffix}.{uuid.uuid4()}')
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
                f'Failed to download file: {self._src_prefix_}{sub_path}')
        except Exception:  # pragma: no cover
            temp_local_path.unlink(missing_ok=True)
            raise

        return local_path

    def upload(self, sub_path, local_path):
        """Upload a local file to a Google Storage bucket.

        Parameters:
            sub_path (str): The path of the destination file in the Google Storage bucket
                given by the source prefix.
            local_path (str or Path): The full path of the local file to upload.

        Returns:
            Path: The Path of the filename, which is the same as the `local_path`
            parameter.

        Raises:
            FileNotFoundError: If the local file does not exist.
        """

        local_path = Path(local_path).expanduser().resolve()

        if not local_path.exists():
            raise FileNotFoundError(f'File does not exist: {local_path}')

        blob = self._bucket.blob(sub_path)
        blob.upload_from_filename(str(local_path))

        return local_path


class FileCacheSourceS3(FileCacheSource):
    """Class that provides access to files stored in AWS S3."""

    def __init__(self, src_prefix, anonymous=False, **kwargs):
        """Initialization for the FileCacheS3 class.

        Parameters:
            src_prefix (str): The prefix for all AWS S3 accesses, of the form
                ``s3://<bucket>``.
            anonymous (bool, optional): If True, access AWS S3 without specifying
                credentials. Otherwise, credentials must be initialized in the program's
                environment.
        """

        src_prefix = src_prefix.rstrip('/')
        if (not src_prefix.startswith('s3://') or
                src_prefix.count('/') != 2):
            raise ValueError(f'Invalid prefix: {src_prefix}')

        super().__init__(src_prefix)

        self._prefix_type = 's3'
        self._client = (boto3.client('s3',
                                     config=botocore.client.Config(
                                         signature_version=botocore.UNSIGNED))
                        if anonymous else boto3.client('s3'))
        self._bucket_name = src_prefix.lstrip('s3://')
        self._cache_subdir = src_prefix.replace('s3://', 's3_')

    def exists(self, sub_path):
        """Check if a file exists without downloading it.

        Parameters:
            sub_path (str): The path of the file in the AWS S3 bucket given by the
                source prefix.

        Returns:
            bool: True if the file (including the bucket) exists. Note that it is possible
            that a file could exist and still not be downloadable due to permissions.
            False will also be returned if the bucket itself does not exist or is not
            accessible.
        """

        ret = True
        try:
            self._client.head_object(Bucket=self._bucket_name, Key=sub_path)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                ret = False
            else:  # pragma: no cover
                raise

        return ret

    def retrieve(self, sub_path, local_path):
        """Retrieve a file from an AWS S3 bucket.

        Parameters:
            sub_path (str): The path of the file in the AWS S3 bucket given by the
                source prefix.
            local_path (str or Path): The path to the destination where the downloaded
                file will be stored.

        Returns:
            Path: The Path where the file was stored (same as `local_path`).

        Raises:
            FileNotFoundError: If the remote file does not exist or the download fails for
                another reason.

        Notes:
            All parent directories in `local_path` are created even if the file download
            fails.

            The download is an atomic operation.
        """

        local_path = Path(local_path).expanduser().resolve()

        local_path.parent.mkdir(parents=True, exist_ok=True)

        temp_local_path = local_path.with_suffix(f'{local_path.suffix}.{uuid.uuid4()}')
        try:
            self._client.download_file(self._bucket_name, sub_path,
                                       str(temp_local_path))
            temp_local_path.rename(local_path)
        except botocore.exceptions.ClientError:
            temp_local_path.unlink(missing_ok=True)
            raise FileNotFoundError(
                f'Failed to download file: {self._src_prefix_}{sub_path}')
        except Exception:  # pragma: no cover
            temp_local_path.unlink(missing_ok=True)
            raise

        return local_path

    def upload(self, sub_path, local_path):
        """Upload a local file to an AWS S3 bucket.

        Parameters:
            sub_path (str): The path of the destination file in the AWS S3 bucket
                given by the source prefix.
            local_path (str or Path): The full path of the local file to upload.

        Returns:
            Path: The Path of the filename, which is the same as the `local_path`
            parameter.

        Raises:
            FileNotFoundError: If the local file does not exist.
        """

        local_path = Path(local_path).expanduser().resolve()

        self._client.upload_file(str(local_path), self._bucket_name, sub_path)

        return local_path
