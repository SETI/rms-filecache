##########################################################################################
# filecache/file_cache_source.py
##########################################################################################

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor
import datetime
from email.utils import parsedate_to_datetime
import os
from pathlib import Path
import platform
import re
import requests
import shutil
import stat
import tempfile
from typing import Any, Iterator, cast
import uuid

import boto3
import botocore

from google.cloud import storage as gs_storage  # type: ignore
import google.api_core.exceptions


class FileCacheSource(ABC):
    """Superclass for all remote file source classes. Do not use directly.

    The :class:`FileCacheSource` subclasses (:class:`FileCacheSourceFile`,
    :class:`FileCacheSourceHTTP`, :class:`FileCacheSourceGS`, and
    :class:`FileCacheSourceS3`) provide direct access to local and remote sources,
    bypassing the caching mechanism of :class:`FileCache`.
    """

    def __init__(self,
                 scheme: str,
                 remote: str,
                 *,
                 anonymous: bool = False):
        """Initialization for the FileCacheSource superclass.

        Note:
            Do not instantiate :class:`FileCacheSource` directly. Instead use one of the
            subclasses (:class:`FileCacheSourceFile`, :class:`FileCacheSourceHTTP`,
            :class:`FileCacheSourceGS`, and :class:`FileCacheSourceS3`).

        Parameters:
            scheme: The scheme of the source, such as ``"gs"`` or ``"file"``.
            remote: The bucket or remote server name. Must be an empty string for
                ``file``.
            anonymous: If True, access cloud resources without specifying credentials. If
                False, credentials must be initialized in the program's environment.
        """

        if scheme not in self.schemes():
            raise ValueError(f'Unsupported scheme: {scheme}')

        if '/' in remote:
            raise ValueError(f'Illegal remote {remote}')

        self._scheme = scheme
        self._remote = remote
        self._anonymous = anonymous
        self._src_prefix = f'{scheme}://{remote}'
        self._src_prefix_ = self._src_prefix + '/'

        # The _cache_subdir attribute is only used by the FileCache class
        self._cache_subdir = ''

    @classmethod
    @abstractmethod
    def schemes(self) -> tuple[str, ...]:
        """The URL schemes supported by this class."""
        ...  # pragma: no cover

    @classmethod
    def primary_scheme(self) -> str:
        """The primary URL scheme supported by this class."""

        return self.schemes()[0]

    @classmethod
    @abstractmethod
    def uses_anonymous(self) -> bool:
        """Whether this class has the concept of anonymous accesses."""
        ...  # pragma: no cover

    @abstractmethod
    def exists(self,
               sub_path: str) -> bool:
        ...  # pragma: no cover

    def exists_multi(self,
                     sub_paths: Sequence[str],
                     *,
                     nthreads: int = 8) -> list[bool]:
        """Check if multiple files exist using threads without downloading them.

        Parameters:
            sub_paths: The path of the files relative to the source prefix to check for
                existence.
            nthreads: The maximum number of threads to use.

        Returns:
            For each entry, True if the file exists. Note that it is possible that a file
            could exist and still not be accessible due to permissions. False if the file
            does not exist. This includes bad bucket or webserver names, lack of
            permission to examine a bucket's contents, etc.
        """

        if not isinstance(nthreads, int) or nthreads <= 0:
            raise ValueError(f'nthreads must be a positive integer, got {nthreads}')

        results = {}
        for sub_path, result in self._exists_object_parallel(sub_paths, nthreads):
            results[sub_path] = result

        ret = []
        for sub_path in sub_paths:
            ret.append(results[sub_path])

        return ret

    def _exists_object(self,
                       sub_path: str) -> bool:
        return self.exists(sub_path)

    def _exists_object_parallel(self,
                                sub_paths: Sequence[str],
                                nthreads: int) -> Iterator[tuple[str, bool]]:
        with ThreadPoolExecutor(max_workers=nthreads) as executor:
            future_to_paths = {executor.submit(self._exists_object, x): x
                               for x in sub_paths}
            for future in futures.as_completed(future_to_paths):
                sub_path = future_to_paths[future]
                yield sub_path, future.result()

    @abstractmethod
    def modification_time(self,
                          sub_path: str) -> float | None:
        ...  # pragma: no cover

    def modification_time_multi(self,
                                sub_paths: Sequence[str],
                                *,
                                nthreads: int = 8) -> list[float | None | Exception]:
        """Get the modification time of multiple files as a Unix timestamp.

        Parameters:
            sub_paths: The path of the files relative to the source prefix to get the
                modification time of.
            nthreads: The maximum number of threads to use.

        Returns:
            For each entry, the modification time as a Unix timestamp if the file exists
            and the time can be retrieved, None otherwise. Any modification time may be an
            Exception if that file does not exist or the modification time cannot be
            retrieved.
        """

        if not isinstance(nthreads, int) or nthreads <= 0:
            raise ValueError(f'nthreads must be a positive integer, got {nthreads}')

        results = {}
        for sub_path, result in self._modification_time_object_parallel(sub_paths,
                                                                        nthreads):
            results[sub_path] = result

        ret = []
        for sub_path in sub_paths:
            ret.append(results[sub_path])

        return ret

    def _modification_time_object(self,
                                  sub_path: str) -> float | None | Exception:
        return self.modification_time(sub_path)

    def _modification_time_object_parallel(self,
                                           sub_paths: Sequence[str],
                                           nthreads: int
                                           ) -> Iterator[tuple[str,
                                                               float | None | Exception]]:
        with ThreadPoolExecutor(max_workers=nthreads) as executor:
            future_to_paths = {executor.submit(self._modification_time_object, x): x
                               for x in sub_paths}
            for future in futures.as_completed(future_to_paths):
                sub_path = future_to_paths[future]
                try:
                    yield sub_path, future.result()
                except Exception as e:
                    yield sub_path, e

    @abstractmethod
    def is_dir(self,
               sub_path: str) -> bool:
        ...  # pragma: no cover

    def is_dir_multi(self,
                     sub_paths: Sequence[str],
                     *,
                     nthreads: int = 8) -> list[bool | Exception]:
        """Check if multiple paths represent directories using threads.

        Parameters:
            sub_paths: The paths relative to the source prefix to check for directory
                status.
            nthreads: The maximum number of threads to use.

        Returns:
            For each entry, True if the path represents a directory, False otherwise,
            or an Exception if the check failed.
        """

        if not isinstance(nthreads, int) or nthreads <= 0:
            raise ValueError(f'nthreads must be a positive integer, got {nthreads}')

        results = {}
        for sub_path, result in self._is_dir_object_parallel(sub_paths, nthreads):
            results[sub_path] = result

        ret = []
        for sub_path in sub_paths:
            ret.append(results[sub_path])

        return ret

    def _is_dir_object(self,
                       sub_path: str) -> bool:
        return self.is_dir(sub_path)

    def _is_dir_object_parallel(self,
                                sub_paths: Sequence[str],
                                nthreads: int) -> Iterator[tuple[str, bool | Exception]]:
        with ThreadPoolExecutor(max_workers=nthreads) as executor:
            future_to_paths = {executor.submit(self._is_dir_object, x): x
                               for x in sub_paths}
            for future in futures.as_completed(future_to_paths):
                sub_path = future_to_paths[future]
                try:
                    yield sub_path, future.result()
                except Exception as e:
                    yield sub_path, e

    @abstractmethod
    def retrieve(self,
                 sub_path: str,
                 local_path: str | Path,
                 *,
                 preserve_mtime: bool = False) -> Path:
        ...  # pragma: no cover

    def retrieve_multi(self,
                       sub_paths: Sequence[str],
                       local_paths: Sequence[str | Path],
                       *,
                       preserve_mtime: bool = False,
                       nthreads: int = 8) -> list[Path | BaseException]:
        """Retrieve multiple files from the storage location using threads.

        Parameters:
            sub_paths: The path of the files to retrieve relative to the source prefix.
            local_paths: The paths to the destinations where the downloaded files will be
                stored.
            preserve_mtime: If True, the modification time of the remote files will be
                copied to the local files.
            nthreads: The maximum number of threads to use.

        Returns:
            A list containing the local paths of the retrieved files. If a file failed to
            download, the entry is the Exception that caused the failure. This list is in
            the same order and has the same length as `local_paths`.

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
                                                               preserve_mtime, nthreads):
            results[sub_path] = result

        ret = []
        for sub_path in sub_paths:
            ret.append(results[sub_path])

        return ret

    def _download_object(self,
                         sub_path: str,
                         local_path: str | Path,
                         preserve_mtime: bool) -> Path:
        self.retrieve(sub_path, local_path, preserve_mtime=preserve_mtime)
        return Path(local_path)

    def _download_object_parallel(self,
                                  sub_paths: Sequence[str],
                                  local_paths: Sequence[str | Path],
                                  preserve_mtime: bool,
                                  nthreads: int) -> Iterator[
                                      tuple[str, Path | BaseException]]:
        with ThreadPoolExecutor(max_workers=nthreads) as executor:
            future_to_paths = {executor.submit(self._download_object,
                                               x[0], x[1], preserve_mtime): x[0]
                               for x in zip(sub_paths, local_paths)}
            for future in futures.as_completed(future_to_paths):
                sub_path = future_to_paths[future]
                exception = future.exception()
                if not exception:
                    yield sub_path, future.result()
                else:
                    yield sub_path, exception

    @abstractmethod
    def upload(self,
               sub_path: str,
               local_path: str | Path,
               *,
               preserve_mtime: bool = False) -> Path:
        ...  # pragma: no cover

    def upload_multi(self,
                     sub_paths: Sequence[str],
                     local_paths: Sequence[str | Path],
                     *,
                     preserve_mtime: bool = False,
                     nthreads: int = 8) -> list[Path | BaseException]:
        """Upload multiple files to a storage location.

        Parameters:
            sub_paths: The path of the destination files relative to the source prefix.
            local_paths: The paths of the files to upload.
            preserve_mtime: If True, the modification time of the local files will be
                copied to the remote files.
            nthreads: The maximum number of threads to use.

        Returns:
            A list containing the local paths of the uploaded files. If a file failed to
            upload, the entry is the Exception that caused the failure. This list is in
            the same order and has the same length as `local_paths`.
        """

        if not isinstance(nthreads, int) or nthreads <= 0:
            raise ValueError(f'nthreads must be a positive integer, got {nthreads}')

        results = {}
        for sub_path, result in self._upload_object_parallel(sub_paths, local_paths,
                                                             preserve_mtime, nthreads):
            results[sub_path] = result

        ret = []
        for sub_path in sub_paths:
            ret.append(results[sub_path])

        return ret

    def _upload_object(self,
                       sub_path: str,
                       local_path: str | Path,
                       preserve_mtime: bool) -> Path:
        self.upload(sub_path, local_path, preserve_mtime=preserve_mtime)
        return Path(local_path)

    def _upload_object_parallel(self,
                                sub_paths: Sequence[str],
                                local_paths: Sequence[str | Path],
                                preserve_mtime: bool,
                                nthreads: int) -> Iterator[tuple[str,
                                                                 Path | BaseException]]:
        with ThreadPoolExecutor(max_workers=nthreads) as executor:
            future_to_paths = {executor.submit(self._upload_object,
                                               x[0], x[1], preserve_mtime): x[0]
                               for x in zip(sub_paths, local_paths)}
            for future in futures.as_completed(future_to_paths):
                sub_path = future_to_paths[future]
                exception = future.exception()
                if not exception:
                    yield sub_path, future.result()
                else:
                    yield sub_path, exception

    @abstractmethod
    def iterdir_metadata(self,
                         sub_path: str) -> Iterator[tuple[str, dict[str, Any] | None]]:
        """Iterate over the contents of a directory.

        Parameters:
            sub_path: The path of the directory relative to the source prefix.

        Yields:
            All files and sub-directories in the given directory (except ``.`` and
            ``..``), in no particular order. Each file or directory is represented by a
            tuple of the form (path, metadata), where path is the path of the file or
            directory relative to the source prefix, and metadata is a dictionary with the
            following keys:

                - ``is_dir``: True if the returned name is a directory, False if it is a
                  file.
                - ``mtime``: The last modification time of the file as a float.
                - ``size``: The approximate size of the file in bytes.

            If the metadata can not be retrieved, None is returned for the metadata.
        """
        ...  # pragma: no cover

    @abstractmethod
    def unlink(self,
               sub_path: str,
               *,
               missing_ok: bool = False) -> str:
        """Remove the given object.

        Parameters:
            sub_path: The path of the file relative to the source prefix to delete.
            missing_ok: True if it is OK to unlink a file that doesn't exist; False to
                raise a FileNotFoundError in this case.

        Returns:
            The sub_path.

        Raises:
            FileNotFoundError: If the file doesn't exist and `missing_ok` is False.
        """
        ...  # pragma: no cover

    def unlink_multi(self,
                     sub_paths: Sequence[str],
                     *,
                     missing_ok: bool = False,
                     nthreads: int = 8) -> list[str | BaseException]:
        """Unlink multiple files in a storage location.

        Parameters:
            sub_paths: The path of the destination files relative to the source prefix.
            missing_ok: True if it is OK to unlink a file that doesn't exist; False to
                raise a FileNotFoundError in this case.
            nthreads: The maximum number of threads to use.

        Returns:
            A list containing the paths of the unlink files. If a file failed to unlink,
            the entry is the Exception that caused the failure. This list is in the same
            order and has the same length as `sub_paths`.
        """

        if not isinstance(nthreads, int) or nthreads <= 0:
            raise ValueError(f'nthreads must be a positive integer, got {nthreads}')

        results = {}
        for sub_path, result in self._unlink_object_parallel(sub_paths, missing_ok,
                                                             nthreads):
            results[sub_path] = result

        ret = []
        for sub_path in sub_paths:
            ret.append(results[sub_path])

        return ret

    def _unlink_object(self,
                       sub_path: str,
                       missing_ok: bool) -> str:
        self.unlink(sub_path, missing_ok=missing_ok)
        return sub_path

    def _unlink_object_parallel(self,
                                sub_paths: Sequence[str],
                                missing_ok: bool,
                                nthreads: int) -> Iterator[tuple[str,
                                                                 str | BaseException]]:
        with ThreadPoolExecutor(max_workers=nthreads) as executor:
            future_to_paths = {executor.submit(self._unlink_object, x, missing_ok): x
                               for x in sub_paths}
            for future in futures.as_completed(future_to_paths):
                sub_path = future_to_paths[future]
                exception = future.exception()
                if not exception:
                    yield sub_path, future.result()
                else:
                    yield sub_path, exception


class FileCacheSourceFile(FileCacheSource):
    """Class that provides direct access to local files.

    This class is unlikely to be directly useful to an external program, as it provides
    essentially no functionality on top of the standard Python filesystem functions.
    """

    def __init__(self,
                 scheme: str,
                 remote: str,
                 *,
                 anonymous: bool = False):
        """Initialization for the FileCacheLocal class.

        Parameters:
            scheme: The scheme of the source. Must be ``"file"`` or ``""``.
            remote: The remote server name. Must be ``""`` since UNC shares are not
                supported.
            anonymous: If True, access cloud resources without specifying credentials. If
                False, credentials must be initialized in the program's environment. Not
                used for this class.
        """

        if remote != '':
            raise ValueError(f'UNC shares are not supported: {remote}')

        super().__init__(scheme, remote, anonymous=anonymous)

        self._cache_subdir = ''

    @classmethod
    def schemes(self) -> tuple[str, ...]:
        """The URL schemes supported by this class."""

        return ('file',)

    @classmethod
    def uses_anonymous(self) -> bool:
        """Whether this class has the concept of anonymous accesses."""

        return False

    def exists(self,
               sub_path: str | Path) -> bool:
        """Check if a file exists without downloading it.

        Parameters:
            sub_path: The absolute path of the local file.

        Returns:
            True if the file exists. Note that it is possible that a file could exist and
            still not be accessible due to permissions.
        """

        return Path(sub_path).is_file()

    def modification_time(self,
                          sub_path: str) -> float | None:
        """Get the modification time of a file as a Unix timestamp."""

        return Path(sub_path).stat().st_mtime

    def is_dir(self,
               sub_path: str) -> bool:
        """Check if a file is a directory."""

        path = Path(sub_path)
        if not path.exists():
            raise FileNotFoundError(f'File does not exist: {sub_path}')

        return path.is_dir()

    def retrieve(self,
                 sub_path: str | Path,
                 local_path: str | Path,
                 *,
                 preserve_mtime: bool = False) -> Path:
        """Retrieve a file from the storage location.

        Parameters:
            sub_path: The absolute path of the local file to retrieve.
            local_path: The path to the desination where the file will be stored. Must be
                the same as `sub_path`.
            preserve_mtime: If True, the modification time of the remote file will be
                copied to the local file. Not used for local files.

        Returns:
            The Path of the filename, which is the same as the `sub_path` parameter.

        Raises:
            ValueError: If `sub_path` and `local_path` are not identical.
            FileNotFoundError: If the file does not exist.

        Notes:
            This method essentially does nothing except check for the existence of the
            file.
        """

        local_path_p = Path(local_path)

        if not local_path_p.is_file():
            raise FileNotFoundError(f'File does not exist: {local_path_p}')

        # We don't actually do anything for local paths since the file is already in the
        # correct location.
        return local_path_p

    def upload(self,
               sub_path: str | Path,
               local_path: str | Path,
               *,
               preserve_mtime: bool = False) -> Path:
        """Upload a file from the local filesystem to the storage location.

        Parameters:
            sub_path: The absolute path of the destination.
            local_path: The absolute path of the local file to upload. Must be the same as
                `sub_path`.
            preserve_mtime: If True, the modification time of the local file will be
                copied to the remote file. Not used for local files.

        Returns:
            The Path of the filename, which is the same as the `local_path` parameter.

        Raises:
            ValueError: If `sub_path` and `local_path` are not identical.
            FileNotFoundError: If the file does not exist.
        """

        local_path_p = Path(local_path)

        if not local_path_p.is_file():
            raise FileNotFoundError(f'File does not exist: {local_path_p}')

        # We don't actually do anything for local paths since the file is already in the
        # correct location.
        return local_path_p

    def iterdir_metadata(self,
                         sub_path: str) -> Iterator[tuple[str, dict[str, Any] | None]]:
        """Iterate over the contents of a directory.

        Parameters:
            sub_path: The absolute path of the directory.

        Yields:
            All files and sub-directories in the given directory (except ``.`` and
            ``..``), in no particular order. Each file or directory is represented by a
            tuple of the form (path, metadata), where path is the path of the file or
            directory relative to the source prefix, and metadata is a dictionary with the
            following keys:

                - ``is_dir``: True if the returned name is a directory, False if it is a
                  file.
                - ``mtime``: The last modification time of the file as a float.
                - ``size``: The approximate size of the file in bytes.

            If the metadata can not be retrieved, None is returned for the metadata.
        """

        sub_path_p = Path(sub_path)
        for obj_name in sub_path_p.iterdir():
            try:
                obj_stat = obj_name.stat()
            except (OSError, IOError):  # pragma: no cover
                # Fallback if stat fails
                metadata: dict[str, Any] | None = None
            else:
                metadata = {
                    'is_dir': stat.S_ISDIR(obj_stat.st_mode),
                    'mtime': obj_stat.st_mtime,
                    'size': obj_stat.st_size
                }

            yield str(obj_name).replace('\\', '/'), metadata

    def unlink(self,
               sub_path: str,
               *,
               missing_ok: bool = False) -> str:
        """Remove the given object.

        Parameters:
            sub_path: The path of the file.
            missing_ok: True if it is OK to unlink a file that doesn't exist; False to
                raise a FileNotFoundError in this case.

        Returns:
            The sub_path.

        Raises:
            FileNotFoundError: If the file doesn't exist and `missing_ok` is False.
        """

        Path(sub_path).unlink(missing_ok=missing_ok)

        return sub_path


_HTML_TAG = re.compile(r'<.*?>')
_END_OF_TABLE = re.compile('.*(</pre>|<hr>|</table|<th colspan).*')
_SIZE_FACTORS = {'K': 1024, 'M': 1024**2, 'G': 1024**3, 'T': 1024**4}


class FileCacheSourceHTTP(FileCacheSource):
    """Class that provides access to files stored on a webserver."""

    def __init__(self,
                 scheme: str,
                 remote: str,
                 *,
                 anonymous: bool = False):
        """Initialization for the FileCacheHTTP class.

        Parameters:
            scheme: The scheme of the source. Must be ``"http"`` or ``"https"``.
            remote: The remote server name.
            anonymous: If True, access cloud resources without specifying credentials. If
                False, credentials must be initialized in the program's environment. Not
                used for this class.
        """

        if remote == '':
            raise ValueError('remote parameter must have a value')

        super().__init__(scheme, remote, anonymous=anonymous)

        self._prefix_type = 'web'
        self._cache_subdir = (self._src_prefix
                              .replace('http://', 'http_')
                              .replace('https://', 'http_'))

    @classmethod
    def schemes(self) -> tuple[str, ...]:
        """The URL schemes supported by this class."""

        return ('http', 'https')

    @classmethod
    def uses_anonymous(self) -> bool:
        """Whether this class has the concept of anonymous accesses."""

        return False

    def exists(self,
               sub_path: str) -> bool:
        """Check if a file exists without downloading it.

        Parameters:
            sub_path: The path of the file on the webserver relative to the source prefix.

        Returns:
            True if the file (including the webserver) exists. Note that it is possible
            that a file could exist and still not be downloadable due to permissions.
        """

        ret = True
        try:
            response = requests.head(self._src_prefix_ + sub_path)
            response.raise_for_status()
        except requests.exceptions.RequestException:
            ret = False

        return ret

    def modification_time(self,
                          sub_path: str) -> float | None:
        """Get the modification time of a file as a Unix timestamp.

        Parameters:
            sub_path: The path of the file on the webserver relative to the source
                prefix.

        Returns:
            The modification time as a float (Unix timestamp) if the file exists and
            the time can be retrieved from HTTP headers. If the file exists but no
            modification time is available, None is returned. If the file does not exist
            or other errors occur, an Exception is raised.

        Notes:
            This method uses HTTP HEAD request to get file metadata without downloading
            the content. It checks for Last-Modified header and converts it to a Unix
            timestamp.
        """

        try:
            # Use HEAD request to get headers without downloading content
            response = requests.head(self._src_prefix_ + sub_path)
            response.raise_for_status()

            # Check for Last-Modified header
            last_modified = response.headers.get('Last-Modified')
            if last_modified:
                # Parse the HTTP date format and convert to Unix timestamp
                # HTTP dates are typically in format: "Wed, 21 Oct 2015 07:28:00 GMT"
                dt = parsedate_to_datetime(last_modified)
                return dt.timestamp()

            return None
        except requests.exceptions.RequestException:
            raise FileNotFoundError('Failed to get HEAD of: '
                                    f'{self._src_prefix_}{sub_path}')

    def is_dir(self,
               sub_path: str) -> bool:
        """Check if a file is a directory.

        Parameters:
            sub_path: The path of the directory on the webserver relative to the source
                prefix.

        Returns:
            True if the path represents a directory, False otherwise.

        Notes:
            This method assumes the provided URL is either a file or a directory that
            will show up as a "fancy index" page. If you provide a URL that is a directory
            but does not support fancy indexing, it will incorrectly return ``False``.
            It is also possible to fool this method by giving it a file URL that appears
            to be a fancy index.
        """

        # Returns FileNotFount if URL can't be read
        # Returns RuntimeError if URL doesn't support fancy indexing, which could mean
        #   it's a file or it's a directory and not indexable
        try:
            for _ in self.iterdir_metadata(sub_path):
                # If there's even one file present, it's a directory
                return True
        except RuntimeError:
            pass

        return False

    def retrieve(self,
                 sub_path: str,
                 local_path: str | Path,
                 *,
                 preserve_mtime: bool = False) -> Path:
        """Retrieve a file from a webserver.

        Parameters:
            sub_path: The path of the file to retrieve relative to the source prefix.
            local_path: The path to the destination where the downloaded file will be
                stored.
            preserve_mtime: If True, the modification time of the remote file will be
                copied to the local file.

        Returns:
            The Path where the file was stored (same as `local_path`).

        Raises:
            FileNotFoundError: If the remote file does not exist or the download fails for
                another reason.

        Notes:
            All parent directories in `local_path` are created even if the file download
            fails.

            The download is an atomic operation.
        """

        if sub_path == '':
            raise ValueError('sub_path can not be empty')

        local_path = Path(local_path)

        url = self._src_prefix_ + sub_path

        local_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise FileNotFoundError(f'Failed to download file: {url}') from e

        temp_local_path = local_path.with_suffix(f'.{local_path.suffix}_{uuid.uuid4()}')
        try:
            with open(temp_local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=1024*1024):
                    f.write(chunk)
            # If windows delete the old file first because we can't rename over an
            # existing file
            if platform.system() == 'Windows':
                local_path.unlink(missing_ok=True)
            temp_local_path.rename(local_path)
        except Exception:
            temp_local_path.unlink(missing_ok=True)
            raise

        if preserve_mtime:
            last_modified = response.headers.get('Last-Modified')
            if last_modified:
                # Parse the HTTP date format and convert to Unix timestamp
                # HTTP dates are typically in format: "Wed, 21 Oct 2015 07:28:00 GMT"
                dt = parsedate_to_datetime(last_modified)
                last_modified_sec = dt.timestamp()
                os.utime(local_path, (last_modified_sec, last_modified_sec))

        return local_path

    def upload(self,
               sub_path: str,
               local_path: str | Path,
               *,
               preserve_mtime: bool = False) -> Path:
        """Upload a local file to a webserver. Not implemented."""

        raise NotImplementedError

    def iterdir_metadata(self,
                         sub_path: str) -> Iterator[tuple[str, dict[str, Any]]]:
        """Iterate over the contents of a directory.

        Parameters:
            sub_path: The path of the directory on the webserver relative to the source
                prefix.

        Yields:
            All files and sub-directories in the given directory (except ``.`` and
            ``..``), in no particular order. Each file or directory is represented by a
            tuple of the form (path, metadata), where path is the path of the file or
            directory relative to the source prefix, and metadata is a dictionary with the
            following keys:

                - ``is_dir``: True if the returned name is a directory, False if it is a
                  file.
                - ``mtime``: The last modification time of the file as a float.
                - ``size``: The approximate size of the file in bytes.

        Raises:
            FileNotFoundError: If the URL does not point to a valid file or directory
                page.
            ConnectionError: If the URL could not be accessed.

        Notes:
            This method relies on the webserver returning a "fancy index" page in some
            recognizable format. If server-side indexing is disabled, this method will
            not work.

            We have no way of knowing what the timezone of the webserver is, and the
            fancy index page does not include the timezone. As a result we assume all
            times are in UTC.
        """

        def _int_size(size: str) -> int | None:
            """Internal method to convert a size string to a size in bytes."""
            if size == '-':
                return None
            if size[-1] in _SIZE_FACTORS:
                return int(float(size[:-1]) * _SIZE_FACTORS[size[-1]] + 0.5)
            return int(size)

        url = self._src_prefix_ + sub_path

        try:
            request = requests.get(url, stream=True, timeout=30)
        except requests.exceptions.ConnectionError as e:
            raise ConnectionError(f'Failed to connect to {url}') from e

        if request.status_code != 200:
            if request.status_code == 404:
                raise FileNotFoundError(f'File not found: {url}')
            message = f'response {request.status_code} received from {url}'
            raise ConnectionError(message)

        # Since we're reading a file, this could be a REALLY BIG file and we won't
        # know it's not a fancy index until we've downloaded the whole thing. So
        # limit the size of the download for sanity.

        max_size = 20*1024*1024  # 20 MB - 5X the size of the MRO C kernels
        downloaded_size = 0
        content_chunks = []

        for chunk in request.iter_content(chunk_size=8192):  # Read in 8KB chunks
            if not chunk:
                break  # No more data

            downloaded_size += len(chunk)
            if downloaded_size > max_size:
                request.close()  # Close the connection to stop downloading
                break
            content_chunks.append(chunk)

        full_content = b"".join(content_chunks)

        text = full_content.decode('latin1')

        # The first line of the fancy index always contains "Parent Directory".
        parts = list(text.partition('Parent Directory'))
        if not parts[-1]:
            raise RuntimeError(f'URL does not support fancy indexing: {url}')

        # Rows are always split by "\n".
        # Sometimes it's a table, sometimes just pre-formatted text
        recs = parts[-1].split('\n')

        # The record after the last table row always contains one of these:
        # "</pre>", "<hr>", "<table", or "<th colspan"
        last = [k for k, rec in enumerate(recs) if _END_OF_TABLE.fullmatch(rec)]

        # Select the table rows
        recs = recs[1:last[0]]

        # Interpret each row
        for rec in recs:
            rec = rec.replace('&nbsp;', '').strip()

            # Remove anything inside quotes
            parts = rec.split('"')
            rec = ''.join(parts[::2])

            # Insert a space before "<td"
            rec = rec.replace('<td', ' <td')

            # Remove anything inside HTML tags
            parts = _HTML_TAG.split(rec)
            rec = ''.join(parts)

            # Interpret the fields
            parts = rec.split()
            basename = parts[0]
            date_str = parts[1] + 'T' + parts[2] + '+00:00'

            mtime = datetime.datetime.fromisoformat(date_str).timestamp()
            size = _int_size(parts[3])

            is_dir = basename.endswith('/')
            basename = basename.rstrip('/')
            metadata = {
                'is_dir': is_dir,
                'mtime': mtime,
                'size': size
            }
            yield (f'{url}/{basename}', metadata)

    def unlink(self,
               sub_path: str,
               *,
               missing_ok: bool = False) -> str:
        """Remove the given object.

        Parameters:
            sub_path: The path of the file on the webserver relative to the source prefix
                to delete.
            missing_ok: True if it is OK to unlink a file that doesn't exist; False to
                raise a FileNotFoundError in this case.

        Returns:
            The sub_path.

        Raises:
            FileNotFoundError: If the file doesn't exist and `missing_ok` is False.
        """

        raise NotImplementedError


class FileCacheSourceGS(FileCacheSource):
    """Class that provides access to files stored in Google Storage."""

    def __init__(self,
                 scheme: str,
                 remote: str,
                 *,
                 anonymous: bool = False):
        """Initialization for the FileCacheGS class.

        Parameters:
            scheme: The scheme of the source. Must be ``"gs"``.
            remote: The bucket name.
            anonymous: If True, access cloud resources without specifying credentials. If
                False, credentials must be initialized in the program's environment. Not
                used for this class.
        """

        if remote == '':
            raise ValueError('remote parameter must have a value')

        super().__init__(scheme, remote, anonymous=anonymous)

        self._client = (gs_storage.Client.create_anonymous_client()
                        if anonymous else gs_storage.Client())
        self._bucket_name = remote
        self._bucket = self._client.bucket(self._bucket_name)
        self._cache_subdir = self._src_prefix.replace('gs://', 'gs_')

    @classmethod
    def schemes(self) -> tuple[str, ...]:
        """The URL schemes supported by this class."""

        return ('gs',)

    @classmethod
    def uses_anonymous(self) -> bool:
        """Whether this class has the concept of anonymous accesses."""

        return True

    def exists(self,
               sub_path: str) -> bool:
        """Check if a file exists without downloading it.

        Parameters:
            sub_path: The path of the file in the Google Storage bucket given by the
                source prefix.

        Returns:
            True if the file (including the bucket) exists. Note that it is possible that
            a file could exist and still not be downloadable due to permissions. False
            will also be returned if the bucket itself does not exist or is not
            accessible.
        """

        blob = self._bucket.blob(sub_path)
        try:
            return bool(blob.exists())
        except Exception:
            return False

    def modification_time(self,
                          sub_path: str) -> float | None:
        """Get the modification time of a file as a Unix timestamp.

        Parameters:
            sub_path: The path of the file in the Google Storage bucket given by the
                source prefix.

        Returns:
            The modification time as a float (Unix timestamp) if the file exists.
            If the file does not exist or other errors occur, an Exception is raised.
        """

        blob = self._bucket.blob(sub_path)
        try:
            blob.reload()
        except (google.api_core.exceptions.BadRequest,  # bad bucket name
                google.api_core.exceptions.NotFound,  # bad filename
                google.cloud.exceptions.NotFound):  # bad filename
            raise FileNotFoundError(f'File not found: {self._src_prefix_}{sub_path}')
        if blob.metadata is not None:
            mtime = blob.metadata.get('goog-reserved-file-mtime')
            if mtime is not None:
                return float(mtime)
        return cast(float, blob.updated.timestamp())

    def is_dir(self,
               sub_path: str) -> bool:
        """Check if a file is a directory.

        Parameters:
            sub_path: The path of the directory in the Google Storage bucket given by the
                source prefix.

        Returns:
            True if the path represents a directory (i.e., there are objects with this
            prefix), False otherwise.

        Notes:
            In Google Cloud Storage, directories are conceptual - they exist as prefixes
            in object names. This method checks if there are any objects with the given
            prefix to determine if it represents a directory.
        """

        if self.exists(sub_path):
            # If it exists, it's a file
            return False

        # If it doesn't exist, check to see if it's a prefix to other files
        try:
            if sub_path:
                # For non-root paths, check if there are objects with the given prefix
                if not sub_path.endswith('/'):
                    sub_path = sub_path + '/'
                blobs = list(self._bucket.list_blobs(prefix=sub_path, max_results=1))
                ret = len(blobs) > 0
            else:
                # For bucket root, check if the bucket exists and has any objects
                blobs = list(self._bucket.list_blobs(max_results=1))
                ret = len(blobs) > 0
        except (google.api_core.exceptions.BadRequest,  # bad bucket name
                google.cloud.exceptions.NotFound):  # bad filename
            raise FileNotFoundError(f'File not found: {self._src_prefix_}{sub_path}')

        if ret:
            return ret
        raise FileNotFoundError(f'File not found: {self._src_prefix_}{sub_path}')

    def retrieve(self,
                 sub_path: str,
                 local_path: str | Path,
                 *,
                 preserve_mtime: bool = False) -> Path:
        """Retrieve a file from a Google Storage bucket.

        Parameters:
            sub_path: The path of the file in the Google Storage bucket given by the
                source prefix.
            local_path: The path to the destination where the downloaded file will be
                stored.
            preserve_mtime: If True, the modification time of the remote file will be
                copied to the local file.

        Returns:
            The Path where the file was stored (same as `local_path`).

        Raises:
            FileNotFoundError: If the remote file does not exist or the download fails for
                another reason.

        Notes:
            All parent directories in `local_path` are created even if the file download
            fails.

            The download is an atomic operation.
        """

        if sub_path == '':
            raise ValueError('sub_path can not be empty')

        local_path = Path(local_path)

        local_path.parent.mkdir(parents=True, exist_ok=True)

        blob = self._bucket.blob(sub_path)

        temp_local_path = local_path.with_suffix(f'.{local_path.suffix}_{uuid.uuid4()}')
        try:
            blob.download_to_filename(str(temp_local_path))
            # If windows delete the old file first because we can't rename over an
            # existing file
            if platform.system() == 'Windows':
                local_path.unlink(missing_ok=True)
            temp_local_path.rename(local_path)
        except (google.api_core.exceptions.BadRequest,  # bad bucket name
                google.cloud.exceptions.NotFound):  # bad filename
            # The google API library will still create the file before noticing
            # that it can't be downloaded, so we have to remove it here
            temp_local_path.unlink(missing_ok=True)
            raise FileNotFoundError(
                f'Failed to download file: {self._src_prefix_}{sub_path}')
        except Exception:  # pragma: no cover
            temp_local_path.unlink(missing_ok=True)
            raise

        if preserve_mtime:
            mtime_sec = None
            blob.reload()
            if blob.metadata is not None:
                mtime_str = blob.metadata.get('goog-reserved-file-mtime')
                if mtime_str is not None:
                    mtime_sec = float(mtime_str)
            if mtime_sec is None and blob.updated is not None:
                mtime_sec = cast(float, blob.updated.timestamp())
            if mtime_sec is None:
                raise RuntimeError('Failed to retrieve modification time from Google '
                                   f'Storage bucket for {self._src_prefix_}{sub_path}')
            os.utime(local_path, (mtime_sec, mtime_sec))

        return local_path

    def upload(self,
               sub_path: str,
               local_path: str | Path,
               *,
               preserve_mtime: bool = False) -> Path:
        """Upload a local file to a Google Storage bucket.

        Parameters:
            sub_path: The path of the destination file in the Google Storage bucket given
                by the source prefix.
            local_path: The absolute path of the local file to upload.
            preserve_mtime: If True, the modification time of the local file will be
                copied to the remote file.

        Returns:
            The Path of the filename, which is the same as the `local_path` parameter.

        Raises:
            FileNotFoundError: If the local file does not exist.
        """

        local_path = Path(local_path)

        if not local_path.exists():
            raise FileNotFoundError(f'File does not exist: {local_path}')

        blob = self._bucket.blob(sub_path)

        if preserve_mtime:
            mtime_sec = local_path.stat().st_mtime
            mtime_str = str(mtime_sec)
            blob_metadata = blob.metadata or {}
            blob_metadata['goog-reserved-file-mtime'] = mtime_str
            # For some reason the Google API doesn't let you update this directly
            blob.metadata = blob_metadata

        blob.upload_from_filename(str(local_path))

        return local_path

    def iterdir_metadata(self,
                         sub_path: str) -> Iterator[tuple[str, dict[str, Any] | None]]:
        """Iterate over the contents of a directory.

        Parameters:
            sub_path: The path of the directory in the Google Storage bucket given
                by the source prefix.

        Yields:
            All files and sub-directories in the given directory (except ``.`` and
            ``..``), in no particular order. Each file or directory is represented by a
            tuple of the form (path, metadata), where path is the path of the file or
            directory relative to the source prefix, and metadata is a dictionary with the
            following keys:

                - ``is_dir``: True if the returned name is a directory, False if it is a
                  file.
                - ``mtime``: The last modification time of the file as a float.
                - ``size``: The approximate size of the file in bytes.

            If the metadata can not be retrieved, None is returned for the metadata.
        """

        if sub_path:
            sub_prefix = f'{sub_path}/'
        else:
            sub_prefix = None
        blobs = self._client.list_blobs(self._bucket_name,
                                        prefix=sub_prefix, delimiter='/')

        # Yield filenames with metadata
        for blob in blobs:
            if blob.name.rstrip('/') != sub_path:
                metadata = {
                    'is_dir': False,
                    'mtime': blob.updated.timestamp(),  # Last modification time
                    'size': blob.size  # File size in bytes
                }
                yield f'{self._src_prefix_}{blob.name}', metadata

        # Yield sub-directories with metadata
        for prefix in blobs.prefixes:
            prefix = prefix.rstrip('/')
            if prefix != sub_path:
                metadata = {
                    'is_dir': True,
                    'mtime': None,  # Directories don't have mod times in GS
                    'size': None  # Directories don't have sizes in GS
                }
                yield f'{self._src_prefix_}{prefix}', metadata

    def unlink(self,
               sub_path: str,
               *,
               missing_ok: bool = False) -> str:
        """Remove the given object.

        Parameters:
            sub_path: The path of the file in the Google Storage bucket given by the
                source prefix to delete.
            missing_ok: True if it is OK to unlink a file that doesn't exist; False to
                raise a FileNotFoundError in this case.

        Returns:
            The sub_path.

        Raises:
            FileNotFoundError: If the file doesn't exist and `missing_ok` is False.
        """

        blob = self._bucket.blob(sub_path)

        try:
            blob.delete()
        except Exception:
            if not missing_ok:
                raise FileNotFoundError

        return sub_path


class FileCacheSourceS3(FileCacheSource):
    """Class that provides access to files stored in AWS S3."""

    def __init__(self,
                 scheme: str,
                 remote: str,
                 *,
                 anonymous: bool = False):
        """Initialization for the FileCacheS3 class.

        Parameters:
            scheme: The scheme of the source. Must be ``"s3"``.
            remote: The bucket name.
            anonymous: If True, access cloud resources without specifying credentials. If
                False, credentials must be initialized in the program's environment. Not
                used for this class.
        """

        if remote == '':
            raise ValueError('remote parameter must have a value')

        super().__init__(scheme, remote, anonymous=anonymous)

        self._client = (boto3.client('s3',
                                     config=botocore.client.Config(
                                         signature_version=botocore.UNSIGNED))
                        if anonymous else boto3.client('s3'))
        self._bucket_name = remote
        self._cache_subdir = self._src_prefix.replace('s3://', 's3_')

    @classmethod
    def schemes(self) -> tuple[str, ...]:
        """The URL schemes supported by this class."""

        return ('s3',)

    @classmethod
    def uses_anonymous(self) -> bool:
        """Whether this class has the concept of anonymous accesses."""

        return True

    def exists(self,
               sub_path: str) -> bool:
        """Check if a file exists without downloading it.

        Parameters:
            sub_path: The path of the file in the AWS S3 bucket given by the source
                prefix.

        Returns:
            True if the file (including the bucket) exists. Note that it is possible that
            a file could exist and still not be downloadable due to permissions. False
            will also be returned if the bucket itself does not exist or is not
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

    def modification_time(self,
                          sub_path: str) -> float | None:
        """Get the modification time of a file as a Unix timestamp.

        Parameters:
            sub_path: The path of the file in the AWS S3 bucket given by the
                source prefix.

        Returns:
            The modification time as a float (Unix timestamp) if the file exists and
            the time can be retrieved. If the file does not exist or other errors occur,
            an Exception is raised.
        """

        try:
            # Use head_object to get metadata without downloading the content
            response = self._client.head_object(Bucket=self._bucket_name, Key=sub_path)
            # Check for custom mtime metadata first (if preserve_mtime was used)
            if 'Metadata' in response and response['Metadata']:
                mtime_str = response['Metadata'].get('mtime')
                if mtime_str is not None:
                    return float(mtime_str)
            # Convert the LastModified time to a Unix timestamp
            return response['LastModified'].timestamp()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                raise FileNotFoundError(f'File not found: {self._src_prefix_}{sub_path}')
            else:
                raise

    def is_dir(self,
               sub_path: str) -> bool:
        """Check if a file is a directory.

        Parameters:
            sub_path: The path of the directory in the AWS S3 bucket given by the
                source prefix.

        Returns:
            True if the path represents a directory (i.e., there are objects with this
            prefix), False otherwise.

        Notes:
            In AWS S3, directories are conceptual - they exist as prefixes in object
            names. This method checks if there are any objects with the given prefix
            to determine if it represents a directory.
        """

        if len(sub_path) > 0 and self.exists(sub_path):
            # If it exists, it's a file
            return False

        # If it doesn't exist, check to see if it's a prefix to other files
        if sub_path:
            # For non-root paths, check if there are objects with the given prefix
            if not sub_path.endswith('/'):
                sub_path = sub_path + '/'
            response = self._client.list_objects_v2(
                Bucket=self._bucket_name,
                Prefix=sub_path,
                MaxKeys=1
            )
        else:
            # For bucket root, check if the bucket exists and has any objects
            response = self._client.list_objects_v2(
                Bucket=self._bucket_name,
                MaxKeys=1
            )

        # Check if there are any contents with this prefix
        contents = response.get('Contents', [])
        ret = len(contents) > 0

        # If there are no files, then the directory doesn't exist; if there are
        # then the directory exists and it's a directory
        if ret:
            return ret
        raise FileNotFoundError(f'File not found: {self._src_prefix_}{sub_path}')

    def retrieve(self,
                 sub_path: str,
                 local_path: str | Path,
                 *,
                 preserve_mtime: bool = False) -> Path:
        """Retrieve a file from an AWS S3 bucket.

        Parameters:
            sub_path: The path of the file in the AWS S3 bucket given by the source
                prefix.
            local_path: The path to the destination where the downloaded file will be
                stored.
            preserve_mtime: If True, the modification time of the remote file will be
                copied to the local file.

        Returns:
            The Path where the file was stored (same as `local_path`).

        Raises:
            FileNotFoundError: If the remote file does not exist or the download fails for
                another reason.

        Notes:
            All parent directories in `local_path` are created even if the file download
            fails.

            The download is an atomic operation.
        """

        if sub_path == '':
            raise ValueError('sub_path can not be empty')

        local_path = Path(local_path)

        local_path.parent.mkdir(parents=True, exist_ok=True)

        temp_local_path = local_path.with_suffix(f'.{local_path.suffix}_{uuid.uuid4()}')
        try:
            self._client.download_file(self._bucket_name, sub_path,
                                       str(temp_local_path))
            # If windows delete the old file first because we can't rename over an
            # existing file
            if platform.system() == 'Windows':
                local_path.unlink(missing_ok=True)
            temp_local_path.rename(local_path)
        except botocore.exceptions.ClientError:
            temp_local_path.unlink(missing_ok=True)
            raise FileNotFoundError(
                f'Failed to download file: {self._src_prefix_}{sub_path}')
        except Exception:  # pragma: no cover
            temp_local_path.unlink(missing_ok=True)
            raise

        if preserve_mtime:
            mtime_src = self.modification_time(sub_path)
            if mtime_src is not None:
                os.utime(local_path, (mtime_src, mtime_src))

        return local_path

    def upload(self,
               sub_path: str,
               local_path: str | Path,
               *,
               preserve_mtime: bool = False) -> Path:
        """Upload a local file to an AWS S3 bucket.

        Parameters:
            sub_path: The path of the destination file in the AWS S3 bucket given by the
                source prefix.
            local_path: The full path of the local file to upload.
            preserve_mtime: If True, the modification time of the local file will be
                copied to the remote file. Not used for local files.

        Returns:
            The Path of the filename, which is the same as the `local_path` parameter.

        Raises:
            FileNotFoundError: If the local file does not exist.
        """

        local_path = Path(local_path)

        if not local_path.exists():
            raise FileNotFoundError(f'File does not exist: {local_path}')

        extra_args = {}
        if preserve_mtime:
            mtime_sec = local_path.stat().st_mtime
            mtime_str = str(mtime_sec)
            extra_args['Metadata'] = {'mtime': mtime_str}

        if extra_args:
            self._client.upload_file(str(local_path), self._bucket_name, sub_path,
                                     ExtraArgs=extra_args)
        else:
            self._client.upload_file(str(local_path), self._bucket_name, sub_path)

        return local_path

    def iterdir_metadata(self,
                         sub_path: str) -> Iterator[tuple[str, dict[str, Any] | None]]:
        """Iterate over the contents of a directory.

        Parameters:
            sub_path: The path of the directory in the AWS S3 bucket given by the source
                prefix.

        Yields:
            All files and sub-directories in the given directory (except ``.`` and
            ``..``), in no particular order. Each file or directory is represented by a
            tuple of the form (path, metadata), where path is the path of the file or
            directory relative to the source prefix, and metadata is a dictionary with the
            following keys:

                - ``is_dir``: True if the returned name is a directory, False if it is a
                  file.
                - ``mtime``: The last modification time of the file as a float.
                - ``size``: The approximate size of the file in bytes.

            If the metadata can not be retrieved, None is returned for the metadata.
        """

        if sub_path:
            sub_prefix = f'{sub_path}/'
        else:
            sub_prefix = ''
        response = self._client.list_objects_v2(Bucket=self._bucket_name,
                                                Prefix=sub_prefix, Delimiter='/')

        if response is not None:
            # Yield filenames with metadata
            contents = response.get('Contents')
            if contents is not None:
                for content in contents:
                    name = content.get('Key')
                    if name is not None:
                        if name.rstrip('/') != sub_path:
                            mtime_obj = content.get('LastModified')
                            mtime = None
                            if mtime_obj is not None:
                                mtime = mtime_obj.timestamp()
                            metadata = {
                                'is_dir': False,
                                'mtime': mtime,
                                'size': content.get('Size', None)
                            }
                            yield f'{self._src_prefix_}{name}', metadata

            # Yield sub-directories with metadata
            contents2 = response.get('CommonPrefixes', [])
            if contents2 is not None:
                for content2 in contents2:
                    prefix = content2.get('Prefix')
                    if prefix is not None:
                        prefix = str(prefix).rstrip('/')
                        metadata = {
                            'is_dir': True,
                            'mtime': None,  # Directories don't have mod dates in S3
                            'size': None   # Directories don't have sizes in S3
                        }
                        yield f'{self._src_prefix_}{prefix}', metadata

    def unlink(self,
               sub_path: str,
               *,
               missing_ok: bool = False) -> str:
        """Remove the given object.

        Parameters:
            sub_path: The path of the file in the Google Storage bucket given by the
                source prefix to delete.
            missing_ok: True if it is OK to unlink a file that doesn't exist; False to
                raise a FileNotFoundError in this case.

        Returns:
            The sub_path.

        Raises:
            FileNotFoundError: If the file doesn't exist and `missing_ok` is False.
        """

        if not missing_ok:
            # S3 doesn't raise an exception when the object doesn't exist so we have
            # check separately.
            if not self.exists(sub_path):
                raise FileNotFoundError

        self._client.delete_object(Bucket=self._bucket_name, Key=sub_path)

        return sub_path


class FileCacheSourceFake(FileCacheSource):
    """Class that simulates a remote file source using a local directory structure.

    This class is useful for testing file operations without requiring actual remote
    connections. Files are stored in a local directory that simulates the remote storage,
    including the need for uploads and downloads. By default, the storage directory is
    ``<TEMPDIR>/.filecache_fake_remote`` and persists across program runs.
    """

    _DEFAULT_STORAGE_DIR = Path(tempfile.gettempdir()) / '.filecache_fake_remote'

    @classmethod
    def get_default_storage_dir(cls) -> Path:
        """Get the current default storage directory for fake remote files.

        Returns:
            The current default storage directory Path.
        """

        return cls._DEFAULT_STORAGE_DIR

    @classmethod
    def set_default_storage_dir(cls, directory: str | Path) -> None:
        """Set the default storage directory for fake remote files.

        Parameters:
            directory: The directory to use as the default storage location. The directory
                is expanded and resolved to an absolute path.
        """

        cls._DEFAULT_STORAGE_DIR = Path(directory).expanduser().resolve()

    @classmethod
    def delete_default_storage_dir(cls) -> None:
        """Delete the current default storage directory and all its contents.

        This is useful for cleanup after testing.
        """

        if cls._DEFAULT_STORAGE_DIR.exists():
            shutil.rmtree(cls._DEFAULT_STORAGE_DIR)

    def __init__(self,
                 scheme: str,
                 remote: str,
                 *,
                 anonymous: bool = False,
                 storage_dir: str | Path | None = None):
        """Initialize the FileCacheSourceFake class.

        Parameters:
            scheme: The scheme of the source. Must be "fake".
            remote: The simulated remote/bucket name.
            anonymous: Not used for this class.
            storage_dir: Base directory in which to store the fake remote files. If None,
                uses the class default storage directory.
        """

        if scheme != 'fake':
            raise ValueError('Scheme must be "fake"')

        if remote == '':
            raise ValueError('remote parameter must have a value')

        super().__init__(scheme, remote, anonymous=anonymous)

        self._storage_base = (Path(storage_dir).expanduser().resolve()
                              if storage_dir is not None
                              else self._DEFAULT_STORAGE_DIR)
        self._storage_dir = self._storage_base / remote
        self._storage_dir.mkdir(parents=True, exist_ok=True)
        self._cache_subdir = self._src_prefix.replace('fake://', 'fake_')

    @classmethod
    def schemes(cls) -> tuple[str, ...]:
        """The URL schemes supported by this class."""

        return ('fake',)

    @classmethod
    def uses_anonymous(cls) -> bool:
        """Whether this class has the concept of anonymous accesses."""

        return False

    def exists(self, sub_path: str) -> bool:
        """Check if a file exists in the fake remote storage.

        Parameters:
            sub_path: The path of the file relative to the storage directory.

        Returns:
            True if the file exists, False otherwise.
        """

        return (self._storage_dir / sub_path).is_file()

    def modification_time(self,
                          sub_path: str) -> float | None:
        """Get the modification time of a file as a Unix timestamp."""

        return (self._storage_dir / sub_path).stat().st_mtime

    def is_dir(self,
               sub_path: str) -> bool:
        """Check if a file is a directory."""

        path = self._storage_dir / sub_path
        if not path.exists():
            raise FileNotFoundError(f'File does not exist: {path}')

        return path.is_dir()

    def retrieve(self,
                 sub_path: str,
                 local_path: str | Path,
                 *,
                 preserve_mtime: bool = False) -> Path:
        """Retrieve a file from the fake remote storage.

        Parameters:
            sub_path: The path of the file relative to the storage directory.
            local_path: The path where the file should be copied to.
            preserve_mtime: If True, the modification time of the fake remote file will be
                copied to the local file.

        Returns:
            The Path where the file was stored (same as local_path).

        Raises:
            FileNotFoundError: If the remote file does not exist.
        """

        source_path = self._storage_dir / sub_path
        local_path = Path(local_path)

        if not source_path.is_file():
            raise FileNotFoundError(f'File does not exist: {self._src_prefix_}{sub_path}')

        local_path.parent.mkdir(parents=True, exist_ok=True)

        temp_local_path = local_path.with_suffix(f'.{local_path.suffix}_{uuid.uuid4()}')
        try:
            shutil.copy2(source_path, temp_local_path)
            # If windows delete the old file first because we can't rename over an
            # existing file
            if platform.system() == 'Windows':
                local_path.unlink(missing_ok=True)
            temp_local_path.rename(local_path)
        except Exception:
            temp_local_path.unlink(missing_ok=True)
            raise

        if preserve_mtime:
            mtime_src = os.path.getmtime(source_path)
            os.utime(local_path, (mtime_src, mtime_src))

        return local_path

    def upload(self,
               sub_path: str,
               local_path: str | Path,
               *,
               preserve_mtime: bool = False) -> Path:
        """Upload a file to the fake remote storage.

        Parameters:
            sub_path: The destination path relative to the storage directory.
            local_path: The path of the local file to upload.
            preserve_mtime: If True, the modification time of the local file will be
                copied to the remote file.

        Returns:
            The Path of the local file that was uploaded.

        Raises:
            FileNotFoundError: If the local file does not exist.
        """

        local_path = Path(local_path)
        if not local_path.is_file():
            raise FileNotFoundError(f'File does not exist: {local_path}')

        dest_path = self._storage_dir / sub_path
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        temp_dest_path = dest_path.with_suffix(f'.{dest_path.suffix}_{uuid.uuid4()}')
        try:
            shutil.copy2(local_path, temp_dest_path)
            # If windows delete the old file first because we can't rename over an
            # existing file
            if platform.system() == 'Windows':
                dest_path.unlink(missing_ok=True)
            temp_dest_path.rename(dest_path)
        except Exception:
            temp_dest_path.unlink(missing_ok=True)
            raise

        if preserve_mtime:
            mtime_src = os.path.getmtime(local_path)
            os.utime(dest_path, (mtime_src, mtime_src))

        return local_path

    def iterdir_metadata(self,
                         sub_path: str) -> Iterator[tuple[str, dict[str, Any] | None]]:
        """Iterate over the contents of a directory in the fake remote storage.

        Parameters:
            sub_path: The path of the directory relative to the storage directory.

        Yields:
            All files and sub-directories in the given directory (except ``.`` and
            ``..``), in no particular order. Each file or directory is represented by a
            tuple of the form (path, metadata), where path is the path of the file or
            directory relative to the source prefix, and metadata is a dictionary with the
            following keys:

                - ``is_dir``: True if the returned name is a directory, False if it is a
                  file.
                - ``mtime``: The last modification time of the file as a float.
                - ``size``: The approximate size of the file in bytes.

            If the metadata can not be retrieved, None is returned for the metadata.
        """

        dir_path = self._storage_dir / sub_path if sub_path else self._storage_dir

        if not dir_path.is_dir():
            return

        for item in dir_path.iterdir():
            relative_path = str(item.relative_to(self._storage_dir)).replace('\\', '/')
            try:
                obj_stat = item.stat()
            except (OSError, IOError):  # pragma: no cover
                # Fallback if stat fails
                metadata: dict[str, Any] | None = None
            else:
                metadata = {
                    'is_dir': stat.S_ISDIR(obj_stat.st_mode),
                    'mtime': obj_stat.st_mtime,
                    'size': obj_stat.st_size
                }

            yield f'{self._src_prefix_}{relative_path}', metadata

    def unlink(self,
               sub_path: str,
               *,
               missing_ok: bool = False) -> str:
        """Remove a file from the fake remote storage.

        Parameters:
            sub_path: The path of the file relative to the storage directory.
            missing_ok: If True, don't raise an error if the file doesn't exist.

        Returns:
            The sub_path that was removed.

        Raises:
            FileNotFoundError: If the file doesn't exist and missing_ok is False.
        """

        file_path = self._storage_dir / sub_path
        try:
            file_path.unlink()
        except FileNotFoundError:
            if not missing_ok:
                raise

        return sub_path
