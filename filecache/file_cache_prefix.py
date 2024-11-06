##########################################################################################
# filecache/file_cache_prefix.py
##########################################################################################

from __future__ import annotations

from collections.abc import Sequence
import contextlib
from pathlib import Path
from typing import (cast,
                    Any,
                    Generator,
                    IO,
                    Optional,
                    TYPE_CHECKING)

if TYPE_CHECKING:
    from .file_cache import FileCache  # Circular import


class FileCachePrefix:
    """Class for interfacing to a FileCache using a URL prefix.

    This class provides a simpler way to abstract away remote access in a FileCache by
    collecting common parameters (`anonymous`, `lock_timeout`, `nthreads`) and a more
    complete prefix (not just the bucket name or URL, but the first part of the access
    path as well) into a single location.
    """

    def __init__(self,
                 prefix: str,
                 filecache: "FileCache",
                 *,
                 anonymous: Optional[bool] = None,
                 lock_timeout: Optional[int] = None,
                 nthreads: Optional[int] = None):
        """Initialization for the FileCachePrefix class.

        Parameters:
            prefix: The prefix for the storage location and any top-level directories.
            file_cache: The :class:`FileCache` in which to store files retrieved from this
                prefix.
            anonymous: If True, access cloud resources without specifying credentials. If
                False, credentials must be initialized in the program's environment. If
                None, use the default setting for the associated :class:`FileCache`
                instance.
            lock_timeout: How long to wait, in seconds, if another process is marked as
                retrieving the file before raising an exception. 0 means to not wait at
                all. A negative value means to never time out. None means to use the
                default value for the associated :class:`FileCache` instance.
            nthreads: The maximum number of threads to use when doing multiple-file
                retrieval or upload. If None, use the default value for the associated
                :class:`FileCache` instance.

        Notes:
            Within a given :class:`FileCache`, :class:`FileCachePrefix` instances that
            reference the same local/remote source will be stored in the same location on
            the local disk. Files downloaded into one instance will thus be visible in the
            other instance.

            Any logging will be made to the `file_cache`'s logger.
        """

        self._filecache = filecache
        self._anonymous = anonymous
        self._lock_timeout = lock_timeout
        if nthreads is not None and (not isinstance(nthreads, int) or nthreads <= 0):
            raise ValueError(f'nthreads must be a positive integer, got {nthreads}')
        self._nthreads = nthreads
        self._upload_counter = 0
        self._download_counter = 0

        if not isinstance(prefix, (str, Path)):
            raise TypeError('prefix is not a str or Path')

        self._prefix_ = str(prefix).replace('\\', '/').rstrip('/') + '/'

        self._filecache._log_debug(f'Initializing prefix {self._prefix_}')

    def exists(self,
               sub_path: str) -> bool:
        """Check if a file exists without downloading it.

        Parameters:
            sub_path: The path of the file relative to the prefix.

        Returns:
            True if the file exists. Note that it is possible that a file could exist and
            still not be downloadable due to permissions. False if the file does not
            exist. This includes bad bucket or webserver names, lack of permission to
            examine a bucket's contents, etc.

        Raises:
            ValueError: If the path is invalidly constructed.
        """

        return self._filecache.exists(f'{self._prefix_}{sub_path}',
                                      anonymous=self._anonymous)

    def get_local_path(self,
                       sub_path: str,
                       *,
                       create_parents: bool = True) -> Path:
        """Return the local path for the given sub_path relative to the prefix.

        Parameters:
            sub_path: The path of the file relative to the prefix.
            create_parents: If True, create all parent directories. This is useful when
                getting the local path of a file that will be uploaded.

        Returns:
            The Path of the filename in the temporary directory, or the absolute path if
            the file source is local. The file does not have to exist because this path
            could be used for writing a file to upload. To facilitate this, a side effect
            of this call (if `create_parents` is True) is that the complete parent
            directory structure will be created by this function as necessary.
        """

        return self._filecache.get_local_path(f'{self._prefix_}{sub_path}',
                                              anonymous=self._anonymous,
                                              create_parents=create_parents)

    def retrieve(self,
                 sub_path: str | Sequence[str],
                 *,
                 nthreads: Optional[int] = None,
                 exception_on_fail: bool = True) -> Path | Exception | list[Path |
                                                                            Exception]:
        """Retrieve a file(s) from the given sub_path and store it in the file cache.

        Parameters:
            sub_path: The path of the file relative to the prefix. If `sub_path` is a list
                or tuple, the complete list of files is retrieved. Depending on the
                storage location, this may be more efficient because files can be
                downloaded in parallel.
            nthreads: The maximum number of threads to use when doing multiple-file
                retrieval or upload. If None, use the default value given when this
                :class:`FileCachePrefix` was created.
            exception_on_fail: If True, if any file does not exist or download fails a
                FileNotFound exception is raised, and if any attempt to acquire a lock or
                wait for another process to download a file fails a TimeoutError is
                raised. If False, the function returns normally and any failed download is
                marked with the Exception that caused the failure in place of the returned
                Path.

        Returns:
            The Path of the filename in the temporary directory (or the original absolute
            path if local). If `sub_path` was a list or tuple of paths, then instead
            return a list of Paths of the filenames in the temporary directory (or the
            original absolute path if local). If `exception_on_fail` is False, any Path
            may be an Exception if that file does not exist or the download failed or a
            timeout occurred.

        Raises:
            FileNotFoundError: If a file does not exist or could not be downloaded, and
                exception_on_fail is True.
            TimeoutError: If we could not acquire the lock to allow downloading of a file
                within the given timeout or, for a multi-file download, if we timed out
                waiting for other processes to download locked files, and
                exception_on_fail is True.

        Notes:
            File download is normally an atomic operation; a program will never see a
            partially-downloaded file, and if a download is interrupted there will be no
            file present. However, when downloading multiple files at the same time, as
            many files as possible are downloaded before an exception is raised.
        """

        old_download_counter = self._filecache.download_counter

        if nthreads is not None and (not isinstance(nthreads, int) or nthreads <= 0):
            raise ValueError(f'nthreads must be a positive integer, got {nthreads}')

        if nthreads is None:
            nthreads = self._nthreads

        try:
            if isinstance(sub_path, (list, tuple)):
                new_sub_path = [f'{self._prefix_}{p}' for p in sub_path]
                ret = self._filecache.retrieve(new_sub_path,
                                               anonymous=self._anonymous,
                                               lock_timeout=self._lock_timeout,
                                               nthreads=nthreads,
                                               exception_on_fail=exception_on_fail)
            else:
                ret = self._filecache.retrieve(f'{self._prefix_}{sub_path}',
                                               anonymous=self._anonymous,
                                               lock_timeout=self._lock_timeout,
                                               exception_on_fail=exception_on_fail)
        finally:
            self._download_counter += (self._filecache.download_counter -
                                       old_download_counter)

        return ret

    def upload(self,
               sub_path: str | Sequence[str],
               *,
               nthreads: Optional[int] = None,
               exception_on_fail: bool = True) -> Path | Exception | list[Path |
                                                                          Exception]:
        """Upload file(s) from the file cache to the storage location(s).

        Parameters:
            sub_path: The path of the file relative to the prefix. If `sub_path` is a list
                or tuple, the complete list of files is uploaded. This may be more
                efficient because files can be uploaded in parallel.
            nthreads: The maximum number of threads to use when doing multiple-file
                retrieval or upload. If None, use the default value given when this
                :class:`FileCachePrefix` was created.
            exception_on_fail: If True, if any file does not exist or upload fails an
                exception is raised. If False, the function returns normally and any
                failed upload is marked with the Exception that caused the failure in
                place of the returned path.

        Returns:
            The Path of the filename in the temporary directory (or the original absolute
            path if local). If `sub_path` was a list or tuple of paths, then instead
            return a list of Paths of the filenames in the temporary directory (or the
            original absolute path if local). If `exception_on_fail` is False, any Path
            may be an Exception if that file does not exist or the upload failed.

        Raises:
            FileNotFoundError: If a file to upload does not exist or the upload failed,
            and exception_on_fail is True.
        """

        old_upload_counter = self._filecache.upload_counter

        if nthreads is not None and (not isinstance(nthreads, int) or nthreads <= 0):
            raise ValueError(f'nthreads must be a positive integer, got {nthreads}')

        if nthreads is None:
            nthreads = self._nthreads

        try:
            if isinstance(sub_path, (list, tuple)):
                new_sub_paths = [f'{self._prefix_}{p}' for p in sub_path]
                ret = self._filecache.upload(new_sub_paths,
                                             anonymous=self._anonymous,
                                             nthreads=nthreads,
                                             exception_on_fail=exception_on_fail)
            else:
                ret = self._filecache.upload(f'{self._prefix_}{sub_path}',
                                             anonymous=self._anonymous,
                                             exception_on_fail=exception_on_fail)
        finally:
            self._upload_counter += (self._filecache.upload_counter -
                                     old_upload_counter)

        return ret

    @contextlib.contextmanager
    def open(self,
             sub_path: str,
             mode: str = 'r',
             *args: Any,
             **kwargs: Any) -> Generator[IO[Any]]:
        """Retrieve+open or open+upload a file as a context manager.

        If `mode` is a read mode (like ``'r'`` or ``'rb'``) then the file will be first
        retrieved by calling :meth:`retrieve` and then opened. If the `mode` is a write
        mode (like ``'w'`` or ``'wb'``) then the file will be first opened for write, and
        when this context manager is exited the file will be uploaded.

        Parameters:
            sub_path: The path of the file relative to the prefix.
            mode: The mode string as you would specify to Python's `open()` function.

        Returns:
            file-like object: The same object as would be returned by the normal `open()`
            function. XXX
        """

        if mode[0] == 'r':
            local_path = cast(Path, self.retrieve(sub_path))
            with open(local_path, mode, *args, **kwargs) as fp:
                yield fp
        else:  # 'w', 'x', 'a'
            local_path = self.get_local_path(sub_path)
            with open(local_path, mode, *args, **kwargs) as fp:
                yield fp
            self.upload(sub_path)

    @property
    def download_counter(self) -> int:
        """The number of actual file downloads that have taken place."""
        return self._download_counter

    @property
    def upload_counter(self) -> int:
        """The number of actual file uploads that have taken place."""
        return self._upload_counter

    @property
    def prefix(self) -> str:
        """The URI prefix including a trailing slash."""
        return self._prefix_

    @property
    def is_local(self) -> bool:
        """A bool indicating whether or not the prefix refers to the local filesystem."""
        return self._prefix_.startswith('file:///') or '://' not in self._prefix_
