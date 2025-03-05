##########################################################################################
# filecache/file_cache.py
##########################################################################################

from __future__ import annotations

import atexit
import contextlib
import logging
from logging import Logger
import os
from pathlib import Path
import platform
import sys
import tempfile
import time
from types import TracebackType
from typing import (cast,
                    Any,
                    IO,
                    Iterator,
                    Literal,
                    Optional,
                    Type,
                    Union)
from typing_extensions import Self
import uuid

import filelock

from .file_cache_source import (FileCacheSource,
                                FileCacheSourceFile,
                                FileCacheSourceHTTP,
                                FileCacheSourceGS,
                                FileCacheSourceS3,
                                FileCacheSourceFake)
from .file_cache_path import FCPath
from .file_cache_types import (StrOrPathOrSeqType,
                               UrlToPathFuncOrSeqType)


# Global cache of all instantiated FileCacheSource since they may involve opening
# a connection and are not specific to a particular FileCache
_SOURCE_CACHE: dict[tuple[str, str, bool], FileCacheSource] = {}


# URL schemes mapping prefix ('gs') to FileCacheSource* class
_SCHEME_CLASSES: dict[str, type[FileCacheSource]] = {}


def register_filecachesource(cls: type[FileCacheSource]) -> None:
    """Register one or more URL FileCacheSource subclasses as URL schemes."""

    for s in cls.schemes():
        _SCHEME_CLASSES[s] = cls


register_filecachesource(FileCacheSourceFile)
register_filecachesource(FileCacheSourceHTTP)
register_filecachesource(FileCacheSourceGS)
register_filecachesource(FileCacheSourceS3)
register_filecachesource(FileCacheSourceFake)


# Default logger for all FileCache instances that didn't specify one explicitly
_GLOBAL_LOGGER: Logger | None = None


def set_global_logger(logger: Logger | None) -> None:
    """Set the global logger for all FileCache instances that don't specify one."""

    global _GLOBAL_LOGGER
    _GLOBAL_LOGGER = logger


def set_easy_logger() -> None:
    """Set a default logger that outputs all messages to stdout."""

    easy_logger = logging.getLogger(__name__)
    easy_logger.setLevel(logging.DEBUG)

    while easy_logger.handlers:
        easy_logger.removeHandler(easy_logger.handlers[0])

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    easy_logger.addHandler(handler)

    set_global_logger(easy_logger)


def get_global_logger() -> Logger | None:
    """Return the current global logger."""

    return _GLOBAL_LOGGER


class FileCache:
    """Class which manages the lifecycle of files from various sources."""

    _FILE_CACHE_PREFIX = '_filecache_'
    _LOCK_PREFIX = '.__lock__'

    def __init__(self,
                 cache_name: Optional[str] = 'global',
                 *,
                 cache_root: Optional[Path | str] = None,
                 delete_on_exit: Optional[bool] = None,
                 mp_safe: Optional[bool] = None,
                 anonymous: bool = False,
                 lock_timeout: int = 60,
                 nthreads: int = 8,
                 url_to_path: Optional[UrlToPathFuncOrSeqType] = None,
                 logger: Optional[Logger | bool] = None):
        r"""Initialization for the FileCache class.

        Parameters:
            cache_name: By default, the file cache will be stored in the subdirectory
                ``_filecache_global`` under the `cache_root` directory. If a name is
                specified explicitly, the file cache will be stored in the subdirectory
                ``_filecache_<cache_name>``. Explicitly naming a cache is useful if other
                programs will want to access the same cache, or if you want the directory
                name to be obvious to users browsing the file system. Using a cache name
                (including the default ``global``) implies that this cache should be
                persistent on exit. If you pass in ``None``, the cache will instead be
                stored in a uniquely-named subdirectory with the prefix ``_filecache_``
                and by default will be deleted on exit.
            cache_root: The directory in which to place caches. By default,
                :class:`FileCache` uses the contents of the environment variable
                ``FILECACHE_CACHE_ROOT``; if not set, then the system temporary directory
                is used, which involves checking the environment variables ``TMPDIR``,
                ``TEMP``, and ``TMP``, and if none of those are set then using
                ``C:\TEMP``, ``C:\TMP``, ``\TEMP``, or ``\TMP`` on Windows and ``/tmp``,
                ``/var/tmp``, or ``/usr/tmp`` on other platforms. The cache will be stored
                in a sub-directory within this directory (see `cache_name`). If
                `cache_root` is specified but the directory does not exist, it is created.
            delete_on_exit: If True, the cache directory and its contents
                are always deleted on program exit or exit from a :class:`FileCache`
                context manager. If False, the cache is never deleted. By default, an
                unnamed cache (`cache_name` is ``None``) will be deleted on exit and a
                named cache will not be deleted on program exit.
            mp_safe: If False, never use multiprocessor-safe locking. If True, always use
                multiprocessor-safe locking. By default, locking is used if `cache_name`
                is specified, as it is assumed that multiple processes will be using the
                named cache simultaneously. If multiple processes will not be using the
                cache simultaneously, a small performance boost can be realized by setting
                `mp_safe` explicitly to False.
            anonymous: The default value for anonymous access to cloud resources.
                If True, access cloud resources without specifying credentials. If False,
                credentials must be initialized in the program's environment.
            lock_timeout: The default value for lock timeouts. This is how long to wait,
                in seconds, if another process is marked as retrieving a file before
                raising an exception. 0 means to not wait at all. A negative value means
                to never time out.
            nthreads: The default value for the maximum number of threads to use when
                doing multiple-file retrieval or upload.
            url_to_path: The default function (or list of functions) that is used to
                translate URLs into local paths. By default, :class:`FileCache` uses a
                directory hierarchy consisting of
                ``<cache_dir>/<cache_name>/<source>/<path>``, where ``source`` is the URL
                prefix converted to a filesystem-friendly format (e.g. ``gs://bucket`` is
                converted to ``gs_bucket``). A user-specified translator function takes
                five arguments::

                    func(scheme: str, remote: str, path: str, cache_dir: Path,
                         cache_subdir: str) -> str | Path

                where `scheme` is the URL scheme (like ``"gs"`` or ``"file"``), `remote`
                is the name of the bucket or webserver or the empty string for a local
                file, `path` is the rest of the URL, `cache_dir` is the top-level
                directory of the cache (``<cache_dir>/<cache_name>``), and `cache_subdir`
                is the subdirectory specific to this scheme and remote. If the translator
                wants to override the default translation, it can return a Path.
                Otherwise, it returns None. If the returned Path is relative, if will be
                appended to `cache_dir`; if it is absolute, it will be used directly (be
                very careful with this, as it has the ability to access files outside of
                the cache directory). If more than one translator is specified, they are
                called in order until one returns a Path, or it falls through to the
                default.
            logger: If False, do not do any logging. If None, use the
                global logger set with :func:`set_global_logger`. Otherwise use the
                specified logger.

        Notes:
            :class:`FileCache` can be used as a context, such as::

                with FileCache(cache_name=None) as fc:
                    ...

            In this case, the cache directory is created on entry to the context and
            deleted on exit. However, if the cache is named, the directory will not be
            deleted on exit unless the ``delete_on_exit=True`` option is used.
        """

        # We try very hard here to make sure that no possible passed-in argument for
        # cache_root or cache_name could result in a directory name that is anything other
        # than a new cache directory. In particular, since we may be deleting this
        # directory later, we want to make sure it's impossible for a bad actor (or just
        # an accidental bad argument) to inject a directory or filename that could result
        # in the deletion of system or user files. One key aspect of this is we do not
        # allow the user to specify the specific subdirectory name without the unique
        # prefix, and we do not allow the directory name to have additional directory
        # components like "..".

        if cache_root is None:
            cache_root = os.environ.get('FILECACHE_CACHE_ROOT')
        if cache_root is None:
            cache_root = tempfile.gettempdir()
        cache_root = Path(cache_root).expanduser().resolve()
        if not cache_root.exists():
            cache_root.mkdir(parents=True, exist_ok=True)
        if not cache_root.is_dir():
            raise ValueError(f'{cache_root} is not a directory')

        if cache_name is None:
            sub_dir = Path(self._FILE_CACHE_PREFIX + str(uuid.uuid4()))
        elif isinstance(cache_name, str):
            if '/' in cache_name or '\\' in cache_name:
                raise ValueError(
                    f'cache_name argument {cache_name} has directory elements')
            sub_dir = Path(self._FILE_CACHE_PREFIX + cache_name)
        else:
            raise TypeError(f'cache_name argument {cache_name} is of improper type')

        is_shared = (cache_name is not None)

        self._delete_on_exit = (delete_on_exit if delete_on_exit is not None
                                else not is_shared)
        self._is_mp_safe = mp_safe if mp_safe is not None else is_shared
        self._anonymous = anonymous
        self._lock_timeout = lock_timeout
        if not isinstance(nthreads, int) or nthreads <= 0:
            raise ValueError(f'nthreads {nthreads} must be a positive integer')
        self._nthreads = nthreads
        if url_to_path is None:
            self._url_to_path = []
        elif isinstance(url_to_path, (list, tuple)):
            self._url_to_path = list(url_to_path)
        else:
            self._url_to_path = [url_to_path]
        self._logger = logger
        self._upload_counter = 0
        self._download_counter = 0

        self._cache_dir = cache_root / sub_dir
        if self._cache_dir.is_dir():
            self._log_info(f'Using existing cache {self._cache_dir}')
        else:
            self._log_info(f'Creating cache {self._cache_dir}')
            # A non-shared cache (which has a unique name) should never already exist
            self._cache_dir.mkdir(exist_ok=is_shared)

        atexit.register(self._maybe_delete_cache)

    def _validate_nthreads(self,
                           nthreads: Optional[int]) -> int:
        if nthreads is not None and (not isinstance(nthreads, int) or nthreads <= 0):
            raise ValueError(f'nthreads must be a positive integer, got {nthreads}')
        if nthreads is None:
            nthreads = self.nthreads
        return nthreads

    @classmethod
    def registered_scheme_prefixes(self) -> tuple[str, ...]:
        return tuple([x + '://' for x in _SCHEME_CLASSES])

    @property
    def cache_dir(self) -> Path:
        """The top-level directory of the cache as a Path object."""

        return self._cache_dir

    @property
    def download_counter(self) -> int:
        """The number of actual file downloads that have taken place."""

        return self._download_counter

    @property
    def upload_counter(self) -> int:
        """The number of actual file uploads that have taken place."""

        return self._upload_counter

    @property
    def anonymous(self) -> bool:
        """The default bool indicating whether to make all cloud accesses anonymous."""

        return self._anonymous

    @property
    def lock_timeout(self) -> int:
        """The default timeout in seconds while waiting for a file lock."""

        return self._lock_timeout

    @property
    def nthreads(self) -> int:
        """The default number of threads to use for multiple-file operations."""

        return self._nthreads

    @property
    def delete_on_exit(self) -> bool:
        """A bool indicating whether this FileCache will be deleted on exit."""

        return self._delete_on_exit

    @property
    def is_mp_safe(self) -> bool:
        """A bool indicating whether this FileCache is multi-processor safe."""

        return self._is_mp_safe

    def _log_debug(self, msg: str) -> None:
        logger = _GLOBAL_LOGGER if self._logger is None else self._logger
        if logger:
            logger.debug(msg)  # type: ignore

    def _log_info(self, msg: str) -> None:
        logger = _GLOBAL_LOGGER if self._logger is None else self._logger
        if logger:
            logger.info(msg)  # type: ignore

    # def _log_warn(self, msg: str) -> None:
    #     logger = _GLOBAL_LOGGER if self._logger is None else self._logger
    #     if logger:
    #         logger.warning(msg)  # type: ignore

    def _log_error(self, msg: str) -> None:
        logger = _GLOBAL_LOGGER if self._logger is None else self._logger
        if logger:
            logger.error(msg)  # type: ignore

    @staticmethod
    def _split_url(url: str) -> tuple[str, str, str]:
        url = url.replace('\\', '/')
        parts = url.split('://')
        if len(parts) == 1:
            # We default to local files
            if not Path(url).is_absolute():
                raise ValueError(f'Local file URL is not absolute: {url}')
            return 'file', '', url
        elif len(parts) == 2:
            slash_split = parts[1].split('/', maxsplit=1)
            if len(slash_split) != 2:
                remote = parts[1]
                sub_path = ''
            else:
                remote, sub_path = slash_split
            scheme = parts[0].lower()
            if scheme == 'file':
                # All file accesses are absolute
                if platform.system() == 'Windows':  # pragma: no cover
                    # file:///c:/dir/file
                    # sub_path will be c:/dir/file so it's already absolute.
                    # If the drive isn't specified, we have a problem and is_absolute
                    # will fail.
                    if not Path(sub_path).is_absolute():
                        raise ValueError(f'Local file URL is not absolute: {url}')
                else:
                    # file:///dir/file
                    # We have to add the / back on the beginning
                    if sub_path:
                        sub_path = f'/{sub_path}'
                sub_path = str(Path(sub_path))
            if scheme not in _SCHEME_CLASSES:
                raise ValueError(f'Unknown scheme {scheme} in {url}')
            return scheme, remote, sub_path
        raise ValueError(f'URL {url} has more than one instance of ://')

    @staticmethod
    def _default_url_to_path(scheme: str,
                             remote: str,
                             path: str,
                             cache_dir: Path,
                             cache_subdir: str) -> Path:
        if scheme == 'file':
            return Path(path)
        return cache_dir / cache_subdir / path

    def _get_source_and_paths(self,
                              url: str | Path,
                              anonymous: bool | None,
                              url_to_path: UrlToPathFuncOrSeqType | None
                              ) -> tuple[FileCacheSource, str, Path]:
        url = str(url)
        if anonymous is None:
            anonymous = self.anonymous
        if url_to_path is None:
            url_to_path = self._url_to_path
        elif isinstance(url_to_path, (list, tuple)):
            url_to_path = list(url_to_path)
        else:
            url_to_path = [url_to_path]

        url_to_path = url_to_path + [FileCache._default_url_to_path]

        scheme, remote, sub_path = self._split_url(url)
        if not _SCHEME_CLASSES[scheme].uses_anonymous():
            # No such thing as needing credentials for a local file or HTTP
            # so don't overconstrain the source cache
            anonymous = False

        key = (scheme, remote, anonymous)
        if key not in _SOURCE_CACHE:
            _SOURCE_CACHE[key] = _SCHEME_CLASSES[scheme](scheme, remote,
                                                         anonymous=anonymous)

        source = _SOURCE_CACHE[key]

        for url_to_path_func in url_to_path:
            local_path = url_to_path_func(scheme, remote, sub_path,
                                          self.cache_dir, source._cache_subdir)
            if local_path is not None:
                local_path = Path(local_path)
                if not local_path.is_absolute():
                    local_path = self.cache_dir / source._cache_subdir / local_path
                break
        else:  # pragma: no cover - can't happen - there's always a default translator
            raise ValueError(
                f'No url to path translator found for {url}')

        return source, sub_path, local_path

    def _lock_path(self, path: Path | str) -> Path:
        path = Path(path)
        return path.parent / f'{self._LOCK_PREFIX}{path.name}'

    def get_local_path(self,
                       url: StrOrPathOrSeqType,
                       *,
                       anonymous: Optional[bool] = None,
                       create_parents: bool = True,
                       url_to_path: Optional[UrlToPathFuncOrSeqType] = None
                       ) -> Path | list[Path]:
        """Return the local path for the given url.

        Parameters:
            url: The URL of the file, including any source prefix. If `url` is a list or
                tuple, all URLs are processed.
            anonymous: If True, access cloud resources without specifying credentials. If
                False, credentials must be initialized in the program's environment. If
                None, use the default setting for this :class:`FileCache` instance.
            create_parents: If True, create all parent directories. This
                is useful when getting the local path of a file that will be uploaded.
            url_to_path: The function (or list of functions) that is used to translate
                URLs into local paths. By default, :class:`FileCache` uses a directory
                hierarchy consisting of ``<cache_dir>/<cache_name>/<source>/<path>``,
                where ``source`` is the URL prefix converted to a filesystem-friendly
                format (e.g. ``gs://bucket`` is converted to ``gs_bucket``). A
                user-specified translator function takes five arguments::

                    func(scheme: str, remote: str, path: str, cache_dir: Path,
                         cache_subdir: str) -> str | Path

                where `scheme` is the URL scheme (like ``"gs"`` or ``"file"``), `remote`
                is the name of the bucket or webserver or the empty string for a local
                file, `path` is the rest of the URL, `cache_dir` is the top-level
                directory of the cache (``<cache_dir>/<cache_name>``), and `cache_subdir`
                is the subdirectory specific to this scheme and remote. If the translator
                wants to override the default translation, it can return a Path.
                Otherwise, it returns None. If the returned Path is relative, if will be
                appended to `cache_dir`; if it is absolute, it will be used directly (be
                very careful with this, as it has the ability to access files outside of
                the cache directory). If more than one translator is specified, they are
                called in order until one returns a Path, or it falls through to the
                default.

                If this parameter is specified, it replaces the default translators for
                this :class:`FileCache` instance. If this parameter is omitted, the
                default translators are used.

        Returns:
            The Path (or list of Paths) of the filename in the temporary directory, or
            as specified by the `url_to_path` translators. The files do not have to exist
            because a Path could be used for writing a file to upload. To facilitate
            this, a side effect of this call (if `create_parents` is True) is that the
            complete parent directory structure will be created for each returned Path.
        """

        if isinstance(url, (list, tuple)):
            new_url = list(url)
        else:
            new_url = [cast(str, url)]

        ret: list[Path] = []

        for one_url in new_url:
            source, sub_path, local_path = self._get_source_and_paths(one_url, anonymous,
                                                                      url_to_path)
            ret.append(local_path)
            if create_parents:
                local_path.parent.mkdir(parents=True, exist_ok=True)
            self._log_debug(f'Returning local path for {one_url} as {local_path}')

        if isinstance(url, (list, tuple)):
            return ret
        return ret[0]

    def exists(self,
               url: StrOrPathOrSeqType,
               *,
               bypass_cache: bool = False,
               anonymous: Optional[bool] = None,
               nthreads: Optional[int] = None,
               url_to_path: Optional[UrlToPathFuncOrSeqType] = None
               ) -> bool | list[bool]:
        """Check if a file exists without downloading it.

        Parameters:
            url: The URL of the file, including any source prefix. If `url` is a list or
                tuple, all URLs are retrieved. This may be more efficient because files
                can be downloaded in parallel. It is OK to retrieve files from multiple
                sources using one call.
            bypass_cache: If False, check for the file first in the local cache, and if
                not found there then on the remote server. If True, only check on the
                remote server.
            anonymous: If specified, override the default setting for anonymous access.
                If True, access cloud resources without specifying credentials. If False,
                credentials must be initialized in the program's environment.
            nthreads: The maximum number of threads to use when doing multiple-file
                retrieval or upload. If None, use the default value for this
                :class:`FileCache` instance.
            url_to_path: The function (or list of functions) that is used to translate
                URLs into local paths. By default, :class:`FileCache` uses a directory
                hierarchy consisting of ``<cache_dir>/<cache_name>/<source>/<path>``,
                where ``source`` is the URL prefix converted to a filesystem-friendly
                format (e.g. ``gs://bucket`` is converted to ``gs_bucket``). A
                user-specified translator function takes five arguments::

                    func(scheme: str, remote: str, path: str, cache_dir: Path,
                         cache_subdir: str) -> str | Path

                where `scheme` is the URL scheme (like ``"gs"`` or ``"file"``), `remote`
                is the name of the bucket or webserver or the empty string for a local
                file, `path` is the rest of the URL, `cache_dir` is the top-level
                directory of the cache (``<cache_dir>/<cache_name>``), and `cache_subdir`
                is the subdirectory specific to this scheme and remote. If the translator
                wants to override the default translation, it can return a Path.
                Otherwise, it returns None. If the returned Path is relative, if will be
                appended to `cache_dir`; if it is absolute, it will be used directly (be
                very careful with this, as it has the ability to access files outside of
                the cache directory). If more than one translator is specified, they are
                called in order until one returns a Path, or it falls through to the
                default.

                If this parameter is specified, it replaces the default translators for
                this :class:`FileCache` instance. If this parameter is omitted, the
                default translators are used.

        Returns:
            True if the file exists (note that it is possible that a file could exist and
            still not be downloadable due to permissions). False if the file does not
            exist. This includes bad bucket or webserver names, lack of permission to
            examine a bucket's contents, etc. If `url` was a list or tuple, then instead
            return a list of bools giving the existence of each url in order.
        """

        nthreads = self._validate_nthreads(nthreads)

        if isinstance(url, (list, tuple)):
            sources = []
            sub_paths = []
            local_paths = []
            for one_url in url:
                source, sub_path, local_path = self._get_source_and_paths(one_url,
                                                                          anonymous,
                                                                          url_to_path)
                sources.append(source)
                sub_paths.append(sub_path)
                local_paths.append(local_path)

            return self._exists_multi(sources, sub_paths, local_paths, nthreads,
                                      bypass_cache)

        self._log_debug(f'Checking file for existence: {url}')

        source, sub_path, local_path = self._get_source_and_paths(str(url),
                                                                  anonymous,
                                                                  url_to_path)

        if not bypass_cache and local_path.exists():
            if source.primary_scheme() == 'file':
                self._log_debug(f'  File exists: {url}')
            else:
                self._log_debug(f'  File exists as cached at {local_path}')
            return True

        elif source.primary_scheme() == 'file':
            return False

        file_ret = source.exists(sub_path)

        if file_ret:
            self._log_debug(f'  File exists: {url}')
        else:
            self._log_debug(f'  File does not exist: {url}')

        return file_ret

    def _exists_multi(self,
                      sources: list[FileCacheSource],
                      sub_paths: list[str],
                      local_paths: list[Path],
                      nthreads: int,
                      bypass_cache: bool) -> list[bool]:
        """Check for the existence of multiple files in storage locations."""

        # Return bools in the same order as sub_paths
        func_ret: list[bool | None] = [None] * len(sources)

        source_dict: dict[str, list[tuple[int, FileCacheSource, str, Path]]] = {}

        # First find all the files that are either local or that we have already cached.
        # For other files, create a list of just the files we need to check and
        # organize them by source; we use the source prefix to distinguish among them.
        self._log_debug('Performing multi-file existence check of:')
        for idx, (source, sub_path, local_path) in enumerate(zip(sources,
                                                                 sub_paths, local_paths)):
            pfx = source._src_prefix_
            if not bypass_cache and local_path.is_file():
                self._log_debug(f'    Cached file  {pfx}{sub_path} at {local_path}')
                func_ret[idx] = True
                continue
            if source.primary_scheme() == 'file':
                self._log_debug(f'    Local file   {pfx}{sub_path}')
                func_ret[idx] = source.exists(sub_path)
                continue
            assert '://' in pfx
            if pfx not in source_dict:
                source_dict[pfx] = []
            source_dict[pfx].append((idx, source, sub_path, local_path))
            self._log_debug(f'    To check {pfx}{sub_path}')

        # Now go through the sources, package up the paths to check, and check
        # them all at once
        for source_pfx in source_dict:
            source = source_dict[source_pfx][0][1]  # All the same
            source_idxes, _, source_sub_paths, source_local_paths = list(
                zip(*source_dict[source_pfx]))
            self._log_debug(
                f'  Performing multi-file exists check for prefix {source_pfx}:')
            for sub_path in source_sub_paths:
                self._log_debug(f'    {sub_path}')
            rets = source.exists_multi(source_sub_paths, nthreads=nthreads)
            assert len(source_idxes) == len(rets)
            for source_ret, source_idx in zip(rets, source_idxes):
                func_ret[source_idx] = source_ret

        self._log_debug('Multi-file exists check complete')

        return cast(list[bool], func_ret)

    def retrieve(self,
                 url: StrOrPathOrSeqType,
                 *,
                 anonymous: Optional[bool] = None,
                 lock_timeout: Optional[int] = None,
                 nthreads: Optional[int] = None,
                 exception_on_fail: bool = True,
                 url_to_path: Optional[UrlToPathFuncOrSeqType] = None
                 ) -> Path | Exception | list[Path | Exception]:
        """Retrieve file(s) from the given location(s) and store in the file cache.

        Parameters:
            url: The URL of the file, including any source prefix. If `url` is a list or
                tuple, all URLs are retrieved. This may be more efficient because files
                can be downloaded in parallel. It is OK to retrieve files from multiple
                sources using one call.
            anonymous: If True, access cloud resources without specifying credentials. If
                False, credentials must be initialized in the program's environment. If
                None, use the default setting for this :class:`FileCache` instance.
            lock_timeout: How long to wait, in seconds, if another process is marked as
                retrieving the file before raising an exception. 0 means to not wait at
                all. A negative value means to never time out. None means to use the
                default value for this :class:`FileCache` instance.
            nthreads: The maximum number of threads to use when doing multiple-file
                retrieval or upload. If None, use the default value for this
                :class:`FileCache` instance.
            exception_on_fail: If True, if any file does not exist or download fails a
                FileNotFound exception is raised, and if any attempt to acquire a lock or
                wait for another process to download a file fails a TimeoutError is
                raised. If False, the function returns normally and any failed download is
                marked with the Exception that caused the failure in place of the returned
                Path.
            url_to_path: The function (or list of functions) that is used to translate
                URLs into local paths. By default, :class:`FileCache` uses a directory
                hierarchy consisting of ``<cache_dir>/<cache_name>/<source>/<path>``,
                where ``source`` is the URL prefix converted to a filesystem-friendly
                format (e.g. ``gs://bucket`` is converted to ``gs_bucket``). A
                user-specified translator function takes five arguments::

                    func(scheme: str, remote: str, path: str, cache_dir: Path,
                         cache_subdir: str) -> str | Path

                where `scheme` is the URL scheme (like ``"gs"`` or ``"file"``), `remote`
                is the name of the bucket or webserver or the empty string for a local
                file, `path` is the rest of the URL, `cache_dir` is the top-level
                directory of the cache (``<cache_dir>/<cache_name>``), and `cache_subdir`
                is the subdirectory specific to this scheme and remote. If the translator
                wants to override the default translation, it can return a Path.
                Otherwise, it returns None. If the returned Path is relative, if will be
                appended to `cache_dir`; if it is absolute, it will be used directly (be
                very careful with this, as it has the ability to access files outside of
                the cache directory). If more than one translator is specified, they are
                called in order until one returns a Path, or it falls through to the
                default.

                If this parameter is specified, it replaces the default translators for
                this :class:`FileCache` instance. If this parameter is omitted, the
                default translators are used.

        Returns:
            The Path of the filename in the temporary directory (or the original absolute
            path if local). If `url` was a list or tuple, then instead return a list of
            Paths of the filenames in the temporary directory (or the original absolute
            path if local). If `exception_on_fail` is False, any Path may be an Exception
            if that file does not exist or the download failed or a timeout occurred.

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

        if lock_timeout is None:
            lock_timeout = self.lock_timeout
        nthreads = self._validate_nthreads(nthreads)

        # Technically we could just do everything as a locked multi-download, but we
        # separate out the cases for efficiency
        if isinstance(url, (list, tuple)):
            sources = []
            sub_paths = []
            local_paths = []
            for one_url in url:
                source, sub_path, local_path = self._get_source_and_paths(one_url,
                                                                          anonymous,
                                                                          url_to_path)
                sources.append(source)
                sub_paths.append(sub_path)
                local_paths.append(local_path)
            if self.is_mp_safe:
                return self._retrieve_multi_locked(sources, sub_paths, local_paths,
                                                   lock_timeout, nthreads,
                                                   exception_on_fail)
            return self._retrieve_multi_unlocked(sources, sub_paths, local_paths,
                                                 nthreads, exception_on_fail)

        url = str(url)
        source, sub_path, local_path = self._get_source_and_paths(url, anonymous,
                                                                  url_to_path)

        if source.primary_scheme() == 'file':
            self._log_debug(f'Retrieving {sub_path} (local file)')
            try:
                return source.retrieve(sub_path, local_path)
            except Exception as e:
                if exception_on_fail:
                    raise
                return e

        # If the file actually exists, it's always safe to return it
        if local_path.is_file():
            self._log_debug(f'Accessing cached file for {url} at {local_path}')
            return local_path

        if self.is_mp_safe:
            return self._retrieve_locked(source, sub_path, local_path, lock_timeout,
                                         exception_on_fail)
        return self._retrieve_unlocked(source, sub_path, local_path, exception_on_fail)

    def _retrieve_unlocked(self,
                           source: FileCacheSource,
                           sub_path: str,
                           local_path: Path,
                           exception_on_fail: bool) -> Path | Exception:
        """Retrieve a single file from the storage location without lock protection."""

        self._log_debug(f'Downloading {source._src_prefix_}{sub_path} into {local_path}')
        try:
            ret = source.retrieve(sub_path, local_path)
        except Exception as e:
            if exception_on_fail:
                raise
            return e

        self._download_counter += 1

        return ret

    def _retrieve_locked(self,
                         source: FileCacheSource,
                         sub_path: str,
                         local_path: Path,
                         lock_timeout: int,
                         exception_on_fail: bool) -> Path | Exception:
        """Retrieve a single file from the storage location with lock protection."""

        lock_path = self._lock_path(local_path)
        lock = filelock.FileLock(lock_path, timeout=lock_timeout)
        try:
            lock.acquire()
        except filelock._error.Timeout as e:
            if exception_on_fail:
                raise
            return e
        self._log_debug(
            f'Downloading {source._src_prefix_}{sub_path} into {local_path} with locking')
        try:
            ret = source.retrieve(sub_path, local_path)
        except Exception as e:
            if exception_on_fail:
                raise
            return e
        finally:
            # There is a potential race condition here in the case of a raised
            # exception, because after we release the lock but before we delete
            # it, someone else could notice the file isn't downloaded and lock
            # it for another download attempt, and then we would delete someone
            # else's lock (because on Linux locks are only advisory). However,
            # we have to do it in this order because otherwise it won't work on
            # Windows, where locks are not just advisory. However, the worst
            # that could happen is we end up attempting to download the file
            # twice.
            lock.release()
            lock_path.unlink(missing_ok=True)

        self._download_counter += 1

        return ret

    def _retrieve_multi_unlocked(self,
                                 sources: list[FileCacheSource],
                                 sub_paths: list[str],
                                 local_paths: list[Path],
                                 nthreads: int,
                                 exception_on_fail: bool) -> list[Path | Exception]:
        """Retrieve multiple files from storage locations without lock protection."""

        # Return Paths (or Exceptions) in the same order as sub_paths
        func_ret: list[Path | BaseException | None] = [None] * len(sources)

        files_not_exist = []

        source_dict: dict[str, list[tuple[int, FileCacheSource, str, Path]]] = {}

        # First find all the files that are either local or that we have already cached.
        # For other files, create a list of just the files we need to retrieve and
        # organize them by source; we use the source prefix to distinguish among them.
        self._log_debug('Performing multi-file retrieval of:')
        for idx, (source, sub_path, local_path) in enumerate(zip(sources,
                                                                 sub_paths, local_paths)):
            pfx = source._src_prefix_
            if source.primary_scheme() == 'file':
                self._log_debug(f'    Local file   {pfx}{sub_path}')
                try:
                    func_ret[idx] = source.retrieve(sub_path, local_path)
                except Exception as e:
                    files_not_exist.append(sub_path)
                    func_ret[idx] = e
                continue
            if local_path.is_file():
                self._log_debug(f'    Cached file  {pfx}{sub_path} at {local_path}')
                func_ret[idx] = local_path
                continue
            assert '://' in pfx
            if pfx not in source_dict:
                source_dict[pfx] = []
            source_dict[pfx].append((idx, source, sub_path, local_path))
            self._log_debug(f'    To download {pfx}{sub_path}')

        # Now go through the sources, package up the paths to retrieve, and retrieve
        # them all at once
        for source_pfx in source_dict:
            source = source_dict[source_pfx][0][1]  # All the same
            source_idxes, _, source_sub_paths, source_local_paths = list(
                zip(*source_dict[source_pfx]))
            self._log_debug(
                f'  Performing multi-file download for prefix {source_pfx}:')
            for sub_path in source_sub_paths:
                self._log_debug(f'    {sub_path}')
            rets = source.retrieve_multi(source_sub_paths, source_local_paths,
                                         nthreads=nthreads)
            assert len(source_idxes) == len(rets)
            for ret, sub_path in zip(rets, source_sub_paths):
                if isinstance(ret, Exception):
                    self._log_debug(f'    Download failed: {sub_path} {ret}')
                    files_not_exist.append(f'{source_pfx}{sub_path}')
                else:
                    self._download_counter += 1

            for source_ret, source_idx in zip(rets, source_idxes):
                func_ret[source_idx] = source_ret

        if files_not_exist:
            self._log_debug('Multi-file retrieval completed with errors')
            if exception_on_fail:
                exc_str = f"File(s) do not exist: {', '.join(files_not_exist)}"
                raise FileNotFoundError(exc_str)
        else:
            self._log_debug('Multi-file retrieval complete')

        return cast(list[Union[Path, Exception]], func_ret)

    def _retrieve_multi_locked(self,
                               sources: list[FileCacheSource],
                               sub_paths: list[str],
                               local_paths: list[Path],
                               lock_timeout: int,
                               nthreads: int,
                               exception_on_fail: bool) -> list[Path | Exception]:
        """Retrieve multiple files from storage locations with lock protection."""

        start_time = time.time()

        # Return Paths (or Exceptions) in the same order as sub_paths
        func_ret: list[Path | BaseException | None] = [None] * len(sources)

        files_not_exist = []

        wait_to_appear = []  # Locked by another process (they are downloading it)

        source_dict: dict[str, list[tuple[int, FileCacheSource, str, Path]]] = {}

        # First find all the files that are either local or that we have already cached.
        # For other files, create a list of just the files we need to retrieve and
        # organize them by source; we use the source prefix to distinguish among them.
        self._log_debug('Performing locked multi-file retrieval of:')
        for idx, (source, sub_path, local_path) in enumerate(zip(sources,
                                                                 sub_paths, local_paths)):
            pfx = source._src_prefix_
            # No need to lock for local files
            if source.primary_scheme() == 'file':
                self._log_debug(f'    Local file   {pfx}{sub_path}')
                try:
                    func_ret[idx] = source.retrieve(sub_path, local_path)
                except Exception as e:
                    files_not_exist.append(sub_path)
                    func_ret[idx] = e
                continue
            # Since all download operations for individual files are atomic, no need to
            # lock if the file actually exists
            if local_path.is_file():
                self._log_debug(f'    Cached file  {pfx}{sub_path} at {local_path}')
                func_ret[idx] = local_path
                continue
            assert '://' in pfx
            if pfx not in source_dict:
                source_dict[pfx] = []
            source_dict[pfx].append((idx, source, sub_path, local_path))
            self._log_debug(f'    To download {pfx}{sub_path}')

        # Now go through the sources, package up the paths to retrieve, and retrieve
        # them all at once
        for source_pfx in source_dict:
            source = source_dict[source_pfx][0][1]  # All the same
            orig_source_idxes, _, orig_source_sub_paths, orig_source_local_paths = list(
                zip(*source_dict[source_pfx]))
            self._log_debug(
                f'  Performing locked multi-file download for prefix {source_pfx}:')
            for sub_path in orig_source_sub_paths:
                self._log_debug(f'      {sub_path}')

            # We first loop through the local paths and try to acquire locks on all
            # the files. If we fail to get a lock on a file, it must be downloading
            # somewhere else, so we just remove it from the list of files to download
            # right now and then wait for it to appear later.
            lock_list = []
            source_idxes = []
            source_sub_paths = []
            source_local_paths = []
            for idx, sub_path, local_path in zip(orig_source_idxes,
                                                 orig_source_sub_paths,
                                                 orig_source_local_paths):
                lock_path = self._lock_path(local_path)
                # We don't actually want to wait for a lock to clear, we just want
                # to know if someone else is downloading the file right now
                lock = filelock.FileLock(lock_path, timeout=0)
                try:
                    lock.acquire()
                except filelock._error.Timeout:
                    self._log_debug(f'    Failed to lock: {sub_path}')
                    wait_to_appear.append((idx, f'{source_pfx}{sub_path}', local_path,
                                           lock_path))
                    continue
                lock_list.append((lock_path, lock))
                source_idxes.append(idx)
                source_sub_paths.append(sub_path)
                source_local_paths.append(local_path)

            # Now we can actually download the files that we locked
            rets = source.retrieve_multi(source_sub_paths, source_local_paths,
                                         nthreads=nthreads)
            assert len(source_sub_paths) == len(rets)
            for ret, sub_path in zip(rets, source_sub_paths):
                if isinstance(ret, Exception):
                    self._log_debug(f'    Download failed: {sub_path} {ret}')
                    files_not_exist.append(f'{source_pfx}{sub_path}')
                else:
                    self._log_debug(f'    Successfully downloaded: {sub_path}')
                    self._download_counter += 1

            # Release all the locks
            for lock_path, lock in lock_list:
                # There is a potential race condition here in the case of a raised
                # exception, because after we release the lock but before we delete
                # it, someone else could notice the file isn't downloaded and lock
                # it for another download attempt, and then we would delete someone
                # else's lock (because on Linux locks are only advisory). However,
                # we have to do it in this order because otherwise it won't work on
                # Windows, where locks are not just advisory. However, the worst
                # that could happen is we end up attempting to download the file
                # twice.
                lock.release()
                lock_path.unlink(missing_ok=True)

            # Record the results
            for source_ret, source_idx in zip(rets, source_idxes):
                func_ret[source_idx] = source_ret

        # If wait_to_appear is not empty, then we failed to acquire at least one lock,
        # which means that another process was downloading the file. So now we just
        # sit here and wait for all of the missing files to magically show up, or for
        # us to time out. If the lock file disappears but the destination file isn't
        # present, that means the other process failed in its download.
        timed_out = False
        while wait_to_appear:
            new_wait_to_appear = []
            for idx, url, local_path, lock_path in wait_to_appear:
                if local_path.is_file():
                    func_ret[idx] = local_path
                    self._log_debug(f'  Downloaded elsewhere: {url}')
                    continue
                if not lock_path.is_file():
                    func_ret[idx] = FileNotFoundError(
                        f'Another process failed to download {url}')
                    self._log_debug(f'  Download elsewhere failed: {url}')
                    continue
                new_wait_to_appear.append((idx, url, local_path, lock_path))

            if not new_wait_to_appear:
                break

            wait_to_appear = new_wait_to_appear
            if time.time() - start_time > lock_timeout:
                exc = TimeoutError(
                    'Timeout while waiting for another process to finish downloading')
                self._log_debug(
                    '  Timeout while waiting for another process to finish downloading:')
                for idx, url, local_path, lock_path in wait_to_appear:
                    func_ret[idx] = exc
                    self._log_debug(f'    {url}')
                if exception_on_fail:
                    raise exc
                timed_out = True
                break
            time.sleep(1)  # Wait 1 second before trying again

        if files_not_exist or timed_out:
            self._log_debug('Multi-file retrieval completed with errors')
            if exception_on_fail and files_not_exist:
                exc_str = f"File(s) do not exist: {', '.join(files_not_exist)}"
                raise FileNotFoundError(exc_str)
        else:
            self._log_debug('Multi-file retrieval complete')

        return cast(list[Union[Path, Exception]], func_ret)

    def upload(self,
               url: StrOrPathOrSeqType,
               *,
               anonymous: Optional[bool] = None,
               nthreads: Optional[int] = None,
               exception_on_fail: bool = True,
               url_to_path: Optional[UrlToPathFuncOrSeqType] = None
               ) -> Path | Exception | list[Path | Exception]:
        """Upload file(s) from the file cache to the storage location(s).

        Parameters:
            url: The URL of the file, including any source prefix. If `url` is a list or
                tuple, the complete list of files is uploaded. This may be more efficient
                because files can be uploaded in parallel.
            anonymous: If True, access cloud resources without specifying credentials. If
                False, credentials must be initialized in the program's environment. If
                None, use the default setting for this :class:`FileCache` instance.
            nthreads: The maximum number of threads to use when doing multiple-file
                retrieval or upload. If None, use the default value for this
                :class:`FileCache` instance.
            exception_on_fail: If True, if any file does not exist or upload fails an
                exception is raised. If False, the function returns normally and any
                failed upload is marked with the Exception that caused the failure in
                place of the returned path.
            url_to_path: The function (or list of functions) that is used to translate
                URLs into local paths. By default, :class:`FileCache` uses a directory
                hierarchy consisting of ``<cache_dir>/<cache_name>/<source>/<path>``,
                where ``source`` is the URL prefix converted to a filesystem-friendly
                format (e.g. ``gs://bucket`` is converted to ``gs_bucket``). A
                user-specified translator function takes five arguments::

                    func(scheme: str, remote: str, path: str, cache_dir: Path,
                         cache_subdir: str) -> str | Path

                where `scheme` is the URL scheme (like ``"gs"`` or ``"file"``), `remote`
                is the name of the bucket or webserver or the empty string for a local
                file, `path` is the rest of the URL, `cache_dir` is the top-level
                directory of the cache (``<cache_dir>/<cache_name>``), and `cache_subdir`
                is the subdirectory specific to this scheme and remote. If the translator
                wants to override the default translation, it can return a Path.
                Otherwise, it returns None. If the returned Path is relative, if will be
                appended to `cache_dir`; if it is absolute, it will be used directly (be
                very careful with this, as it has the ability to access files outside of
                the cache directory). If more than one translator is specified, they are
                called in order until one returns a Path, or it falls through to the
                default.

                If this parameter is specified, it replaces the default translators for
                this :class:`FileCache` instance. If this parameter is omitted, the
                default translators are used.

        Returns:
            The Path of the filename in the cache directory (or the original absolute path
            if local). If `url` was a list or tuple of paths, then instead return a list
            of Paths of the filenames in the temporary directory (or the original full
            path if local). If `exception_on_fail` is False, any Path may be an Exception
            if that file does not exist or the upload failed.

        Raises:
            FileNotFoundError: If a file to upload does not exist or the upload failed,
                and exception_on_fail is True.
        """

        nthreads = self._validate_nthreads(nthreads)

        if isinstance(url, (list, tuple)):
            sources = []
            sub_paths = []
            local_paths = []
            for one_url in url:
                source, sub_path, local_path = self._get_source_and_paths(one_url,
                                                                          anonymous,
                                                                          url_to_path)
                sources.append(source)
                sub_paths.append(sub_path)
                local_paths.append(local_path)
            return self._upload_multi(sources, sub_paths, local_paths, nthreads,
                                      exception_on_fail)

        url = str(url)
        source, sub_path, local_path = self._get_source_and_paths(url, anonymous,
                                                                  url_to_path)

        if source.primary_scheme() == 'file':
            self._log_debug(f'Uploading {local_path} (local file)')
        else:
            self._log_debug(f'Uploading {local_path} to {source._src_prefix_}{sub_path}')

        try:
            ret = source.upload(sub_path, local_path)
        except Exception as e:
            if exception_on_fail:
                raise e
            else:
                return e

        self._upload_counter += 1

        return ret

    def _upload_multi(self,
                      sources: list[FileCacheSource],
                      sub_paths: list[str],
                      local_paths: list[Path],
                      nthreads: int,
                      exception_on_fail: bool) -> list[Path | Exception]:
        """Upload multiple files to storage locations."""

        func_ret: list[Path | BaseException | None] = [None] * len(sources)

        files_not_exist = []
        files_failed = []

        source_dict: dict[str, list[tuple[int, FileCacheSource, str, Path]]] = {}

        # First find all the files that are either local or that we have already cached.
        # For other files, create a list of just the files we need to upload and
        # organize them by source; we use the source prefix to distinguish among them.
        self._log_debug('Performing multi-file upload of:')
        for idx, (source, sub_path, local_path) in enumerate(zip(sources,
                                                                 sub_paths, local_paths)):
            pfx = source._src_prefix_
            if source.primary_scheme() == 'file':
                try:
                    func_ret[idx] = source.upload(sub_path, local_path)
                    self._log_debug(f'  Local file     {pfx}{sub_path}')
                except FileNotFoundError as e:
                    self._log_debug(f'  LOCAL FILE DOES NOT EXIST {pfx}{sub_path}')
                    files_not_exist.append(sub_path)
                    func_ret[idx] = e
                continue
            if not Path(local_path).is_file():
                self._log_debug(f'  LOCAL FILE DOES NOT EXIST {pfx}{sub_path}')
                files_not_exist.append(sub_path)
                func_ret[idx] = FileNotFoundError(
                    f'File does not exist: {pfx}{sub_path}')
                continue
            assert '://' in pfx
            if pfx not in source_dict:
                source_dict[pfx] = []
            source_dict[pfx].append((idx, source, sub_path, local_path))

        # Now go through the sources, package up the paths to upload, and upload
        # them all at once
        for source_pfx in source_dict:
            source = source_dict[source_pfx][0][1]  # All the same
            source_idxes, _, source_sub_paths, source_local_paths = list(
                zip(*source_dict[source_pfx]))
            self._log_debug(
                f'Performing multi-file upload for prefix {source_pfx}:')
            for sub_path in source_sub_paths:
                self._log_debug(f'  {sub_path}')
            rets = source.upload_multi(source_sub_paths, source_local_paths,
                                       nthreads=nthreads)
            assert len(source_idxes) == len(rets)
            for ret, local_path in zip(rets, source_local_paths):
                if isinstance(ret, Exception):
                    self._log_debug(f'    Upload failed: {sub_path} {ret}')
                    files_failed.append(str(local_path))
                else:
                    self._upload_counter += 1

            for source_ret, source_idx in zip(rets, source_idxes):
                func_ret[source_idx] = source_ret

        if exception_on_fail:
            exc_str = ''
            if files_not_exist:
                exc_str += f"File(s) do not exist: {', '.join(files_not_exist)}"
            if files_failed:
                if exc_str:
                    exc_str += ' AND '
                exc_str += f"Failed to upload file(s): {', '.join(files_failed)}"
            if exc_str:
                raise FileNotFoundError(exc_str)

        return cast(list[Union[Path, Exception]], func_ret)

    @contextlib.contextmanager
    def open(self,
             url: str | Path,
             mode: str = 'r',
             *args: Any,
             anonymous: Optional[bool] = None,
             lock_timeout: Optional[int] = None,
             url_to_path: Optional[UrlToPathFuncOrSeqType] = None,
             **kwargs: Any) -> Iterator[IO[Any]]:
        """Retrieve+open or open+upload a file as a context manager.

        If `mode` is a read mode (like ``'r'`` or ``'rb'``) then the file will be first
        retrieved by calling :meth:`retrieve` and then opened. If the `mode` is a write
        mode (like ``'w'`` or ``'wb'``) then the file will be first opened for write, and
        when this context manager is exited the file will be uploaded.

        Parameters:
            url: The filename to open.
            mode: The mode string as you would specify to Python's `open()` function.
            **args: Any additional arguments are passed to the Python ``open()`` function.
            anonymous: If True, access cloud resources without specifying credentials. If
                False, credentials must be initialized in the program's environment. If
                None, use the default setting for this :class:`FileCache` instance.
            lock_timeout: How long to wait, in seconds, if another process is marked as
                retrieving the file before raising an exception. 0 means to not wait at
                all. A negative value means to never time out. If None, use the default
                value for this :class:`FileCache` instance.
            url_to_path: The function (or list of functions) that is used to translate
                URLs into local paths. By default, :class:`FileCache` uses a directory
                hierarchy consisting of ``<cache_dir>/<cache_name>/<source>/<path>``,
                where ``source`` is the URL prefix converted to a filesystem-friendly
                format (e.g. ``gs://bucket`` is converted to ``gs_bucket``). A
                user-specified translator function takes five arguments::

                    func(scheme: str, remote: str, path: str, cache_dir: Path,
                         cache_subdir: str) -> str | Path

                where `scheme` is the URL scheme (like ``"gs"`` or ``"file"``), `remote`
                is the name of the bucket or webserver or the empty string for a local
                file, `path` is the rest of the URL, `cache_dir` is the top-level
                directory of the cache (``<cache_dir>/<cache_name>``), and `cache_subdir`
                is the subdirectory specific to this scheme and remote. If the translator
                wants to override the default translation, it can return a Path.
                Otherwise, it returns None. If the returned Path is relative, if will be
                appended to `cache_dir`; if it is absolute, it will be used directly (be
                very careful with this, as it has the ability to access files outside of
                the cache directory). If more than one translator is specified, they are
                called in order until one returns a Path, or it falls through to the
                default.

                If this parameter is specified, it replaces the default translators for
                this :class:`FileCache` instance. If this parameter is omitted, the
                default translators are used.
            **kwargs: Any additional arguments are passed to the Python ``open()``
                function.

        Returns:
            The same object as would be returned by the normal `open()` function.
        """

        if mode[0] == 'r':
            local_path = cast(Path, self.retrieve(url, anonymous=anonymous,
                                                  lock_timeout=lock_timeout,
                                                  url_to_path=url_to_path))
            with open(local_path, mode, *args, **kwargs) as fp:
                yield fp
        else:  # 'w', 'x', 'a'
            local_path = cast(Path, self.get_local_path(url, anonymous=anonymous,
                                                        url_to_path=url_to_path))
            with open(local_path, mode, *args, **kwargs) as fp:
                yield fp
            self.upload(url, anonymous=anonymous, url_to_path=url_to_path)

    def iterdir(self,
                url: str | Path,
                *,
                anonymous: Optional[bool] = None,
                ) -> Iterator[str]:
        """Enumerate the files and sub-directories in a directory.

        This function always accesses a remote location (ignoring the local cache),
        if appropriate, because there is no way to know if the local cache contains
        all of the files and sub-directories present in the remote.

        Parameters:
            url: The URL of the directory, including any source prefix.
            anonymous: If specified, override the default setting for anonymous access.
                If True, access cloud resources without specifying credentials. If False,
                credentials must be initialized in the program's environment.

        Yields:
            All files and sub-directories in the directory given by the url, in no
            particular order. Special directories ``.`` and ``..`` are ignored.
        """

        self._log_debug(f'Iterating directory contents: {url}')

        source, sub_path, _ = self._get_source_and_paths(url, anonymous, None)

        for obj_name, _ in source.iterdir_type(sub_path):
            yield obj_name

    def iterdir_type(self,
                     url: str | Path,
                     *,
                     anonymous: Optional[bool] = None,
                     ) -> Iterator[tuple[str, bool]]:
        """Enumerate the files and sub-dirs in a directory indicating which is a dir.

        This function always accesses a remote location (ignoring the local cache),
        if appropriate, because there is no way to know if the local cache contains
        all of the files and sub-directories present in the remote.

        Parameters:
            url: The URL of the directory, including any source prefix.
            anonymous: If specified, override the default setting for anonymous access.
                If True, access cloud resources without specifying credentials. If False,
                credentials must be initialized in the program's environment.

        Yields:
            All files and sub-directories in the directory given by the url, in no
            particular order. Special directories ``.`` and ``..`` are ignored. The bool
            is True if the returned name is a directory, False if it is a file.
        """

        self._log_debug(f'Iterating directory contents: {url}')

        source, sub_path, _ = self._get_source_and_paths(url, anonymous, None)

        for obj_name, is_dir in source.iterdir_type(sub_path):
            yield obj_name, is_dir

    def unlink(self,
               url: StrOrPathOrSeqType,
               *,
               missing_ok: bool = False,
               anonymous: Optional[bool] = None,
               nthreads: Optional[int] = None,
               exception_on_fail: bool = True,
               url_to_path: Optional[UrlToPathFuncOrSeqType] = None
               ) -> str | Exception | list[str | Exception]:
        """Remove a file, including any locally cached copy.

        Parameters:
            url: The URL of the file, including any source prefix. If `url` is a list or
                tuple, all URLs are unlinked.
            missing_ok: True if it is OK to unlink a file that doesn't exist; False to
                raise a FileNotFoundError in this case.
            anonymous: If specified, override the default setting for anonymous access.
                If True, access cloud resources without specifying credentials. If False,
                credentials must be initialized in the program's environment.
            nthreads: The maximum number of threads to use when doing multiple-file
                retrieval or upload. If None, use the default value for this
                :class:`FileCache` instance.
            exception_on_fail: If True, if any file does not exist or upload fails an
                exception is raised. If False, the function returns normally and any
                failed upload is marked with the Exception that caused the failure in
                place of the returned path.
            url_to_path: The function (or list of functions) that is used to translate
                URLs into local paths. By default, :class:`FileCache` uses a directory
                hierarchy consisting of ``<cache_dir>/<cache_name>/<source>/<path>``,
                where ``source`` is the URL prefix converted to a filesystem-friendly
                format (e.g. ``gs://bucket`` is converted to ``gs_bucket``). A
                user-specified translator function takes five arguments::

                    func(scheme: str, remote: str, path: str, cache_dir: Path,
                         cache_subdir: str) -> str | Path

                where `scheme` is the URL scheme (like ``"gs"`` or ``"file"``), `remote`
                is the name of the bucket or webserver or the empty string for a local
                file, `path` is the rest of the URL, `cache_dir` is the top-level
                directory of the cache (``<cache_dir>/<cache_name>``), and `cache_subdir`
                is the subdirectory specific to this scheme and remote. If the translator
                wants to override the default translation, it can return a Path.
                Otherwise, it returns None. If the returned Path is relative, if will be
                appended to `cache_dir`; if it is absolute, it will be used directly (be
                very careful with this, as it has the ability to access files outside of
                the cache directory). If more than one translator is specified, they are
                called in order until one returns a Path, or it falls through to the
                default.

                If this parameter is specified, it replaces the default translators for
                this :class:`FileCache` instance. If this parameter is omitted, the
                default translators are used.

        Returns:
            The Path of the filename in the cache directory (or the original absolute path
            if local). If `url` was a list or tuple of paths, then instead return a list
            of Paths of the filenames in the temporary directory (or the original full
            path if local). If `exception_on_fail` is False, any Path may be an Exception
            if that file does not exist and missing_ok is True.

        Notes:
            If a URL points to a remote location, the locally cached version (if any) is
            only removed if the unlink of the remote location succeeded.

        Raises:
            FileNotFoundError: If a file to unlink does not exist or the unlink failed,
                and exception_on_fail is True.
        """

        nthreads = self._validate_nthreads(nthreads)

        if isinstance(url, (list, tuple)):
            sources = []
            sub_paths = []
            local_paths = []
            url2 = [str(x) for x in url]
            for one_url in url2:
                source, sub_path, local_path = self._get_source_and_paths(one_url,
                                                                          anonymous,
                                                                          url_to_path)
                sources.append(source)
                sub_paths.append(sub_path)
                local_paths.append(local_path)
            return self._unlink_multi(url2, sources, sub_paths, local_paths,
                                      missing_ok, nthreads, exception_on_fail)

        url3 = str(url)
        source, sub_path, local_path = self._get_source_and_paths(url3, anonymous,
                                                                  url_to_path)

        self._log_debug(f'Unlinking {url3}')
        try:
            source.unlink(sub_path, missing_ok=missing_ok)
            local_path.unlink(missing_ok=True)  # Don't care if it's cached or not
        except Exception as e:
            if exception_on_fail:
                raise e
            else:
                return e

        return sub_path

    def _unlink_multi(self,
                      urls: list[str],
                      sources: list[FileCacheSource],
                      sub_paths: list[str],
                      local_paths: list[Path],
                      missing_ok: bool,
                      nthreads: int,
                      exception_on_fail: bool) -> list[str | Exception]:
        """Unlink multiple files."""

        func_ret: list[str | BaseException | None] = [None] * len(sources)

        files_not_exist = []

        source_dict: dict[str, list[tuple[int, FileCacheSource, str, Path]]] = {}

        # First find all the files that are either local or that we have already cached.
        # For other files, create a list of just the files we need to retrieve and
        # organize them by source; we use the source prefix to distinguish among them.
        self._log_debug('Performing multi-file unlink of:')
        for idx, (url, source, sub_path, local_path) in enumerate(zip(urls, sources,
                                                                      sub_paths,
                                                                      local_paths)):
            pfx = source._src_prefix_
            if source.primary_scheme() == 'file':
                try:
                    source.unlink(sub_path, missing_ok=missing_ok)
                    func_ret[idx] = str(url)
                    self._log_debug(f'  Local file     {pfx}{sub_path}')
                except FileNotFoundError as e:
                    self._log_debug(f'  LOCAL FILE DOES NOT EXIST {pfx}{sub_path}')
                    files_not_exist.append(sub_path)
                    func_ret[idx] = e
                continue
            assert '://' in pfx
            if pfx not in source_dict:
                source_dict[pfx] = []
            source_dict[pfx].append((idx, source, sub_path, local_path))

        # Now go through the sources, package up the paths to unlink, and unlink
        # them all at once
        for source_pfx in source_dict:
            source = source_dict[source_pfx][0][1]  # All the same
            source_idxes, _, source_sub_paths, source_local_paths = list(
                zip(*source_dict[source_pfx]))
            self._log_debug(
                f'Performing multi-file unlink for prefix {source_pfx}:')
            for sub_path in source_sub_paths:
                self._log_debug(f'  {sub_path}')
            rets = source.unlink_multi(source_sub_paths, missing_ok=missing_ok,
                                       nthreads=nthreads)
            assert len(source_idxes) == len(rets)
            for ret2, local_path, url in zip(rets, source_local_paths, urls):
                if isinstance(ret2, Exception):
                    self._log_debug(f'    Unlink failed: {url} {ret2}')
                    files_not_exist.append(str(url))
                else:
                    local_path.unlink(missing_ok=True)  # Remove from cache

            for source_ret, source_idx in zip(rets, source_idxes):
                if isinstance(source_ret, Exception):
                    func_ret[source_idx] = source_ret
                else:
                    # We want to return the entire URL, not just the sub_path,
                    # so we have to add the prefix back on
                    func_ret[source_idx] = str(urls[source_idx])

        if exception_on_fail and not missing_ok:
            exc_str = ''
            if files_not_exist:
                exc_str += f"File(s) do not exist: {', '.join(files_not_exist)}"
            if exc_str:
                raise FileNotFoundError(exc_str)

        return cast(list[Union[str, Exception]], func_ret)

    def new_path(self,
                 path: str | Path | FCPath,
                 *,
                 anonymous: Optional[bool] = None,
                 lock_timeout: Optional[int] = None,
                 nthreads: Optional[int] = None,
                 url_to_path: Optional[UrlToPathFuncOrSeqType] = None
                 ) -> FCPath:
        """Create a new FCPath with the given prefix.

        Parameters:
            path: The path.
            anonymous: If True, access cloud resources without specifying credentials. If
                False, credentials must be initialized in the program's environment. If
                None, use the default setting for this :class:`FileCache` instance.
            lock_timeout: How long to wait, in seconds, if another process is marked as
                retrieving the file before raising an exception. 0 means to not wait at
                all. A negative value means to never time out. None means to use the
                default value for this :class:`FileCache` instance.
            nthreads: The maximum number of threads to use when doing multiple-file
                retrieval or upload. If None, use the default value for this
                :class:`FileCache` instance.
            url_to_path: The function (or list of functions) that is used to translate
                URLs into local paths. By default, :class:`FileCache` uses a directory
                hierarchy consisting of ``<cache_dir>/<cache_name>/<source>/<path>``,
                where ``source`` is the URL prefix converted to a filesystem-friendly
                format (e.g. ``gs://bucket`` is converted to ``gs_bucket``). A
                user-specified translator function takes five arguments::

                    func(scheme: str, remote: str, path: str, cache_dir: Path,
                         cache_subdir: str) -> str | Path

                where `scheme` is the URL scheme (like ``"gs"`` or ``"file"``), `remote`
                is the name of the bucket or webserver or the empty string for a local
                file, `path` is the rest of the URL, `cache_dir` is the top-level
                directory of the cache (``<cache_dir>/<cache_name>``), and `cache_subdir`
                is the subdirectory specific to this scheme and remote. If the translator
                wants to override the default translation, it can return a Path.
                Otherwise, it returns None. If the returned Path is relative, if will be
                appended to `cache_dir`; if it is absolute, it will be used directly (be
                very careful with this, as it has the ability to access files outside of
                the cache directory). If more than one translator is specified, they are
                called in order until one returns a Path, or it falls through to the
                default.

                If None, use the default translators for this :class:`FileCache` instance.
        """

        if isinstance(path, (Path, FCPath)):
            path = str(path)
        if not isinstance(path, str):
            raise TypeError('path is not a str or Path or FCPath')
        path = path.replace('\\', '/').rstrip('/')
        if anonymous is None:
            anonymous = self.anonymous
        if lock_timeout is None:
            lock_timeout = self.lock_timeout
        nthreads = self._validate_nthreads(nthreads)
        return FCPath(path,
                      filecache=self,
                      anonymous=anonymous,
                      lock_timeout=lock_timeout,
                      nthreads=nthreads,
                      url_to_path=url_to_path)

    def _maybe_delete_cache(self) -> None:
        """Delete this cache if delete_on_exit is True."""

        if self._delete_on_exit:
            self.delete_cache()

    def delete_cache(self) -> None:
        """Delete all files stored in the cache including the cache directory.

        Notes:
            It is permissible to call :meth:`delete_cache` more than once. It is also
            permissible to call :meth:`delete_cache`, then perform more operations that
            place files in the cache, then call :meth:`delete_cache` again.
        """

        self._log_debug(f'Deleting cache {self._cache_dir}')

        # Verify this is really a cache directory before walking it and deleting
        # every file. We are just being paranoid to make sure this never does a
        # "rm -rf" on a real directory like "/".
        if not Path(self._cache_dir).name.startswith(self._FILE_CACHE_PREFIX):
            raise ValueError(
                f'Cache directory does not start with {self._FILE_CACHE_PREFIX}')

        # Delete all of the files and subdirectories we left behind, including the
        # file cache directory itself.
        # We would like to use Path.walk() but that was only added in Python 3.12. We
        # allow remove and rmdir to fail with FileNotFoundError because we could have two
        # programs deleting a shared cache at the same time fighting each other, or
        # someone could have asked for the local path to a file and then never written
        # anything there.
        for root, dirs, files in os.walk(self._cache_dir, topdown=False):
            for name in files:
                if name.startswith(self._LOCK_PREFIX):
                    self._log_error(
                        f'Deleting cache that has an active lock file: {root}/{name}')
                self._log_debug(f'  Removing file {root}/{name}')
                try:
                    os.remove(os.path.join(root, name))
                except FileNotFoundError:  # pragma: no cover - race condition only
                    pass
            for name in dirs:
                self._log_debug(f'  Removing dir {root}/{name}')
                try:
                    os.rmdir(os.path.join(root, name))
                except FileNotFoundError:  # pragma: no cover - race condition only
                    pass

        self._log_debug(f'  Removing dir {self._cache_dir}')
        try:
            os.rmdir(self._cache_dir)
        except FileNotFoundError:  # pragma: no cover - race condition only
            pass

    def __enter__(self) -> Self:
        """Enter the context manager for creating a FileCache."""
        return self

    def __exit__(self,
                 exc_type: Optional[Type[BaseException]],
                 exc_inst: Optional[BaseException],
                 exc_tb: Optional[TracebackType]) -> Literal[False]:
        """Exit the context manage for a FileCache, executing any cache deletion."""
        self._maybe_delete_cache()
        # Since the cache is deleted, no need to delete it again later
        atexit.unregister(self._maybe_delete_cache)

        return False
