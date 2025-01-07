##########################################################################################
# filecache/file_cache_path.py
##########################################################################################

from __future__ import annotations

import contextlib
import functools
import os
from pathlib import Path
import re
import sys
from typing import (cast,
                    Any,
                    Callable,
                    Generator,
                    Iterator,
                    IO,
                    Optional,
                    TYPE_CHECKING)

if TYPE_CHECKING:  # pragma: no cover
    from .file_cache import FileCache  # Circular import

from .file_cache_types import (StrOrPathOrSeqType,
                               UrlToPathFuncOrSeqType)


# This FileCache is used when an FCPath is created without specifying a particular
# FileCache and the FCPath is actually used to perform an operation that needs that
# FileCache.
_DEFAULT_FILECACHE: Optional[FileCache] = None


class FCPath:
    """Rewrite of the Python pathlib.Path class that supports URLs and FileCache.

    This class provides a simpler way to abstract away remote access in a FileCache by
    emulating the Python pathlib.Path class. At the same time, it can collect common
    parameters (`anonymous`, `lock_timeout`, `nthreads`) into a single location so that
    they do not have to be specified on every method call.
    """

    _filecache: Optional["FileCache"]
    _anonymous: Optional[bool]
    _lock_timeout: Optional[int]
    _nthreads: Optional[int]
    _url_to_path: Optional[UrlToPathFuncOrSeqType]

    def __init__(self,
                 *paths: str | Path | FCPath | None,
                 filecache: Optional["FileCache"] = None,
                 anonymous: Optional[bool] = None,
                 lock_timeout: Optional[int] = None,
                 nthreads: Optional[int] = None,
                 url_to_path: Optional[UrlToPathFuncOrSeqType] = None,
                 copy_from: Optional[FCPath] = None
                 ):
        """Initialization for the FCPath class.

        Parameters:
            paths: The path(s). These may be absolute or relative paths. They are joined
                together to form a final path. File operations can only be performed on
                absolute paths.
            file_cache: The :class:`FileCache` in which to store files retrieved from this
                path. If not specified, the default global :class:`FileCache` will be
                used.
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

                If None, use the default translators for the associated :class:`FileCache`
                instance.
            copy_from: An FCPath instance to copy internal parameters (`file_cache`,
                `anonymous`, `lock_timeout`, `nthreads`, and `url_to_path`) from. If
                specified, any values for these parameters in this constructor are
                ignored. Used internally and should not be used by external programmers.
        """

        self._path = self._join(*paths)

        if copy_from is None and len(paths) > 0 and isinstance(paths[0], FCPath):
            copy_from = paths[0]

        if copy_from is not None:
            self._filecache = copy_from._filecache
            self._anonymous = copy_from._anonymous
            self._lock_timeout = copy_from._lock_timeout
            self._nthreads = copy_from._nthreads
            self._url_to_path = copy_from._url_to_path
        else:
            self._filecache = filecache
            self._anonymous = anonymous
            self._lock_timeout = lock_timeout
            if nthreads is not None and (not isinstance(nthreads, int) or nthreads <= 0):
                raise ValueError(f'nthreads must be a positive integer, got {nthreads}')
            self._nthreads = nthreads
            self._url_to_path = url_to_path

        self._pathlib: Optional[Path] = None
        self._upload_counter = 0
        self._download_counter = 0

    def _validate_nthreads(self,
                           nthreads: Optional[int]) -> int | None:
        if nthreads is not None and (not isinstance(nthreads, int) or nthreads <= 0):
            raise ValueError(f'nthreads must be a positive integer, got {nthreads}')
        if nthreads is None:
            nthreads = self._nthreads
        return nthreads

    @staticmethod
    def _split_parts(path: str | Path) -> tuple[str, str, str]:
        """Split a path into drive, root, and remainder of path."""

        from .file_cache import FileCache  # Circular import avoidance

        path = str(path).replace('\\', '/')
        drive = ''
        root = ''
        if len(path) >= 2 and path[0].isalpha() and path[1] == ':':
            # Windows C:
            drive = path[0:2].upper()
            path = path[2:]

        elif path.startswith('//'):
            # UNC //host/share
            path2 = path[2:]

            try:
                idx = path2.index('/')
            except ValueError:
                raise ValueError(f'UNC path does not include share name {path!r}')
            if idx == 0:
                raise ValueError(f'UNC path does not include hostname {path!r}')

            try:
                idx2 = path2[idx+1:].index('/')
            except ValueError:
                # It's just a share name like //host/share
                drive = path
                path = ''
            else:
                # It's a share plus path like //host/share/path
                # We include the leading /
                if idx2 == 0:
                    raise ValueError(f'UNC path does not include share {path!r}')
                drive = path[:idx+idx2+3]
                path = path[idx+idx2+3:]

        elif path.startswith(FileCache.registered_scheme_prefixes()):
            # Cloud
            idx = path.index('://')
            path2 = path[idx+3:]
            if path2 == '':
                raise ValueError(f'URI does not include remote name {path!r}')
            try:
                idx2 = path2.index('/')
            except ValueError:
                # It's just a remote name like gs://bucket; we still make it absolute
                drive = path
                path = '/'
            else:
                # It's a remote name plus path like gs://bucket/path
                # We include the leading /
                if idx2 == 0 and not path.startswith('file://'):
                    raise ValueError(f'URI does not include remote name {path!r}')
                drive = path[:idx+idx2+3]
                path = path[idx+idx2+3:]

        if path.startswith('/'):
            root = '/'

        if path != root:
            path = path.rstrip('/')

        return drive, root, path

    @staticmethod
    def _split(path: str) -> tuple[str, str]:
        """Split a path into head,tail similarly to os.path.split."""

        if path == '':
            return '', ''
        drive, root, subpath = FCPath._split_parts(path)
        if '/' not in subpath:
            return drive, subpath
        if root == '/' and subpath == root:
            return drive, ''
        idx = subpath.rindex('/')
        return drive + subpath[:idx].rstrip('/'), subpath[idx+1:]

    @staticmethod
    def _is_absolute(path: str) -> bool:
        """Check if a path string is an absolute path."""

        return FCPath._split_parts(path)[1] == '/'

    @staticmethod
    def _join(*paths: str | Path | FCPath | None) -> str:
        """Join multiple strings together into a single path.

        Any time an absolute path is found in the path list, the new path starts
        over.
        """

        ret = ''
        for path in paths:
            if path is None:
                continue
            if not isinstance(path, (str, Path, FCPath)):
                raise TypeError(f'path {path!r} is not a str, Path, or FCPath')
            path = str(path)
            if not path:
                continue
            drive, root, subpath = FCPath._split_parts(path)
            if root == '/':  # Absolute path - start over
                ret = ''
            if ret == '':
                ret = drive
            elif ret != '' and ret[-1] != '/' and subpath != '' and subpath[0] != '/':
                ret += '/'
            if not (subpath == '/' and '://' in drive):
                ret = ret + subpath

        return ret

    @staticmethod
    def _filename(path: str) -> str:
        """Return just the filename part of a path."""

        _, _, subpath = FCPath._split_parts(path)
        if '/' not in subpath:
            return subpath
        return subpath[subpath.rfind('/') + 1:]

    @property
    def _stack(self) -> tuple[str, list[str]]:
        """Split the path into a 2-tuple (anchor, parts).

        *anchor* is the uppermost parent of the path (equivalent to path.parents[-1]), and
        *parts* is a reversed list of parts following the anchor.
        """

        path = self._path
        parent, name = FCPath._split(path)
        names = []
        while path != parent:
            names.append(name)
            path = parent
            parent, name = FCPath._split(path)
        return path, names

    def __str__(self) -> str:
        return self._path

    @property
    def path(self) -> str:
        """Return this path as a string."""

        return self._path

    def as_pathlib(self) -> Path:
        """Return this path as a pathlib Path object."""

        if self._pathlib is None:
            if not self.is_local():
                raise ValueError(f'Cannot convert {self} to pathlib.Path')
            self._pathlib = Path(self._path)
        return self._pathlib

    def as_posix(self) -> str:
        """Return this FCPath as a POSIX path. This is a str using only forward slashes.

        Notes:
            Because URLs are not really supported in POSIX format, we just return the
            URL as-is, including any scheme and remote.

        Returns:
            This path as a POSIX path.
        """

        return self._path

    @property
    def drive(self) -> str:
        """The drive associated with this FCPath.

        Notes:
            Examples:

                For a Windows path: '' or 'C:'

                For a UNC share: '//host/share'

                For a cloud resource: 'gs://bucket'
        """

        return self._split_parts(self._path)[0]

    @property
    def root(self) -> str:
        """The root of this FCPath; '/' if absolute, '' otherwise."""

        return self._split_parts(self._path)[1]

    @property
    def anchor(self) -> str:
        """The anchor of this FCPath, which is drive + root."""

        return ''.join(self._split_parts(self._path)[0:2])

    @property
    def suffix(self) -> str:
        """The final component's last suffix, if any, including the leading period."""

        name = FCPath._filename(self._path)
        i = name.rfind('.')
        if 0 < i < len(name) - 1:
            return name[i:]
        else:
            return ''

    @property
    def suffixes(self) -> list[str]:
        """A list of the final component's suffixes, including the leading periods."""

        name = FCPath._filename(self._path)
        if name.endswith('.'):
            return []
        name = name.lstrip('.')
        return ['.' + suffix for suffix in name.split('.')[1:]]

    @property
    def stem(self) -> str:
        """The final path component, minus its last suffix."""

        name = FCPath._filename(self._path)
        i = name.rfind('.')
        if 0 < i < len(name) - 1:
            return name[:i]
        else:
            return name

    def with_name(self, name: str) -> FCPath:
        """Return a new FCPath with the filename changed.

        Parameters:
            name: The new filename to replace the final path component with.

        Returns:
            A new FCPath with the final component replaced. The new FCPath will have
            the same parameters (`filecache`, etc.) as the source FCPath.
        """

        drive, root, subpath = FCPath._split_parts(self._path)
        drive2, root2, subpath2 = FCPath._split_parts(name)
        if drive2 != '' or root2 != '' or subpath2 == '' or '/' in subpath2:
            raise ValueError(f"Invalid name {name!r}")

        if '/' not in subpath:
            return FCPath(drive + name, copy_from=self)

        return FCPath(drive + subpath[:subpath.rfind('/')+1:] + name,
                      copy_from=self)

    def with_stem(self, stem: str) -> FCPath:
        """Return a new FCPath with the stem (the filename minus the suffix) changed.

        Parameters:
            stem: The new stem.

        Returns:
            A new FCPath with the final component's stem replaced. The new FCPath will
            have the same parameters (`filecache`, etc.) as the source FCPath.
        """

        suffix = self.suffix
        if not suffix:
            return self.with_name(stem)
        elif not stem:
            # If the suffix is non-empty, we can't make the stem empty.
            raise ValueError(f"{self!r} has a non-empty suffix")
        else:
            return self.with_name(stem + suffix)

    def with_suffix(self, suffix: str) -> FCPath:
        """Return a new FCPath with the file suffix changed.

        If the path has no suffix, add the given suffix. If the given suffix is an empty
        string, remove the suffix from the path.

        Parameters:
            suffix: The new suffix to use.

        Returns:
            A new FCPath with the final component's suffix replaced. The new FCPath will
            have the same parameters (`filecache`, etc.) as the source FCPath.
        """

        stem = self.stem
        if not stem:
            # If the stem is empty, we can't make the suffix non-empty.
            raise ValueError(f"{self!r} has an empty name")
        elif suffix and not (suffix.startswith('.') and len(suffix) > 1):
            raise ValueError(f"Invalid suffix {suffix!r}")
        else:
            return self.with_name(stem + suffix)

    @property
    def parts(self) -> tuple[str, ...]:
        """An object providing sequence-like access to the components in the path."""

        anchor, parts = self._stack
        if anchor:
            parts.append(anchor)
        return tuple(reversed(parts))

    def joinpath(self,
                 *pathsegments: str | Path | FCPath | None) -> FCPath:
        """Combine this path with additional paths.

        Parameters:
            pathsegments: One or more additional paths to join with this path.

        Returns:
            A new FCPath that is a combination of this path and the additional paths. The
            new FCPath will have the same parameters (`filecache`, etc.) as the source
            FCPath.
        """

        return FCPath(self._path, *pathsegments, copy_from=self)

    def __truediv__(self,
                    other: str | Path | FCPath | None) -> FCPath:
        """Combine this path with an additional path.

        Parameters:
            other: The path to join with this path.

        Returns:
            A new FCPath that is a combination of this path and the other path. The new
            FCPath will have the same parameters (`filecache`, etc.) as the current
            FCPath.
        """

        return FCPath(self._path, other, copy_from=self)

    def __rtruediv__(self, other: str | Path | FCPath) -> FCPath:
        """Combine an additional path with this path.

        Parameters:
            other: The path to join with this path.

        Returns:
            A new FCPath that is a combination of the other path and this path. The new
            FCPath will have the same parameters (`filecache`, etc.) as the other path if
            the other path is an FCPath; otherwise it will have the same parameters as
            the current FCPath.
        """

        if isinstance(other, FCPath):  # pragma: no cover
            # This shouldn't be possible to hit because __truediv__ will catch it
            return FCPath(other, self._path, copy_from=other)
        else:
            return FCPath(other, self._path, copy_from=self)

    def __repr__(self) -> str:
        return f'FCPath({self._path!r})'

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, FCPath):
            return NotImplemented
        return self._path == other._path

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, FCPath):
            return NotImplemented
        return self._path < other._path

    def __le__(self, other: object) -> bool:
        if not isinstance(other, FCPath):
            return NotImplemented
        return self._path <= other._path

    def __gt__(self, other: object) -> bool:
        if not isinstance(other, FCPath):
            return NotImplemented
        return self._path > other._path

    def __ge__(self, other: object) -> bool:
        if not isinstance(other, FCPath):
            return NotImplemented
        return self._path >= other._path

    @property
    def name(self) -> str:
        """The final component of the path."""

        return FCPath._split(self._path)[1]

    @property
    def parent(self) -> FCPath:
        """The logical parent of the path.

        The new FCPath will have the same parameters (`filecache`, etc.) as the original
        path.
        """

        parent = FCPath._split(self._path)[0]
        if self._path != parent:
            return FCPath(parent, copy_from=self)
        return self

    @property
    def parents(self) -> tuple[FCPath, ...]:
        """A sequence of this path's logical parents."""

        path = self._path
        parent = FCPath._split(path)[0]
        parents = []
        while path != parent:
            parents.append(FCPath(parent, copy_from=self))
            path = parent
            parent = FCPath._split(path)[0]
        return tuple(parents)

    def is_absolute(self) -> bool:
        """True if the path is absolute."""

        return FCPath._is_absolute(self._path)

    def as_absolute(self) -> FCPath:
        """Return the absolute version of this possibly-relative path."""

        if FCPath._is_absolute(self._path):
            return self
        return FCPath(self.as_pathlib().expanduser().absolute().resolve(),
                      copy_from=self)

    def match(self,
              path_pattern: str | Path | FCPath) -> bool:
        """Return True if this path matches the given pattern.

        If the pattern is relative, matching is done from the right; otherwise, the entire
        path is matched. The recursive wildcard ``**`` is *not* supported by this method
        (it just acts like ``*``).

        See pathlib.Path.match for full documentation.
        """

        if not isinstance(path_pattern, FCPath):
            path_pattern = FCPath(path_pattern)
        path_parts = self.parts[::-1]
        pattern_parts = path_pattern.parts[::-1]
        if not pattern_parts:
            raise ValueError('empty pattern')
        if len(path_parts) < len(pattern_parts):
            return False
        if len(path_parts) > len(pattern_parts) and path_pattern.anchor:
            return False
        globber = _StringGlobber(self)
        for path_part, pattern_part in zip(path_parts, pattern_parts):
            match = globber.compile(pattern_part)
            if match(path_part) is None:
                return False
        return True

    def full_match(self,
                   pattern: str | Path | FCPath) -> bool:
        """Return True if this path matches the given glob-style pattern.

        The pattern is matched against the entire path.

        See pathlib.Path.full_match for full documentation.
        """

        if not isinstance(pattern, FCPath):
            pattern = FCPath(pattern)
        globber = _StringGlobber(self, recursive=True)
        match = globber.compile(str(pattern))
        return match(self._path) is not None

    @property
    def _filecache_to_use(self) -> "FileCache":
        from .file_cache import FileCache
        global _DEFAULT_FILECACHE
        if self._filecache is None:
            if _DEFAULT_FILECACHE is None:
                _DEFAULT_FILECACHE = FileCache()
            return _DEFAULT_FILECACHE
        return self._filecache

    def _make_paths_absolute(self,
                             sub_path: Optional[StrOrPathOrSeqType]) -> str | list[str]:
        if isinstance(sub_path, (list, tuple)):
            new_sub_paths: list[str] = []
            for p in sub_path:
                new_sub_path = FCPath._join(self._path, p)
                if not FCPath._is_absolute(new_sub_path):
                    new_sub_path = (FCPath(Path(new_sub_path)
                                           .expanduser().absolute().resolve())
                                    .as_posix())
                new_sub_paths.append(new_sub_path)
            return new_sub_paths
        new_sub_path = FCPath._join(self._path, sub_path)
        if not FCPath._is_absolute(new_sub_path):
            return (FCPath(Path(new_sub_path).expanduser().absolute().resolve())
                    .as_posix())
        return new_sub_path

    def get_local_path(self,
                       sub_path: Optional[StrOrPathOrSeqType] = None,
                       *,
                       create_parents: bool = True,
                       url_to_path: Optional[UrlToPathFuncOrSeqType] = None,
                       ) -> Path | list[Path]:
        """Return the local path for the given sub_path relative to this path.

        Parameters:
            sub_path: The path of the file relative to this path. If not specified, this
                path is used. If `sub_path` is a list or tuple, all paths are processed.
                If the resulting derived path is not absolute, it is assumed to be a
                relative local path and is converted to an absolute path by expanding
                usernames and resolving links.
            create_parents: If True, create all parent directories. This is useful when
                getting the local path of a file that will be uploaded.
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

                If None, use the default value given when this :class:`FCPath` was
                created.
        Returns:
            The Path (or list of Paths) of the filename in the temporary directory, or as
            specified by the `url_to_path` translators. The files do not have to exist
            because a Path could be used for writing a file to upload. To facilitate this,
            a side effect of this call (if `create_parents` is True) is that the complete
            parent directory structure will be created for each returned Path.
        """

        new_sub_path = self._make_paths_absolute(sub_path)

        return self._filecache_to_use.get_local_path(cast(StrOrPathOrSeqType,
                                                          new_sub_path),
                                                     anonymous=self._anonymous,
                                                     create_parents=create_parents,
                                                     url_to_path=url_to_path)

    def exists(self,
               sub_path: Optional[StrOrPathOrSeqType] = None,
               *,
               bypass_cache: bool = False,
               nthreads: Optional[int] = None,
               url_to_path: Optional[UrlToPathFuncOrSeqType] = None
               ) -> bool | list[bool]:
        """Check if a file exists without downloading it.

        Parameters:
            sub_path: The path of the file relative to this path. If not specified, this
                path is used. If the resulting derived path is not absolute, it is assumed
                to be a relative local path and is converted to an absolute path by
                expanding usernames and resolving links.
            bypass_cache: If False, check for the file first in the local cache, and if
                not found there then on the remote server. If True, only check on the
                remote server.
            nthreads: The maximum number of threads to use when doing multiple-file
                retrieval or upload. If None, use the default value given when this
                :class:`FCPath` was created.
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

                If None, use the default value given when this :class:`FCPath` was
                created.
        Returns:
            True if the file exists. Note that it is possible that a file could exist and
            still not be downloadable due to permissions. False if the file does not
            exist. This includes bad bucket or webserver names, lack of permission to
            examine a bucket's contents, etc.
        """

        nthreads = self._validate_nthreads(nthreads)

        new_sub_path = self._make_paths_absolute(sub_path)

        return self._filecache_to_use.exists(cast(StrOrPathOrSeqType,
                                                  new_sub_path),
                                             bypass_cache=bypass_cache,
                                             nthreads=nthreads,
                                             anonymous=self._anonymous,
                                             url_to_path=url_to_path)

    def retrieve(self,
                 sub_path: Optional[StrOrPathOrSeqType] = None,
                 *,
                 lock_timeout: Optional[int] = None,
                 nthreads: Optional[int] = None,
                 exception_on_fail: bool = True,
                 url_to_path: Optional[UrlToPathFuncOrSeqType] = None
                 ) -> Path | Exception | list[Path | Exception]:
        """Retrieve a file(s) from the given sub_path and store it in the file cache.

        Parameters:
            sub_path: The path of the file relative to this path. If not specified, this
                path is used. If `sub_path` is a list or tuple, the complete list of files
                is retrieved. Depending on the storage location, this may be more
                efficient because files can be downloaded in parallel. If the resulting
                derived path is not absolute, it is assumed to be a relative local path
                and is converted to an absolute path by expanding usernames and resolving
                links.
            nthreads: The maximum number of threads to use when doing multiple-file
                retrieval or upload. If None, use the default value given when this
                :class:`FCPath` was created.
            lock_timeout: How long to wait, in seconds, if another process is marked as
                retrieving the file before raising an exception. 0 means to not wait at
                all. A negative value means to never time out. None means to use the
                default value given when this :class:`FCPath` was created.
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

                If None, use the default value given when this :class:`FCPath` was
                created.
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

        old_download_counter = self._filecache_to_use.download_counter

        nthreads = self._validate_nthreads(nthreads)

        if lock_timeout is None:
            lock_timeout = self._lock_timeout

        new_sub_path = self._make_paths_absolute(sub_path)

        try:
            ret = self._filecache_to_use.retrieve(cast(StrOrPathOrSeqType,
                                                       new_sub_path),
                                                  anonymous=self._anonymous,
                                                  lock_timeout=lock_timeout,
                                                  nthreads=nthreads,
                                                  exception_on_fail=exception_on_fail,
                                                  url_to_path=url_to_path)
        finally:
            self._download_counter += (self._filecache_to_use.download_counter -
                                       old_download_counter)

        return ret

    def upload(self,
               sub_path: Optional[StrOrPathOrSeqType] = None,
               *,
               nthreads: Optional[int] = None,
               exception_on_fail: bool = True,
               url_to_path: Optional[UrlToPathFuncOrSeqType] = None
               ) -> Path | Exception | list[Path | Exception]:
        """Upload file(s) from the file cache to the storage location(s).

        Parameters:
            sub_path: The path of the file relative to this path. If not specified, this
                path is used. If `sub_path` is a list or tuple, the complete list of files
                is uploaded. This may be more efficient because files can be uploaded in
                parallel. If the resulting derived path is not absolute, it is assumed to
                be a relative local path and is converted to an absolute path by expanding
                usernames and resolving links.
            nthreads: The maximum number of threads to use when doing multiple-file
                retrieval or upload. If None, use the default value given when this
                :class:`FileCache` was created.
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

                If None, use the default value given when this :class:`FCPath` was
                created.
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

        old_upload_counter = self._filecache_to_use.upload_counter

        nthreads = self._validate_nthreads(nthreads)

        new_sub_path = self._make_paths_absolute(sub_path)

        try:
            ret = self._filecache_to_use.upload(cast(StrOrPathOrSeqType,
                                                     new_sub_path),
                                                anonymous=self._anonymous,
                                                nthreads=nthreads,
                                                exception_on_fail=exception_on_fail,
                                                url_to_path=url_to_path)
        finally:
            self._upload_counter += (self._filecache_to_use.upload_counter -
                                     old_upload_counter)

        return ret

    @contextlib.contextmanager
    def open(self,
             sub_path: Optional[str] = None,
             mode: str = 'r',
             *args: Any,
             url_to_path: Optional[UrlToPathFuncOrSeqType] = None,
             **kwargs: Any) -> Iterator[IO[Any]]:
        """Retrieve+open or open+upload a file as a context manager.

        If `mode` is a read mode (like ``'r'`` or ``'rb'``) then the file will be first
        retrieved by calling :meth:`retrieve` and then opened. If the `mode` is a write
        mode (like ``'w'`` or ``'wb'``) then the file will be first opened for write, and
        when this context manager is exited the file will be uploaded.

        Parameters:
            sub_path: The path of the file relative to this path. If not specified, this
                path is used.
            mode: The mode string as you would specify to Python's `open()` function.
            **args: Any additional arguments are passed to the Python ``open()`` function.
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

                If None, use the default value given when this :class:`FCPath` was
                created.
            **kwargs: Any additional arguments are passed to the Python ``open()``
                function.

        Returns:
            IO object: The same object as would be returned by the normal `open()`
            function.
        """

        if mode[0] == 'r':
            local_path = cast(Path, self.retrieve(sub_path, url_to_path=url_to_path))
            with open(local_path, mode, *args, **kwargs) as fp:
                yield fp
        else:  # 'w', 'x', 'a'
            local_path = cast(Path, self.get_local_path(sub_path,
                                                        url_to_path=url_to_path))
            with open(local_path, mode, *args, **kwargs) as fp:
                yield fp
            self.upload(sub_path, url_to_path=url_to_path)

    def unlink(self,
               sub_path: Optional[StrOrPathOrSeqType] = None,
               *,
               missing_ok: bool = False,
               nthreads: Optional[int] = None,
               exception_on_fail: bool = True,
               url_to_path: Optional[UrlToPathFuncOrSeqType] = None
               ) -> str | Exception | list[str | Exception]:
        """Remove this file or link.

        Parameters:
            sub_path: The path of the file relative to this path. If not specified, this
                path is used. If `sub_path` is a list or tuple, the complete list of files
                is retrieved. Depending on the storage location, this may be more
                efficient because files can be downloaded in parallel. If the resulting
                derived path is not absolute, it is assumed to be a relative local path
                and is converted to an absolute path by expanding usernames and resolving
                links.
            missing_ok: True to ignore attempting to unlink a file that doesn't exist.
            nthreads: The maximum number of threads to use when doing multiple-file
                retrieval or upload. If None, use the default value given when this
                :class:`FileCache` was created.
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

                If None, use the default value given when this :class:`FCPath` was
                created.

        See `pathlib.Path.unlink` for full documentation.
        """

        nthreads = self._validate_nthreads(nthreads)

        new_sub_path = self._make_paths_absolute(sub_path)

        return self._filecache_to_use.unlink(cast(StrOrPathOrSeqType,
                                                  new_sub_path),
                                             missing_ok=missing_ok,
                                             anonymous=self._anonymous,
                                             nthreads=nthreads,
                                             exception_on_fail=exception_on_fail,
                                             url_to_path=url_to_path)

    @property
    def download_counter(self) -> int:
        """The number of actual file downloads that have taken place."""

        return self._download_counter

    @property
    def upload_counter(self) -> int:
        """The number of actual file uploads that have taken place."""

        return self._upload_counter

    def is_local(self) -> bool:
        """True if the path refers to the local filesystem."""

        return self._path.startswith('file:///') or '://' not in self._path

    def is_file(self) -> bool:
        """True if this path exists and is a regular file."""

        return cast(bool, self.exists())

    def read_bytes(self, **kwargs: Any) -> bytearray:
        """Download and open the file in bytes mode, read it, and close the file.

        Any additional arguments are passed to the Python ``open()`` function.
        """

        with self.open(mode='rb', **kwargs) as f:
            return cast(bytearray, f.read())

    def read_text(self, **kwargs: Any) -> str:
        """Download and open the file in text mode, read it, and close the file.

        Any additional arguments are passed to the Python ``open()`` function.
        """

        with self.open(mode='r', **kwargs) as f:
            return cast(str, f.read())

    def write_bytes(self, data: Any, **kwargs: Any) -> int:
        """Open the file in bytes mode, write to it, and close and upload the file.

        Any additional arguments are passed to the Python ``open()`` function.
        """

        # type-check for the buffer interface before truncating the file
        view = memoryview(data)
        with self.open(mode='wb', **kwargs) as f:
            return f.write(view)

    def write_text(self, data: Any, **kwargs: Any) -> int:
        """Open the file in text mode, write to it, and close and upload the file.

        Any additional arguments are passed to the Python ``open()`` function.
        """

        if not isinstance(data, str):
            raise TypeError('data must be str, not %s' %
                            data.__class__.__name__)
        with self.open(mode='w', **kwargs) as f:
            return f.write(data)

    def iterdir(self) -> Iterator[FCPath]:
        """Yield FCPath objects of the current path's directory contents.

        The children are yielded in arbitrary order, and the special entries '.' and '..'
        are not included.
        """

        for obj in self._filecache_to_use.iterdir(self._path):
            yield FCPath(obj, copy_from=self)

    def iterdir_type(self) -> Iterator[tuple[FCPath, bool]]:
        """Yield FCPath objects of the current directory's contents, with an is_dir flag.

        The children are yielded in arbitrary order, and the special entries '.' and '..'
        are not included.
        """

        for obj, is_dir in self._filecache_to_use.iterdir_type(self._path):
            yield FCPath(obj, copy_from=self), is_dir

    def glob(self,
             pattern: str | Path | FCPath) -> Generator[FCPath]:
        """Yield all existing files and directories matching the given relative pattern.

        Notes:
            If the FCPath is local, then the normal `pathlib.Path.glob()` method is
            called. If the pattern is only `**`, this function had different behavior
            before Python 3.13 (only directories returned) and in Python 3.13 and later
            (both files and directories are returned). In contrast, when the FCPath is
            remote, we always return all files and directories. To be safe, do not use
            `**` but instead always use `**/*`.
        """

        if not isinstance(pattern, FCPath):
            pattern = FCPath(pattern)

        if pattern.is_absolute():
            raise NotImplementedError('Non-relative patterns are unsupported')

        if self.is_local():
            for res in self.as_pathlib().glob(pattern.path):
                yield FCPath(res, copy_from=self)
            return

        parts = pattern.path.split('/')
        select = _StringGlobber(self, recursive=True).selector(parts[::-1])
        for path in select(self.path):
            yield FCPath(path, copy_from=self)

    def rglob(self,
              pattern: str | Path | FCPath) -> Generator[FCPath]:
        """Yield all existing files and directories matching the given relative pattern.

        This is like calling :meth:`FCPath.glob()` with ``**/`` added in front of the
        pattern.

        Notes:
            If the FCPath is local, then the normal `pathlib.Path.glob()` method is
            called. If the pattern is only `**`, this function had different behavior
            before Python 3.13 (only directories returned) and in Python 3.13 and later
            (both files and directories are returned). In contrast, when the FCPath is
            remote, we always return all files and directories. To be safe, do not use
            `**` but instead always use `**/*`.
        """

        if not isinstance(pattern, FCPath):
            pattern = FCPath(pattern)
        pattern = '**' / pattern
        return self.glob(pattern)

    def walk(self,
             top_down: bool = True
             ) -> Iterator[tuple[FCPath, list[str], list[str]]]:
        """Walk the directory tree from this directory.

        See `pathlib.Path.walk` for full documentation.
        """

        paths: list[FCPath | tuple[FCPath, list[str], list[str]]] = [self]
        while paths:
            path = paths.pop()
            if isinstance(path, tuple):
                yield path
                continue
            dirnames: list[str] = []
            filenames: list[str] = []
            if not top_down:
                paths.append((path, dirnames, filenames))
            for child, is_dir in path.iterdir_type():
                if is_dir:
                    if not top_down:
                        paths.append(child)
                    dirnames.append(child.name)
                else:
                    filenames.append(child.name)
            if top_down:
                yield path, dirnames, filenames
                paths += [path.joinpath(d) for d in reversed(dirnames)]

    def rename(self,
               target: str | Path | FCPath) -> FCPath:
        """Rename this path to the target path.

        Both the source and target paths must be absolute, and must be in the same
        location (e.g. both local files or both in the same GS bucket). Because cloud
        platforms do not support renaming of files, this is accomplished by downloading
        the source file, uploading it with the new name, and deleting the original
        version. If the target already exists, it will be overwritten. If the downloading
        or uploading fails, the copy in the local cache is removed to eliminate ambiguity.
        If there is only a copy in the local cache and the source path does not exist on
        the remote, the rename will still succeed by uploading a copy to the target path.

        Parameters:
            target: The path to rename to.

        Returns:
            The new FCPath instance pointing to the target path.
        """

        if not isinstance(target, FCPath):
            target = FCPath(target)
        target = target.as_absolute()

        src = self.as_absolute()

        if src.is_local() != target.is_local():
            raise ValueError('Unable to rename files between local and remote locations: '
                             f'{src.path!r} and f{target.path!r}')

        drive1, root1, subpath1 = FCPath._split_parts(src.path)
        drive2, root2, subpath2 = FCPath._split_parts(target.path)

        if drive1 != drive2 or root1 != root2:
            raise ValueError('Unable to rename files across locations: '
                             f'{src.path!r} and f{target.path!r}')

        if src.is_local():
            # Local to local - just do an OS rename and be done with it
            target.parent.mkdir(parents=True, exist_ok=True)
            src.as_pathlib().rename(target.as_pathlib())
            return target

        # Since you generally can't rename on a remote cloud location, first
        # download the file, then rename it locally, then upload it to the new name,
        # then delete the old name
        local_path = cast(Path, src.retrieve())
        target_local_path = cast(Path, target.get_local_path())
        local_path.rename(target_local_path)  # Rename in the cache
        try:
            target.upload()  # Upload the new version
        except Exception:
            target_local_path.unlink(missing_ok=True)
            raise
        src.unlink(missing_ok=True)  # Delete the old version

        return target

    def replace(self, target: str | FCPath) -> FCPath:
        """Rename this path to the target path, overwriting if that path exists.

        Both the source and target paths must be absolute, and must be in the same
        location (e.g. both local files or both in the same GS bucket). Because cloud
        platforms do not support renaming of files, this is accomplished by downloading
        the source file, uploading it with the new name, and deleting the original
        version. If the target already exists, it will be overwritten.

        Parameters:
            target: The path to rename to.

        Returns:
            The new FCPath instance pointing to the target path.
        """

        return self.rename(target)

    if sys.version_info >= (3, 12):
        def relative_to(self,
                        other: str | Path | FCPath,
                        *,
                        walk_up: bool = False) -> FCPath:
            """Return the relative path to another path.

            See `pathlib.Path.relative_to` for full documentation.
            """

            if not isinstance(other, FCPath):
                other = FCPath(other)

            if self.is_local():
                return FCPath(self.as_pathlib().relative_to(other.as_pathlib(),
                                                            walk_up=walk_up),
                              copy_from=self)

            if walk_up:
                raise NotImplementedError(
                    'walk_up is not supported for non-local FCPaths')

            if not self._path.startswith(other._path):
                raise ValueError(f"{str(self)!r} is not in the subpath of {str(other)!r}")

            return FCPath(self._path[len(other._path)+1:], copy_from=self)
    else:
        def relative_to(self,
                        other: str | Path | FCPath) -> FCPath:
            """Return the relative path to another path.

            See `pathlib.Path.relative_to` for full documentation.
            """

            if not isinstance(other, FCPath):
                other = FCPath(other)

            if self.is_local():
                return FCPath(self.as_pathlib().relative_to(other.as_pathlib()),
                              copy_from=self)

            if not self._path.startswith(other._path):
                raise ValueError(f"{str(self)!r} is not in the subpath of {str(other)!r}")

            return FCPath(self._path[len(other._path)+1:], copy_from=self)

    def is_relative_to(self,
                       other: str | Path | FCPath) -> bool:
        """Return True if the path is relative to another path.

        See `pathlib.Path.is_relative_to` for full documentation.
        """

        if not isinstance(other, FCPath):
            other = FCPath(other)

        return self._path.startswith(other._path)

    def is_reserved(self) -> bool:
        """True if the path contains a special reserved name.

        See `pathlib.Path.is_reserved` for full documentation.
        """

        if not self.is_local():
            raise NotImplementedError('is_reserved on a remote file is not implemented')

        return self.as_pathlib().is_reserved()

    def stat(self,
             *,
             follow_symlinks: bool = True) -> Any:
        """Return the result of the stat() system call on this path.

        Only valid for local files. See `pathlib.Path.stat` for full documentation.
        """

        if not self.is_local():
            raise NotImplementedError('stat on a remote file is not implemented')

        return self.as_pathlib().stat(follow_symlinks=follow_symlinks)

    def lstat(self) -> Any:
        """Like stat(), except if the path points to a symlink, the symlink's
        status information is returned, rather than its target's.

        Only valid for local files. See `pathlib.Path.lstat` for full documentation.
        """

        if not self.is_local():
            raise NotImplementedError('lstat on a remote file is not implemented')

        return self.as_pathlib().lstat()

    if sys.version_info >= (3, 13):
        def is_dir(self, *, follow_symlinks: bool = True) -> bool:
            """Whether this path is a directory."""

            if not self.is_local():
                raise NotImplementedError(
                    'is_dir on a remote directory is not implemented')

            return self.as_pathlib().is_dir(follow_symlinks=follow_symlinks)
    else:
        def is_dir(self) -> bool:
            """Whether this path is a directory."""

            if not self.is_local():
                raise NotImplementedError(
                    'is_dir on a remote directory is not implemented')

            return self.as_pathlib().is_dir()

    def is_mount(self) -> bool:
        """Check if this path is a mount point.

        Only valid for local directories. See `pathlib.Path.is_mount` for full
        documentation.
        """

        if not self.is_local():
            raise NotImplementedError('is_mount on a remote directory is not implemented')

        return self.as_pathlib().is_mount()

    def is_symlink(self) -> bool:
        """Whether this path is a symbolic link.

        Only valid for local files. See `pathlib.Path.is_symlink` for full documentation.
        """

        if not self.is_local():
            raise NotImplementedError('is_symlink on a remote file is not implemented')

        return self.as_pathlib().is_symlink()

    if sys.version_info >= (3, 12):
        def is_junction(self) -> bool:
            """Whether this path is a junction.

            Only valid for local files. See `pathlib.Path.is_junction` for full
            documentation.
            """

            if not self.is_local():
                raise NotImplementedError(
                    'is_junction on a remote file is not implemented')

            return self.as_pathlib().is_junction()

    def is_block_device(self) -> bool:
        """Whether this path is a block device.

        Only valid for local files. See `pathlib.Path.is_block_device` for full
        documentation.
        """

        if not self.is_local():
            raise NotImplementedError(
                'is_block_device on a remote file is not implemented')

        return self.as_pathlib().is_block_device()

    def is_char_device(self) -> bool:
        """Whether this path is a character device.

        Only valid for local files. See `pathlib.Path.is_char_device` for full
        documentation.
        """

        if not self.is_local():
            raise NotImplementedError(
                'is_char_device on a remote file is not implemented')

        return self.as_pathlib().is_char_device()

    def is_fifo(self) -> bool:
        """Whether this path is a FIFO.

        Only valid for local files. See `pathlib.Path.is_fifo` for full documentation.
        """

        if not self.is_local():
            raise NotImplementedError('is_fifo on a remote file is not implemented')

        return self.as_pathlib().is_fifo()

    def is_socket(self) -> bool:
        """Whether this path is a socket.

        Only valid for local files. See `pathlib.Path.is_socket` for full documentation.
        """

        if not self.is_local():
            raise NotImplementedError('is_socket on a remote file is not implemented')

        return self.as_pathlib().is_socket()

    def samefile(self,
                 other_path: str | Path | FCPath) -> bool:
        """True if this path and the given path refer to the same file.

        Unlink the `pathlib.Path.samefile` version, this function only looks to see
        if the URLs are identical. Thus symlinks, hardlinks, etc. are ignored.
        """

        if not isinstance(other_path, FCPath):
            other_path = FCPath(other_path)

        return self._path == other_path._path

    def absolute(self) -> FCPath:
        """Return an absolute version of this path.

        For non-local paths, this just returns the URL. For local paths, it does the same
        operations as `pathlib.Path.absolute`. See `pathlib.Path.absolute` for full
        documentation.
        """

        if not self.is_local():
            return self

        return FCPath(self.as_pathlib().absolute(), copy_from=self)

    @classmethod
    def cwd(cls) -> FCPath:
        """Return a new FCPath pointing to the current working directory.

        See `pathlib.Path.cwd` for full documentation.
        """

        return FCPath(Path.cwd())

    def expanduser(self) -> FCPath:
        """Return a new FCPath with expanded ~ and ~user constructs.

        See `pathlib.Path.expanduser` for full documentation.
        """

        if self.is_local():
            return FCPath(self.as_pathlib().expanduser(), copy_from=self)

        return self

    @classmethod
    def home(cls) -> FCPath:
        """Return a new FCPath pointing to expanduser('~').

        See `pathlib.Path.home` for full documentation.
        """

        return FCPath(os.path.expanduser('~'))

    def readlink(self) -> FCPath:
        """Return the FCPath to which the symbolic link points.

        Only valid for local files. See `pathlib.Path.readlink` for full documentation.
        """

        if not self.is_local():
            raise NotImplementedError('readlink on a remote file is not implemented')

        return FCPath(self.as_pathlib().readlink())

    def resolve(self,
                strict: bool = False) -> FCPath:
        """Return the absolute path with resolved symlinks.

        See `pathlib.Path.resolve` for full documentation.
        """

        if self.is_local():
            return FCPath(self.as_pathlib().absolute().resolve(strict=strict),
                          copy_from=self)

        return self

    def symlink_to(self,
                   target: str,
                   target_is_directory: bool = False) -> None:
        """Make this path a symlink pointing to the target path.

        Only valid for local files. See `pathlib.Path.symlink_to` for full documentation.
        """

        if not self.is_local():
            raise NotImplementedError('symlink_to on a remote file is not implemented')

        self.as_pathlib().symlink_to(target, target_is_directory=target_is_directory)

    def hardlink_to(self,
                    target: str) -> None:
        """Make this path a hard link pointing to the same file as *target*.

        Only valid for local files. See `pathlib.Path.hardlink_to` for full documentation.
        """

        if not self.is_local():
            raise NotImplementedError('hardlink_to on a remote file is not implemented')

        self.as_pathlib().hardlink_to(target)

    def touch(self,
              mode: int = 0o666,
              exist_ok: bool = True) -> None:
        """Create this file, if it doesn't exist.

        See `pathlib.Path.touch` for full documentation.
        """

        if self.is_local():
            self.as_pathlib().touch(mode=mode, exist_ok=exist_ok)

        if not self.exists():
            self.write_bytes(b'')
        else:
            # Read and write the file to update the creation time
            self.retrieve()
            self.upload()

    def mkdir(self,
              mode: int = 0o777,
              parents: bool = False,
              exist_ok: bool = False) -> None:
        """Create a new directory at this given path.

        Only valid for local directories. See `pathlib.Path.mkdir` for full documentation.
        """

        if not self.is_local():
            raise NotImplementedError('mkdir on a remote directory is not implemented')

        self.as_pathlib().mkdir(mode=mode, parents=parents, exist_ok=exist_ok)

    def chmod(self,
              mode: int,
              *,
              follow_symlinks: bool = True) -> None:
        """Change the permissions of the path, like os.chmod().

        Only valid for local files. See `pathlib.Path.chmod` for full documentation.
        """

        if not self.is_local():
            raise NotImplementedError('chmod on a remote file is not implemented')

        self.as_pathlib().chmod(mode=mode, follow_symlinks=follow_symlinks)

    def lchmod(self,
               mode: int) -> None:
        """Like chmod(), except if the path points to a symlink, the symlink's
        permissions are changed, rather than its target's.

        Only valid for local files. See `pathlib.Path.lchmod` for full documentation.
        """

        if not self.is_local():
            raise NotImplementedError('lchmod on a remote file is not implemented')

        self.as_pathlib().lchmod(mode=mode)

    def rmdir(self) -> None:
        """Remove this directory. The directory must be empty.

        Only valid for local directories. See `pathlib.Path.rmdir` for full documentation.
        """

        if not self.is_local():
            raise NotImplementedError('rmdir on a remote directory is not implemented')

        self.as_pathlib().rmdir()

    if sys.version_info >= (3, 13):
        def owner(self, *,
                  follow_symlinks: bool = True) -> str:
            """Return the login name of the file owner.

            Only valid for local files. See `pathlib.Path.owner` for full documentation.
            """

            if not self.is_local():
                raise NotImplementedError('owner on a remote file is not implemented')

            return self.as_pathlib().owner(follow_symlinks=follow_symlinks)

        def group(self, *,
                  follow_symlinks: bool = True) -> str:
            """Return the group name of the file gid.

            Only valid for local files. See `pathlib.Path.group` for full documentation.
            """

            if not self.is_local():
                raise NotImplementedError('group on a remote file is not implemented')

            return self.as_pathlib().group(follow_symlinks=follow_symlinks)
    else:
        def owner(self) -> str:
            """Return the login name of the file owner.

            Only valid for local files. See `pathlib.Path.owner` for full documentation.
            """

            if not self.is_local():
                raise NotImplementedError('owner on a remote file is not implemented')

            return self.as_pathlib().owner()

        def group(self) -> str:
            """Return the group name of the file gid.

            Only valid for local files. See `pathlib.Path.group` for full documentation.
            """

            if not self.is_local():
                raise NotImplementedError('group on a remote file is not implemented')

            return self.as_pathlib().group()

    @classmethod
    def from_uri(cls,
                 uri: str) -> FCPath:
        """Return a new FCPath from the given URI."""

        return FCPath(uri)

    def as_uri(self) -> str:
        """Return the path as a URI."""

        if not self.is_absolute():
            raise ValueError("relative path can't be expressed as a file URI")

        if not self.is_local() or self.path.startswith('file://'):
            return self._path

        drive, root, subpath = FCPath._split_parts(self._path)
        if len(drive) == 2:
            # It's a path on a local drive => 'file:///c:/a/b'
            return f'file:///{self._path}'
        elif drive:
            # It's a path on a network drive => 'file://host/share/a/b'
            return f'file:{self._path}'
        # It's a posix path => 'file:///etc/hosts'
        return f'file://{self._path}'


def _translate2(pat: str,
                STAR: str,
                QUESTION_MARK: str) -> list[str]:
    res: list[str] = []
    add = res.append
    i, n = 0, len(pat)
    while i < n:
        c = pat[i]
        i = i+1
        if c == '*':
            # compress consecutive `*` into one
            if (not res) or res[-1] is not STAR:
                add(STAR)
        elif c == '?':
            add(QUESTION_MARK)
        elif c == '[':
            j = i
            if j < n and pat[j] == '!':
                j = j+1
            if j < n and pat[j] == ']':
                j = j+1
            while j < n and pat[j] != ']':
                j = j+1
            if j >= n:
                add('\\[')
            else:
                stuff = pat[i:j]
                if '-' not in stuff:
                    stuff = stuff.replace('\\', r'\\')
                else:
                    chunks = []
                    k = i+2 if pat[i] == '!' else i+1
                    while True:
                        k = pat.find('-', k, j)
                        if k < 0:
                            break
                        chunks.append(pat[i:k])
                        i = k+1
                        k = k+3
                    chunk = pat[i:j]
                    if chunk:
                        chunks.append(chunk)
                    else:
                        chunks[-1] += '-'
                    # Remove empty ranges -- invalid in RE.
                    for k in range(len(chunks)-1, 0, -1):
                        if chunks[k-1][-1] > chunks[k][0]:
                            chunks[k-1] = chunks[k-1][:-1] + chunks[k][1:]
                            del chunks[k]
                    # Escape backslashes and hyphens for set difference (--).
                    # Hyphens that create ranges shouldn't be escaped.
                    stuff = '-'.join(s.replace('\\', r'\\').replace('-', r'\-')
                                     for s in chunks)
                # Escape set operations (&&, ~~ and ||).
                stuff = re.sub(r'([&~|])', r'\\\1', stuff)
                i = j+1
                if not stuff:
                    # Empty range: never match.
                    add('(?!)')
                elif stuff == '!':
                    # Negated empty range: match any character.
                    add('.')
                else:
                    if stuff[0] == '!':
                        stuff = '^' + stuff[1:]
                    elif stuff[0] in ('^', '['):
                        stuff = '\\' + stuff
                    add(f'[{stuff}]')
        else:
            add(re.escape(c))
    assert i == n
    return res


magic_check = re.compile('([*?[])')
magic_check_bytes = re.compile(b'([*?[])')


def _translate(pat: str,
               *,
               recursive: bool = False) -> str:
    """Translate a pathname with shell wildcards to a regular expression.

    If `recursive` is true, the pattern segment '**' will match any number of
    path segments.
    """
    not_sep = '[^/]'
    one_last_segment = f'[^/.]{not_sep}*'
    one_segment = f'{one_last_segment}/'
    any_segments = f'(?:{one_segment})*'
    any_last_segments = f'{any_segments}(?:{one_last_segment})?'

    results = []
    parts = re.split('/', pat)
    last_part_idx = len(parts) - 1
    for idx, part in enumerate(parts):
        if part == '*':
            results.append(one_segment if idx < last_part_idx else one_last_segment)
        elif recursive and part == '**':
            if idx < last_part_idx:
                if parts[idx + 1] != '**':
                    results.append(any_segments)
            else:
                results.append(any_last_segments)
        else:
            if part:
                if part[0] in '*?':
                    results.append(r'(?!\.)')
                results.extend(_translate2(part, f'{not_sep}*', not_sep))
            if idx < last_part_idx:
                results.append('/')
    res = ''.join(results)
    return fr'(?s:{res})\Z'


@functools.lru_cache(maxsize=512)
def _compile_pattern(pat: str,
                     recursive: bool = True) -> Any:
    """Compile given glob pattern to a re.Pattern object (observing case
    sensitivity)."""
    regex = _translate(pat, recursive=recursive)
    return re.compile(regex).match


class _StringGlobber:
    """Class providing shell-style pattern matching and globbing.
    """

    def __init__(self,
                 copy_from: FCPath,
                 recursive: bool = False) -> None:
        self.recursive = recursive
        self.copy_from = copy_from

    # High-level methods

    def compile(self,
                pat: str) -> Any:
        return _compile_pattern(pat, self.recursive)

    def selector(self,
                 parts: list[str]) -> Any:
        """Returns a function that selects from a given path, walking and
        filtering according to the glob-style pattern parts in *parts*.
        """
        if not parts:
            return self.select_exists
        part = parts.pop()
        if self.recursive and part == '**':
            selector = self.recursive_selector
        else:
            selector = self.wildcard_selector
        return selector(part, parts)

    def wildcard_selector(self,
                          part: str,
                          parts: list[str]) -> Callable[[str, bool], Generator[FCPath]]:
        """Returns a function that selects direct children of a given path,
        filtering by pattern.
        """

        match = None if part == '*' else self.compile(part)
        dir_only = bool(parts)
        if dir_only:
            select_next = self.selector(parts)

        def select_wildcard(path: str,
                            exists: bool = False) -> Generator[FCPath]:
            entries = list(FCPath(path, copy_from=self.copy_from).iterdir_type())
            for entry, dir in entries:
                if match is None or match(entry.name):
                    if dir_only and not dir:
                        continue
                    if dir_only:
                        yield from select_next(entry, exists=True)
                    else:
                        yield entry
        return select_wildcard

    def recursive_selector(self,
                           part: str,
                           parts: list[str]) -> Callable[[str, bool], Generator[FCPath]]:
        """Returns a function that selects a given path and all its children,
        recursively, filtering by pattern.
        """
        # Optimization: consume following '**' parts, which have no effect.
        while parts and parts[-1] == '**':
            parts.pop()

        # Optimization: consume and join any following non-special parts here,
        # rather than leaving them for the next selector. They're used to
        # build a regular expression, which we use to filter the results of
        # the recursive walk. As a result, non-special pattern segments
        # following a '**' wildcard don't require additional filesystem access
        # to expand.
        match = None if part == '**' else self.compile(part)
        dir_only = bool(parts)
        select_next = self.selector(parts)

        def select_recursive(path: str,
                             exists: bool = False) -> Generator[FCPath]:
            path_str = str(path)
            if path_str and path_str[-1] != '/':
                path_str = f'{path_str}/'
            match_pos = len(path_str)
            if match is None or match(path_str, match_pos):
                yield from select_next(path_str, exists)
            stack = [path_str]
            while stack:
                yield from select_recursive_step(stack, match_pos)

        def select_recursive_step(stack: list[str],
                                  match_pos: int) -> Generator[Any]:
            path = stack.pop()
            entries = list(FCPath(path, copy_from=self.copy_from).iterdir_type())
            for entry, is_dir in entries:
                if is_dir or not dir_only:
                    if match is None or match(str(entry), match_pos):
                        if dir_only:
                            yield from select_next(entry, exists=True)
                        else:
                            # Optimization: directly yield the path if this is
                            # last pattern part.
                            yield entry
                    if is_dir:
                        stack.append(entry.path)

        return select_recursive

    def select_exists(self,
                      path: str,
                      exists: bool = False) -> Generator[str]:
        """Yields the given path, if it exists.
        """
        if exists:
            # Optimization: this path is already known to exist, e.g. because
            # it was returned from os.iterdir(), so we skip calling exists().
            yield path
        else:
            if FCPath(path, copy_from=self.copy_from).exists():
                yield path
