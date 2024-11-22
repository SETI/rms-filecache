##########################################################################################
# filecache/file_cache_path.py
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

if TYPE_CHECKING:  # pragma: no cover
    from .file_cache import FileCache  # Circular import

from .file_cache_types import UrlToPathFuncType


class FCPath:
    """Class for interfacing to a FileCache using a URL prefix.

    This class provides a simpler way to abstract away remote access in a FileCache by
    collecting common parameters (`anonymous`, `lock_timeout`, `nthreads`) and a more
    complete prefix (not just the bucket name or URL, but the first part of the access
    path as well) into a single location.
    """

    def __init__(self,
                 path: str | Path,
                 *paths: list[str | Path],
                 filecache: Optional["FileCache"] = None,
                 anonymous: Optional[bool] = None,
                 lock_timeout: Optional[int] = None,
                 nthreads: Optional[int] = None,
                 url_to_path: Optional[UrlToPathFuncType |
                                       Sequence[UrlToPathFuncType]] = None,
                 copy_from: Optional[FCPath] = None
                 ):
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
        Notes:
            Within a given :class:`FileCache`, :class:`FileCachePrefix` instances that
            reference the same local/remote source will be stored in the same location on
            the local disk. Files downloaded into one instance will thus be visible in the
            other instance.

            Any logging will be made to the `file_cache`'s logger.
        """

        self._path = self._join(path, *paths)

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

        self._upload_counter = 0
        self._download_counter = 0

    @staticmethod
    def _split_parts(path: str | Path) -> tuple[str, str, str]:
        """Split a path into drive, root, and remainder of path."""

        from .file_cache import FileCache  # Circular import avoidance

        path = str(path).replace('\\', '/')
        drive = ''
        root = ''
        if len(path) >= 2 and path[0].isalpha() and path[1] == ':':
            # Windows C:
            drive = path[0:2]
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
    def _is_absolute(path: str) -> bool:
        """Check if a path string is an absolute path."""

        return FCPath._split_parts(path)[1] == '/'

    @staticmethod
    def _join(*paths: list[str | None]) -> str:
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
        _, _, subpath = FCPath._split_parts(path)
        if '/' not in subpath:
            return subpath
        return subpath[subpath.rfind('/') + 1:]

    def __str__(self) -> str:
        return self._path

    def as_posix(self) -> str:
        return self._path

    @property
    def drive(self) -> str:
        # Windows c: (if any)
        # UNC //host/share
        # Cloud gs://bucket
        # '' otherwise
        return self._split_parts(self._path)[0]

    @property
    def root(self) -> str:
        # / if absolute, otherwise ''
        return self._split_parts(self._path)[1]

    @property
    def anchor(self) -> str:
        # drive + root
        return ''.join(self._split_parts(self._path)[0:2])

    @property
    def suffix(self) -> str:
        """
        The final component's last suffix, if any.

        This includes the leading period. For example: '.txt'
        """
        name = FCPath._filename(self._path)
        i = name.rfind('.')
        if 0 < i < len(name) - 1:
            return name[i:]
        else:
            return ''

    @property
    def suffixes(self) -> list[str]:
        """
        A list of the final component's suffixes, if any.

        These include the leading periods. For example: ['.tar', '.gz']
        """
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
        """Return a new path with the file name changed."""
        drive, root, subpath = FCPath._split_parts(self._path)
        drive2, root2, subpath2 = FCPath._split_parts(name)
        if drive2 != '' or root2 != '' or subpath2 == '' or '/' in subpath2:
            raise ValueError(f"Invalid name {name!r}")

        if '/' not in subpath:
            return FCPath(drive + name, copy_from=self)

        return FCPath(drive + subpath[:subpath.rfind('/')+1:] + name,
                      copy_from=self)

    def with_stem(self, stem: str) -> FCPath:
        """Return a new path with the stem changed."""
        suffix = self.suffix
        if not suffix:
            return self.with_name(stem)
        elif not stem:
            # If the suffix is non-empty, we can't make the stem empty.
            raise ValueError(f"{self!r} has a non-empty suffix")
        else:
            return self.with_name(stem + suffix)

    def with_suffix(self, suffix: str) -> FCPath:
        """Return a new path with the file suffix changed.  If the path
        has no suffix, add given suffix.  If the given suffix is an empty
        string, remove the suffix from the path.
        """
        stem = self.stem
        if not stem:
            # If the stem is empty, we can't make the suffix non-empty.
            raise ValueError(f"{self!r} has an empty name")
        elif suffix and not (suffix.startswith('.') and len(suffix) > 1):
            raise ValueError(f"Invalid suffix {suffix!r}")
        else:
            return self.with_name(stem + suffix)

    def relative_to(self, other, *, walk_up=False):  # XXX
        """Return the relative path to another path identified by the passed
        arguments.  If the operation is not possible (because this is not
        related to the other path), raise ValueError.

        The *walk_up* parameter controls whether `..` may be used to resolve
        the path.
        """
        if not isinstance(other, PurePathBase):
            other = self.with_segments(other)
        anchor0, parts0 = self._stack
        anchor1, parts1 = other._stack
        if anchor0 != anchor1:
            raise ValueError(f'{self._raw_path!r} and {other._raw_path!r} have different anchors')
        while parts0 and parts1 and parts0[-1] == parts1[-1]:
            parts0.pop()
            parts1.pop()
        for part in parts1:
            if not part or part == '.':
                pass
            elif not walk_up:
                raise ValueError(f'{self._raw_path!r} is not in the subpath of {other._raw_path!r}')
            elif part == '..':
                raise ValueError(f"'..' segment in {other._raw_path!r} cannot be walked")
            else:
                parts0.append('..')
        return self.with_segments('', *reversed(parts0))

    def is_relative_to(self, other):  # XXX
        """Return True if the path is relative to another path or False.
        """
        if not isinstance(other, PurePathBase):
            other = self.with_segments(other)
        anchor0, parts0 = self._stack
        anchor1, parts1 = other._stack
        if anchor0 != anchor1:
            return False
        while parts0 and parts1 and parts0[-1] == parts1[-1]:
            parts0.pop()
            parts1.pop()
        for part in parts1:
            if part and part != '.':
                return False
        return True

    @property
    def parts(self):  # XXX
        """An object providing sequence-like access to the
        components in the filesystem path."""
        anchor, parts = self._stack
        if anchor:
            parts.append(anchor)
        return tuple(reversed(parts))

    def joinpath(self, *pathsegments: list[str | Path | FCPath]) -> FCPath:
        """Combine this path with one or several arguments, and return a
        new path representing either a subpath (if all arguments are relative
        paths) or a totally different path (if one of the arguments is
        anchored).
        """
        return FCPath(self._path, *pathsegments, copy_from=self)

    def __truediv__(self, other: str | Path | FCPath) -> FCPath:
        return FCPath(self._path, other, copy_from=self)

    def __rtruediv__(self, other: str | Path | FCPath) -> FCPath:
        if isinstance(other, FCPath):
            return FCPath(other, self._path, copy_from=other)
        else:
            return FCPath(other, self._path, copy_from=self)

    @property
    def _stack(self):
        """
        Split the path into a 2-tuple (anchor, parts), where *anchor* is the
        uppermost parent of the path (equivalent to path.parents[-1]), and
        *parts* is a reversed list of parts following the anchor.
        """
        split = self.parser.split
        path = self._raw_path
        parent, name = split(path)
        names = []
        while path != parent:
            names.append(name)
            path = parent
            parent, name = split(path)
        return path, names

    @property
    def parent(self):
        """The logical parent of the path."""
        path = self._raw_path
        parent = self.parser.split(path)[0]
        if path != parent:
            parent = self.with_segments(parent)
            parent._resolving = self._resolving
            return parent
        return self

    @property
    def parents(self):
        """A sequence of this path's logical parents."""
        split = self.parser.split
        path = self._raw_path
        parent = split(path)[0]
        parents = []
        while path != parent:
            parents.append(self.with_segments(parent))
            path = parent
            parent = split(path)[0]
        return tuple(parents)

    def is_absolute(self):
        """True if the path is absolute (has both a root and, if applicable,
        a drive)."""
        return FCPath._is_absolute(self._path)

    def match(self, path_pattern, *, case_sensitive=None):
        """
        Return True if this path matches the given pattern. If the pattern is
        relative, matching is done from the right; otherwise, the entire path
        is matched. The recursive wildcard '**' is *not* supported by this
        method.
        """
        if not isinstance(path_pattern, PurePathBase):
            path_pattern = self.with_segments(path_pattern)
        path_parts = self.parts[::-1]
        pattern_parts = path_pattern.parts[::-1]
        if not pattern_parts:
            raise ValueError("empty pattern")
        if len(path_parts) < len(pattern_parts):
            return False
        if len(path_parts) > len(pattern_parts) and path_pattern.anchor:
            return False
        globber = self._globber(sep, case_sensitive)
        for path_part, pattern_part in zip(path_parts, pattern_parts):
            match = globber.compile(pattern_part)
            if match(path_part) is None:
                return False
        return True

    def full_match(self, pattern, *, case_sensitive=None):
        """
        Return True if this path matches the given glob-style pattern. The
        pattern is matched against the entire path.
        """
        if not isinstance(pattern, PurePathBase):
            pattern = self.with_segments(pattern)
        if case_sensitive is None:
            case_sensitive = _is_case_sensitive(self.parser)
        globber = self._globber(pattern.parser.sep, case_sensitive, recursive=True)
        match = globber.compile(pattern._pattern_str)
        return match(self._path) is not None

    def get_local_path(self,
                       sub_path: Optional[str | Path | Sequence[str | Path]] = None,
                       *,
                       create_parents: bool = True,
                       url_to_path: Optional[UrlToPathFuncType |
                                             Sequence[UrlToPathFuncType]] = None,
                       ) -> Path | list[Path]:
        """Return the local path for the given sub_path relative to the prefix.

        Parameters:
            sub_path: The path of the file relative to the prefix. If `sub_path` is a list
                or tuple, all paths are processed.
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

                If None, use the default value given when this :class:`FileCachePrefix`
                was created.
        Returns:
            The Path (or list of Paths) of the filename in the temporary directory, or
            as specified by the `url_to_path` translators. The files do not have to exist
            because a Path could be used for writing a file to upload. To facilitate
            this, a side effect of this call (if `create_parents` is True) is that the
            complete parent directory structure will be created for each returned Path.
        """

        if isinstance(sub_path, (list, tuple)):
            new_sub_path = [FCPath._join(self._path, p) for p in sub_path]
            return self._filecache.get_local_path(new_sub_path,
                                                  anonymous=self._anonymous,
                                                  create_parents=create_parents,
                                                  url_to_path=url_to_path)

        return self._filecache.get_local_path(FCPath._join(self._path, sub_path),
                                              anonymous=self._anonymous,
                                              create_parents=create_parents,
                                              url_to_path=url_to_path)

    def exists(self,
               sub_path: Optional[str | Path | Sequence[str | Path]] = None,
               *,
               bypass_cache: bool = False,
               nthreads: Optional[int] = None,
               url_to_path: Optional[UrlToPathFuncType |
                                     Sequence[UrlToPathFuncType]] = None
               ) -> bool | list[bool]:
        """Check if a file exists without downloading it.

        Parameters:
            sub_path: The path of the file relative to the prefix.
            bypass_cache: If False, check for the file first in the local cache, and if
                not found there then on the remote server. If True, only check on the
                remote server.
            nthreads: The maximum number of threads to use when doing multiple-file
                retrieval or upload. If None, use the default value given when this
                :class:`FileCachePrefix` was created.
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

                If None, use the default value given when this :class:`FileCachePrefix`
                was created.
        Returns:
            True if the file exists. Note that it is possible that a file could exist and
            still not be downloadable due to permissions. False if the file does not
            exist. This includes bad bucket or webserver names, lack of permission to
            examine a bucket's contents, etc.

        Raises:
            ValueError: If the path is invalidly constructed.
        """

        if nthreads is not None and (not isinstance(nthreads, int) or nthreads <= 0):
            raise ValueError(f'nthreads must be a positive integer, got {nthreads}')
        if nthreads is None:
            nthreads = self._nthreads

        if isinstance(sub_path, (list, tuple)):
            new_sub_path = [FCPath._join(self._path, p) for p in sub_path]
            return self._filecache.exists(new_sub_path,
                                          bypass_cache=bypass_cache,
                                          nthreads=nthreads,
                                          anonymous=self._anonymous,
                                          url_to_path=url_to_path)

        return self._filecache.exists(FCPath._join(self._path, sub_path),
                                      bypass_cache=bypass_cache,
                                      anonymous=self._anonymous,
                                      url_to_path=url_to_path)

    def retrieve(self,
                 sub_path: Optional[str | Sequence[str]] = None,
                 *,
                 lock_timeout: Optional[int] = None,
                 nthreads: Optional[int] = None,
                 exception_on_fail: bool = True,
                 url_to_path: Optional[UrlToPathFuncType |
                                       Sequence[UrlToPathFuncType]] = None
                 ) -> Path | Exception | list[Path | Exception]:
        """Retrieve a file(s) from the given sub_path and store it in the file cache.

        Parameters:
            sub_path: The path of the file relative to the prefix. If `sub_path` is a list
                or tuple, the complete list of files is retrieved. Depending on the
                storage location, this may be more efficient because files can be
                downloaded in parallel.
            nthreads: The maximum number of threads to use when doing multiple-file
                retrieval or upload. If None, use the default value given when this
                :class:`FileCachePrefix` was created.
            lock_timeout: How long to wait, in seconds, if another process is marked as
                retrieving the file before raising an exception. 0 means to not wait at
                all. A negative value means to never time out. None means to use the
                default value given when this :class:`FileCachePrefix` was created.
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

                If None, use the default value given when this :class:`FileCachePrefix`
                was created.
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

        if lock_timeout is None:
            lock_timeout = self._lock_timeout

        try:
            if isinstance(sub_path, (list, tuple)):
                new_sub_path = [FCPath._join(self._path, p) for p in sub_path]
                ret = self._filecache.retrieve(new_sub_path,
                                               anonymous=self._anonymous,
                                               lock_timeout=lock_timeout,
                                               nthreads=nthreads,
                                               exception_on_fail=exception_on_fail,
                                               url_to_path=url_to_path)
            else:
                ret = self._filecache.retrieve(FCPath._join(self._path, sub_path),
                                               anonymous=self._anonymous,
                                               lock_timeout=lock_timeout,
                                               exception_on_fail=exception_on_fail,
                                               url_to_path=url_to_path)
        finally:
            self._download_counter += (self._filecache.download_counter -
                                       old_download_counter)

        return ret

    def upload(self,
               sub_path: Optional[str | Sequence[str]] = None,
               *,
               nthreads: Optional[int] = None,
               exception_on_fail: bool = True,
               url_to_path: Optional[UrlToPathFuncType |
                                     Sequence[UrlToPathFuncType]] = None
               ) -> Path | Exception | list[Path | Exception]:
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

                If None, use the default value given when this :class:`FileCachePrefix`
                was created.
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
                new_sub_paths = [FCPath._join(self._path, p) for p in sub_path]
                ret = self._filecache.upload(new_sub_paths,
                                             anonymous=self._anonymous,
                                             nthreads=nthreads,
                                             exception_on_fail=exception_on_fail,
                                             url_to_path=url_to_path)
            else:
                ret = self._filecache.upload(FCPath._join(self._path, sub_path),
                                             anonymous=self._anonymous,
                                             exception_on_fail=exception_on_fail,
                                             url_to_path=url_to_path)
        finally:
            self._upload_counter += (self._filecache.upload_counter -
                                     old_upload_counter)

        return ret

    @contextlib.contextmanager
    def open(self,
             sub_path: Optional[str] = None,
             mode: str = 'r',
             *args: Any,
             url_to_path: Optional[UrlToPathFuncType |
                                   Sequence[UrlToPathFuncType]] = None,
             **kwargs: Any) -> Generator[IO[Any]]:
        """Retrieve+open or open+upload a file as a context manager.

        If `mode` is a read mode (like ``'r'`` or ``'rb'``) then the file will be first
        retrieved by calling :meth:`retrieve` and then opened. If the `mode` is a write
        mode (like ``'w'`` or ``'wb'``) then the file will be first opened for write, and
        when this context manager is exited the file will be uploaded.

        Parameters:
            sub_path: The path of the file relative to the prefix.
            mode: The mode string as you would specify to Python's `open()` function.
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

                If None, use the default value given when this :class:`FileCachePrefix`
                was created.
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

    @property
    def download_counter(self) -> int:
        """The number of actual file downloads that have taken place."""
        return self._download_counter

    @property
    def upload_counter(self) -> int:
        """The number of actual file uploads that have taken place."""
        return self._upload_counter

    @property
    def is_local(self) -> bool:  # XXX
        """A bool indicating whether or not the prefix refers to the local filesystem."""
        return self._path.startswith('file:///') or '://' not in self._path

    ### Unsupported operations

    def _not_implemented_msg(cls, attribute):
        return f"{cls.__name__}.{attribute} is unsupported"

    def stat(self, *, follow_symlinks=True):
        """
        Return the result of the stat() system call on this path, like
        os.stat() does.
        """
        raise NotImplementedError(self._not_implemented_msg('stat()'))

    def lstat(self):
        """
        Like stat(), except if the path points to a symlink, the symlink's
        status information is returned, rather than its target's.
        """
        raise NotImplementedError(self._not_implemented_msg('lstat()'))

    def is_dir(self, *, follow_symlinks=True):
        """
        Whether this path is a directory.
        """
        try:
            return S_ISDIR(self.stat(follow_symlinks=follow_symlinks).st_mode)
        except OSError as e:
            if not _ignore_error(e):
                raise
            # Path doesn't exist or is a broken symlink
            # (see http://web.archive.org/web/20200623061726/https://bitbucket.org/pitrou/pathlib/issues/12/ )
            return False
        except ValueError:
            # Non-encodable path
            return False

    def is_file(self, *, follow_symlinks=True):
        """
        Whether this path is a regular file (also True for symlinks pointing
        to regular files).
        """
        try:
            return S_ISREG(self.stat(follow_symlinks=follow_symlinks).st_mode)
        except OSError as e:
            if not _ignore_error(e):
                raise
            # Path doesn't exist or is a broken symlink
            # (see http://web.archive.org/web/20200623061726/https://bitbucket.org/pitrou/pathlib/issues/12/ )
            return False
        except ValueError:
            # Non-encodable path
            return False

    def is_mount(self):
        """
        Check if this path is a mount point
        """
        raise NotImplementedError(self._not_implemented_msg('is_mount()'))

    def is_symlink(self):
        """
        Whether this path is a symbolic link.
        """
        raise NotImplementedError(self._not_implemented_msg('is_symlink()'))

    def is_junction(self):
        """
        Whether this path is a junction.
        """
        raise NotImplementedError(self._not_implemented_msg('is_junction()'))

    def is_block_device(self):
        """
        Whether this path is a block device.
        """
        raise NotImplementedError(self._not_implemented_msg('is_block_device()'))

    def is_char_device(self):
        """
        Whether this path is a character device.
        """
        raise NotImplementedError(self._not_implemented_msg('is_char_device()'))

    def is_fifo(self):
        """
        Whether this path is a FIFO.
        """
        raise NotImplementedError(self._not_implemented_msg('is_fifo()'))

    def is_socket(self):
        """
        Whether this path is a socket.
        """
        raise NotImplementedError(self._not_implemented_msg('is_socket()'))

    def samefile(self, other_path):
        """Return whether other_path is the same or not as this file
        (as returned by os.path.samefile()).
        """
        raise NotImplementedError(self._not_implemented_msg('samefile()'))

    def read_bytes(self):  # XXX
        """
        Open the file in bytes mode, read it, and close the file.
        """
        with self.open(mode='rb') as f:
            return f.read()

    def read_text(self, encoding=None, errors=None, newline=None):  # XXX
        """
        Open the file in text mode, read it, and close the file.
        """
        with self.open(mode='r', encoding=encoding, errors=errors, newline=newline) as f:
            return f.read()

    def write_bytes(self, data):  # XXX
        """
        Open the file in bytes mode, write to it, and close the file.
        """
        # type-check for the buffer interface before truncating the file
        view = memoryview(data)
        with self.open(mode='wb') as f:
            return f.write(view)

    def write_text(self, data, encoding=None, errors=None, newline=None):
        """
        Open the file in text mode, write to it, and close the file.
        """
        if not isinstance(data, str):
            raise TypeError('data must be str, not %s' %
                            data.__class__.__name__)
        with self.open(mode='w', encoding=encoding, errors=errors, newline=newline) as f:
            return f.write(data)

    def iterdir(self):  # XXX
        """Yield path objects of the directory contents.

        The children are yielded in arbitrary order, and the
        special entries '.' and '..' are not included.
        """
        raise NotImplementedError(self._not_implemented_msg('iterdir()'))

    def _glob_selector(self, parts, case_sensitive, recurse_symlinks):  # XXX
        if case_sensitive is None:
            case_sensitive = _is_case_sensitive(self.parser)
            case_pedantic = False
        else:
            # The user has expressed a case sensitivity choice, but we don't
            # know the case sensitivity of the underlying filesystem, so we
            # must use scandir() for everything, including non-wildcard parts.
            case_pedantic = True
        recursive = True if recurse_symlinks else _no_recurse_symlinks
        globber = self._globber(self.parser.sep, case_sensitive, case_pedantic, recursive)
        return globber.selector(parts)

    def glob(self, pattern, *, case_sensitive=None, recurse_symlinks=True):  # XXX
        """Iterate over this subtree and yield all existing files (of any
        kind, including directories) matching the given relative pattern.
        """
        if not isinstance(pattern, PurePathBase):
            pattern = self.with_segments(pattern)
        anchor, parts = pattern._stack
        if anchor:
            raise NotImplementedError("Non-relative patterns are unsupported")
        select = self._glob_selector(parts, case_sensitive, recurse_symlinks)
        return select(self)

    def rglob(self, pattern, *, case_sensitive=None, recurse_symlinks=True):  # XXX
        """Recursively yield all existing files (of any kind, including
        directories) matching the given relative pattern, anywhere in
        this subtree.
        """
        if not isinstance(pattern, PurePathBase):
            pattern = self.with_segments(pattern)
        pattern = '**' / pattern
        return self.glob(pattern, case_sensitive=case_sensitive, recurse_symlinks=recurse_symlinks)

    def walk(self, top_down=True, on_error=None, follow_symlinks=False):  # XXX
        """Walk the directory tree from this directory, similar to os.walk()."""
        paths = [self]
        while paths:
            path = paths.pop()
            if isinstance(path, tuple):
                yield path
                continue
            dirnames = []
            filenames = []
            if not top_down:
                paths.append((path, dirnames, filenames))
            try:
                for child in path.iterdir():
                    try:
                        if child.is_dir(follow_symlinks=follow_symlinks):
                            if not top_down:
                                paths.append(child)
                            dirnames.append(child.name)
                        else:
                            filenames.append(child.name)
                    except OSError:
                        filenames.append(child.name)
            except OSError as error:
                if on_error is not None:
                    on_error(error)
                if not top_down:
                    while not isinstance(paths.pop(), tuple):
                        pass
                continue
            if top_down:
                yield path, dirnames, filenames
                paths += [path.joinpath(d) for d in reversed(dirnames)]

    def absolute(self):
        """Return an absolute version of this path
        No normalization or symlink resolution is performed.

        Use resolve() to resolve symlinks and remove '..' segments.
        """
        raise NotImplementedError(self._not_implemented_msg('absolute()'))

    @classmethod
    def cwd(cls):
        """Return a new path pointing to the current working directory."""
        # We call 'absolute()' rather than using 'os.getcwd()' directly to
        # enable users to replace the implementation of 'absolute()' in a
        # subclass and benefit from the new behaviour here. This works because
        # os.path.abspath('.') == os.getcwd().
        raise NotImplementedError(self._not_implemented_msg('cwd()'))

    def expanduser(self):
        """ Return a new path with expanded ~ and ~user constructs
        (as returned by os.path.expanduser)
        """
        raise NotImplementedError(self._not_implemented_msg('expanduser()'))

    @classmethod
    def home(cls):
        """Return a new path pointing to expanduser('~').
        """
        raise NotImplementedError(self._not_implemented_msg('home()'))

    def readlink(self):
        """
        Return the path to which the symbolic link points.
        """
        raise NotImplementedError(self._not_implemented_msg('readlink()'))

    def resolve(self, strict=False):
        """
        Make the path absolute, resolving all symlinks on the way and also
        normalizing it.
        """
        raise NotImplementedError(self._not_implemented_msg('resolve()'))

    def symlink_to(self, target, target_is_directory=False):
        """
        Make this path a symlink pointing to the target path.
        Note the order of arguments (link, target) is the reverse of os.symlink.
        """
        raise NotImplementedError(self._not_implemented_msg('symlink_to()'))

    def hardlink_to(self, target):
        """
        Make this path a hard link pointing to the same file as *target*.

        Note the order of arguments (self, target) is the reverse of os.link's.
        """
        raise NotImplementedError(self._not_implemented_msg('hardlink_to()'))

    def touch(self, mode=0o666, exist_ok=True):
        """
        Create this file with the given access mode, if it doesn't exist.
        """
        raise NotImplementedError(self._not_implemented_msg('touch()'))

    def mkdir(self, mode=0o777, parents=False, exist_ok=False):
        """
        Create a new directory at this given path.
        """
        raise NotImplementedError(self._not_implemented_msg('mkdir()'))

    def rename(self, target):
        """
        Rename this path to the target path.

        The target path may be absolute or relative. Relative paths are
        interpreted relative to the current working directory, *not* the
        directory of the Path object.

        Returns the new Path instance pointing to the target path.
        """
        raise NotImplementedError(self._not_implemented_msg('rename()'))

    def replace(self, target):
        """
        Rename this path to the target path, overwriting if that path exists.

        The target path may be absolute or relative. Relative paths are
        interpreted relative to the current working directory, *not* the
        directory of the Path object.

        Returns the new Path instance pointing to the target path.
        """
        raise NotImplementedError(self._not_implemented_msg('replace()'))

    def chmod(self, mode, *, follow_symlinks=True):
        """
        Change the permissions of the path, like os.chmod().
        """
        raise NotImplementedError(self._not_implemented_msg('chmod()'))

    def lchmod(self, mode):
        """
        Like chmod(), except if the path points to a symlink, the symlink's
        permissions are changed, rather than its target's.
        """
        raise NotImplementedError(self._not_implemented_msg('lchmod()'))

    def unlink(self, missing_ok=False):
        """
        Remove this file or link.
        If the path is a directory, use rmdir() instead.
        """
        raise NotImplementedError(self._not_implemented_msg('unlink()'))

    def rmdir(self):
        """
        Remove this directory.  The directory must be empty.
        """
        raise NotImplementedError(self._not_implemented_msg('rmdir()'))

    def owner(self, *, follow_symlinks=True):
        """
        Return the login name of the file owner.
        """
        raise NotImplementedError(self._not_implemented_msg('owner()'))

    def group(self, *, follow_symlinks=True):
        """
        Return the group name of the file gid.
        """
        raise NotImplementedError(self._not_implemented_msg('group()'))

    @classmethod
    def from_uri(cls, uri):
        """Return a new path from the given 'file' URI."""
        raise NotImplementedError(cls._not_implemented_msg('from_uri()'))

    def as_uri(self):
        """Return the path as a URI."""
        raise NotImplementedError(self._not_implemented_msg('as_uri()'))
