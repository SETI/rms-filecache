##########################################################################################
# filecache/file_cache_types.py
##########################################################################################

from __future__ import annotations

from pathlib import Path
from typing import Callable, Union

# We have to use Union here instead of | for compatibility with Python 3.9 and 3.10
StrOrSeqType = Union[str, list[str], tuple[str, ...]]

StrOrPathType = Union[str, Path]
StrOrPathOrSeqType = Union[str, Path,
                           list[Union[str, Path]],
                           tuple[Union[str, Path], ...]]

UrlToPathFuncType = Callable[[str, str, str, Path, str], Union[str, Path, None]]
UrlToPathFuncOrSeqType = (Union[UrlToPathFuncType,
                                list[UrlToPathFuncType],
                                tuple[UrlToPathFuncType, ...]])
