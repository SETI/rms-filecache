from __future__ import annotations

from pathlib import Path
from typing import Callable, Union

# We have to use Union here instead of | for compatibility with Python 3.9 and 3.10
UrlToPathFuncType = Callable[[str, str, str, Path, str], Union[str, Path, None]]
