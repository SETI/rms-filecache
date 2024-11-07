from pathlib import Path
from typing import Callable

UrlToPathFuncType = Callable[[str, str, str, Path, str], Path | None]
