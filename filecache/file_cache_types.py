from pathlib import Path
from typing import Callable

UrlToPathFuncType = Callable[[str, str, str, Path, str], str | Path | None]
