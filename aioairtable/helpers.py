import json
from functools import partial
from typing import Final

__all__ = ("json_dumps", "get_python_version", "get_software", "debug")

json_dumps: Final = partial(json.dumps, ensure_ascii=False)


def get_python_version() -> str:
    from sys import version_info as version

    return f"{version.major}.{version.minor}.{version.micro}"


def get_software() -> str:
    from . import __version__

    return f"Python/{get_python_version()} aioairtable/{__version__}"


def debug() -> bool:
    return __debug__
