import re
from typing import Final

from aioairtable.helpers import get_python_version, get_software

VERSION_RXP_STR: Final[str] = r"\d\.\d{1,2}\.[a-z0-9]+"


def test_get_python_version() -> None:
    version_match = re.match(VERSION_RXP_STR, get_python_version())
    assert version_match is not None


def test_get_software() -> None:
    software_rxp = f"Python/{VERSION_RXP_STR} aioairtable/{VERSION_RXP_STR}"
    assert re.match(software_rxp, get_software()) is not None
