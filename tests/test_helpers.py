import re
from typing import Final

from aioairtable.helpers import debug, get_python_version, get_software

version_rxp: Final = r'\d\.\d{1,2}\.[a-z0-9]+'


def test_get_python_version() -> None:
    version_match = re.match(version_rxp, get_python_version())
    assert version_match is not None


def test_get_software() -> None:
    software_rxp = f'Python/{version_rxp} aioairtable/{version_rxp}'
    assert re.match(software_rxp, get_software()) is not None


def test_debug() -> None:
    assert isinstance(debug(), bool)
