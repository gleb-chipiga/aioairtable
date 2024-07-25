__all__ = ("get_python_version", "get_software")


def get_python_version() -> str:
    from sys import version_info as version

    return f"{version.major}.{version.minor}.{version.micro}"


def get_software() -> str:
    from . import __version__

    return f"Python/{get_python_version()} aioairtable/{__version__}"
