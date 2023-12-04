from pymetagen._version import _get_version
from pymetagen.pymetagen import *  # noqa: F403

__version__ = _get_version()


def main():
    print(f"pymetagen version: {__version__}")


if __name__ == "__main__":
    main()
