from importlib.metadata import version

from pymetagen.metagen import *  # noqa: F403

__version__ = version("pymetagen")


def main():
    print(f"pymetagen version: {__version__}")


if __name__ == "__main__":
    main()
