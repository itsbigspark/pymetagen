import re
from pathlib import Path


def get_release_text(
    changelog_path: Path = Path("CHANGELOG.md"),
    pyproject_path: Path = Path("pyproject.toml"),
) -> str:
    """Get the release text from the changelog.

    Args:
        changelog_path (Path): Path to the changelog file.
        pyproject_path (Path): Path to the pyproject.toml file.

    Returns:
        str: The release text.
    """
    pyproject = pyproject_path.read_text()
    version_line = re.search(
        r"version\s*=\s*['\"](\d+\.\d+\.\d+\.\d+)['\"]", pyproject
    )
    assert version_line
    version: str = version_line.group(1)

    changelog = changelog_path.read_text()
    release_texts = changelog.split("## ")
    for release_text in release_texts:
        if version in release_text:
            text = release_text
            break
    text_lines = text.splitlines()
    text = "\n".join(text_lines[1:])

    return text


if __name__ == "__main__":
    print(get_release_text())
