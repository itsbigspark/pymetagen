import pytest
from click import Path
from click.testing import CliRunner

from pymetagen.app import cli


@pytest.mark.parametrize(
    "input_path",
    [
        "input_csv_path",
        "input_parquet_path",
        "input_xlsx_path",
    ],
)
def test_cli(
    input_path: str, tmp_dir_path: Path, request: pytest.FixtureRequest
) -> None:
    input_path = request.getfixturevalue(input_path)
    runner = CliRunner()
    outpath: Path = tmp_dir_path / "meta.csv"
    result = runner.invoke(cli, ["-i", input_path, "-o", outpath])

    assert result.exit_code == 0

    assert outpath.exists()
    assert outpath.is_file()
    assert outpath.stat().st_size > 0
