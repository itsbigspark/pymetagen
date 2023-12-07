import pytest
from click import Path
from click.testing import CliRunner

from pymetagen.app import cli
from pymetagen.datatypes import MetaGenSupportedLoadingModes


@pytest.mark.parametrize(
    "mode",
    MetaGenSupportedLoadingModes.list(),
)
class TestCli:
    @pytest.mark.parametrize(
        "input_path",
        [
            "input_csv_path",
            "input_parquet_path",
            "input_xlsx_path",
        ],
    )
    def test_cli(
        self,
        input_path: str,
        tmp_dir_path: Path,
        request: pytest.FixtureRequest,
        mode: MetaGenSupportedLoadingModes,
    ) -> None:
        input_path = request.getfixturevalue(input_path)
        runner = CliRunner()
        outpath: Path = tmp_dir_path / "meta.csv"
        result = runner.invoke(
            cli, ["-i", input_path, "-o", outpath, "-m", mode]
        )

        assert result.exit_code == 0

        assert outpath.exists()
        assert outpath.is_file()
        assert outpath.stat().st_size > 0

    def test_input_not_exist(
        self, tmp_dir_path: Path, mode: MetaGenSupportedLoadingModes
    ) -> None:
        runner = CliRunner()
        outpath: Path = tmp_dir_path / "meta.csv"
        result = runner.invoke(
            cli, ["-i", "not_exist_path", "-o", outpath, "-m", mode]
        )

        assert result.exit_code == 2
        assert "does not exist" in result.output

        assert not outpath.exists()
