from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

from pymetagen.app import cli
from pymetagen.datatypes import MetaGenSupportedLoadingMode
from pymetagen.utils import InspectionMode


@pytest.mark.parametrize(
    "mode",
    MetaGenSupportedLoadingMode.values(),
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
    def test_cli_metadata(
        self,
        input_path: str,
        tmp_dir_path: Path,
        request: pytest.FixtureRequest,
        mode: MetaGenSupportedLoadingMode,
    ) -> None:
        input_path = request.getfixturevalue(input_path)
        runner = CliRunner()
        outpath: Path = tmp_dir_path / "meta.csv"
        result = runner.invoke(
            cli, ["metadata", "-i", input_path, "-o", str(outpath), "-m", mode]
        )

        assert result.exit_code == 0

        assert outpath.exists()
        assert outpath.is_file()
        assert outpath.stat().st_size > 0

    @pytest.mark.parametrize(
        "input_path",
        [
            "input_csv_path",
            "input_parquet_path",
            "input_xlsx_path",
        ],
    )
    def test_cli_inspect(
        self,
        input_path: str,
        request: pytest.FixtureRequest,
        mode: MetaGenSupportedLoadingMode,
    ) -> None:
        input_path = request.getfixturevalue(input_path)
        runner = CliRunner()
        result = runner.invoke(cli, ["inspect", "-i", input_path, "-m", mode])

        assert result.exit_code == 0

    @pytest.mark.parametrize(
        "input_path",
        [
            "input_csv_path",
            "input_parquet_path",
            "input_xlsx_path",
        ],
    )
    def test_cli_inspect_writing(
        self,
        input_path: str,
        tmp_dir_path: Path,
        request: pytest.FixtureRequest,
        mode: MetaGenSupportedLoadingMode,
    ) -> None:
        input_path = request.getfixturevalue(input_path)
        runner = CliRunner()
        outpath: Path = tmp_dir_path / "meta.csv"
        result = runner.invoke(
            cli,
            ["inspect", "-i", input_path, "-o", str(outpath), "-m", mode],
        )

        assert result.exit_code == 0

        assert outpath.exists()
        assert outpath.is_file()
        assert outpath.stat().st_size > 0

    @pytest.mark.parametrize(
        "input_path",
        [
            "input_csv_path",
            "input_parquet_path",
            "input_xlsx_path",
        ],
    )
    def test_cli_extracts_writing(
        self,
        input_path: str,
        tmp_dir_path: Path,
        request: pytest.FixtureRequest,
        mode: MetaGenSupportedLoadingMode,
    ) -> None:
        inpath: Path = request.getfixturevalue(input_path)
        runner = CliRunner()
        outpath = tmp_dir_path / "trial.csv"
        mode = (
            MetaGenSupportedLoadingMode.EAGER
            if inpath.suffix == ".xlsx"
            else mode
        )
        result = runner.invoke(
            cli,
            [
                "extracts",
                "-i",
                str(inpath),
                "-o",
                str(outpath),
                "-m",
                mode,
            ],
        )

        assert result.exit_code == 0

        for inspection_mode in InspectionMode.values():
            assert outpath.with_name(
                f"{outpath.stem}-{inspection_mode}{outpath.suffix}"
            ).exists()
            assert outpath.with_name(
                f"{outpath.stem}-{inspection_mode}{outpath.suffix}"
            ).is_file()
            assert (
                outpath.with_name(
                    f"{outpath.stem}-{inspection_mode}{outpath.suffix}"
                )
                .stat()
                .st_size
                > 0
            )

    @pytest.mark.parametrize(
        ["sql_query"],
        [
            [
                """SELECT title, release_year, imdb_score
                FROM testdata
                WHERE release_year > 1990
                ORDER BY imdb_score DESC
                """
            ],
            ["tests/data/filter_query.sql"],
        ],
    )
    def test_cli_filter_by_sql_query(
        self,
        tmp_dir_path: Path,
        mode: MetaGenSupportedLoadingMode,
        sql_query: str,
    ) -> None:
        runner = CliRunner()
        outpath: Path = tmp_dir_path / "testdata.csv"
        result = runner.invoke(
            cli,
            [
                "filter",
                "-i",
                "tests/data/testdata.csv",
                "-o",
                str(outpath),
                "--mode",
                mode,
                "-q",
                sql_query,
            ],
        )

        assert result.exit_code == 0

        assert outpath.exists()
        assert outpath.is_file()
        assert outpath.stat().st_size > 0
