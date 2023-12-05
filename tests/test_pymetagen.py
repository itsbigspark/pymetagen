from pathlib import Path

import pytest

from pymetagen import MetaGen
from pymetagen.datatypes import MetaGenSupportedLoadingModes
from pymetagen.exceptions import (
    FileTypeUnsupportedError,
    LoadingModeUnsupportedError,
)

input_paths = ["input_csv_path", "input_parquet_path", "input_xlsx_path"]


@pytest.mark.parametrize(
    "data",
    [
        "eager_data",
        pytest.param(
            "lazy_data",
            marks=pytest.mark.xfail(
                reason="LazyFrame is not fully supported yet"
            ),
        ),
    ],
)
class TestMetaGen:
    """
    Test the MetaGen class on both eager and lazy data.

    The lazy data test is marked as xfail because the current implementation
    of MetaGen does not fully support lazy data yet.

    Each test is parametrized with the data fixture name and should run:

    ```
    data = request.getfixturevalue(data)
    ```

    to get the data.
    """

    def test_init(self, data: str, request: pytest.FixtureRequest):
        data = request.getfixturevalue(data)
        MetaGen(
            data=data,
        )

    def test_compute_metadata(self, data: str, request: pytest.FixtureRequest):
        data = request.getfixturevalue(data)
        metadata = MetaGen(
            data=data,
        ).compute_metadata()

        assert set(metadata.index) == set(data.columns)


@pytest.mark.parametrize(
    "mode",
    MetaGenSupportedLoadingModes.list(),
)
class TestMetaGenFromPath:
    @pytest.mark.parametrize(
        "path",
        input_paths,
    )
    def test_from_path(
        self,
        path: str,
        mode: MetaGenSupportedLoadingModes,
        request: pytest.FixtureRequest,
    ):
        path: Path = request.getfixturevalue(path)
        MetaGen.from_path(
            path=path,
            mode=mode,
        )

    def test_unsupported_path(
        self,
        tmp_dir_path: Path,
        mode: MetaGenSupportedLoadingModes,
    ):
        with pytest.raises(FileTypeUnsupportedError):
            MetaGen.from_path(
                path=tmp_dir_path / "test.unsupported",
                mode=mode,
            )


def test_from_path_unsupported_mode(
    tmp_dir_path: Path,
):
    with pytest.raises(LoadingModeUnsupportedError):
        MetaGen.from_path(
            path=tmp_dir_path / "test.csv",
            mode="unsupported_mode",
        )
