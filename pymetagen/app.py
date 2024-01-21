from pathlib import Path

import click

from pymetagen import MetaGen, __version__
from pymetagen.utils import inspect_data


@click.group(
    "metagen", context_settings={"help_option_names": ["-h", "--help"]}
)
@click.version_option(version=__version__, prog_name="metagen")
def cli():
    """A tool to generate metadata tabular data with the ability to inspect
    data."""


@click.command(
    "metadata", context_settings={"help_option_names": ["-h", "--help"]}
)
@click.version_option(version=__version__, prog_name="metagen")
@click.option(
    "-i",
    "--input",
    type=click.Path(
        exists=True,
        file_okay=True,
        dir_okay=True,
        path_type=Path,
        readable=True,
    ),
    required=True,
    help="Input file path. Can be of type: .csv, .parquet, .xlsx, .json",
)
@click.option(
    "-o",
    "--output",
    type=click.Path(
        file_okay=True, dir_okay=False, path_type=Path, writable=True
    ),
    required=True,
    help="Output file path. Can be of type: .csv, .parquet, .xlsx, .json",
)
@click.option(
    "-d",
    "--descriptions",
    type=click.Path(
        exists=True,
        file_okay=True,
        dir_okay=False,
        path_type=Path,
        readable=True,
    ),
    required=False,
    help=(
        "(optional) Path to a JSON file containing descriptions for each"
        " column."
    ),
)
@click.option(
    "-m",
    "--mode",
    type=click.Choice(["lazy", "eager"], case_sensitive=False),
    callback=lambda ctx, param, value: value.lower(),
    default="eager",
    required=False,
    help="(optional) Whether to use lazy or eager mode. Defaults to eager.",
)
def metadata(
    input: Path, output: Path, descriptions: Path | None, mode: str
) -> None:
    """
    A tool to generate metadata for tabular data.
    """
    click.echo(f"Generating metadata for {input}...")
    metagen = MetaGen.from_path(
        path=input, descriptions_path=descriptions, mode=mode
    )
    metagen.write_metadata(outpath=output)


@click.command(
    "inspect", context_settings={"help_option_names": ["-h", "--help"]}
)
@click.option(
    "-i",
    "--input",
    type=click.Path(
        exists=True,
        file_okay=True,
        dir_okay=True,
        path_type=Path,
        readable=True,
    ),
    required=True,
    help="Input file path. Can be of type: .csv, .parquet, .xlsx, .json",
)
@click.option(
    "-m",
    "--mode",
    type=click.Choice(["lazy", "eager"], case_sensitive=False),
    callback=lambda ctx, param, value: value.lower(),
    default="lazy",
    required=False,
    help="(optional) Whether to use lazy or eager mode. Defaults to lazy.",
)
@click.option(
    "-n",
    "--number-rows",
    type=int,
    default=10,
    help="(optional) Maximum number of rows to show. Defaults to 10.",
)
@click.option(
    "--fmt-str-lengths",
    type=int,
    default=50,
    help=(
        "(optional) Maximum number of characters for string in a column."
        " Defaults to 50."
    ),
)
def inspect(
    input: Path, mode: str, number_rows: int, fmt_str_lengths: int
) -> None:
    """
    A tool to inspect a data set.
    """
    click.echo(f"Inspecting file {input}:")
    metagen = MetaGen.from_path(path=input, mode=mode)
    columns_length = len(metagen.data.columns)
    inspect_data(
        df=metagen.data,
        tbl_cols=columns_length,
        tbl_rows=number_rows,
        fmt_str_lengths=fmt_str_lengths,
    )


cli.add_command(metadata)
cli.add_command(inspect)


if __name__ == "__main__":
    cli(auto_envvar_prefix="METAGEN")
