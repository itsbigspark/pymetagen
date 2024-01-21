from pathlib import Path

import click

from pymetagen import MetaGen, __version__


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
    type=click.INT,
    default=10,
    help="(optional) Maximum number of rows to show. Defaults to 10.",
)
@click.option(
    "--fmt-str-lengths",
    type=click.INT,
    default=50,
    help=(
        "(optional) Maximum number of characters for string in a column."
        " Defaults to 50."
    ),
)
@click.option(
    "-im",
    "--inspection-mode",
    type=click.Choice(["head", "tail", "sample"], case_sensitive=False),
    callback=lambda ctx, param, value: value.lower(),
    default="head",
    required=False,
    help=(
        "(optional) Whether to use head, tail or a random sample inspection"
        " mode. Defaults to head."
    ),
)
@click.option(
    "--random-seed",
    type=click.INT,
    default=None,
    required=False,
    help=(
        "(optional) Seed for the random number generator when the sample"
        " inspect mode option is activated. Defaults to None."
    ),
)
@click.option(
    "--with-replacement",
    type=click.BOOL,
    default=False,
    required=False,
    help=(
        "(optional) Allow values to be sampled more than once when the sample"
        " inspect mode option is activated. Defaults to False."
    ),
)
def inspect(
    input: Path,
    mode: str,
    number_rows: int,
    fmt_str_lengths: int,
    inspection_mode: str,
    random_seed: int,
    with_replacement: bool,
) -> None:
    """
    A tool to inspect a data set.
    """
    click.echo(f"Inspecting file {input}:")
    metagen = MetaGen.from_path(path=input, mode=mode)
    columns_length = len(metagen.data.columns)
    metagen.inspect_data(
        mode=mode,
        tbl_cols=columns_length,
        tbl_rows=number_rows,
        fmt_str_lengths=fmt_str_lengths,
        inspection_mode=inspection_mode,
        random_seed=random_seed,
        with_replacement=with_replacement,
    )


@click.command(
    "extract", context_settings={"help_option_names": ["-h", "--help"]}
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
    "-o",
    "--output",
    type=click.Path(
        file_okay=True, dir_okay=False, path_type=Path, writable=True
    ),
    required=True,
    help="Output file path. Can be of type: .csv, .parquet, .xlsx, .json",
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
    type=click.INT,
    default=10,
    help="(optional) Maximum number of rows to show. Defaults to 10.",
)
@click.option(
    "-im",
    "--extraction-mode",
    type=click.Choice(["head", "tail", "sample"], case_sensitive=False),
    callback=lambda ctx, param, value: value.lower(),
    default="head",
    required=False,
    help=(
        "(optional) Whether to use head, tail or a random sample extraction"
        " mode. Defaults to head."
    ),
)
@click.option(
    "--random-seed",
    type=click.INT,
    default=None,
    required=False,
    help=(
        "(optional) Seed for the random number generator when the sample"
        " extraction mode option is activated. Defaults to None."
    ),
)
@click.option(
    "--with-replacement",
    type=click.BOOL,
    default=False,
    required=False,
    help=(
        "(optional) Allow values to be sampled more than once when the sample"
        " extraction mode option is activated. Defaults to False."
    ),
)
def extract(
    input: Path,
    output: Path,
    mode: str,
    number_rows: int,
    extraction_mode: str,
    random_seed: int,
    with_replacement: bool,
) -> None:
    """
    A tool to inspect a data set.
    """
    click.echo(
        f"Creating {extraction_mode} extract from file {input} in {output}"
    )
    metagen = MetaGen.from_path(path=input, mode=mode)
    metagen.extract_data(
        mode=mode,
        tbl_rows=number_rows,
        inspection_mode=extraction_mode,
        random_seed=random_seed,
        with_replacement=with_replacement,
        inplace=True,
    )
    metagen.write_data(outpath=output)


cli.add_command(metadata)
cli.add_command(inspect)
cli.add_command(extract)


if __name__ == "__main__":
    cli(auto_envvar_prefix="METAGEN")
