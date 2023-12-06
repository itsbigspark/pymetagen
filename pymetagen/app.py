from pathlib import Path

import click

from pymetagen import MetaGen, __version__


@click.command(
    "metagen", context_settings={"help_option_names": ["-h", "--help"]}
)
@click.version_option(version=__version__, prog_name="metagen")
@click.option(
    "-i",
    "--input",
    type=click.Path(
        exists=True, file_okay=True, dir_okay=False, path_type=Path
    ),
    required=True,
    help="Input file path. Can be of type: .csv, .parquet, .xlsx, .json",
)
@click.option(
    "-o",
    "--output",
    type=click.Path(file_okay=True, dir_okay=False, path_type=Path),
    required=True,
    help="Output file path. Can be of type: .csv, .parquet, .xlsx, .json",
)
def cli(input: Path, output: Path) -> None:
    """
    A tool to generate metadata for tabular data.
    """
    click.echo(f"Generating metadata for {input}...")
    metagen = MetaGen.from_path(path=input)
    metagen.write_metadata(outpath=output)


if __name__ == "__main__":
    cli(auto_envvar_prefix="METAGEN")
