import click
from .dsm2_reader import read_hydro_dss, read_gates_dss
from hecdss import HecDss
import pandas as pd


@click.group()
def cli():
    """SDG Data Processing Tools"""


@cli.command()
@click.argument("file", type=str)
@click.argument("output", type=str)
def hydro(file, output):
    """Process DSS Output"""
    click.echo(f"processing the file: {file}")
    try:
        data: pd.DataFrame = read_hydro_dss(file)
        data.to_csv(output)
    except Exception as e:
        click.echo("oops! I can't process that kind of dss file")
        click.echo(e)


@cli.command()
@click.option("--name", prompt="your name", help="what is your name, supply this here")
def hello(name):
    click.echo(f"hello {name}")


if __name__ == "__main__":
    cli()
