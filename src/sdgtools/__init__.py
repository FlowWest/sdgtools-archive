from .dss_reader import get_all_data_from_dsm2_dss, make_regex_from_parts
from .h5_reader import get_output_channel_names
from .echo import read_echo_file
from .db import insert_dsm2_data
import pandas as pd
import rich_click as click
import sqlite3
import h5py
import os
import pathlib
from contextlib import contextmanager
# from hecdss import HecDss

click.rich_click.USE_MARKDOWN = True


def insert_into_database(data, db):
    db_conn = sqlite3.connect(db)
    db_cursor = db_conn.cursor()
    records = data.to_dict(orient="records")
    db_cursor.executemany(
        """INSERT INTO data (datetime, node, parameter, value, unit, scenario)
                            VALUES (:datetime, :node, :parameter, :value, :unit, :scenario)""",
        records,
    )

    db_conn.commit()
    db_conn.close()


@click.group(
    help="""
        SDG Data Processing Tools

        This command line interface provides a set tools for processing dss and hdf5 output from DSM2
        simulations. For more information use --help on any of the subcommands.
        """,
)
def cli(): ...


# DSS processing
@cli.command()
@click.argument("file", type=str)
@click.argument("output", type=str)
@click.option(
    "-f",
    "--regex-filter",
    help="regex filter used to subset the dss by parts. You can create a regex filter from parts using the utility function `make_regex_from_parts`",
    default=make_regex_from_parts(),
)
def dss(file, output, regex_filter):
    """
    Process DSM2 DSS Output.


    Processing DSS Output uses functions from pyhecdss and allows for extracting all or a subset of data
    from a dss file. You can optionally pass in a regex filter which will be applied to subset the dss by
    parts.
    """
    try:
        if not os.path.exists(file):
            click.secho(f"Error: File not found {file}", err=True, fg="red", nl=True)
            return

        data: pd.DataFrame = get_all_data_from_dsm2_dss(file, regex_filter)

        if len(data) == 0:
            click.secho("no data returned", fg="green")
            return

        click.echo(click.style("\nStarting csv write...", fg="green"))
        data.to_csv(output, index=False)
        click.secho("finished writing to file: ", fg="green", nl=False)
        click.secho(f"{output}", fg="yellow", nl=True)

    except Exception as e:
        click.echo(click.style(f"processing the file: {file}", fg="red"))
        click.echo(
            click.style(
                "\n\nUnable to process DSS file",
                fg="red",
            )
        )
        click.echo(print(e))
        return


@cli.command()
@click.argument("file")
def h5(file):
    """
    Process HDF5 File
    """
    click.echo("processing h5 file")
    try:
        h5 = h5py.File(file, "r")
        data = get_output_channel_names(h5)
        h5.close()
        print(data)
        click.echo("done!")
        return data
    except FileNotFoundError as e:
        click.echo(
            click.style(
                f"The file provided '{file}' was not found! Please check path to file and try again",
                fg="red",
            )
        )


@cli.command()
@click.argument("echofile")
@click.option(
    "--table", "-t", help="a commma seperated list of tables to read from echo file"
)
def echo(echofile, table):
    """
    Process Echo file: csv will be generated for all tables extracted from the echo file

    """
    tables = table.split(",")
    click.echo(f"processing {tables}")
    filepath = pathlib.Path(echofile)
    data = read_echo_file(filepath, tables)
    for k, v in data.items():
        v.to_csv(f"{k}_echo_file.csv", index=False)


@cli.command()
@click.argument("input_file")
@click.argument("output_file")
@click.option("--kind", "-k", help="perform this post process routine")
def process(input_file, output_file, kind):
    """
    Perform post process on existing CSV file.

    """
    ...


@cli.group(
    help="""
    Database interactions.

    This command allows you to interfact with the datasbase. You can upload and query the system
    to export data.
    """
)
def db(): ...


@db.command()
@click.argument("file")
@click.argument("scenario_name")
@click.argument("connection_string")
def insert(file: str, scenario_name: str, connection_string: str):
    """
    Database: Insert Scenario Data
    """
    data = pd.read_csv(file, dtype={"scenario_id": int, "value": float})
    insert_dsm2_data(data, scenario_name, connection_string)


if __name__ == "__main__":
    cli()
