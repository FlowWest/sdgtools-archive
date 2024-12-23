from .dsm2_reader import read_hydro_dss, read_gates_dss
from .h5_reader import get_output_channel_names
import pandas as pd
import rich_click as click
import numpy as np
import sqlite3
import h5py

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


@cli.command()
@click.argument("file", type=str)
@click.argument("output", type=str)
@click.option(
    "-d",
    "--database-file",
    default=None,
    help="should results be inserted into database? Ommit to just write to file, or provide path to sqlite3 database to perform data insert",
)
def hydro(file, output, database_file):
    """
    Process Hydro DSS Output.

    Processing Hydro DSS Output assumes the following from the DSS parts.

    * Part B: gate, column name "gate"
    * Part C: measured parameter, column name "parameter"
    * Part F: scenario, column name "scenario"
    """
    click.echo(click.style(f"processing the file: {file}", fg="green"))
    try:
        data: pd.DataFrame = read_hydro_dss(file)
        click.echo(click.style("\nStarting csv write...\n", fg="green"))
        data.to_csv(output, index=False)
        click.echo(click.style(f"\nfinished writting to file {output}\n", fg="green"))

    except Exception as e:
        click.echo(click.style(f"processing the file: {file}", fg="red"))
        click.echo(
            click.style(
                "\n\noops! I can't process that kind of dss file, most likely reason is dss file must be version 7",
                fg="red",
            )
        )
        return

    if database_file is not None:
        try:
            click.echo(
                click.style(
                    f"writting results to database {database_file}...", fg="green"
                )
            )
            insert_into_database(data, database_file)
        except Exception as e:
            click.echo(click.style(f"Error writting to databae file", fg="red"))
            click.echo(click.style(f"full error from database\n{e}", fg="red"))
        click.echo(click.style(f"done!\n", fg="green"))


@cli.command()
@click.argument("file")
@click.argument("output")
def sdg(file, output, delim):
    """
    Process SDG DSS Output.

    __FILE__: the dss file to process

    __OUTPUT__: the filename to create for processed data

    Processing SDG DSS Output assumes the following from the DSS parts.

    * Part B: gate operation, column name "gate_op"
    * Part F: scenario name, column name "scenario"
    """
    click.echo(f"processing file {file}")
    try:
        data: pd.DataFrame = read_gates_dss(file)
        data.to_csv(output)
    except Exception as e:
        click.echo("unable to process file")
        click.echo(e)


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


if __name__ == "__main__":
    cli()
