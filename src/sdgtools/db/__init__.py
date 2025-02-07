import psycopg2
from psycopg2 import sql
import pandas as pd


def insert_scenario(conn_creds: dict, scenario_name: str, comments: str | None = None):
    conn = psycopg2.connect(**conn_creds)
    cur = conn.cursor()
    q = sql.SQL("INSERT INTO scenarios (name, comments) VALUES (%s, %s)")
    cur.execute(q, (scenario_name, comments))

    conn.commit()
    cur.close()
    conn.close()


def insert_dsm2_data(
    data: pd.DataFrame, scenario_name: str, table: str, conn_creds: dict
):
    """
    Insert data into database
    """
    conn = psycopg2.connect(**conn_creds)
    cur = conn.cursor()
    scenario_id_q = sql.SQL("SELECT id from scenarios where name = %s")
    cur.execute(scenario_id_q, (scenario_name,))
    scenario_id = cur.fetchall()
    if len(scenario_id) == 0:
        try:
            scenario_id = insert_scenario(scenario_name, conn_creds)
        except Exception as e:
            print(e)
    scenario_id = scenario_id[0][0]
    records = data.to_records(index=False)
    q = sql.SQL("INSERT INTO dsm2 ()")


def insert_scenario(scenario_name: str, conn_creds: dict):
    try:
        with psycopg2.connect(**conn_creds) as conn:
            with conn.cursor() as cur:
                insert_scenario_q = (
                    "INSERT INTO scenarios(name) VALUES(%s) RETURNING id"
                )
                cur.execute(insert_scenario_q, (scenario_name,))
                return cur.fetchall()
    except psycopg2.errors.UniqueViolation:
        print(f"the scenario with name '{scenario_name}' already exists")
