import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
import pandas as pd


def insert_dsm2_data(data: pd.DataFrame, scenario_name: str, conn_creds: dict | str):
    """
    Insert data into database
    """
    if isinstance(conn_creds, dict):
        conn_creds = f"postgresql://{conn_creds['user']}:{conn_creds['password']}@{conn_creds['host']}:{conn_creds['port']}/{conn_creds['dbname']}"

    with psycopg2.connect(conn_creds) as conn:
        with conn.cursor() as cur:
            scenario_id_q = sql.SQL("SELECT id from scenarios where name = %s")
            cur.execute(scenario_id_q, (scenario_name,))
            scenario_id = cur.fetchall()
            if len(scenario_id) == 0:
                try:
                    scenario_id = insert_scenario(scenario_name, conn_creds)
                except Exception as e:
                    print(e)
            scenario_id = scenario_id[0][0]
            data["scenario_id"] = int(scenario_id)
            data["datetime"] = data["datetime"].dt.to_pydatetime()
            query = """
                INSERT INTO dsm2 (datetime, node, param, value, unit, scenario_id)
                VALUES %s
                ON CONFLICT (scenario_id, datetime, node, param) DO NOTHING
            """
            page_size = 100000
            records_dict = data.to_dict(orient='records')
            t = [tuple(record.values()) for record in records_dict]
            execute_values(cur, query, t, page_size=page_size)


def insert_scenario(scenario_name: str, conn_creds: str):
    try:
        with psycopg2.connect(conn_creds) as conn:
            with conn.cursor() as cur:
                insert_scenario_q = (
                    "INSERT INTO scenarios(name) VALUES(%s) RETURNING id"
                )
                cur.execute(insert_scenario_q, (scenario_name,))
                return cur.fetchall()
    except psycopg2.errors.UniqueViolation:
        print(f"the scenario with name '{scenario_name}' already exists")
