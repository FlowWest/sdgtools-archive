import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
import pandas as pd
import io

def insert_dsm2_data(data: pd.DataFrame, scenario_name: str, conn_creds: dict | str):
    if isinstance(conn_creds, dict):
        conn_creds = f"postgresql://{conn_creds['user']}:{conn_creds['password']}@{conn_creds['host']}:{conn_creds['port']}/{conn_creds['dbname']}"

    with psycopg2.connect(conn_creds) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM scenarios WHERE name = %s", (scenario_name,))
            scenario_id = cur.fetchone()
            if not scenario_id:
                scenario_id = insert_scenario(scenario_name, conn_creds)
            scenario_id = scenario_id[0]

            data["scenario_id"] = scenario_id
            # data["datetime"] = data["datetime"].dt.to_pydatetime()
            data["datetime"] = data["datetime"].dt.strftime('%Y-%m-%d %H:%M:%S')

            buffer = io.StringIO()
            data.to_csv(buffer, index=False, header=False)
            buffer.seek(0)

            copy_sql = """
                COPY dsm2(datetime, node, param, value, unit, scenario_id)
                FROM STDIN WITH CSV
            """
            try:
                cur.copy_expert(copy_sql, buffer)
                conn.commit()
            except Exception as e:
                conn.rollback()
                print(f"Error during COPY: {e}")

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
