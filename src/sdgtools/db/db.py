import psycopg2
import pandas as pd
from sdgtools.readers.scenario import ScenarioData


# insert all the data
def insert(scenario: ScenarioData):
    scenario_name = ScenarioData.name
    scenario_config = ScenarioData.echo_config
