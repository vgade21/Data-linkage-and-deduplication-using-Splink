import os
import pandas as pd
from src.core.table_processor import TableCleaner
from unify_dm_core.core_connection import DatabaseConnector
from unify_dm_core.core_config import YAMLConfig
from unify_dm_core.core_sql_runner import SQLRunner
from src.systems.fvms_staging.tables.person.person_a.person_a import Person_a
from src.systems.fvms_staging.tables.person.person_b.person_b import Person_b
from src.systems.fvms_staging.tables.person.person_c.person_c import Person_c
from src.systems.fvms_staging.tables.person.person_d.person_d import Person_d


class Person(TableCleaner):
    def __init__(self, df: pd.DataFrame = pd.DataFrame()):
        self.df = df
        self.table_name = self.__class__.__name__.lower()

    def load(self, *args):
        self.config: YAMLConfig = args[1]
        self.fvms_staging_conn = DatabaseConnector(
            self.config.config["TARGET_SERVER_ADDRESS"],
            self.config.config["FVMS_STAGE_SYSTEM_LOADER"],
            self.config,
        )
        self.server = self.config.config["TARGET_SERVER_ADDRESS"]
        self.database = self.config.config["FVMS_STAGE_SYSTEM_LOADER"]

    def process(self, df):
        persons = (Person_a(), Person_b(), Person_c(), Person_d())

        for p in persons:
            p.execute(self.config, self.fvms_staging_conn)

        sql_file_name = [
            "fv_invp_DQI24599.sql",  # person_e
            "fv_all_person_lookup.sql",  # person_f
            "fv_person.sql",  # person_g
        ]
        file_path = os.path.dirname(os.path.realpath(__file__))
        sql_runner = SQLRunner(server=self.server, database=self.database)
        sql_runner.runs_multi(file_path, sql_file_name)

    def upload(self, conn):
        pass
