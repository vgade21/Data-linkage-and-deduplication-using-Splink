from loguru import logger
import pandas as pd
import re
import os
import numpy as np
import phonetics
import warnings

from unify_dm_core.core_sqlalchemy import SQLAlchemyOperations
from sqlalchemy.dialects.mssql import INTEGER, VARCHAR, BIT, DATETIME
from unify_dm_core.core_connection import DatabaseConnector
from unify_dm_core.core_config import YAMLConfig
from unify_dm_core.core_sql_runner import SQLRunner
from splink import Linker, DuckDBAPI

pd.options.mode.chained_assignment = None
warnings.filterwarnings("ignore", category=UserWarning)

# Script Purpose: Use a trained linking model to link child records and de-duplicate them.
#                 https://moj-analytical-services.github.io/splink/index.html
# Feature DQI: 24183, 35420, 37973, 24127
# Input: Original data from child and involved_person tables along with suburb and contact_number (actual number)
#        The child data is split into exact matches (df_new_duplicates_exact) and the rest (df_training_data), we apply the linking model to the rest of the data.
# Output: a mapping table (DO_NOT_MIGRATE_child_dedup) between the child table ids and master_id (updated_cluster_id).

query_4 = """
WITH PersonLink AS (
    SELECT
        l.*,
        ROW_NUMBER() OVER (PARTITION BY l.id_dbo_child ORDER BY l.incident_id_dbo_involved_person DESC) AS rn
    FROM
        fvms_clean.dbo.DO_NOT_MIGRATE_Names_LinkTable l
),
CTE_person_data AS (
	SELECT
		p.id,
		p.country_of_origin_id,
		p.date_of_birth,
		p.ethnicity_other,
		p.indigenous_status_id,
		p.sex,
        p.year_of_arrival,
        p.child_cni_number,
        p.child_version,
		-- Split given_names
		LEFT(l.given_names_dbo_involved_person, CHARINDEX(' ', l.given_names_dbo_involved_person + ' ') - 1) AS given_name_1,
		CASE
			WHEN CHARINDEX(' ', l.given_names_dbo_involved_person) > 0
			THEN LEFT(
					STUFF(l.given_names_dbo_involved_person, 1, CHARINDEX(' ', l.given_names_dbo_involved_person), ''),
					CHARINDEX(' ', STUFF(l.given_names_dbo_involved_person, 1, CHARINDEX(' ', l.given_names_dbo_involved_person), '') + ' ') - 1
				)
			ELSE NULL
		END AS given_name_2,
		CASE
			WHEN CHARINDEX(' ', l.given_names_dbo_involved_person) > 0
				AND CHARINDEX(' ', STUFF(l.given_names_dbo_involved_person, 1, CHARINDEX(' ', l.given_names_dbo_involved_person), '')) > 0
			THEN LTRIM(
					STUFF(
						STUFF(l.given_names_dbo_involved_person, 1, CHARINDEX(' ', l.given_names_dbo_involved_person), ''),
						1, CHARINDEX(' ', STUFF(l.given_names_dbo_involved_person, 1, CHARINDEX(' ', l.given_names_dbo_involved_person), '')), ''
					)
				)
			ELSE NULL
		END AS given_name_3,
		l.surname_dbo_involved_person as surname,
		-- Contact number priority
		COALESCE(l.mobile_phone_dbo_involved_person, l.home_phone_dbo_involved_person, l.work_phone_dbo_involved_person) AS contact_number,
		a.suburb
	FROM
		fvms_clean.dbo.child p
		LEFT JOIN PersonLink l
			ON p.id = l.id_dbo_child AND l.rn = 1
		LEFT JOIN fvms_clean.dbo.address a
			ON l.address_id_dbo_involved_person = a.id
),
CTE_Deduplicated AS (
    SELECT
		p.id,
		p.country_of_origin_id,
		p.date_of_birth,
		p.ethnicity_other,
		p.indigenous_status_id,
		p.sex,
        p.year_of_arrival,
        p.child_cni_number,
        p.child_version,
        p.given_name_1,
        p.given_name_2,
        p.given_name_3,
        p.surname,
        P.contact_number,
        P.suburb,
        ROW_NUMBER() OVER (
            PARTITION BY p.date_of_birth, p.given_name_1, p.given_name_2, p.given_name_3, p.surname, p.sex
            ORDER BY p.id
        ) AS rn
    FROM
        CTE_person_data p
	WHERE (given_name_1 IS NULL OR LOWER(given_name_1) NOT LIKE '%unborn%' AND LOWER(given_name_1) NOT LIKE '%unbourn%')
    AND (given_name_2 IS NULL OR LOWER(given_name_2) NOT LIKE '%unborn%' AND LOWER(given_name_2) NOT LIKE '%unbourn%')
    AND (given_name_3 IS NULL OR LOWER(given_name_3) NOT LIKE '%unborn%' AND LOWER(given_name_3) NOT LIKE '%unbourn%')
    AND (surname      IS NULL OR LOWER(surname)      NOT LIKE '%unborn%' AND LOWER(surname)      NOT LIKE '%unbourn%')
    -- At least one of each pair is not NULL
    AND NOT (date_of_birth IS NULL AND given_name_1 IS NULL)
    AND NOT (date_of_birth IS NULL AND surname IS NULL)
    AND NOT (given_name_1 IS NULL AND surname IS NULL)
)
SELECT
    id,
    country_of_origin_id,
    date_of_birth,
    ethnicity_other,
    indigenous_status_id,
    sex,
    year_of_arrival,
    child_cni_number,
    child_version,
    given_name_1,
    given_name_2,
    given_name_3,
    surname,
    contact_number,
    suburb
FROM
    CTE_Deduplicated
WHERE
    rn = 1;
"""

# Loading the exact matches found in person table (we DO NOT run the linking model on this set data).
query_8 = """
WITH PersonLink AS (
    SELECT
        l.*,
        ROW_NUMBER() OVER (PARTITION BY l.id_dbo_child ORDER BY l.incident_id_dbo_involved_person DESC) AS rn
    FROM
        fvms_clean.dbo.DO_NOT_MIGRATE_Names_LinkTable l
),
CTE_person_data AS (
	SELECT
		p.id,
		p.country_of_origin_id,
		p.date_of_birth,
		p.ethnicity_other,
		p.indigenous_status_id,
		p.sex,
        p.year_of_arrival,
        p.child_cni_number,
        p.child_version,
		-- Split given_names
		LEFT(l.given_names_dbo_involved_person, CHARINDEX(' ', l.given_names_dbo_involved_person + ' ') - 1) AS given_name_1,
		CASE
			WHEN CHARINDEX(' ', l.given_names_dbo_involved_person) > 0
			THEN LEFT(
					STUFF(l.given_names_dbo_involved_person, 1, CHARINDEX(' ', l.given_names_dbo_involved_person), ''),
					CHARINDEX(' ', STUFF(l.given_names_dbo_involved_person, 1, CHARINDEX(' ', l.given_names_dbo_involved_person), '') + ' ') - 1
				)
			ELSE NULL
		END AS given_name_2,
		CASE
			WHEN CHARINDEX(' ', l.given_names_dbo_involved_person) > 0
				AND CHARINDEX(' ', STUFF(l.given_names_dbo_involved_person, 1, CHARINDEX(' ', l.given_names_dbo_involved_person), '')) > 0
			THEN LTRIM(
					STUFF(
						STUFF(l.given_names_dbo_involved_person, 1, CHARINDEX(' ', l.given_names_dbo_involved_person), ''),
						1, CHARINDEX(' ', STUFF(l.given_names_dbo_involved_person, 1, CHARINDEX(' ', l.given_names_dbo_involved_person), '')), ''
					)
				)
			ELSE NULL
		END AS given_name_3,
        l.surname_dbo_involved_person as surname,
		-- Contact number priority
		COALESCE(l.mobile_phone_dbo_involved_person, l.home_phone_dbo_involved_person, l.work_phone_dbo_involved_person) AS contact_number,
		a.suburb
	FROM
		fvms_clean.dbo.child p
		LEFT JOIN PersonLink l
			ON p.id = l.id_dbo_child AND l.rn = 1
		LEFT JOIN fvms_clean.dbo.address a
			ON l.address_id_dbo_involved_person = a.id
),
DuplicateRecords AS (
    SELECT
		p.id,
		p.country_of_origin_id,
		p.date_of_birth,
		p.ethnicity_other,
		p.indigenous_status_id,
		p.sex,
        p.year_of_arrival,
        p.child_cni_number,
        p.child_version,
        p.given_name_1,
        p.given_name_2,
        p.given_name_3,
        p.surname,
        P.contact_number,
        P.suburb,
        ROW_NUMBER() OVER (
            PARTITION BY p.date_of_birth, p.given_name_1, p.given_name_2, p.given_name_3, p.surname, p.sex
            ORDER BY p.id
        ) AS RowNum,
        -- Assign master_id
        CASE
        WHEN p.given_name_1 IS NULL AND p.surname IS NULL THEN p.id -- Handle case where given_name_1 and surname are NULL
        -- If any of the columns contain 'unborn' or 'unbourn'
        WHEN
			(p.given_name_1		IS NOT NULL AND LOWER(p.given_name_1) LIKE '%unborn%' OR LOWER(p.given_name_1) LIKE '%unbourn%')
            OR (p.given_name_2	IS NOT NULL AND LOWER(p.given_name_2) LIKE '%unborn%' OR LOWER(p.given_name_2) LIKE '%unbourn%')
            OR (p.given_name_3	IS NOT NULL AND LOWER(p.given_name_3) LIKE '%unborn%' OR LOWER(p.given_name_3) LIKE '%unbourn%')
            OR (p.surname		IS NOT NULL AND LOWER(p.surname)      LIKE '%unborn%' OR LOWER(p.surname)      LIKE '%unbourn%')
			OR (p.date_of_birth IS NULL		AND p.given_name_1 IS NULL)
			OR (p.date_of_birth IS NULL		AND p.surname IS NULL)
			OR (p.given_name_1	IS NULL		AND p.surname IS NULL)

        THEN p.id
        ELSE MIN(p.id) OVER (
            PARTITION BY
                p.given_name_1, p.given_name_2, p.given_name_3, p.surname, p.date_of_birth, p.sex
        )
        END AS master_id
    FROM
        CTE_person_data p
)
SELECT
    id,
    country_of_origin_id,
    date_of_birth,
    ethnicity_other,
    indigenous_status_id,
    sex,
    year_of_arrival,
    child_cni_number,
    child_version,
    given_name_1,
    given_name_2,
    given_name_3,
    surname,
    contact_number,
    suburb,
    master_id
FROM
    DuplicateRecords
WHERE
    RowNum > 1
    OR (given_name_1 IS NOT NULL AND given_name_1 LIKE '%unborn%' OR given_name_1 LIKE '%unbourn%')
    OR (given_name_2 IS NOT NULL AND given_name_2 LIKE '%unborn%' OR given_name_2 LIKE '%unbourn%')
    OR (given_name_3 IS NOT NULL AND given_name_3 LIKE '%unborn%' OR given_name_3 LIKE '%unbourn%')
    OR (surname      IS NOT NULL AND surname      LIKE '%unborn%' OR surname      LIKE '%unbourn%')
	OR (date_of_birth IS NULL		AND given_name_1 IS NULL)
	OR (date_of_birth IS NULL		AND surname IS NULL)
	OR (given_name_1	IS NULL		AND surname IS NULL)
ORDER BY
    master_id, -- Group duplicates together
    id; -- Ensure duplicates are ordered correctly
"""


class Person_b:
    def __init__(self, df: pd.DataFrame = pd.DataFrame()):
        self.df = df
        self.table_name = self.__class__.__name__.lower()

    def execute(self, config: YAMLConfig, staging_conn: DatabaseConnector):
        df = self.load(config)
        self.df = self.process(df)
        self.upload(staging_conn)

    def load(self, config: YAMLConfig):
        # creating a new engine as earlier script uses the staging database
        clean_conn = DatabaseConnector(
            config.config["TARGET_SERVER_ADDRESS"],
            config.config["FVMS_CLEAN_SYSTEM_LOADER"],
            config,
        )
        sql_ops = SQLAlchemyOperations(clean_conn.engine, self.table_name)
        self.df_new_duplicates_exact = sql_ops.get_table_by_query(query_8)
        self.df_training_data = sql_ops.get_table_by_query(query_4)
        return self.df

    def process(self, df):
        self.df = df
        self.person_lookup = self.dedup_child(self.df_new_duplicates_exact, self.df_training_data)

    def upload(self, conn: DatabaseConnector):
        col_type = {
            "id": VARCHAR(),
            "country_of_origin_id": INTEGER(),
            "date_of_birth": DATETIME(),
            "ethnicity_other": VARCHAR(),
            "indigenous_status_id": INTEGER(),
            "sex": VARCHAR(),
            "year_of_arrival": VARCHAR(),
            "child_cni_number": VARCHAR(),
            "child_version": INTEGER(),
            "given_name_1": VARCHAR(),
            "given_name_2": VARCHAR(),
            "given_name_3": VARCHAR(),
            "surname": VARCHAR(),
            "contact_number": VARCHAR(),
            "suburb": VARCHAR(),
            "name_concat": VARCHAR(),
            "new_id": INTEGER(),
            "updated_cluster_id": VARCHAR(),
            "can_exclude": BIT(),
        }
        sql_ops = SQLAlchemyOperations(conn.engine, self.table_name)
        sql_ops.write_to_db_bcp(self.person_lookup, "DO_NOT_MIGRATE_child_dedup", types=col_type)
        self.clean_via_sql()

    def clean_via_sql(self):
        # ADD new sql script here
        sql_file_name = ["fv_dedup_child.sql"]
        file_path = os.path.dirname(os.path.realpath(__file__))

        # creating a new engine as '.sql' scripts use the staging database
        config = YAMLConfig("local_configuration.yaml")
        server = config.config["TARGET_SERVER_ADDRESS"]
        database = config.config["FVMS_STAGE_SYSTEM_LOADER"]
        sql_runner = SQLRunner(server, database)
        sql_runner.runs_multi(file_path, sql_file_name)

    def dedup_child(self, df_new_duplicates_exact: pd.DataFrame, df_training_data: pd.DataFrame) -> pd.DataFrame:
        """
        Merging the clustered child records with the exact match child records.
        The resulting clusters_pandas_df_6 contains:
            All original records from df_training_data.
            Records from df_new_duplicates_exact with the appropriate cluster_id:
            Either the matched cluster_id from clusters_pandas_df or the id from df_new_duplicates_exact if no match was found.
        """

        # --------------------------------------------------------------------------------
        # 0) Data Prerequisites
        # --------------------------------------------------------------------------------

        # Function to clean apostrophes, hyphens, and lowercase fields for specified columns
        def clean_dataframe_columns(df, columns_to_clean):
            """
            Cleans the specified columns in a dataframe by:
            - Removing apostrophes (') and hyphens (-)
            - Converting text to lowercase
            """

            def clean_field(value):
                if isinstance(value, str):  # Ensure the value is a string
                    value = value.replace("'", "").replace("-", "").lower()
                return value

            # Create a copy of the dataframe
            df_cleaned = df.copy()
            # Apply the cleaning function to each specified column
            for column in columns_to_clean:
                df_cleaned[column] = df_cleaned[column].apply(clean_field)

            return df_cleaned

        # Columns to clean
        columns_to_clean = ["given_name_1", "given_name_2", "given_name_3", "surname"]
        # Clean the dataframe
        df_training_data_2A = clean_dataframe_columns(df_training_data, columns_to_clean)

        """
        function to clean the suburb column as per below:
        1) Remove everything between brackets and the brackets themselves
        2) Make blank if contains '-' or '/'
        3) Remove numbers at the beginning or end
        4) Remove ' and replace '.' with a single space
        5) Remove everything before and including certain road-type keywords (with optional comma), only if keyword is at word boundary
        6) Remove everything after a comma and including the comma
        7) Remove everything after a number (and the number itself) found anywhere in the string
        8) Replace multiple spaces with a single space, and strip leading/trailing spaces
        """

        def clean_suburb(suburb):
            if pd.isna(suburb):
                return None
            s = str(suburb).strip()
            if not s:
                return None

            # 1)
            s = re.sub(r"\([^)]*\)", "", s)

            # 2)
            if "-" in s or "/" in s:
                return None

            # 3)
            s = re.sub(r"^\d+", "", s)  # start
            s = re.sub(r"\d+$", "", s)  # end

            # 4)
            s = s.replace("'", "")
            s = s.replace(".", " ")

            # 5)
            keywords = ["road", "street", "avenue", "drive", "highway", "rd", "crescent", "st", "parade"]
            pattern = r"(?:\s(?:" + "|".join(keywords) + r")(?:,)?)(.*)$"
            match = re.search(pattern, s, flags=re.IGNORECASE)
            if match:
                s = match.group(1).strip()

            # 6)
            s = s.split(",", 1)[0].strip()

            # 7)
            s = re.split(r"\d", s, maxsplit=1)[0]

            # 8)
            s = re.sub(r"\s+", " ", s).strip()

            return s if s else None

        # Apply to the dataframe
        df_training_data_2 = df_training_data_2A.copy()
        df_training_data_2["suburb"] = df_training_data_2A["suburb"].apply(clean_suburb)
        df_new_duplicates_exact_2 = df_new_duplicates_exact.copy()
        df_new_duplicates_exact_2["suburb"] = df_new_duplicates_exact_2["suburb"].apply(clean_suburb)

        # Creating the double metaphone and concatenated columns
        def create_columns(df):
            def dmetaphone_name(name):
                if name is None:
                    return None
                else:
                    return phonetics.dmetaphone(name)

            # Create a copy of the input dataframe
            df_new = df.copy()

            # Apply double metaphone to given_name_1, given_name_2, and surname
            df_new["given_name_1_dm"] = df_new["given_name_1"].apply(dmetaphone_name)
            df_new["given_name_2_dm"] = df_new["given_name_2"].apply(dmetaphone_name)
            df_new["surname_dm"] = df_new["surname"].apply(dmetaphone_name)

            # Create the concatenated given_names and surname column
            df_new["name_concat"] = df_new["given_name_1"].fillna("") + " " + df_new["surname"].fillna("")
            df_new["name_concat"] = df_new["name_concat"].str.strip()

            return df_new

        df_training_data_3 = create_columns(df_training_data_2)

        # --------------------------------------------------------------------------------
        # 4) Initialize the linker and run the model training
        # --------------------------------------------------------------------------------

        # Loading a pre-trained model
        settings = "./splink_models/fvms_child_model_1e.json"
        linker = Linker(df_training_data_3, settings, db_api=DuckDBAPI())

        # --------------------------------------------------------------------------------
        # 5) Predict matches and retrieve the match scores
        # --------------------------------------------------------------------------------

        print()
        # Predict matching records with a threshold_match_probability
        df_predictions = linker.inference.predict(threshold_match_probability=0.95)

        # Converting to a Pandas dataframe
        # 40,209 records (pairwise comparisons above the threshold)
        df_predictions_pandas = df_predictions.as_pandas_dataframe()

        print()
        # Clusters the pairwise match predictions into groups of connected records.
        # Records with an estimated match probability at or above threshold_match_probability are considered to be a match (i.e. they represent the same entity).
        df_clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(  # noqa
            df_predictions, threshold_match_weight=22
        )
        # Converting to a Pandas dataframe
        # 64,512 records (same number of records as df_training_data_2)
        clusters_pandas_df = df_clusters.as_pandas_dataframe()

        # 64,512 records - training data (first duplicated (exact) row + rest)
        # 102,779 records - exact match data (remaining duplicated (exact) rows)
        logger.info("")
        logger.info(
            f"Row count of training data: {len(df_training_data_3)} and Row count of exact match data: {len(df_new_duplicates_exact)} and total dataset for prediction: {len(df_new_duplicates_exact) + len(df_training_data_3)}"
        )
        logger.info(f"Row count of predictions: {len(df_predictions_pandas)}")
        logger.info(f"Row count of clusters: {len(clusters_pandas_df)}")

        # --------------------------------------------------------------------------------
        # 7) Post-processing to generate final mapping and de-dup tables
        # --------------------------------------------------------------------------------

        def process_clusters(exact, clusters):
            exact_df = exact.copy()
            clusters_df = clusters.copy()
            # Get unique master_id values from exact_df
            unique_master_ids = exact_df["master_id"].unique()

            # Find cluster_id in clusters_df for these master_ids using the id column as the link
            master_id_to_cluster_id = clusters_df[clusters_df["id"].isin(unique_master_ids)][["id", "cluster_id"]]

            # Merge master_id to cluster_id mapping back to exact_df
            df_new_duplicates_exact_with_cluster = pd.merge(
                exact_df,
                master_id_to_cluster_id,
                left_on="master_id",
                right_on="id",
                suffixes=("_duplicate", "_cluster"),
                how="left",
            )

            # For master_ids not found in clusters_df, set cluster_id to the original id
            df_new_duplicates_exact_with_cluster["cluster_id"] = df_new_duplicates_exact_with_cluster[
                "cluster_id"
            ].fillna(df_new_duplicates_exact_with_cluster["id_duplicate"])

            # Select only the necessary columns for appending
            df_new_duplicates_to_append = df_new_duplicates_exact_with_cluster[
                [
                    "cluster_id",
                    "id_duplicate",
                    "country_of_origin_id",
                    "date_of_birth",
                    "ethnicity_other",
                    "indigenous_status_id",
                    "sex",
                    "year_of_arrival",
                    "child_cni_number",
                    "child_version",
                    "given_name_1",
                    "given_name_2",
                    "given_name_3",
                    "surname",
                    "contact_number",
                    "suburb",
                ]
            ].rename(columns={"id_duplicate": "id"})

            # Append records from df_new_duplicates_to_append to clusters_df
            clusters_df_2 = pd.concat([clusters_df, df_new_duplicates_to_append], ignore_index=True)

            # Sort the dataframe by cluster_id and id in ascending order
            clusters_df_2 = clusters_df_2.sort_values(by=["cluster_id", "id"], ascending=True)

            clusters_df_2 = clusters_df_2[
                [
                    "cluster_id",
                    "id",
                    "country_of_origin_id",
                    "date_of_birth",
                    "ethnicity_other",
                    "indigenous_status_id",
                    "sex",
                    "year_of_arrival",
                    "child_cni_number",
                    "child_version",
                    "given_name_1",
                    "given_name_2",
                    "given_name_3",
                    "surname",
                    "contact_number",
                    "suburb",
                    "name_concat",
                ]
            ]

            return clusters_df_2

        # Pandas datetime objects (used in datetime64[ns] columns) only support dates between 1677-09-21 and 2262-04-11. Dates outside this range (eg: 2988-02-16 - id: 407524) are considered invalid, leading to the OutOfBoundsDatetime error.
        # Function to handle invalid dates
        def handle_out_of_bounds_dates(df, date_column):
            df_copy = df.copy()
            # Coerce dates to datetime, ignoring errors for invalid dates
            df_copy[date_column] = pd.to_datetime(df_copy[date_column], errors="coerce")

            # Define the valid Pandas datetime range
            min_valid_date = pd.Timestamp("1677-09-21")
            max_valid_date = pd.Timestamp("2262-04-11")

            # Set dates outside the valid range to NaT
            df_copy.loc[
                (df_copy[date_column] < min_valid_date) | (df_copy[date_column] > max_valid_date), date_column
            ] = pd.NaT

            return df_copy

        df_new_duplicates_exact_2 = handle_out_of_bounds_dates(df_new_duplicates_exact_2, "date_of_birth")
        clusters_pandas_df = handle_out_of_bounds_dates(clusters_pandas_df, "date_of_birth")
        # 167,291 records and 17 columns (same as total record count for cleaned child table)
        clusters_pandas_df_2 = process_clusters(df_new_duplicates_exact_2, clusters_pandas_df)

        # Unique count of id: 167,291
        # Unique count of cluster_id: 53,888
        # Percentage reduction: 67.79%

        logger.info("")
        logger.info(f"Unique count of id: {clusters_pandas_df_2['id'].nunique()}")
        logger.info(f"Unique count of cluster_id: {clusters_pandas_df_2['cluster_id'].nunique()}")
        logger.info(
            f"Percentage reduction: {((clusters_pandas_df_2['id'].nunique() - clusters_pandas_df_2['cluster_id'].nunique()) / clusters_pandas_df_2['id'].nunique()) * 100:.2f}%"
        )
        logger.info("")

        """
        Bringing in the comparison vector value columns "gamma_" into the clustered dataframe.
        Each comparison has comparison levels with comparison vector values starting from 0, 1, onwards,
        indicating which comparison level the pairwise record match satisfied for a column.
        """

        def merge_predictions_with_clusters(predictions, clusters):
            predictions_df = predictions.copy()
            clusters_df = clusters.copy()
            # Define the columns to extract from predictions_df
            match_columns = [
                "match_weight",
                "match_probability",
                "gamma_surname",
                "gamma_given_name_1",
                "gamma_given_name_2",
                "gamma_date_of_birth",
                "gamma_sex",
                "gamma_contact_number",
                "gamma_suburb",
                "gamma_given_name_1_surname",
            ]

            # Create a key for easy merging
            predictions_df["key_l"] = predictions_df["id_l"].astype(str) + "_" + predictions_df["id_r"].astype(str)
            predictions_df["key_r"] = predictions_df["id_r"].astype(str) + "_" + predictions_df["id_l"].astype(str)

            clusters_df["key"] = clusters_df["cluster_id"].astype(int).astype(str) + "_" + clusters_df["id"].astype(str)

            # Merge on both possible key combinations
            merged_df = clusters_df.merge(
                predictions_df.set_index("key_l")[match_columns], left_on="key", right_index=True, how="left"
            ).merge(
                predictions_df.set_index("key_r")[match_columns],
                left_on="key",
                right_index=True,
                how="left",
                suffixes=("_left", "_right"),
            )

            # Combine values from both merge attempts
            for col in match_columns:
                merged_df[col] = merged_df[f"{col}_left"].combine_first(merged_df[f"{col}_right"])
                merged_df.drop(columns=[f"{col}_left", f"{col}_right"], inplace=True)

            # Drop the temporary key column
            merged_df.drop(columns=["key"], inplace=True)

            # Step 2: Assign default values where cluster_id == id
            condition = merged_df["cluster_id"] == merged_df["id"]
            merged_df.loc[condition, match_columns] = [
                1,  # match_weight
                1,  # match_probability
                3,  # gamma_surname
                3,  # gamma_given_name_1
                3,  # gamma_given_name_2
                5,  # gamma_date_of_birth
                1,  # gamma_sex
                1,  # gamma_contact_number
                2,  # gamma_suburb
                5,  # gamma_given_name_1_surname
            ]

            return merged_df

        # 167,291 records and 27 columns
        clusters_pandas_df_3 = merge_predictions_with_clusters(df_predictions_pandas, clusters_pandas_df_2)

        """
        A dictionary is created for the comparison vector values in each "gamma_" column.
        Each number is mapped to its corresponding descriptive value.
        """

        def map_gamma_values(value, mapping_dict):
            """Maps numerical gamma values to their corresponding descriptive values."""
            return mapping_dict.get(value, np.nan)  # Default to NULL if value is not in mapping_dict

        def process_gamma_descriptions(clusters):
            clusters_df = clusters.copy()
            """
            Processes gamma columns in clusters_df and creates corresponding descriptive columns.

            Parameters:
                clusters_df (pd.DataFrame): Input dataframe with gamma columns.

            Returns:
                pd.DataFrame: Updated dataframe with descriptive gamma columns.
            """
            # Mapping rules
            gamma_mappings = {
                "gamma_surname": {
                    -1: "Invalid (NULL)",
                    3: "Exact match",
                    2: "Jaro-Winkler distance >= 0.9",
                    1: "Array intersection size >= 1",
                    0: "No Match",
                },
                "gamma_given_name_1": {
                    -1: "Invalid (NULL)",
                    3: "Exact match",
                    2: "Jaro-Winkler distance >= 0.9",
                    1: "Array intersection size >= 1",
                    0: "No Match",
                },
                "gamma_given_name_2": {
                    -1: "Invalid (NULL)",
                    3: "Exact match",
                    2: "Jaro-Winkler distance >= 0.9",
                    1: "Array intersection size >= 1",
                    0: "No Match",
                },
                "gamma_date_of_birth": {
                    -1: "Invalid (NULL)",
                    5: "Exact match",
                    4: "DamerauLevenshtein distance <= 1",
                    3: "Abs date difference <= 1 month",
                    2: "Abs date difference <= 1 year",
                    1: "Abs date difference <= 10 year",
                    0: "No Match",
                },
                "gamma_sex": {-1: "Invalid (NULL)", 1: "Exact match", 0: "No Match"},
                "gamma_contact_number": {-1: "Invalid (NULL)", 1: "Exact match", 0: "No Match"},
                "gamma_suburb": {
                    -1: "Invalid (NULL)",
                    2: "Exact match",
                    1: "Damerau-Levenshtein distance <= 2",
                    0: "No Match",
                },
                "gamma_given_name_1_surname": {
                    -1: "Invalid (NULL)",
                    5: "Exact match on name_concat",
                    4: "Match on reversed cols: given_name_1 and surname (both directions)",
                    3: "Jaro-Winkler distance of given_name_1 >= 0.9 AND Jaro-Winkler distance of surname >= 0.9",
                    2: "Exact match on surname",
                    1: "Exact match on given_name_1",
                    0: "No Match",
                },
            }

            # Create clusters_df_2 as a copy of clusters_df
            clusters_df_2 = clusters_df.copy()

            # Process each gamma column
            for gamma_col, mapping_dict in gamma_mappings.items():
                desc_col = gamma_col + "_desc"  # Create new descriptive column name
                clusters_df_2[desc_col] = clusters_df_2[gamma_col].map(lambda x: map_gamma_values(x, mapping_dict))

            return clusters_df_2

        # 167,291 records and 35 columns
        clusters_pandas_df_4 = process_gamma_descriptions(clusters_pandas_df_3)

        """
        Updates the "gamma_" description columns based on whether the record exists in df_new_duplicates_exact_2.
        "Association match (Exact)" - These were duplicate records that were removed initially (for faster computation) but later added and matched to the master record of their exact match.
            Eg:
            A = B - exact match (B was removed from the dataset but A was left behind)
            A = C - match passes threshold
            C = B - match through association of A = C (B was added)
        "Association match" - These matches didn't meet the threshold we set, but through associations of matches that did meet the threshold.
            Eg:
            A = B - match passes threshold
            A != C - match doesn't pass threshold
            B = C - match passes threshold
            A = C - match through association of A = B & B = C
        """

        def update_association_match(exact, clusters):
            exact_df = exact.copy()
            clusters_df = clusters.copy()
            # Define the columns to be updated
            gamma_columns = [
                "gamma_surname_desc",
                "gamma_given_name_1_desc",
                "gamma_given_name_2_desc",
                "gamma_date_of_birth_desc",
                "gamma_sex_desc",
                "gamma_contact_number_desc",
                "gamma_suburb_desc",
                "gamma_given_name_1_surname_desc",
            ]

            # Identify records where match_weight is NULL
            null_weight_mask = clusters_df["match_weight"].isna()

            # Identify records in exact_df
            exact_match_ids = set(exact_df["id"])

            # Apply 'Association match (Exact)' where id is in exact_df
            clusters_df.loc[null_weight_mask & clusters_df["id"].isin(exact_match_ids), gamma_columns] = (
                "Association match (Exact)"
            )

            # Apply 'Association match' for remaining NULL match_weight records
            clusters_df.loc[null_weight_mask & ~clusters_df["id"].isin(exact_match_ids), gamma_columns] = (
                "Association match"
            )

            return clusters_df

        # 167,291 records and 35 columns
        # 76 records as "Association match"
        # 101,289 records as "Association match (Exact)"
        clusters_pandas_df_5 = update_association_match(df_new_duplicates_exact_2, clusters_pandas_df_4)

        # Add a new empty column 'new_id'
        clusters_pandas_df_5["new_id"] = 1
        # Add a new empty column 'updated_cluster_id'
        clusters_pandas_df_5["updated_cluster_id"] = "1_1"

        """
        function to set the 'can_exclude' flag to 1 for the below, otherwise 0 (Unborn children are the exception):
        If NULL in given_name_1 and surname.
        If non-NULL in given_name_1 but NULL in surname and date_of_birth.
        If non-NULL in surname but NULL in given_name_1 and date_of_birth.
        """

        def can_exclude_flag(row):
            # Exception: If 'unborn' in given_name_1 or surname, always return 0
            for col in ["given_name_1", "surname"]:
                val = row.get(col)
                if pd.notna(val) and "unborn" in str(val).lower():
                    return 0
            # If NULL in given_name_1 and surname
            if pd.isna(row["given_name_1"]) and pd.isna(row["surname"]):
                return 1
            # If non-NULL in given_name_1 but NULL in surname and date_of_birth
            if pd.notna(row["given_name_1"]) and pd.isna(row["surname"]) and pd.isna(row["date_of_birth"]):
                return 1
            # If non-NULL in surname but NULL in given_name_1 and date_of_birth
            if pd.notna(row["surname"]) and pd.isna(row["given_name_1"]) and pd.isna(row["date_of_birth"]):
                return 1
            return 0

        clusters_pandas_df_6 = clusters_pandas_df_5.copy()
        clusters_pandas_df_6["can_exclude"] = clusters_pandas_df_6.apply(can_exclude_flag, axis=1)

        """
        function to check all four name columns for 'unborn'.
        Sets date_of_birth to None if found, otherwise keeps the original value.
        """

        def set_dob_null_if_unborn(row):
            for col in ["given_name_1", "given_name_2", "given_name_3", "surname"]:
                value = row.get(col)
                if pd.notna(value) and "unborn" in str(value).lower():
                    return None
            return row.get("date_of_birth")

        # Apply the function and set date_of_birth accordingly
        clusters_pandas_df_6["date_of_birth"] = clusters_pandas_df_6.apply(set_dob_null_if_unborn, axis=1)

        # 167,291 records - DO_NOT_MIGRATE_child_dedup
        # 53,888 records - child
        # 67.79% reduction

        return clusters_pandas_df_6
