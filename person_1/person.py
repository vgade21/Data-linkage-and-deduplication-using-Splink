import pandas as pd

from unify_dm_core.core_sqlalchemy import SQLAlchemyOperations
from sqlalchemy.dialects.mssql import INTEGER, VARCHAR, TINYINT

from src.core.table_processor import TableCleaner
from unify_dm_core.core_connection import DatabaseConnector
from unify_dm_core.core_config import YAMLConfig
from unify_dm_core.core_sql_runner import SQLRunner
from splink import Linker, DuckDBAPI
import os
import numpy as np
import phonetics
import warnings


pd.options.mode.chained_assignment = None
warnings.filterwarnings("ignore", category=UserWarning)

# Takes around 6 min to run

# Script Purpose: Use a trained linking model to link person records and de-duplicate them.
#                 https://moj-analytical-services.github.io/splink/index.html
# Feature DQI: 31584
# Input: Original person table along with sex_id description, suburb_code, suburb_description, postcode, contact_number (actual number))
#        The person data is split into exact matches (df_new_duplicates_exact) and the rest (df_training_data), we apply the linking model to the rest of the data.
# Output: de-duplicated person table (person_dedup) and a mapping table (DO_NOT_MIGRATE_person_lookup) between the original and de-duplicated person tables.

query_8 = """
-- 44,493 records
WITH DistinctContact AS (
    SELECT
        id,
        mobile_phone_number,
        ah_phone_number,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY id) AS RowNum
    FROM
        cms_staging.dbo.contact
),
DuplicateRecords AS (
    SELECT
	    p.id,
        p.version,
        p.country_of_origin_id,
        p.date_created,
	    p.date_of_birth,
	    p.given_name_1,
	    p.given_name_2,
	    p.given_name_3,
        p.indigenous_status_id,
        p.last_updated,
        p.ne_reason,
        p.not_editable,
        p.primary_address_id,
        p.primary_contact_id,
        p.racial_appearance_id,
        p.sex_id,
	    p.surname,
        p.care_of_address,
        p.spi,
        p.is_business,
        p.is_employee,
        p.can_exclude,
        -- Handle the 'code' from table 'sex'
        CASE
            WHEN s.code IN ('U', 'X', 'V') THEN NULL
            ELSE s.code
        END AS sex,
        -- Collect 'code' and 'description' from 'suburb'
        CASE
            WHEN sb.code = '~~~~' THEN NULL
            ELSE sb.code
        END AS suburb_code,
        sb.description AS suburb_description,
        a.postcode,
        -- Collect mobile_phone_number or fallback to ah_phone_number
        COALESCE(dc.mobile_phone_number, dc.ah_phone_number) AS contact_number,
        ROW_NUMBER() OVER (
            PARTITION BY p.date_of_birth, p.given_name_1, p.given_name_2, p.given_name_3, p.surname,
                CASE
                    WHEN s.code IN ('U', 'X', 'V') THEN NULL
                    ELSE s.code
                END
            ORDER BY p.id
        ) AS RowNum,
        -- Assign master_id
        CASE
            WHEN p.given_name_1 IS NULL AND p.surname IS NULL THEN p.id -- Handle case where given_name_1 and surname are NULL
            ELSE MIN(p.id) OVER (
                PARTITION BY
                    p.given_name_1, p.given_name_2, p.given_name_3, p.surname, p.date_of_birth,
                    CASE WHEN s.code IN ('U', 'X', 'V') THEN NULL ELSE s.code END
            )
        END AS master_id
    FROM
        cms_staging.dbo.DO_NOT_MIGRATE_person p
    LEFT JOIN
        DistinctContact dc ON p.primary_contact_id = dc.id AND dc.RowNum = 1
    LEFT JOIN
        cms_staging.dbo.address a ON p.primary_address_id = a.id
    LEFT JOIN
        cms_staging.dbo.suburb sb ON a.suburb_id = sb.id
    LEFT JOIN
        cms_staging.dbo.sex s ON p.sex_id = s.id
)
SELECT
    id,
    version,
    country_of_origin_id,
    date_created,
    date_of_birth,
    given_name_1,
    given_name_2,
    given_name_3,
    indigenous_status_id,
    last_updated,
    ne_reason,
    not_editable,
    primary_address_id,
    primary_contact_id,
    racial_appearance_id,
    sex_id,
    surname,
    care_of_address,
    spi,
    is_business,
    is_employee,
    can_exclude,
    sex,
    suburb_code,
    suburb_description,
    postcode,
    contact_number,
    master_id
FROM
    DuplicateRecords
WHERE
    RowNum > 1
ORDER BY
    master_id, -- Group duplicates together
    id; -- Ensure duplicates are ordered correctly
"""

query_4 = """
-- 568,514 records
WITH CTE_Deduplicated AS (
	SELECT
	    p.id,
        p.version,
        p.country_of_origin_id,
        p.date_created,
	    p.date_of_birth,
	    p.given_name_1,
	    p.given_name_2,
	    p.given_name_3,
        p.indigenous_status_id,
        p.last_updated,
        p.ne_reason,
        p.not_editable,
        p.primary_address_id,
        p.primary_contact_id,
        p.racial_appearance_id,
        p.sex_id,
	    p.surname,
        p.care_of_address,
        p.spi,
        p.is_business,
        p.is_employee,
        p.can_exclude,
	    -- Handle the 'code' from table 'sex'
	    CASE
	        WHEN s.code IN ('U', 'X', 'V') THEN NULL
	        ELSE s.code
	    END AS sex,
	    -- Collect 'code' and 'description' from 'suburb'
	    CASE
		    WHEN sb.code = '~~~~' THEN NULL
		    ELSE sb.code
	    END AS suburb_code,
	    sb.description AS suburb_description,
        a.postcode,
	    -- Collect mobile_phone_number or fallback to ah_phone_number
	    COALESCE(c.mobile_phone_number, c.ah_phone_number) AS contact_number,
	    ROW_NUMBER() OVER (
            PARTITION BY p.date_of_birth, p.given_name_1, p.given_name_2, p.given_name_3, p.surname,
                CASE
                    WHEN s.code IN ('U', 'X', 'V') THEN NULL
                    ELSE s.code
                END
            ORDER BY p.id
        ) AS rn
	FROM
	    cms_staging.dbo.DO_NOT_MIGRATE_person p
	LEFT JOIN
	    cms_staging.dbo.contact c ON p.primary_contact_id = c.id
	LEFT JOIN
	    cms_staging.dbo.address a ON p.primary_address_id = a.id
	LEFT JOIN
	    cms_staging.dbo.suburb sb ON a.suburb_id = sb.id
	LEFT JOIN
	    cms_staging.dbo.sex s ON p.sex_id = s.id
)
SELECT
    id,
    version,
    country_of_origin_id,
    date_created,
    date_of_birth,
    given_name_1,
    given_name_2,
    given_name_3,
    indigenous_status_id,
    last_updated,
    ne_reason,
    not_editable,
    primary_address_id,
    primary_contact_id,
    racial_appearance_id,
    sex_id,
    surname,
    care_of_address,
    spi,
    is_business,
    is_employee,
    can_exclude,
    sex,
    suburb_code,
    suburb_description,
    postcode,
    contact_number
FROM
    CTE_Deduplicated
WHERE
    rn = 1;
"""


class Person(TableCleaner):
    def __init__(self, df: pd.DataFrame = pd.DataFrame()):
        self.df = df
        self.table_name = self.__class__.__name__.lower()

    def load(self, *args):
        # creating a new engine as earlier script uses the staging database
        config = args[1]
        staging_conn = DatabaseConnector(
            config.config["TARGET_SERVER_ADDRESS"],
            config.config["CMS_STAGE_SYSTEM_LOADER"],
            config,
        )
        sql_ops = SQLAlchemyOperations(staging_conn.engine, self.table_name)
        self.df_new_duplicates_exact = sql_ops.get_table_by_query(query_8)
        self.df_training_data = sql_ops.get_table_by_query(query_4)
        return self.df

    def process(self, df):
        self.df = df
        self.person_lookup = self.dedup_person(self.df_new_duplicates_exact, self.df_training_data)

    def upload(self, conn: DatabaseConnector):
        col_type = {
            "id": VARCHAR(),
            "version": INTEGER(),
            "country_of_origin_id": INTEGER(),
            "indigenous_status_id": INTEGER(),
            "ne_reason": VARCHAR(),
            "not_editable": TINYINT(),
            "primary_address_id": INTEGER(),
            "primary_contact_id": INTEGER(),
            "racial_appearance_id": INTEGER(),
            "sex_id": INTEGER(),
            "care_of_address": TINYINT(),
            "spi": VARCHAR(),
            "is_business": INTEGER(),
            "is_employee": INTEGER(),
            "can_exclude": INTEGER(),
            "new_id": INTEGER(),
            "updated_cluster_id": VARCHAR(),
        }
        sql_ops = SQLAlchemyOperations(conn.engine, self.table_name)
        sql_ops.write_to_db_bcp(self.person_lookup, "DO_NOT_MIGRATE_person_lookup", types=col_type)
        self.clean_via_sql()

    def clean_via_sql(self):
        # ADD new sql script here
        sql_file_name = ["dedup_per.sql"]
        file_path = os.path.dirname(os.path.realpath(__file__))

        # creating a new engine as '.sql' scripts use the staging database
        config = YAMLConfig("local_configuration.yaml")
        server = config.config["TARGET_SERVER_ADDRESS"]
        database = config.config["CMS_STAGE_SYSTEM_LOADER"]
        sql_runner = SQLRunner(server, database)
        sql_runner.runs_multi(file_path, sql_file_name)

    def dedup_person(self, df_new_duplicates_exact: pd.DataFrame, df_training_data: pd.DataFrame) -> pd.DataFrame:
        """
        Merging the clustered person records with the exact match person records.
        The resulting clusters_pandas_df_2 contains:
            All original records from clusters_pandas_df.
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
        df_training_data_2 = clean_dataframe_columns(df_training_data, columns_to_clean)

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
        settings = "./splink_models/person_model_2.json"
        linker = Linker(df_training_data_3, settings, db_api=DuckDBAPI())

        # --------------------------------------------------------------------------------
        # 5) Predict matches and retrieve the match scores
        # --------------------------------------------------------------------------------

        print()
        # Predict matching records with a threshold_match_probability
        df_predictions = linker.inference.predict(threshold_match_probability=0.95)

        # Converting to a Pandas dataframe
        # 586,062 records (pairwise comparisons above the threshold)
        df_predictions_pandas = df_predictions.as_pandas_dataframe()

        print()
        # Clusters the pairwise match predictions into groups of connected records.
        # Records with an estimated match probability at or above threshold_match_probability are considered to be a match (i.e. they represent the same entity).
        df_clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(  # noqa
            df_predictions, threshold_match_weight=22
        )
        # Converting to a Pandas dataframe
        # 568,514 records (same number of records as df_training_data_2)
        clusters_pandas_df = df_clusters.as_pandas_dataframe()

        # 568,514 records - training data (first duplicated (exact) row + rest)
        # 44,493 records - exact match data (remaining duplicated (exact) rows)
        print()
        print(
            f"Row count of training data: {len(df_training_data_3)} and Row count of exact match data: {len(df_new_duplicates_exact)} and total dataset for prediction: {len(df_new_duplicates_exact) + len(df_training_data_3)}"
        )
        print(f"Row count of predictions: {len(df_predictions_pandas)}")
        print(f"Row count of clusters: {len(clusters_pandas_df)}")

        # --------------------------------------------------------------------------------
        # 7) Post-processing to generate final mapping and de-dup tables
        # --------------------------------------------------------------------------------

        def process_clusters(df_new_duplicates_exact, clusters_pandas_df):
            # Get unique master_id values from df_new_duplicates_exact
            unique_master_ids = df_new_duplicates_exact["master_id"].unique()

            # Find cluster_id in clusters_pandas_df for these master_ids using the id column as the link
            master_id_to_cluster_id = clusters_pandas_df[clusters_pandas_df["id"].isin(unique_master_ids)][
                ["id", "cluster_id"]
            ]

            # Merge master_id to cluster_id mapping back to df_new_duplicates_exact
            df_new_duplicates_exact_with_cluster = pd.merge(
                df_new_duplicates_exact,
                master_id_to_cluster_id,
                left_on="master_id",
                right_on="id",
                suffixes=("_duplicate", "_cluster"),
                how="left",
            )

            # For master_ids not found in clusters_pandas_df, set cluster_id to the original id
            df_new_duplicates_exact_with_cluster["cluster_id"] = df_new_duplicates_exact_with_cluster[
                "cluster_id"
            ].fillna(df_new_duplicates_exact_with_cluster["id_duplicate"])

            # Select only the necessary columns for appending
            df_new_duplicates_to_append = df_new_duplicates_exact_with_cluster[
                [
                    "cluster_id",
                    "id_duplicate",
                    "version",
                    "country_of_origin_id",
                    "date_created",
                    "date_of_birth",
                    "given_name_1",
                    "given_name_2",
                    "given_name_3",
                    "indigenous_status_id",
                    "last_updated",
                    "ne_reason",
                    "not_editable",
                    "primary_address_id",
                    "primary_contact_id",
                    "racial_appearance_id",
                    "sex_id",
                    "surname",
                    "care_of_address",
                    "spi",
                    "is_business",
                    "is_employee",
                    "can_exclude",
                    "sex",
                    "suburb_code",
                    "suburb_description",
                    "postcode",
                    "contact_number",
                ]
            ].rename(columns={"id_duplicate": "id"})

            # Append records from df_new_duplicates_to_append to clusters_pandas_df
            clusters_pandas_df_2 = pd.concat([clusters_pandas_df, df_new_duplicates_to_append], ignore_index=True)

            # Sort the dataframe by cluster_id and id in ascending order
            clusters_pandas_df_2 = clusters_pandas_df_2.sort_values(by=["cluster_id", "id"], ascending=True)

            clusters_pandas_df_2 = clusters_pandas_df_2[
                [
                    "cluster_id",
                    "id",
                    "version",
                    "country_of_origin_id",
                    "date_created",
                    "date_of_birth",
                    "given_name_1",
                    "given_name_2",
                    "given_name_3",
                    "indigenous_status_id",
                    "last_updated",
                    "ne_reason",
                    "not_editable",
                    "primary_address_id",
                    "primary_contact_id",
                    "racial_appearance_id",
                    "sex_id",
                    "surname",
                    "care_of_address",
                    "spi",
                    "is_business",
                    "is_employee",
                    "can_exclude",
                    "sex",
                    "suburb_code",
                    "suburb_description",
                    "postcode",
                    "contact_number",
                    "name_concat",
                ]
            ]

            return clusters_pandas_df_2

        # Pandas datetime objects (used in datetime64[ns] columns) only support dates between 1677-09-21 and 2262-04-11. Dates outside this range (eg: 2988-02-16 - id: 407524) are considered invalid, leading to the OutOfBoundsDatetime error.
        # Function to handle invalid dates
        def handle_out_of_bounds_dates(df, date_column):
            # Coerce dates to datetime, ignoring errors for invalid dates
            df[date_column] = pd.to_datetime(df[date_column], errors="coerce")

            # Define the valid Pandas datetime range
            min_valid_date = pd.Timestamp("1677-09-21")
            max_valid_date = pd.Timestamp("2262-04-11")

            # Set dates outside the valid range to NaT
            df.loc[(df[date_column] < min_valid_date) | (df[date_column] > max_valid_date), date_column] = pd.NaT

            return df

        # df_new_duplicates_exact = handle_out_of_bounds_dates(df_new_duplicates_exact, 'date_of_birth')
        clusters_pandas_df = handle_out_of_bounds_dates(clusters_pandas_df, "date_of_birth")
        # 612,828 records and 25 columns (same as total record count for cleaned person table)
        clusters_pandas_df_2 = process_clusters(df_new_duplicates_exact, clusters_pandas_df)

        # using data from cms_clean and staging:
        # Unique count of id: 612,828
        # Unique count of cluster_id: 460,422
        # Percentage reduction: 24.87%

        print()
        print(f"Unique count of id: {clusters_pandas_df_2['id'].nunique()}")
        print(f"Unique count of cluster_id: {clusters_pandas_df_2['cluster_id'].nunique()}")
        print(
            f"Percentage reduction: {((clusters_pandas_df_2['id'].nunique() - clusters_pandas_df_2['cluster_id'].nunique()) / clusters_pandas_df_2['id'].nunique()) * 100:.2f}%"
        )
        print()

        """
        Bringing in the comparison vector value columns "gamma_" into the clustered dataframe.
        Each comparison has comparison levels with comparison vector values starting from 0, 1, onwards,
        indicating which comparison level the pairwise record match satisfied for a column.
        """

        def merge_predictions_with_clusters(df_predictions_pandas, clusters_pandas_df_2):
            # Define the columns to extract from df_predictions_pandas
            match_columns = [
                "match_weight",
                "match_probability",
                "gamma_surname",
                "gamma_given_name_1",
                "gamma_given_name_2",
                "gamma_date_of_birth",
                "gamma_sex",
                "gamma_contact_number",
                "gamma_suburb_code",
                "gamma_given_name_1_surname",
            ]

            # Create a key for easy merging
            df_predictions_pandas["key_l"] = (
                df_predictions_pandas["id_l"].astype(str) + "_" + df_predictions_pandas["id_r"].astype(str)
            )
            df_predictions_pandas["key_r"] = (
                df_predictions_pandas["id_r"].astype(str) + "_" + df_predictions_pandas["id_l"].astype(str)
            )

            clusters_pandas_df_2["key"] = (
                clusters_pandas_df_2["cluster_id"].astype(str) + "_" + clusters_pandas_df_2["id"].astype(str)
            )

            # Merge on both possible key combinations
            merged_df = clusters_pandas_df_2.merge(
                df_predictions_pandas.set_index("key_l")[match_columns], left_on="key", right_index=True, how="left"
            ).merge(
                df_predictions_pandas.set_index("key_r")[match_columns],
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
                2,  # gamma_suburb_code
                5,  # gamma_given_name_1_surname
            ]

            return merged_df

        # 612,828 records and 35 columns
        clusters_pandas_df_3 = merge_predictions_with_clusters(df_predictions_pandas, clusters_pandas_df_2)

        """
        A dictionary is created for the comparison vector values in each "gamma_" column.
        Each number is mapped to its corresponding descriptive value.
        """

        def map_gamma_values(value, mapping_dict):
            """Maps numerical gamma values to their corresponding descriptive values."""
            return mapping_dict.get(value, np.nan)  # Default to NULL if value is not in mapping_dict

        def process_gamma_descriptions(clusters_pandas_df_3):
            """
            Processes gamma columns in clusters_pandas_df_3 and creates corresponding descriptive columns.

            Parameters:
                clusters_pandas_df_3 (pd.DataFrame): Input dataframe with gamma columns.

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
                "gamma_suburb_code": {
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

            # Create clusters_pandas_df_4 as a copy of clusters_pandas_df_3
            clusters_pandas_df_4 = clusters_pandas_df_3.copy()

            # Process each gamma column
            for gamma_col, mapping_dict in gamma_mappings.items():
                desc_col = gamma_col + "_desc"  # Create new descriptive column name
                clusters_pandas_df_4[desc_col] = clusters_pandas_df_4[gamma_col].map(
                    lambda x: map_gamma_values(x, mapping_dict)
                )

            return clusters_pandas_df_4

        # 612,828 records and 43 columns
        clusters_pandas_df_4 = process_gamma_descriptions(clusters_pandas_df_3)

        """
        Updates the "gamma_" description columns based on whether the record exists in df_new_duplicates_exact.
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

        def update_association_match(df_new_duplicates_exact, clusters_pandas_df_4):
            # Define the columns to be updated
            gamma_columns = [
                "gamma_surname_desc",
                "gamma_given_name_1_desc",
                "gamma_given_name_2_desc",
                "gamma_date_of_birth_desc",
                "gamma_sex_desc",
                "gamma_contact_number_desc",
                "gamma_suburb_code_desc",
                "gamma_given_name_1_surname_desc",
            ]

            # Identify records where match_weight is NULL
            null_weight_mask = clusters_pandas_df_4["match_weight"].isna()

            # Identify records in df_new_duplicates_exact
            exact_match_ids = set(df_new_duplicates_exact["id"])

            # Apply 'Association match (Exact)' where id is in df_new_duplicates_exact
            clusters_pandas_df_4.loc[
                null_weight_mask & clusters_pandas_df_4["id"].isin(exact_match_ids), gamma_columns
            ] = "Association match (Exact)"

            # Apply 'Association match' for remaining NULL match_weight records
            clusters_pandas_df_4.loc[
                null_weight_mask & ~clusters_pandas_df_4["id"].isin(exact_match_ids), gamma_columns
            ] = "Association match"

            return clusters_pandas_df_4

        # 612,828 records and 43 columns
        # 1,346 records as "Association match"
        # 42,921 records as "Association match (Exact)"
        clusters_pandas_df_5 = update_association_match(df_new_duplicates_exact, clusters_pandas_df_4)

        # Add a new empty column 'new_id'
        clusters_pandas_df_5["new_id"] = 1
        # Add a new empty column 'updated_cluster_id'
        clusters_pandas_df_5["updated_cluster_id"] = "1_1"

        # 612,908 records - DO_NOT_MIGRATE_person_lookup
        # 460,425 records - person
        # 24.88% reduction

        # Note: The clusters that group person records, and consequently their new assigned person IDs, will remain consistent across script reruns, provided:
        # 1) the underlying legacy dataset remains unchanged.
        # 2) the person cleansing script (`old_person.py`) remains unchanged in a way that does not affect grouping logic
        # (e.g., changes to name cleansing or inclusion/exclusion of business/employee records may result in different clusters).

        return clusters_pandas_df_5
