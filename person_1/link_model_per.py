import pandas as pd
import numpy as np
import pyodbc
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from tqdm import tqdm
import warnings
from splink import Linker, block_on, DuckDBAPI, SettingsCreator  # noqa
import splink.comparison_library as cl  # noqa
from splink.exploratory import profile_columns, similarity_analysis, completeness_chart  # noqa
from splink.blocking_analysis import (
    cumulative_comparisons_to_be_scored_from_blocking_rules_chart,
    n_largest_blocks,
)  # noqa
import phonetics  # noqa: E402
import duckdb  # noqa

# https://github.com/moj-analytical-services/splink/blob/master/splink/comparison_library.py#L35
# https://github.com/moj-analytical-services/splink/blob/master/splink/comparison_level_library.py
# https://moj-analytical-services.github.io/splink/api_docs/comparison_library.html
from splink.comparison_library import (  # noqa
    ExactMatch,
    LevenshteinAtThresholds,
    DamerauLevenshteinAtThresholds,
    NameComparison,
    DateOfBirthComparison,
    ArrayIntersectAtSizes,
    ForenameSurnameComparison,
)

pd.options.mode.chained_assignment = None
warnings.filterwarnings("ignore", category=UserWarning)

# Script Purpose: build and train a Splink model (based on Fellegi-Sunter's model of record linkage) for probabilistic record linkage that allows us to link person records and de-duplicate them.
#                 https://moj-analytical-services.github.io/splink/index.html
# Feature DQI: 31584
# Input: Original person table (without 'spi' column) along with sex_id description, suburb_code, suburb_description, postcode, contact_number (actual number))
# Output: Trained linking model, de-duplicated person table (person_dedup) and a mapping table (DO_NOT_MIGRATE_person_lookup) between the original and de-duplicated person tables.

# https://www.youtube.com/watch?v=msz3T741KQI
# https://www.youtube.com/watch?v=qDkiw9hnBEw

# Preparing for Linking
# https://toolkit.data.gov.au/data-integration/data-integration-projects/preparing-for-linking.html

# Introductory tutorial
# https://github.com/moj-analytical-services/splink/blob/master/docs/demos/tutorials/00_Tutorial_Introduction.ipynb

# Building the SparkSession and name it :'pandas to spark'
# https://github.com/moj-analytical-services/splink/blob/master/docs/topic_guides/performance/optimising_spark.md
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("pandas to spark").getOrCreate()

# # Updating primary_contact_id post de-duplication of the contact table
# server = "ATLASSBXSQL03.ems.tas.gov.au"
# database = "staging"
# connection_string = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;TrustServerCertificate=yes;"
# connection = pyodbc.connect(connection_string)
# cursor = connection.cursor()

# query_5 = """
# -- Updates the primary_contact_id in the person table to the corresponding default_id value in the contact_lookup table, if primary_contact_id found in contact_lookup.
# -- 182,186 records updated
# UPDATE p
# SET p.primary_contact_id = c.default_id
# FROM staging.dbo.person p
# INNER JOIN staging.dbo.contact_lookup c
#     ON p.primary_contact_id = c.id;
# """

# cursor.execute(query_5)
# connection.commit()

# # Close the connection
# cursor.close()
# connection.close()

# Check for records in the person table with non-NULL reference id that aren't linked to the respective tables
"""
SELECT p.*
FROM staging.dbo.person p
LEFT JOIN staging.dbo.contact c ON p.primary_contact_id = c.id
WHERE p.primary_contact_id is not null and c.id IS NULL;

SELECT p.*
FROM staging.dbo.person p
LEFT JOIN staging.dbo.address a ON p.primary_address_id = a.id
WHERE p.primary_address_id IS NOT NULL AND a.id IS NULL;

SELECT p.*
FROM staging.dbo.person p
LEFT JOIN staging.dbo.sex s ON p.sex_id = s.id
WHERE p.sex_id IS NOT NULL AND s.id IS NULL;

SELECT a.*
FROM staging.dbo.address a
LEFT JOIN staging.dbo.suburb s ON a.suburb_id = s.id
WHERE a.suburb_id IS NOT NULL AND s.id IS NULL;
"""

server = "ATLASSBXSQL03.ems.tas.gov.au"
database = "staging"
connection_string = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;TrustServerCertificate=yes;"
connection = pyodbc.connect(connection_string)
cursor = connection.cursor()

query_4 = """
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
	    staging.dbo.person p
	LEFT JOIN
	    staging.dbo.contact c ON p.primary_contact_id = c.id
	LEFT JOIN
	    staging.dbo.address a ON p.primary_address_id = a.id
	LEFT JOIN
	    staging.dbo.suburb sb ON a.suburb_id = sb.id
	LEFT JOIN
	    staging.dbo.sex s ON p.sex_id = s.id
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

# 585,187 records (86,361 duplicates removed)
df_training_data = pd.read_sql(query_4, connection)

# Loading the exact matches found in person table (we DO NOT run the linking model on this set data).
query_8 = """
WITH DistinctContact AS (
    SELECT
        id,
        mobile_phone_number,
        ah_phone_number,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY id) AS RowNum
    FROM
        staging.dbo.contact
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
        staging.dbo.person p
    LEFT JOIN
        DistinctContact dc ON p.primary_contact_id = dc.id AND dc.RowNum = 1
    LEFT JOIN
        staging.dbo.address a ON p.primary_address_id = a.id
    LEFT JOIN
        staging.dbo.suburb sb ON a.suburb_id = sb.id
    LEFT JOIN
        staging.dbo.sex s ON p.sex_id = s.id
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

# 86,361 records
df_new_duplicates_exact = pd.read_sql(query_8, connection)

# query_7 = """
# WITH SingleContact AS (
#     SELECT
#         id,
#         mobile_phone_number,
#         ah_phone_number,
#         ROW_NUMBER() OVER (PARTITION BY id ORDER BY id ASC) AS row_num
#     FROM staging.dbo.contact
# )
# SELECT
#     p.id,
#     p.version,
#     p.country_of_origin_id,
#     p.date_created,
#     p.date_of_birth,
#     p.given_name_1,
#     p.given_name_2,
#     p.given_name_3,
#     p.indigenous_status_id,
#     p.last_updated,
#     p.ne_reason,
#     p.not_editable,
#     p.primary_address_id,
#     p.primary_contact_id,
#     p.racial_appearance_id,
#     p.sex_id,
#     p.surname,
#     p.care_of_address,
#     -- Handle the 'code' from table 'sex'
#     CASE
#         WHEN s.code IN ('U', 'X', 'V') THEN NULL
#         ELSE s.code
#     END AS sex,
#     -- Collect 'code' and 'description' from 'suburb'
#     CASE
#         WHEN sb.code = '~~~~' THEN NULL
#         ELSE sb.code
#     END AS suburb_code,
#     sb.description AS suburb_description,
#     a.postcode,
#     -- Collect mobile_phone_number or fallback to ah_phone_number
#     COALESCE(c.mobile_phone_number, c.ah_phone_number) AS contact_number
# FROM
#     staging.dbo.person p
# LEFT JOIN
#     SingleContact c ON p.primary_contact_id = c.id AND c.row_num = 1
# LEFT JOIN
#     staging.dbo.address a ON p.primary_address_id = a.id
# LEFT JOIN
#     staging.dbo.suburb sb ON a.suburb_id = sb.id
# LEFT JOIN
#     staging.dbo.sex s ON p.sex_id = s.id;
# """

# # 671,548 records (total record count for cleaned person table)
# df_test_data = pd.read_sql(query_7, connection)

# Close the connection
cursor.close()
connection.close()

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
# df_test_data_2 = clean_dataframe_columns(df_test_data, columns_to_clean)

# --------------------------------------------------------------------------------
# 1) Imports
# --------------------------------------------------------------------------------

# --------------------------------------------------------------------------------
# 2) Exploratory Analysis
# --------------------------------------------------------------------------------
"""
Left chart shows the distribution of all values in the column. It is a summary of the skew of value frequencies.
The middle chart shows the counts of the ten most common values in the column. These correspond to the 10 leftmost "steps" in the left chart.
The right chart shows the counts of the ten least common values in the column. These correspond to the 10 rightmost "steps" in the left chart.

Columns with higher cardinality (number of distinct values) are usually more useful for data linking.
Highly skewed columns are less useful for linkage.
Ideally all of the columns in datasets used for linkage would be high cardinality with a low skew (i.e. many distinct values that are evenly distributed).
To take this skew into account, we can build Splink models with Term Frequency Adjustments.
These adjustments will increase the amount of evidence for rare matching values and reduce the amount of evidence for common matching values.
"""
# There is skew in both given_names and surname columns.
# given_name_1 like "michael", given_name_2 like "john" and surname like "smith" are very common

profile_columns(  # noqa
    df_training_data_2,
    column_expressions=[
        "given_name_1",
        "given_name_2",
        "surname",
        "date_of_birth",
        "sex",
        "suburb_code",
        "contact_number",
    ],
    db_api=DuckDBAPI(),
)

# Completeness chart to understand how often it's non-NULL
completeness_chart(df_training_data_2, db_api=DuckDBAPI())

# --------------------------------------------------------------------------------
# 3) Define the Splink settings including blocking rules and comparisons
# --------------------------------------------------------------------------------

"""
This is part of a two step process to link data:
    Use blocking rules to generate candidate pairwise record comparisons
    Use a probabilistic linkage model to score these candidate pairs, to determine which ones should be linked

Bocking rules specify which pairwise comparisons to generate, instead of comparing every row with every other row.
When we specify multiple blocking rules, Splink will generate all comparison pairs that meet any one of the rules.
We can often specify multiple blocking rules such that it becomes highly implausible that a true match would not meet at least one of these blocking criteria.

The aims of your blocking rules are twofold:
    Eliminate enough non-matching comparison pairs so your record linkage job is small enough to compute
    Eliminate as few truly matching pairs as possible (ideally none)
"""
"""
To generate the subset of record comparisons where the first name and surname matches, we can specify the following blocking rule:
    block_on("first_name", "surname")
When executed, this blocking rule will be converted to a SQL statement with the following form:
    SELECT ...
    FROM input_tables as l
    INNER JOIN input_tables as r
    ON l.first_name = r.first_name AND l.surname = r.surname
"""
"""
Blocking rules are order dependent, therefore each bar in this chart shows the additional comparisons generated on top of the previous blocking rules.
The main aim of this chart is to understand how many comparisons are generated by blocking rules that the Splink model will consider.
The number of comparisons is the main primary driver of the amount of computational resource required for Splink model training, predictions etc.
If a model is taking hours to run, it could be helpful to reduce the number of comparisons by defining more restrictive blocking rules.
For instance, there are many people who could share the same first_name in the example above you may want to add an additional requirement for a match on
dob as well to reduce the number of records the model needs to consider - block_on("first_name", "dob")

https://github.com/moj-analytical-services/splink/blob/master/docs/topic_guides/blocking/blocking_rules.md
https://github.com/moj-analytical-services/splink/blob/master/docs/topic_guides/blocking/performance.md
"""

# Counting the number of comparisons generated by Blocking Rules.
# For Spark try starting with fewer than 100 million comparisons, before scaling up.

# block_on("given_names")
# block_on("surname")
# block_on("date_of_birth")

# count_comparisons_from_blocking_rule(
#     table_or_tables=df_training_data_2,
#     blocking_rule= block_on("given_names"),
#     link_type="dedupe_only",
#     db_api=DuckDBAPI()
# )

"""
Since the same record comparison may be created by several blocking rules, and Splink automatically deduplicates these comparisons, we cannot simply total the number of comparisons generated by each rule individually.
Below chart shows the marginal (additional) comparisons generated by each blocking rule, after deduplication.
"""
"""
block_on("given_name_1", "substr(surname, 1, 1)") - 29,756,227
block_on("surname", "substr(given_name_1, 1, 1)") - 8,598,176
block_on("substr(surname, 1, 1)", "date_of_birth") - 337,917
block_on("substr(given_name_1, 1, 1)", "date_of_birth") - 357,802
block_on("date_of_birth") - 4,368,319
Total - 43,418,441
"""
# block_on("substr(surname, 1, 2)", "date_of_birth")
blocking_rules = [
    block_on("given_name_1", "substr(surname, 1, 1)"),
    block_on("surname", "substr(given_name_1, 1, 1)"),
    block_on("substr(surname, 1, 1)", "date_of_birth"),
    block_on("substr(given_name_1, 1, 1)", "date_of_birth"),
    block_on("date_of_birth"),
]

cumulative_comparisons_to_be_scored_from_blocking_rules_chart(
    unique_id_column_name="id",
    table_or_tables=df_training_data_2,
    blocking_rules=blocking_rules,
    db_api=DuckDBAPI(),
    link_type="dedupe_only",
)

"""
Blocking rules can be affected by skew:
some values of a field may be much more common than others, and this can lead to a disproportionate number of comparisons being generated.
"""
# Finding 'worst offending' values for your blocking rule
# block_on("given_names")
# block_on("surname")
# block_on("date_of_birth")

"""
michael b - 793881 comparisons being generated
smith j - 504100 comparisons being generated
1990-01-01 - 6400 comparisons being generated
"""
test = n_largest_blocks(  # noqa
    table_or_tables=df_training_data_2,
    blocking_rule=block_on("surname", "substr(given_name_1, 1, 1)"),
    link_type="dedupe_only",
    db_api=DuckDBAPI(),
    n_largest=5,
)
test.as_pandas_dataframe()

# Method to see the different ComparisonLevels in Splink's library of comparison functions

# ExactMatch("column_name")
# LevenshteinAtThresholds("column_name", 2)
# NameComparison("column_name", jaro_winkler_thresholds=0.9)
# EmailComparison("column_name")
# DateOfBirthComparison("column_name", input_is_string=False, datetime_thresholds=[1], datetime_metrics=["day"])
# ForenameSurnameComparison(forename_col_name="column_name_1", surname_col_name="column_name_2", jaro_winkler_thresholds =0.9, forename_surname_concat_col_name="column_name_3")
comparison = NameComparison("column_name", jaro_winkler_thresholds=0.9, dmeta_col_name="column_name_dm")
print(comparison.get_comparison("duckdb").human_readable_description)

"""
A model is composed of many Comparisons, which between them assess the similarity of all of the columns, and then combine these measures to get an overall similarity score for pairwise record comparison.
Each Comparison usually measures the similarity of a single column in the data, and each Comparison is made up of one or more ComparisonLevels.
Comparison levels specify how input values should be compared. Each level corresponds to an assessment of similarity, such as exact match, Jaro-Winkler match, etc
Within each Comparison are n discrete ComparisonLevels. Each ComparisonLevel defines a discrete gradation (category) of similarity within a Comparison.
There can be as many ComparisonLevels as you want.
Pairwise record comparisons for a column need to satisfy at least one the ComparisonLevels.

ComparisonLevels are nested within Comparisons:
    Data Linking Model
    ├─-- Comparison: Date of birth
    │    ├─-- ComparisonLevel: Exact match
    │    ├─-- ComparisonLevel: One character difference
    │    ├─-- ComparisonLevel: All other
    ├─-- Comparison: Surname
    │    ├─-- ComparisonLevel: Exact match on surname
    │    ├─-- ComparisonLevel: All other
    │    etc.

https://github.com/moj-analytical-services/splink/blob/master/docs/topic_guides/comparisons/choosing_comparators.ipynb
https://github.com/moj-analytical-services/splink/blob/master/docs/topic_guides/comparisons/comparators.md
https://github.com/moj-analytical-services/splink/blob/24667bb17df13a01a55d881563ed27a197cdfc87/docs/topic_guides/data_preparation/feature_engineering.md#phonetic-transformations
https://github.com/moj-analytical-services/splink/blob/master/docs/topic_guides/comparisons/out_of_the_box_comparisons.ipynb

"""


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
# df_test_data_3 = create_columns(df_test_data_2)

# Splink settings, a Python dictionary which controls all of the configuration of a Splink model. Comparisons are specified here.
settings = SettingsCreator(
    unique_id_column_name="id",
    link_type="dedupe_only",
    # Blocking rules - defines what comparisons will be created, will only check for duplicates amongst these records.
    # has to meet at least one of the below blocking rules.
    blocking_rules_to_generate_predictions=blocking_rules,
    # Comparisons and ComparisonLevels
    # When comparing records, we will use information from only the below columns to compute a match score.
    comparisons=[
        # Make term frequency adjustments for this comparison level for this input column - because some values appear much more frequently than others.
        # A weight of 1.0 is a full adjustment. A weight of 0.0 is no adjustment.
        NameComparison("surname", jaro_winkler_thresholds=0.9, dmeta_col_name="surname_dm").configure(
            term_frequency_adjustments=True
        ),
        # Compare given_name_1 using a string similarity measure
        # LevenshteinAtThresholds("given_names", 1),
        NameComparison("given_name_1", jaro_winkler_thresholds=0.9, dmeta_col_name="given_name_1_dm").configure(
            term_frequency_adjustments=True
        ),
        # Compare given_name_2 using a string similarity measure
        # LevenshteinAtThresholds("given_names", 1),
        NameComparison("given_name_2", jaro_winkler_thresholds=0.9, dmeta_col_name="given_name_2_dm").configure(
            term_frequency_adjustments=True
        ),
        # Compare date_of_birth with exact match
        DateOfBirthComparison(
            "date_of_birth",
            input_is_string=False,
            datetime_thresholds=[1, 1, 10],
            datetime_metrics=["month", "year", "year"],
        ).configure(term_frequency_adjustments=True),
        # ExactMatch("date_of_birth").configure(term_frequency_adjustments=True),
        # Compare sex with exact match
        ExactMatch("sex"),
        # Compare contact_number with exact match
        ExactMatch("contact_number").configure(term_frequency_adjustments=True),
        # Compare suburb_code using a string similarity measure
        DamerauLevenshteinAtThresholds("suburb_code", distance_threshold_or_thresholds=2).configure(
            term_frequency_adjustments=True
        ),
        # Compare for given_name_1 and surname columns
        ForenameSurnameComparison(
            forename_col_name="given_name_1",
            surname_col_name="surname",
            jaro_winkler_thresholds=0.9,
            forename_surname_concat_col_name="name_concat",
        ).configure(term_frequency_adjustments=True),
    ],
    # Whether to keep intermediate columns used by Splink or not (need to set to True if we wish to create waterfall charts using the predictions)
    retain_intermediate_calculation_columns=True,
    # additional_columns_to_retain=False
)

# --------------------------------------------------------------------------------
# 4) Initialize the linker and run the model training
# --------------------------------------------------------------------------------
"""
Now that we have specified our linkage model, we need to estimate the probability_two_random_records_match, u, and m parameters.
    The probability_two_random_records_match parameter is the probability that two records taken at random from your input data represent a match (typically a very small number).
    The u values are the proportion of records falling into each ComparisonLevel amongst truly non-matching records.
    The m values are the proportion of records falling into each ComparisonLevel amongst truly matching records

We can estimate these parameters using unlabeled data. If we have labels, then we can estimate them even more accurately.

https://github.com/moj-analytical-services/splink/blob/master/docs/topic_guides/training/training_rationale.md
https://github.com/moj-analytical-services/splink/blob/master/docs/topic_guides/performance/drivers_of_performance.md
"""

# Match weights depend on the m and u values. We need parameter estimates of both.
# Creating linker:
# linker = Linker(df_training_data_3, settings, db_api=SparkAPI(spark_session=spark))
linker = Linker(df_training_data_3, settings, db_api=DuckDBAPI())

# Estimate the lambda, m and u parameters (Fellegi-Sunter):

# Estimate lambda (the Bayesian prior)
"""
In some cases, the probability_two_random_records_match will be known. More generally, this parameter is unknown and needs to be estimated.
It can be estimated accurately enough for most purposes by combining a series of deterministic matching rules and a guess of the recall corresponding to those rules.
Here we guess that these deterministic matching rules have a recall of about 70%. That means, between them, the rules recover 70% of all true matches.
"""
deterministic_rules = [
    block_on("given_name_1", "given_name_2", "surname", "sex", "date_of_birth"),
    block_on("given_name_1", "given_name_2", "surname", "sex", "date_of_birth", "suburb_code"),
]
linker.training.estimate_probability_two_random_records_match(deterministic_rules, recall=0.9)

# Training u values (false match patterns)
"""
The larger the random sample, the more accurate the predictions.
We control this using the max_pairs parameter.
For large datasets, we recommend using at least 10 million - but the higher the better and 1 billion is often appropriate for larger datasets.
1e8 - takes 1.5 min
5e8 - takes 7 min
1e9 - memory error
"""
linker.training.estimate_u_using_random_sampling(max_pairs=5e8)

"""
This algorithm estimates the m values by generating pairwise record comparisons, and using them to maximise a likelihood function.
Each estimation pass requires the user to configure an estimation blocking rule to reduce the number of record comparisons generated to a manageable level.

Stability within each session is indicated by the speed of convergence of the algorithm.
This is shown in the terminal output during training. In general, the fewer iterations required to converge the better.

In our first estimation pass, we block on given_names and surname, meaning we will generate all record comparisons that have first_name and surname exactly equal.
Recall we are trying to estimate the m values of the model, i.e. proportion of records falling into each ComparisonLevel amongst **truly matching records**.
This means that, in this training session, we cannot estimate parameter estimates for the first_name or surname columns, since they will be equal for all the comparisons we do.

In a second estimation pass, we block on date_of_birth.
This allows us to estimate parameters for the first_name and surname comparisons.
Between the two estimation passes, we now have parameter estimates for all comparisons.
"""
# Training m values (true match patterns)
# Need to run training session for each blocking rule separately
training_blocking_rules = [
    block_on("given_name_1", "given_name_2", "surname"),
    block_on("given_name_1", "surname", "date_of_birth"),
    block_on("sex", "date_of_birth", "suburb_code"),
]

for rule in training_blocking_rules:
    linker.training.estimate_parameters_using_expectation_maximisation(rule)

# View all match weights for the trained Fellegi-Sunter model, which tell us the relative importance for/against a match for each of our comparison levels.
# https://github.com/moj-analytical-services/splink/blob/master/docs/charts/match_weights_chart.ipynb
linker.visualisations.match_weights_chart()
# View the individual u and m parameter estimates, which tells us about the prevalence of coincidences and mistakes.
# https://github.com/moj-analytical-services/splink/blob/master/docs/charts/m_u_parameters_chart.ipynb
linker.visualisations.m_u_parameters_chart()
# We can also compare the estimates that were produced by the different EM (Expectation maximisation) training sessions
# To see if the model training runs are consistent. If there is a lot of variation between model training sessions, this can suggest some instability in the mod
linker.visualisations.parameter_estimate_comparisons_chart()

###########
# Define the model iteration
###########
model_name = "person_model_2"

# Saving the model
# We can save the model, including our estimated parameters, to a .json file, so we can use it next time.
path = f"./splink_models/{model_name}.json"
linker.misc.save_model_to_json(path, overwrite=True)  # noqa

# Loading a pre-trained model
# When using a pre-trained model, you can read in the model from a json and recreate the linker object to make new pairwise predictions.
settings = "./splink_models/person_model_2.json"
linker = Linker(df_training_data_3, settings, db_api=DuckDBAPI())

"""
Unlinkable records are those which do not contain enough information to be linked.
'Unlinkable records' are records with such poor data quality they don't even link to themselves at a high enough probability to be accepted as matches.
A simple example would be a record containing only 'John Smith', and null in all other fields.
This record may link to other records, but we'll never know because there's not enough information to disambiguate any potential links.

A high proportion of unlinkable records is an indication of poor quality in the input dataset.
In the below chart, we can see that about 0.3% of records in the input dataset are unlinkable at a threshold match weight of 6.64 (corresponding to a match probability of around 98%)
For this dataset and this trained model, we can see that most records are (theoretically) linkable: At a match weight 6, around around 99% of records could be linked to themselves.

https://github.com/moj-analytical-services/splink/blob/master/docs/charts/unlinkables_chart.ipynb
"""
# Detecting unlinkable records
linker.evaluation.unlinkables_chart()

# --------------------------------------------------------------------------------
# 5) Predict matches and retrieve the match scores
# --------------------------------------------------------------------------------

"""
We use linker.predict() to run the model.
Under the hood this will:
    Generate all pairwise record comparisons that match at least one of the blocking_rules_to_generate_predictions
    Use the rules specified in the Comparisons to evaluate the similarity of the input data
    Use the estimated match weights, applying term frequency adjustments where requested to produce the final match_weight and match_probability scores

Optionally, a threshold_match_probability or threshold_match_weight can be provided, which will drop any row where the predicted score is below the threshold.
"""
# Now we have trained a model, we can move on to using it predict matching records.
# The result of linker.predict() is a list of pairwise record comparisons and their associated scores.
# This is a Splink dataframe (type - DuckDBDataFrame)

# Specifying a threshold_match_probability or threshold_match_weight simply filters the output to records above that threshold.
# This avoids outputting all pariwise combinations, which may be too many to store in memory.
# We should set a threshold that returns a reasonable number of records for us to review.
# threshold_match_probability=0.95
# threshold_match_weight=22
df_predictions = linker.inference.predict(threshold_match_probability=0.95)

# Generate a histogram that shows the distribution of match weights in df_predictions
linker.visualisations.match_weights_histogram(df_predictions)

# Converting to a Pandas dataframe
# 615,824 records (pairwise comparisons above the threshold)
df_predictions_pandas = df_predictions.as_pandas_dataframe()

# Saving Splink dataframe to csv (if the record count is very large it won't save all records to CSV)
path = f"/workspaces/unify_2_1_source_to_staging/temp_db/{model_name}_predictions.csv"
df_predictions.to_csv(path, overwrite=True)

# Saving Splink dataframe to parquet format.
# SELECT match_weight, match_probability, id_l, id_r, surname_l, surname_r, given_name_1_l, given_name_1_r, given_name_2_l, given_name_2_r, date_of_birth_l, date_of_birth_r, sex_l, sex_r, contact_number_l, contact_number_r, suburb_code_l, suburb_code_r, name_concat_l, name_concat_r
# FROM data WHERE match_weight > 22 ORDER BY match_weight ASC
path = f"/workspaces/unify_2_1_source_to_staging/temp_db/{model_name}_predictions.parquet"
df_predictions.to_parquet(path, overwrite=True)

# We should now identify an optimal match weight threshold.
# By reviewing records in df_predictions we can determine which threshold_match_weight have little to no false positive matches.
# running query (SELECT count(*) as count FROM data WHERE match_weight > 22) on parquet explorer
# 173,288 records - person_model_1 - threshold_match_weight > 25
# 164,015 records - person_model_2 - threshold_match_weight > 22

# # Load Parquet file into a DuckDBPyRelation (not sure how to convert it back to a splink dataframe/DuckDBDataFrame)
# # Path to the Parquet file
# path = f"/workspaces/unify_2_1_source_to_staging/temp_db/{model_name}_predictions.parquet"
# # Connect to DuckDB (in-memory database)
# con = duckdb.connect()
# # Read the Parquet file into a DuckDBPyRelation
# df_predictions_test = con.execute(f"SELECT * FROM read_parquet('{path}')")

# from splink.internals.duckdb.dataframe import SplinkDataFrame

"""
An alternative representation of this result is more useful, where each row is an input record, and where records link, they are assigned to the same cluster.
"""

# Clusters the pairwise match predictions into groups of connected records.
# Records with an estimated match probability at or above threshold_match_probability are considered to be a match (i.e. they represent the same entity).
df_clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(  # noqa
    df_predictions, threshold_match_weight=22
)
# Converting to a Pandas dataframe
# 585,187 records (same number of records as df_training_data_2)
clusters_pandas_df = df_clusters.as_pandas_dataframe()

# Saving Splink dataframe to csv (if the record count is very large it won't save all records to CSV)
path = f"/workspaces/unify_2_1_source_to_staging/temp_db/{model_name}_clusters.csv"
df_clusters.to_csv(path, overwrite=True)

# Saving Splink dataframe to parquet format.
path = f"/workspaces/unify_2_1_source_to_staging/temp_db/{model_name}_clusters.parquet"
df_clusters.to_parquet(path, overwrite=True)

"""
SELECT
    COUNT(DISTINCT cluster_id) AS unique_cluster_id_count,
    COUNT(DISTINCT id) AS unique_id_count
FROM
    data

585,187 - person records original
459,213 - person records after deduplication
125,974 - person records merged duplicates (22% reduction)

671,548 - person records actual
86,361 - person records exact duplicates
212,335 - person records duplicates total (31% reduction)
"""

# --------------------------------------------------------------------------------
# 6) Visualising predictions
# --------------------------------------------------------------------------------

# Now we have made predictions with a model, we can move on to visualising it to understand how it is working.

"""
The waterfall chart provides a means of visualising individual predictions to understand how Splink computed the final matchweight for a particular pairwise record comparison.
How to interpret the chart
    The first bar (labelled "Prior") is the match weight if no additional knowledge of features is taken into account, and can be thought of as similar to the y-intercept in a simple regression.
    Each subsequent bar shows the match weight for a comparison. These bars can be positive or negative depending on whether the given comparison gives positive or negative evidence for the two records being a match.
    Additional bars are added for comparisons with term frequency adjustments. For example, the chart above has term frequency adjustments for first_name so there is an extra tf_first_name bar showing how the frequency of a given name impacts the amount of evidence for the two records being a match.
    The final bar represents total match weight for the pair of records. This match weight can also be translated into a final match probability, and the corresponding match probability is shown on the right axis (note the logarithmic scale).
To plot a waterfall chart, the user chooses one or more records from the results of linker.inference.predict(), and provides these records to the linker.visualisations.waterfall_chart() function.
"""

# Subset certain perdictions based on a SQL query
pairwise_comparison = df_predictions.as_pandas_dataframe().query("id_l in ('100137') or id_r in ('100137')")
# Convert the DataFrame to a list of dictionaries
pairwise_list = pairwise_comparison.to_dict(orient="records")
# Waterfall chart for these records
linker.visualisations.waterfall_chart(pairwise_list, filter_nulls=False)

# # Subset certain perdictions based on a SQL query
# pairwise_comparison = df_predictions.as_pandas_dataframe().query("id_l in ('386140') or id_r in ('386140')")
# # Convert the DataFrame to a list of dictionaries
# pairwise_list = pairwise_comparison.to_dict(orient='records')
# # Waterfall chart for these records
# linker.visualisations.waterfall_chart(pairwise_list, filter_nulls=False)

# Waterfall chart for certain random records
records_to_view = df_predictions.as_record_dict(limit=10)
linker.visualisations.waterfall_chart(records_to_view, filter_nulls=False)

# Comparison viewer dashboard
# The comparison viewer dashboard takes this one step further by producing an interactive dashboard that contains example predictions from across the spectrum of match scores.
path = f"/workspaces/unify_2_1_source_to_staging/temp_db/dashboard_{model_name}.html"
linker.visualisations.comparison_viewer_dashboard(df_predictions, path, overwrite=True, num_example_rows=10)
# To view html file in your browser go to saved location

"""
Cluster studio is an interactive dashboards that visualises the results of clustering your predictions.
It provides examples of clusters of different sizes.
The shape and size of clusters can be indicative of problems with record linkage, so it provides a tool to help you find potential false positive and negative links.
"""

# Cluster studio dashboard
path = f"/workspaces/unify_2_1_source_to_staging/temp_db/cluster_studio_{model_name}.html"
linker.visualisations.cluster_studio_dashboard(
    df_predictions,
    df_clusters,
    path,
    sampling_method="by_cluster_size",
    overwrite=True,
)

# --------------------------------------------------------------------------------
# 7) Post-processing to generate final mapping and de-dup tables
# --------------------------------------------------------------------------------

"""
Merging the clustered person records with the exact match person records.
The resulting clusters_pandas_df_2 contains:
    All original records from clusters_pandas_df.
    Records from df_new_duplicates_exact with the appropriate cluster_id:
    Either the matched cluster_id from clusters_pandas_df.
    Or the id from df_new_duplicates_exact if no match was found.
"""


def process_clusters(df_new_duplicates_exact, clusters_pandas_df):
    # Get unique master_id values from df_new_duplicates_exact
    unique_master_ids = df_new_duplicates_exact["master_id"].unique()

    # Find cluster_id in clusters_pandas_df for these master_ids using the id column as the link
    master_id_to_cluster_id = clusters_pandas_df[clusters_pandas_df["id"].isin(unique_master_ids)][["id", "cluster_id"]]

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
    df_new_duplicates_exact_with_cluster["cluster_id"] = df_new_duplicates_exact_with_cluster["cluster_id"].fillna(
        df_new_duplicates_exact_with_cluster["id_duplicate"]
    )

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
# 671,548 records and 25 columns (same as total record count for cleaned person table)
clusters_pandas_df_2 = process_clusters(df_new_duplicates_exact, clusters_pandas_df)

# using data from cms_clean and staging:
# Unique count of id: 671,548
# Unique count of cluster_id: 460,756
# Percentage reduction: 31.39%

print(f"Unique count of id: {clusters_pandas_df_2['id'].nunique()}")
print(f"Unique count of cluster_id: {clusters_pandas_df_2['cluster_id'].nunique()}")
print(
    f"Percentage reduction: {((clusters_pandas_df_2['id'].nunique() - clusters_pandas_df_2['cluster_id'].nunique()) / clusters_pandas_df_2['id'].nunique()) * 100:.2f}%"
)


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


# 671,548 records and 35 columns
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


# 671,548 records and 43 columns
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
    clusters_pandas_df_4.loc[null_weight_mask & clusters_pandas_df_4["id"].isin(exact_match_ids), gamma_columns] = (
        "Association match (Exact)"
    )

    # Apply 'Association match' for remaining NULL match_weight records
    clusters_pandas_df_4.loc[null_weight_mask & ~clusters_pandas_df_4["id"].isin(exact_match_ids), gamma_columns] = (
        "Association match"
    )

    return clusters_pandas_df_4


# 671,548 records and 43 columns
# 2147 records as "Association match"
# 84,818 records as "Association match (Exact)"
clusters_pandas_df_5 = update_association_match(df_new_duplicates_exact, clusters_pandas_df_4)


# Connection parameters
server = "ATLASSBXSQL03.ems.tas.gov.au"
database = "staging"
connection_string = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;TrustServerCertificate=yes;"
connection = pyodbc.connect(connection_string)
cursor = connection.cursor()

# Check if the table exists
check_table_query = """
IF OBJECT_ID('staging.dbo.DO_NOT_MIGRATE_person_lookup', 'U') IS NOT NULL
BEGIN
    TRUNCATE TABLE staging.dbo.DO_NOT_MIGRATE_person_lookup
END
ELSE
BEGIN
    CREATE TABLE staging.dbo.DO_NOT_MIGRATE_person_lookup (
        cluster_id INT NULL,
        id INT NULL,
        version INT NULL,
        country_of_origin_id INT NULL,
        date_created DATETIME NULL,
        date_of_birth DATETIME NULL,
        given_name_1 VARCHAR(100) NULL,
        given_name_2 VARCHAR(100) NULL,
        given_name_3 VARCHAR(100) NULL,
        indigenous_status_id INT NULL,
        last_updated DATETIME NULL,
        ne_reason VARCHAR(255) NULL,
        not_editable TINYINT NULL,
        primary_address_id INT NULL,
        primary_contact_id INT NULL,
        racial_appearance_id INT NULL,
        sex_id INT NULL,
        surname VARCHAR(100) NULL,
        care_of_address TINYINT NULL,
        sex VARCHAR(100) NULL,
        suburb_code VARCHAR(100) NULL,
        suburb_description VARCHAR(100) NULL,
        postcode VARCHAR(10) NULL,
        contact_number INT NULL,
        name_concat VARCHAR(200) NULL,
        match_weight FLOAT NULL,
        match_probability FLOAT NULL,
        gamma_surname INT NULL,
        gamma_given_name_1 INT NULL,
        gamma_given_name_2 INT NULL,
        gamma_date_of_birth INT NULL,
        gamma_sex INT NULL,
        gamma_contact_number INT NULL,
        gamma_suburb_code INT NULL,
        gamma_given_name_1_surname INT NULL,
        gamma_surname_desc VARCHAR (100) NULL,
        gamma_given_name_1_desc VARCHAR (100) NULL,
        gamma_given_name_2_desc VARCHAR (100) NULL,
        gamma_date_of_birth_desc VARCHAR (100) NULL,
        gamma_sex_desc VARCHAR (100) NULL,
        gamma_contact_number_desc VARCHAR (100) NULL,
        gamma_suburb_code_desc VARCHAR (100) NULL,
        gamma_given_name_1_surname_desc VARCHAR (100) NULL,
        new_id INT NULL
    )
END
"""
# Execute the query to check, truncate, or create the table
cursor.execute(check_table_query)
connection.commit()

# Define table name
table_name = "DO_NOT_MIGRATE_person_lookup"


def chunker(seq, size):
    # from http://stackoverflow.com/a/434328
    return (seq[pos : pos + size] for pos in range(0, len(seq), size))


def upload_result(results: pd.DataFrame, table_name: str):
    server = "ATLASSBXSQL03.ems.tas.gov.au"
    database = "staging"
    driver = "ODBC Driver 18 for SQL Server"
    connection_url = URL.create(
        "mssql+pyodbc",
        host=server,
        database=database,
        # Using trusted_connection to enable Windows Authentication for database access
        query={
            "driver": driver,
            "trusted_Connection": "yes",
            "TrustServerCertificate": "yes",
        },
    )
    engine = create_engine(connection_url, connect_args={"connect_timeout": 60}, fast_executemany=True)

    chunksize = int(len(results) / 40)
    with tqdm(total=len(results)) as pbar:
        for i, cdf in enumerate(chunker(results, chunksize)):
            replace = "replace" if i == 0 else "append"
            cdf.to_sql(table_name, engine, if_exists=replace, index=False, schema="dbo")
            pbar.update(chunksize)


# Takes about 14 min to run
# 671,548 records and 43 columns
upload_result(clusters_pandas_df_5, table_name)

# Updating table DO_NOT_MIGRATE_person_lookup with the original given_names and surname.
query_10 = """
-- Update DO_NOT_MIGRATE_person_lookup with data from person using id as the link
-- 671,548 records updated
UPDATE c
SET
    c.given_name_1 = p.given_name_1,
    c.given_name_2 = p.given_name_2,
    c.given_name_3 = p.given_name_3,
    c.surname = p.surname,
    c.name_concat = CONCAT(p.given_name_1, ' ', p.surname)
FROM
    staging.dbo.DO_NOT_MIGRATE_person_lookup c
INNER JOIN
    staging.dbo.person p
ON
    c.id = p.id;
"""

# Execute the query
cursor.execute(query_10)
connection.commit()

# Creating new table person_dedup with Master records with new IDs.
# Missing values in master records are filled using non-master records by combining partial person information.
# The DO_NOT_MIGRATE_person_lookup updated to retain a reference (new_id) to the new master record.
query_9 = """
-- Create or truncate person_dedup
IF OBJECT_ID('staging.dbo.person_dedup', 'U') IS NOT NULL
BEGIN
    TRUNCATE TABLE staging.dbo.person_dedup
END
ELSE
BEGIN
    CREATE TABLE staging.dbo.person_dedup (
    	cluster_id VARCHAR(100) NULL,
        id INT NULL,
        version INT NULL,
        country_of_origin_id INT NULL,
        date_created DATETIME NULL,
        date_of_birth DATETIME NULL,
        given_name_1 VARCHAR(100) NULL,
        given_name_2 VARCHAR(100) NULL,
        given_name_3 VARCHAR(100) NULL,
        indigenous_status_id INT NULL,
        last_updated DATETIME NULL,
        ne_reason VARCHAR(255) NULL,
        not_editable TINYINT NULL,
        primary_address_id INT NULL,
        primary_contact_id INT NULL,
        racial_appearance_id INT NULL,
        sex_id INT NULL,
        surname VARCHAR(100) NULL,
        care_of_address TINYINT NULL,
    )
END;

-- Insert master records into person_dedup
-- 460,756 records inserted - the unique count of cluster_id
INSERT INTO staging.dbo.person_dedup (
    cluster_id, version, country_of_origin_id, date_created, date_of_birth, given_name_1, given_name_2,
    given_name_3, indigenous_status_id, last_updated, ne_reason, not_editable, primary_address_id,
    primary_contact_id, racial_appearance_id, sex_id, surname, care_of_address
)
SELECT
    cluster_id, version, country_of_origin_id, date_created, date_of_birth, given_name_1, given_name_2,
    given_name_3, indigenous_status_id, last_updated, ne_reason, not_editable, primary_address_id,
    primary_contact_id, racial_appearance_id, sex_id, surname, care_of_address
FROM staging.dbo.DO_NOT_MIGRATE_person_lookup
WHERE id = cluster_id
ORDER BY cluster_id;
"""

# Execute the query
# 460,756 records (unique count of cluster_id)
cursor.execute(query_9)
connection.commit()

query_11 = """
-- COALESCE missing values from non-master records in DESCENDING ORDER of last_updated
-- 101,803 records updated
-- First CTE: Rank the records in descending order based on last_updated for each cluster_id
WITH RankedData AS (
    SELECT
        cluster_id,
        version,
        country_of_origin_id,
        date_of_birth,
        given_name_1,
        given_name_2,
        given_name_3,
        indigenous_status_id,
        ne_reason,
        not_editable,
        primary_address_id,
        primary_contact_id,
        racial_appearance_id,
        sex_id,
        surname,
        care_of_address,
        last_updated,
        ROW_NUMBER() OVER (
            PARTITION BY cluster_id ORDER BY
            CASE WHEN country_of_origin_id IS NOT NULL THEN 0 ELSE 1 END, last_updated DESC
        ) AS rn_country_of_origin_id,
        ROW_NUMBER() OVER (
            PARTITION BY cluster_id ORDER BY
            CASE WHEN version IS NOT NULL THEN 0 ELSE 1 END, last_updated DESC
        ) AS rn_version,
        ROW_NUMBER() OVER (
            PARTITION BY cluster_id ORDER BY
            CASE WHEN date_of_birth IS NOT NULL THEN 0 ELSE 1 END, last_updated DESC
        ) AS rn_date_of_birth,
        ROW_NUMBER() OVER (
            PARTITION BY cluster_id ORDER BY
            CASE WHEN given_name_1 IS NOT NULL THEN 0 ELSE 1 END, last_updated DESC
        ) AS rn_given_name_1,
        ROW_NUMBER() OVER (
            PARTITION BY cluster_id ORDER BY
            CASE WHEN given_name_2 IS NOT NULL THEN 0 ELSE 1 END, last_updated DESC
        ) AS rn_given_name_2,
        ROW_NUMBER() OVER (
            PARTITION BY cluster_id ORDER BY
            CASE WHEN given_name_3 IS NOT NULL THEN 0 ELSE 1 END, last_updated DESC
        ) AS rn_given_name_3,
        ROW_NUMBER() OVER (
            PARTITION BY cluster_id ORDER BY
            CASE WHEN indigenous_status_id IS NOT NULL THEN 0 ELSE 1 END, last_updated DESC
        ) AS rn_indigenous_status_id,
        ROW_NUMBER() OVER (
            PARTITION BY cluster_id ORDER BY
            CASE WHEN ne_reason IS NOT NULL THEN 0 ELSE 1 END, last_updated DESC
        ) AS rn_ne_reason,
        ROW_NUMBER() OVER (
            PARTITION BY cluster_id ORDER BY
            CASE WHEN not_editable IS NOT NULL THEN 0 ELSE 1 END, last_updated DESC
        ) AS rn_not_editable,
        ROW_NUMBER() OVER (
            PARTITION BY cluster_id ORDER BY
            CASE WHEN primary_address_id IS NOT NULL THEN 0 ELSE 1 END, last_updated DESC
        ) AS rn_primary_address_id,
        ROW_NUMBER() OVER (
            PARTITION BY cluster_id ORDER BY
            CASE WHEN primary_contact_id IS NOT NULL THEN 0 ELSE 1 END, last_updated DESC
        ) AS rn_primary_contact_id,
        ROW_NUMBER() OVER (
            PARTITION BY cluster_id ORDER BY
            CASE WHEN racial_appearance_id IS NOT NULL THEN 0 ELSE 1 END, last_updated DESC
        ) AS rn_racial_appearance_id,
        ROW_NUMBER() OVER (
            PARTITION BY cluster_id ORDER BY
            CASE WHEN sex_id IS NOT NULL THEN 0 ELSE 1 END, last_updated DESC
        ) AS rn_sex_id,
        ROW_NUMBER() OVER (
            PARTITION BY cluster_id ORDER BY
            CASE WHEN surname IS NOT NULL THEN 0 ELSE 1 END, last_updated DESC
        ) AS rn_surname,
        ROW_NUMBER() OVER (
            PARTITION BY cluster_id ORDER BY
            CASE WHEN care_of_address IS NOT NULL THEN 0 ELSE 1 END, last_updated DESC
        ) AS rn_care_of_address
    FROM staging.dbo.DO_NOT_MIGRATE_person_lookup
    WHERE id <> cluster_id
),
-- Second CTE: Select only the best record for each column within each cluster_id
NonMasterRecords AS (
    SELECT
        cluster_id,
        MAX(CASE WHEN rn_version = 1 THEN version END) AS version,
        MAX(CASE WHEN rn_country_of_origin_id = 1 THEN country_of_origin_id END) AS country_of_origin_id,
        MAX(CASE WHEN rn_date_of_birth = 1 THEN date_of_birth END) AS date_of_birth,
        MAX(CASE WHEN rn_given_name_1 = 1 THEN given_name_1 END) AS given_name_1,
        MAX(CASE WHEN rn_given_name_2 = 1 THEN given_name_2 END) AS given_name_2,
        MAX(CASE WHEN rn_given_name_3 = 1 THEN given_name_3 END) AS given_name_3,
        MAX(CASE WHEN rn_indigenous_status_id = 1 THEN indigenous_status_id END) AS indigenous_status_id,
        MAX(CASE WHEN rn_ne_reason = 1 THEN ne_reason END) AS ne_reason,
        MAX(CASE WHEN rn_not_editable = 1 THEN not_editable END) AS not_editable,
        MAX(CASE WHEN rn_primary_address_id = 1 THEN primary_address_id END) AS primary_address_id,
        MAX(CASE WHEN rn_primary_contact_id = 1 THEN primary_contact_id END) AS primary_contact_id,
        MAX(CASE WHEN rn_racial_appearance_id = 1 THEN racial_appearance_id END) AS racial_appearance_id,
        MAX(CASE WHEN rn_sex_id = 1 THEN sex_id END) AS sex_id,
        MAX(CASE WHEN rn_surname = 1 THEN surname END) AS surname,
        MAX(CASE WHEN rn_care_of_address = 1 THEN care_of_address END) AS care_of_address
    FROM RankedData
    GROUP BY cluster_id
)
-- Update the master records in staging.dbo.person_dedup
UPDATE f
SET
    f.version = COALESCE(f.version, n.version),
    f.country_of_origin_id = COALESCE(f.country_of_origin_id, n.country_of_origin_id),
    f.date_of_birth = COALESCE(f.date_of_birth, n.date_of_birth),
    f.given_name_1 = COALESCE(f.given_name_1, n.given_name_1),
    f.given_name_2 = COALESCE(f.given_name_2, n.given_name_2),
    f.given_name_3 = COALESCE(f.given_name_3, n.given_name_3),
    f.indigenous_status_id = COALESCE(f.indigenous_status_id, n.indigenous_status_id),
    f.ne_reason = COALESCE(f.ne_reason, n.ne_reason),
    f.not_editable = COALESCE(f.not_editable, n.not_editable),
    f.primary_address_id = COALESCE(f.primary_address_id, n.primary_address_id),
    f.primary_contact_id = COALESCE(f.primary_contact_id, n.primary_contact_id),
    f.racial_appearance_id = COALESCE(f.racial_appearance_id, n.racial_appearance_id),
    f.sex_id = COALESCE(f.sex_id, n.sex_id),
    f.surname = COALESCE(f.surname, n.surname),
    f.care_of_address = COALESCE(f.care_of_address, n.care_of_address)
FROM staging.dbo.person_dedup f
JOIN NonMasterRecords n ON n.cluster_id = f.cluster_id;
"""

# Execute the query
cursor.execute(query_11)
connection.commit()


"""
-- COALESCE missing values from non-master records in DESCENDING ORDER of last_updated
-- 101,803 records updated
WITH NonMasterRecords AS (
    SELECT
        cluster_id,
        LEFT(STRING_AGG(CONVERT(INT, version), ', ') WITHIN GROUP (ORDER BY last_updated DESC),
            CHARINDEX(',', STRING_AGG(CONVERT(INT, version), ', ') WITHIN GROUP (ORDER BY last_updated DESC) + ',') - 1) AS version,

        LEFT(STRING_AGG(CONVERT(INT, country_of_origin_id), ', ') WITHIN GROUP (ORDER BY last_updated DESC),
            CHARINDEX(',', STRING_AGG(CONVERT(INT, country_of_origin_id), ', ') WITHIN GROUP (ORDER BY last_updated DESC) + ',') - 1) AS country_of_origin_id,

        LEFT(STRING_AGG(CONVERT(VARCHAR, date_of_birth, 120), ', ') WITHIN GROUP (ORDER BY last_updated DESC),
            CHARINDEX(',', STRING_AGG(CONVERT(VARCHAR, date_of_birth, 120), ', ') WITHIN GROUP (ORDER BY last_updated DESC) + ',') - 1) AS date_of_birth,

        LEFT(STRING_AGG(given_name_1, ', ') WITHIN GROUP (ORDER BY last_updated DESC),
            CHARINDEX(',', STRING_AGG(given_name_1, ', ') WITHIN GROUP (ORDER BY last_updated DESC) + ',') - 1) AS given_name_1,

        LEFT(STRING_AGG(given_name_2, ', ') WITHIN GROUP (ORDER BY last_updated DESC),
            CHARINDEX(',', STRING_AGG(given_name_2, ', ') WITHIN GROUP (ORDER BY last_updated DESC) + ',') - 1) AS given_name_2,

        LEFT(STRING_AGG(given_name_3, ', ') WITHIN GROUP (ORDER BY last_updated DESC),
            CHARINDEX(',', STRING_AGG(given_name_3, ', ') WITHIN GROUP (ORDER BY last_updated DESC) + ',') - 1) AS given_name_3,

        LEFT(STRING_AGG(CONVERT(INT, indigenous_status_id), ', ') WITHIN GROUP (ORDER BY last_updated DESC),
            CHARINDEX(',', STRING_AGG(CONVERT(INT, indigenous_status_id), ', ') WITHIN GROUP (ORDER BY last_updated DESC) + ',') - 1) AS indigenous_status_id,

        LEFT(STRING_AGG(ne_reason, ', ') WITHIN GROUP (ORDER BY last_updated DESC),
            CHARINDEX(',', STRING_AGG(ne_reason, ', ') WITHIN GROUP (ORDER BY last_updated DESC) + ',') - 1) AS ne_reason,

        LEFT(STRING_AGG(CONVERT(INT, not_editable), ', ') WITHIN GROUP (ORDER BY last_updated DESC),
            CHARINDEX(',', STRING_AGG(CONVERT(INT, not_editable), ', ') WITHIN GROUP (ORDER BY last_updated DESC) + ',') - 1) AS not_editable,

        LEFT(STRING_AGG(CONVERT(INT, primary_address_id), ', ') WITHIN GROUP (ORDER BY last_updated DESC),
            CHARINDEX(',', STRING_AGG(CONVERT(INT, primary_address_id), ', ') WITHIN GROUP (ORDER BY last_updated DESC) + ',') - 1) AS primary_address_id,

        LEFT(STRING_AGG(CONVERT(INT, primary_contact_id), ', ') WITHIN GROUP (ORDER BY last_updated DESC),
            CHARINDEX(',', STRING_AGG(CONVERT(INT, primary_contact_id), ', ') WITHIN GROUP (ORDER BY last_updated DESC) + ',') - 1) AS primary_contact_id,

        LEFT(STRING_AGG(CONVERT(INT, racial_appearance_id), ', ') WITHIN GROUP (ORDER BY last_updated DESC),
            CHARINDEX(',', STRING_AGG(CONVERT(INT, racial_appearance_id), ', ') WITHIN GROUP (ORDER BY last_updated DESC) + ',') - 1) AS racial_appearance_id,

        LEFT(STRING_AGG(CONVERT(INT, sex_id), ', ') WITHIN GROUP (ORDER BY last_updated DESC),
            CHARINDEX(',', STRING_AGG(CONVERT(INT, sex_id), ', ') WITHIN GROUP (ORDER BY last_updated DESC) + ',') - 1) AS sex_id,

        LEFT(STRING_AGG(surname, ', ') WITHIN GROUP (ORDER BY last_updated DESC),
            CHARINDEX(',', STRING_AGG(surname, ', ') WITHIN GROUP (ORDER BY last_updated DESC) + ',') - 1) AS surname,

        LEFT(STRING_AGG(CONVERT(INT, care_of_address), ', ') WITHIN GROUP (ORDER BY last_updated DESC),
            CHARINDEX(',', STRING_AGG(CONVERT(INT, care_of_address), ', ') WITHIN GROUP (ORDER BY last_updated DESC) + ',') - 1) AS care_of_address

    FROM staging.dbo.DO_NOT_MIGRATE_person_lookup
    WHERE id <> cluster_id
    GROUP BY cluster_id
)
UPDATE f
SET
    f.version = COALESCE(f.version, n.version),
    f.country_of_origin_id = COALESCE(f.country_of_origin_id, n.country_of_origin_id),
    f.date_of_birth = COALESCE(f.date_of_birth, n.date_of_birth),
    f.given_name_1 = COALESCE(f.given_name_1, n.given_name_1),
    f.given_name_2 = COALESCE(f.given_name_2, n.given_name_2),
    f.given_name_3 = COALESCE(f.given_name_3, n.given_name_3),
    f.indigenous_status_id = COALESCE(f.indigenous_status_id, n.indigenous_status_id),
    f.ne_reason = COALESCE(f.ne_reason, n.ne_reason),
    f.not_editable = COALESCE(f.not_editable, n.not_editable),
    f.primary_address_id = COALESCE(f.primary_address_id, n.primary_address_id),
    f.primary_contact_id = COALESCE(f.primary_contact_id, n.primary_contact_id),
    f.racial_appearance_id = COALESCE(f.racial_appearance_id, n.racial_appearance_id),
    f.sex_id = COALESCE(f.sex_id, n.sex_id),
    f.surname = COALESCE(f.surname, n.surname),
    f.care_of_address = COALESCE(f.care_of_address, n.care_of_address)
FROM staging.dbo.person_dedup f
JOIN NonMasterRecords n ON n.cluster_id = f.cluster_id;
"""


query_12 = """
-- Populate the id column in table person_dedup
-- 460,756 records updated
WITH CTE AS (
    SELECT id, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS new_id
    FROM staging.dbo.person_dedup
)
UPDATE CTE
SET id = new_id;

-- Update DO_NOT_MIGRATE_person_lookup with the new_id column
IF COL_LENGTH('staging.dbo.DO_NOT_MIGRATE_person_lookup', 'new_id') IS NULL
    ALTER TABLE staging.dbo.DO_NOT_MIGRATE_person_lookup ADD new_id INT;
"""

# Execute the query
cursor.execute(query_12)
connection.commit()

query_13 = """
-- 671,548 records updated
UPDATE c
SET c.new_id = f.id
FROM staging.dbo.DO_NOT_MIGRATE_person_lookup c
JOIN staging.dbo.person_dedup f ON c.cluster_id = f.cluster_id;
"""

# Execute the query
cursor.execute(query_13)
connection.commit()

# Close the database connection
cursor.close()
connection.close()
