import pandas as pd
import numpy as np
import pyodbc
import re
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
    JaccardAtThresholds,
    DateOfBirthComparison,
    ArrayIntersectAtSizes,
    ForenameSurnameComparison,
)

pd.options.mode.chained_assignment = None
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

# Script Purpose: build and train a Splink model (based on Fellegi-Sunter's model of record linkage) for probabilistic record linkage that allows us to link child records and de-duplicate them.
#                 https://moj-analytical-services.github.io/splink/index.html
# Feature DQI: 24183, 35420, 37973, 24127
# Input: Original child table (selected columns) along with suburb and contact_number (actual number))
# Output: a mapping table (DO_NOT_MIGRATE_child_dedup) between the child table ids and master_id (updated_cluster_id).

server = "ATLASSBXSQL02.ems.tas.gov.au"
database = "fvms_clean"
connection_string = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;TrustServerCertificate=yes;"
connection = pyodbc.connect(connection_string)
cursor = connection.cursor()

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
	WHERE
        (given_name_1     IS NULL OR LOWER(given_name_1) NOT LIKE '%unborn%' AND LOWER(given_name_1) NOT LIKE '%unbourn%')
        AND (given_name_2 IS NULL OR LOWER(given_name_2) NOT LIKE '%unborn%' AND LOWER(given_name_2) NOT LIKE '%unbourn%')
        AND (given_name_3 IS NULL OR LOWER(given_name_3) NOT LIKE '%unborn%' AND LOWER(given_name_3) NOT LIKE '%unbourn%')
        AND (surname      IS NULL OR LOWER(surname)      NOT LIKE '%unborn%' AND LOWER(surname)      NOT LIKE '%unbourn%')
        -- At least one of each pair is not NULL
        AND NOT (date_of_birth  IS NULL AND given_name_1 IS NULL)
        AND NOT (date_of_birth  IS NULL AND surname IS NULL)
        AND NOT (given_name_1   IS NULL AND surname IS NULL)
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

# 64,513 records (167,291 - 64,513 = 102,778 exact duplicates removed) and 15 columns
df_training_data = pd.read_sql(query_4, connection)

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
    OR (given_name_1    IS NOT NULL AND given_name_1 LIKE '%unborn%' OR given_name_1 LIKE '%unbourn%')
    OR (given_name_2    IS NOT NULL AND given_name_2 LIKE '%unborn%' OR given_name_2 LIKE '%unbourn%')
    OR (given_name_3    IS NOT NULL AND given_name_3 LIKE '%unborn%' OR given_name_3 LIKE '%unbourn%')
    OR (surname         IS NOT NULL AND surname      LIKE '%unborn%' OR surname      LIKE '%unbourn%')
	OR (date_of_birth   IS NULL		AND given_name_1 IS NULL)
	OR (date_of_birth   IS NULL		AND surname IS NULL)
	OR (given_name_1	IS NULL		AND surname IS NULL)
ORDER BY
    master_id, -- Group duplicates together
    id; -- Ensure duplicates are ordered correctly
"""

# 102,778 records and 16 columns
df_new_duplicates_exact = pd.read_sql(query_8, connection)

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
        "suburb",
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
# block_on("given_name_1", "substr(surname, 1, 1)")
# block_on("surname", "substr(given_name_1, 1, 1)")
# block_on("date_of_birth")

"""
lachlan b - 2916 comparisons being generated
j smith - 7921 comparisons being generated
2010-01-01 - 1764 comparisons being generated
"""
test = n_largest_blocks(  # noqa
    table_or_tables=df_training_data_2,
    blocking_rule=block_on("date_of_birth"),
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
        # Compare suburb using a string similarity measure
        DamerauLevenshteinAtThresholds("suburb", distance_threshold_or_thresholds=2).configure(
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
linker = Linker(df_training_data_3, settings, db_api=DuckDBAPI())

# Estimate the lambda, m and u parameters (Fellegi-Sunter):

# Estimate lambda (the Bayesian prior)
"""
In some cases, the probability_two_random_records_match will be known. More generally, this parameter is unknown and needs to be estimated.
It can be estimated accurately enough for most purposes by combining a series of deterministic matching rules and a guess of the recall corresponding to those rules.
Here we guess that these deterministic matching rules have a recall of about 90%. That means, between them, the rules recover 90% of all true matches.
"""
deterministic_rules = [
    block_on("given_name_1", "given_name_2", "surname", "sex", "date_of_birth"),
    block_on("given_name_1", "given_name_2", "surname", "sex", "date_of_birth", "suburb"),
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
    block_on("sex", "date_of_birth", "suburb"),
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

"""
Disagreement penalties (ELSE) are driven by the m probabilities of the model, and therefore are particularly susceptible to being poorly estimated
(because you have to use expectation maximisation to estimate m probabilities, which doesn't always work that well).
At the moment there isn't a great way to manually set the m values. We hope to add better support for this in future.
For the moment, the easiest way is probably to change the JSON and load again.

Note: It's best not to manually adjust the match weights like this, as the model learns if a comparison level is
a strong/weak indicator of a match/non-match, leading to the match weights. Therefore, it reflects reality.

Save model to JSON, with a new name, and compare the match weights.
https://github.com/moj-analytical-services/splink/discussions/2362
https://github.com/moj-analytical-services/splink/pull/2379
"""
# # To override the ELSE part of the model (All other comparisons)
field_comparison = linker._settings_obj._get_comparison_by_output_column_name("surname")
else_comparison_level = field_comparison._get_comparison_level_by_comparison_vector_value(0)
else_comparison_level._m_probability = 0.0001

field_comparison = linker._settings_obj._get_comparison_by_output_column_name("given_name_1")
else_comparison_level = field_comparison._get_comparison_level_by_comparison_vector_value(0)
else_comparison_level._m_probability = 0.0001

field_comparison = linker._settings_obj._get_comparison_by_output_column_name("given_name_2")
else_comparison_level = field_comparison._get_comparison_level_by_comparison_vector_value(0)
else_comparison_level._m_probability = 0.03

field_comparison = linker._settings_obj._get_comparison_by_output_column_name("date_of_birth")
exact_comparison_level = field_comparison._get_comparison_level_by_comparison_vector_value(5)
exact_comparison_level._m_probability = 0.04

field_comparison = linker._settings_obj._get_comparison_by_output_column_name("sex")
exact_comparison_level = field_comparison._get_comparison_level_by_comparison_vector_value(0)
exact_comparison_level._m_probability = 0.03

field_comparison = linker._settings_obj._get_comparison_by_output_column_name("contact_number")
exact_comparison_level = field_comparison._get_comparison_level_by_comparison_vector_value(1)
exact_comparison_level._m_probability = 0.02

linker.visualisations.match_weights_chart()

###########
# Define the model iteration
###########

model_name = "fvms_child_model_1e"

# Saving the model
# We can save the model, including our estimated parameters, to a .json file, so we can use it next time.
path = f"./temp_db/{model_name}.json"
linker.misc.save_model_to_json(path, overwrite=True)  # noqa

# Loading a pre-trained model
# When using a pre-trained model, you can read in the model from a json and recreate the linker object to make new pairwise predictions.
settings = "./temp_db/fvms_child/fvms_child_model_1e.json"
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
"""
The linker.predict() results include several intermediate calculation columns that provide insights into how Splink calculates match scores.

tf_ (Term Frequency)
    The values in these columns represent how common a specific value is in the dataset.
    The higher the term frequency value, the more unique the term is.
    Splink uses term frequency to reduce the impact of highly frequent values that might cause false positives.
    Splink uses term frequency values in Bayes Factor calculations to adjust the impact of a match:
        If a value is very common, its match weight is reduced.
        If a value is rare, its match weight is increased.

gamma_ Columns (Comparison Outcomes)
    represent the comparison level assigned to a column for a specific record pair.
    These values are assigned based on the comparison logic defined in your comparison_columns.

bf_ Columns (Bayes Factors)
    These represent the Bayes Factor (BF) for each comparison column, quantifying how much evidence a particular column provides toward a match.
    A higher bf_ value means a stronger indication of a match.

bf_tf_ Columns (Bayes Factors with Term Frequency Adjustments)
    These are the Bayes Factors after adjusting for term frequency.
    Term frequency adjustment down-weights very common values to reduce false positives.
    For example, a comparison of "Smith" might contribute less than "Rutherford", because "Smith" is a common surname.
    If "bf_given_names_surname" = 2.5", but "bf_tf_adj_given_names_surname" = 1.8", it means the adjusted Bayes Factor for the name comparison was down-weighted, possibly because it's a common name.
"""

# Now we have trained a model, we can move on to using it predict matching records.
# The result of linker.predict() is a list of pairwise record comparisons and their associated scores.
# This is a Splink dataframe (type - DuckDBDataFrame)

# Specifying a threshold_match_probability or threshold_match_weight simply filters the output to records above that threshold.
# This avoids outputting all pariwise combinations, which may be too many to store in memory.
# We should set a threshold that returns a reasonable number of records for us to review.
df_predictions = linker.inference.predict(threshold_match_probability=0.95)

# Generate a histogram that shows the distribution of match weights in df_predictions
linker.visualisations.match_weights_histogram(df_predictions)

# Converting to a Pandas dataframe
# 41,024 records (pairwise comparisons above the threshold)
df_predictions_pandas = df_predictions.as_pandas_dataframe()

# Saving Splink dataframe to csv (if the record count is very large it won't save all records to CSV)
path = f"./temp_db/{model_name}_predictions.csv"
df_predictions.to_csv(path, overwrite=True)

# Saving Splink dataframe to parquet format.
# SELECT match_weight, match_probability, id_l, id_r, surname_l, surname_r, given_name_1_l, given_name_1_r, given_name_2_l, given_name_2_r, date_of_birth_l, date_of_birth_r, sex_l, sex_r, contact_number_l, contact_number_r, suburb_l, suburb_r, name_concat_l, name_concat_r
# FROM data WHERE match_weight > 22 ORDER BY match_weight ASC
path = f"./temp_db/{model_name}_predictions.parquet"
df_predictions.to_parquet(path, overwrite=True)

"""
We should now identify an optimal match weight threshold.
By reviewing records in df_predictions we can determine which threshold_match_weight have little to no false positive matches.
Run below query in parquet explorer for each match_weight to identify at which point we have little to no false positives.

SELECT match_weight, match_probability, id_l, id_r, surname_l, surname_r, given_name_1_l, given_name_1_r,
given_name_2_l, given_name_2_r, date_of_birth_l, date_of_birth_r, sex_l, sex_r,
contact_number_l, contact_number_r, suburb_l, suburb_r, name_concat_l, name_concat_r
FROM data
WHERE match_weight > 15 AND match_weight < 16
ORDER BY match_weight asc

Look at the below filters to check if we have any incorrect matches at a given match_weight level:

surname_l != surname_r
given_name_1_l != given_name_1_r
date_of_birth_l != date_of_birth_r
"""

"""
An alternative representation of this result is more useful, where each row is an input record, and where records link, they are assigned to the same cluster.
"""

# Clusters the pairwise match predictions into groups of connected records.
# Records with an estimated match probability at or above threshold_match_probability are considered to be a match (i.e. they represent the same entity).
df_clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(  # noqa
    df_predictions, threshold_match_weight=22
)
# Converting to a Pandas dataframe
# 64,570 records (same number of records as df_training_data_2)
clusters_pandas_df = df_clusters.as_pandas_dataframe()

# Saving Splink dataframe to csv (if the record count is very large it won't save all records to CSV)
path = f"./temp_db/{model_name}_clusters.csv"
df_clusters.to_csv(path, overwrite=True)

# Saving Splink dataframe to parquet format.
path = f"./temp_db/{model_name}_clusters.parquet"
df_clusters.to_parquet(path, overwrite=True)

"""
SELECT
    COUNT(DISTINCT cluster_id) AS unique_cluster_id_count,
    COUNT(DISTINCT id) AS unique_id_count
FROM
    data

64,570 - child records original
49,591 - child records after deduplication
14,979 - child records merged duplicates (23% reduction)

167,291 - child records actual
102,721 - child records exact duplicates
117,700 - child records duplicates total (70% reduction)
"""

"""
Check the largest clusters:

SELECT cluster_id, count(*)
FROM data
GROUP BY cluster_id
ORDER BY count(*) desc
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
path = f"./temp_db/dashboard_{model_name}.html"
linker.visualisations.comparison_viewer_dashboard(df_predictions, path, overwrite=True, num_example_rows=10)
# To view html file in your browser go to saved location

"""
Cluster studio is an interactive dashboards that visualises the results of clustering your predictions.
It provides examples of clusters of different sizes.
The shape and size of clusters can be indicative of problems with record linkage, so it provides a tool to help you find potential false positive and negative links.
"""

# Cluster studio dashboard
path = f"./temp_db/cluster_studio_{model_name}.html"
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
    df_new_duplicates_exact_with_cluster["cluster_id"] = df_new_duplicates_exact_with_cluster["cluster_id"].fillna(
        df_new_duplicates_exact_with_cluster["id_duplicate"]
    )

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
    df_copy.loc[(df_copy[date_column] < min_valid_date) | (df_copy[date_column] > max_valid_date), date_column] = pd.NaT

    return df_copy


df_new_duplicates_exact_2 = handle_out_of_bounds_dates(df_new_duplicates_exact_2, "date_of_birth")
clusters_pandas_df = handle_out_of_bounds_dates(clusters_pandas_df, "date_of_birth")
# 167,291 records and 17 columns (same as total record count for cleaned child table)
clusters_pandas_df_2 = process_clusters(df_new_duplicates_exact_2, clusters_pandas_df)

# Unique count of id: 167,291
# Unique count of cluster_id: 51,023
# Percentage reduction: 69.50%

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
    clusters_df.loc[null_weight_mask & ~clusters_df["id"].isin(exact_match_ids), gamma_columns] = "Association match"

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
# 49,591 records - child
# 70% reduction

# Note: The clusters that group person records, and consequently their new assigned person IDs, will remain consistent across script reruns, provided:
# 1) the underlying legacy dataset remains unchanged.
# 2) the person cleansing script (`old_person.py`) remains unchanged in a way that does not affect grouping logic
# (e.g., changes to name cleansing or inclusion/exclusion of business/employee records may result in different clusters).
