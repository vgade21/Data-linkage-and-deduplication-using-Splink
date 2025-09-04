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

# Script Purpose: build and train a Splink model (based on Fellegi-Sunter's model of record linkage) for probabilistic record linkage that allows us to link parent of child records and de-duplicate them.
#                 https://moj-analytical-services.github.io/splink/index.html
# Feature DQI: 24183, 35420, 37973
# Input: Original child table (selected parent columns)
# Output: a mapping table (DO_NOT_MIGRATE_parent_dedup) between the child table ids (with parent suffix) and master_id (updated_cluster_id).

server = "ATLASSBXSQL02.ems.tas.gov.au"
database = "fvms_clean"
connection_string = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;TrustServerCertificate=yes;"
connection = pyodbc.connect(connection_string)
cursor = connection.cursor()

query_4 = """
WITH CTE_person_data AS (
    -- Parent 1
    SELECT
        id,
        CAST(id AS VARCHAR) + '_p1' AS id_parent,
        parent1address as address,
        parent1date_of_birth as date_of_birth,
        parent1indigenous_status_id as indigenous_status_id,
        -- Split given_names
        LEFT(parent1given_names, CHARINDEX(' ', parent1given_names + ' ') - 1) AS given_name_1,
        CASE
            WHEN CHARINDEX(' ', parent1given_names) > 0
            THEN LEFT(
                    STUFF(parent1given_names, 1, CHARINDEX(' ', parent1given_names), ''),
                    CHARINDEX(' ', STUFF(parent1given_names, 1, CHARINDEX(' ', parent1given_names), '') + ' ') - 1
                )
            ELSE NULL
        END AS given_name_2,
        CASE
            WHEN CHARINDEX(' ', parent1given_names) > 0
                AND CHARINDEX(' ', STUFF(parent1given_names, 1, CHARINDEX(' ', parent1given_names), '')) > 0
            THEN LTRIM(
                    STUFF(
                        STUFF(parent1given_names, 1, CHARINDEX(' ', parent1given_names), ''),
                        1, CHARINDEX(' ', STUFF(parent1given_names, 1, CHARINDEX(' ', parent1given_names), '')), ''
                    )
                )
            ELSE NULL
        END AS given_name_3,
        parent1surname AS surname,
        'parent_1' AS parent_type
    FROM
        fvms_clean.dbo.child

    UNION ALL

    -- Parent 2
    SELECT
        id,
        CAST(id AS VARCHAR) + '_p2' AS id_parent,
        parent2address as address,
        parent2date_of_birth as date_of_birth,
        parent2indigenous_status_id as indigenous_status_id,
        -- Split given_names
        LEFT(parent2given_names, CHARINDEX(' ', parent2given_names + ' ') - 1) AS given_name_1,
        CASE
            WHEN CHARINDEX(' ', parent2given_names) > 0
            THEN LEFT(
                    STUFF(parent2given_names, 1, CHARINDEX(' ', parent2given_names), ''),
                    CHARINDEX(' ', STUFF(parent2given_names, 1, CHARINDEX(' ', parent2given_names), '') + ' ') - 1
                )
            ELSE NULL
        END AS given_name_2,
        CASE
            WHEN CHARINDEX(' ', parent2given_names) > 0
                AND CHARINDEX(' ', STUFF(parent2given_names, 1, CHARINDEX(' ', parent2given_names), '')) > 0
            THEN LTRIM(
                    STUFF(
                        STUFF(parent2given_names, 1, CHARINDEX(' ', parent2given_names), ''),
                        1, CHARINDEX(' ', STUFF(parent2given_names, 1, CHARINDEX(' ', parent2given_names), '')), ''
                    )
                )
            ELSE NULL
        END AS given_name_3,
        parent2surname AS surname,
        'parent_2' AS parent_type
    FROM
        fvms_clean.dbo.child
),
CTE_Deduplicated AS (
    SELECT
        id,
        id_parent,
        address,
        date_of_birth,
        indigenous_status_id,
        given_name_1,
        given_name_2,
        given_name_3,
        surname,
        parent_type,
        ROW_NUMBER() OVER (
            PARTITION BY date_of_birth, given_name_1, given_name_2, given_name_3, surname
            ORDER BY id_parent
        ) AS rn
    FROM
        CTE_person_data
    WHERE
        (given_name_1     IS NULL OR LOWER(given_name_1) NOT LIKE '%unborn%' AND LOWER(given_name_1) NOT LIKE '%unbourn%')
        AND (given_name_2 IS NULL OR LOWER(given_name_2) NOT LIKE '%unborn%' AND LOWER(given_name_2) NOT LIKE '%unbourn%')
        AND (given_name_3 IS NULL OR LOWER(given_name_3) NOT LIKE '%unborn%' AND LOWER(given_name_3) NOT LIKE '%unbourn%')
        AND (surname      IS NULL OR LOWER(surname)      NOT LIKE '%unborn%' AND LOWER(surname)      NOT LIKE '%unbourn%')
        AND (surname      IS NULL OR LOWER(surname)      NOT LIKE '%SEPARATED%')
        -- At least one of each pair is not NULL
        AND NOT (date_of_birth  IS NULL AND given_name_1 IS NULL)
        AND NOT (date_of_birth  IS NULL AND surname IS NULL)
        AND NOT (given_name_1   IS NULL AND surname IS NULL)
)
SELECT
    id,
    id_parent,
    address,
    date_of_birth,
    indigenous_status_id,
    given_name_1,
    given_name_2,
    given_name_3,
    surname,
    parent_type
FROM
    CTE_Deduplicated
WHERE
    rn = 1;
"""

# 36,107 records (167,291 * 2 - 36,107 = 298,475 exact duplicates removed) and 10 columns
df_training_data = pd.read_sql(query_4, connection)

# Loading the exact matches found in person table (we DO NOT run the linking model on this set data).
query_8 = """
WITH CTE_person_data AS (
    -- Parent 1
    SELECT
        id,
        CAST(id AS VARCHAR) + '_p1' AS id_parent,
        parent1address as address,
        parent1date_of_birth as date_of_birth,
        parent1indigenous_status_id as indigenous_status_id,
        -- Split given_names
        LEFT(parent1given_names, CHARINDEX(' ', parent1given_names + ' ') - 1) AS given_name_1,
        CASE
            WHEN CHARINDEX(' ', parent1given_names) > 0
            THEN LEFT(
                    STUFF(parent1given_names, 1, CHARINDEX(' ', parent1given_names), ''),
                    CHARINDEX(' ', STUFF(parent1given_names, 1, CHARINDEX(' ', parent1given_names), '') + ' ') - 1
                )
            ELSE NULL
        END AS given_name_2,
        CASE
            WHEN CHARINDEX(' ', parent1given_names) > 0
                AND CHARINDEX(' ', STUFF(parent1given_names, 1, CHARINDEX(' ', parent1given_names), '')) > 0
            THEN LTRIM(
                    STUFF(
                        STUFF(parent1given_names, 1, CHARINDEX(' ', parent1given_names), ''),
                        1, CHARINDEX(' ', STUFF(parent1given_names, 1, CHARINDEX(' ', parent1given_names), '')), ''
                    )
                )
            ELSE NULL
        END AS given_name_3,
        parent1surname AS surname,
        'parent_1' AS parent_type
    FROM
        fvms_clean.dbo.child

    UNION ALL

    -- Parent 2
    SELECT
        id,
        CAST(id AS VARCHAR) + '_p2' AS id_parent,
        parent2address as address,
        parent2date_of_birth as date_of_birth,
        parent2indigenous_status_id as indigenous_status_id,
        -- Split given_names
        LEFT(parent2given_names, CHARINDEX(' ', parent2given_names + ' ') - 1) AS given_name_1,
        CASE
            WHEN CHARINDEX(' ', parent2given_names) > 0
            THEN LEFT(
                    STUFF(parent2given_names, 1, CHARINDEX(' ', parent2given_names), ''),
                    CHARINDEX(' ', STUFF(parent2given_names, 1, CHARINDEX(' ', parent2given_names), '') + ' ') - 1
                )
            ELSE NULL
        END AS given_name_2,
        CASE
            WHEN CHARINDEX(' ', parent2given_names) > 0
                AND CHARINDEX(' ', STUFF(parent2given_names, 1, CHARINDEX(' ', parent2given_names), '')) > 0
            THEN LTRIM(
                    STUFF(
                        STUFF(parent2given_names, 1, CHARINDEX(' ', parent2given_names), ''),
                        1, CHARINDEX(' ', STUFF(parent2given_names, 1, CHARINDEX(' ', parent2given_names), '')), ''
                    )
                )
            ELSE NULL
        END AS given_name_3,
        parent2surname AS surname,
        'parent_2' AS parent_type
    FROM
        fvms_clean.dbo.child
),
DuplicateRecords AS (
    SELECT
        id,
        id_parent,
        address,
        date_of_birth,
        indigenous_status_id,
        given_name_1,
        given_name_2,
        given_name_3,
        surname,
        parent_type,
        ROW_NUMBER() OVER (
            PARTITION BY date_of_birth, given_name_1, given_name_2, given_name_3, surname
            ORDER BY id_parent
        ) AS RowNum,
        -- Assign master_id
        CASE
        WHEN given_name_1 IS NULL AND surname IS NULL THEN id_parent -- Handle case where given_name_1 and surname are NULL
        -- If any of the columns contain 'unborn' or 'unbourn'
        WHEN
			(p.given_name_1		IS NOT NULL AND LOWER(p.given_name_1) LIKE '%unborn%' OR LOWER(p.given_name_1) LIKE '%unbourn%')
            OR (p.given_name_2	IS NOT NULL AND LOWER(p.given_name_2) LIKE '%unborn%' OR LOWER(p.given_name_2) LIKE '%unbourn%')
            OR (p.given_name_3	IS NOT NULL AND LOWER(p.given_name_3) LIKE '%unborn%' OR LOWER(p.given_name_3) LIKE '%unbourn%')
            OR (p.surname		IS NOT NULL AND LOWER(p.surname)      LIKE '%unborn%' OR LOWER(p.surname)      LIKE '%unbourn%')
            OR (p.surname		IS NOT NULL AND LOWER(p.surname)      LIKE '%SEPARATED%')
			OR (p.date_of_birth IS NULL		AND p.given_name_1 IS NULL)
			OR (p.date_of_birth IS NULL		AND p.surname IS NULL)
			OR (p.given_name_1	IS NULL		AND p.surname IS NULL)
        THEN id_parent
        ELSE MIN(id_parent) OVER (
            PARTITION BY
                given_name_1, given_name_2, given_name_3, surname, date_of_birth
        )
        END AS master_id
    FROM
        CTE_person_data p
)
SELECT
    id,
    id_parent,
    address,
    date_of_birth,
    indigenous_status_id,
    given_name_1,
    given_name_2,
    given_name_3,
    surname,
    parent_type,
    master_id
FROM
    DuplicateRecords
WHERE
    RowNum > 1
    OR (given_name_1    IS NOT NULL AND given_name_1 LIKE '%unborn%' OR given_name_1 LIKE '%unbourn%')
    OR (given_name_2    IS NOT NULL AND given_name_2 LIKE '%unborn%' OR given_name_2 LIKE '%unbourn%')
    OR (given_name_3    IS NOT NULL AND given_name_3 LIKE '%unborn%' OR given_name_3 LIKE '%unbourn%')
    OR (surname         IS NOT NULL AND surname      LIKE '%unborn%' OR surname      LIKE '%unbourn%')
    OR (surname		IS NOT NULL AND surname      LIKE '%SEPARATED%')
	OR (date_of_birth   IS NULL		AND given_name_1 IS NULL)
	OR (date_of_birth   IS NULL		AND surname IS NULL)
	OR (given_name_1	IS NULL		AND surname IS NULL)
ORDER BY
    master_id, -- Group duplicates together
    id; -- Ensure duplicates are ordered correctly
"""

# 298,475 records and 11 columns
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
function to extract the suburb column from the address column as per below:
Extracts suburb:
    Looks for a set of keywords (like 'road', 'drive', etc.) and if found, moves everything after the keyword to address_2_clean.
    If no main keyword match, looks for secondary keywords ('st', 'court').
    If no keyword is found, splits on the first comma.

Post-processing of address_2_clean:
    Removes anything inside brackets ( ... ).
    Replaces non-alphabetic characters (except spaces) with a single space.
    Removes specified keywords (e.g., 'TAS', 'state', 'unit', country/state names, etc.) if they appear at the start, end, or between spaces in the field.

Nullifies generic/excluded values:
    Sets address_2_clean to None if it matches common non-suburbs like 'TAS', 'unknown', country names, or Australian states (case-insensitive).
    Removes everything before and including another set of road/street-type keywords, if found.
    Removes specific state abbreviations ('TAS', 'WA', 'VIC', etc.) wherever they appear as a word.

Final clean-up:
    Replaces multiple spaces with a single space again.
    Trims leading/trailing spaces.
    Sets any remaining empty strings to None.
"""


def process_addresses(df_old):
    # 1) Create a copy of the address column
    df = df_old.copy()
    df["address_2"] = df["address"]

    # 2) Move everything after the specified keywords to address_2_clean
    # More specific keywords first
    main_keywords = [
        "street",
        "road",
        "rd",
        "avenue",
        "ave",
        "drive",
        "dr",
        "place",
        "pl",
        "grove",
        "crescent",
        "crs",
        "parade",
        "highway",
        "close",
        "way",
        "crt",
        "circle",
    ]
    # Secondary keywords
    secondary_keywords = ["st", "court"]

    # Compile patterns
    # Matches: keyword as a word boundary, [optional . or ,], then space, then rest of string
    main_pattern = r"\b(?:" + "|".join(main_keywords) + r")\b[.,]?\s+(.*)"
    secondary_pattern = r"\b(?:" + "|".join(secondary_keywords) + r")\b[.,]?\s+(.*)"

    def split_address(addr):
        if pd.isna(addr):
            return None, addr
        # Try main keywords first
        match = re.search(main_pattern, addr, flags=re.IGNORECASE)
        if match:
            return match.group(1).strip(), None
        # If no match, try 'st' or 'way'
        match2 = re.search(secondary_pattern, addr, flags=re.IGNORECASE)
        if match2:
            return match2.group(1).strip(), None
        return None, addr

    result = df["address_2"].apply(split_address)
    df["address_2_clean"] = [x[0] for x in result]
    df["address_2"] = [x[1] for x in result]

    # 3) For rows where no keyword+comma match, fall back to splitting on the first comma
    def split_by_comma(addr):
        if pd.isna(addr):
            return None, addr
        idx = addr.find(",")
        if idx != -1:
            return addr[idx + 1 :].strip(), None
        else:
            return None, addr

    mask = df["address_2_clean"].isna() & df["address_2"].notna()
    result2 = df.loc[mask, "address_2"].apply(split_by_comma)
    df.loc[mask, "address_2_clean"] = [x[0] for x in result2]
    df.loc[mask, "address_2"] = [x[1] for x in result2]

    # 4) Post-cleaning on address_2_clean
    def post_clean(addr):
        if pd.isna(addr):
            return None
        s = str(addr)

        # Remove everything between brackets including the brackets.
        s = re.sub(r"\([^)]*\)", "", s)

        # Replace all non-alpha characters except spaces with a single space.
        s = re.sub(r"[^A-Za-z\s]", " ", s)

        # Replace multiple spaces with a single space.
        s = re.sub(r"\s+", " ", s)

        # Remove trailing/leading spaces.
        s = s.strip()

        # Remove keywords in the specified positions (start with space, end with space, between spaces)
        kw_list = [
            "in",
            "TAS",
            "of abode",
            "unit",
            "tower",
            "state",
            "null",
            "unknown",
            "Lighthouse",
            "Hotel",
            "Tandara Motor Inn",
            "Turners Beach Caravan Park",
            "new zealand",
            "Germany",
            "Azerbaijan",
            "Czech Republic",
            "China",
            "United States of America",
            "United Kingdom",
            "Queensland",
            "Victoria",
            "Perth",
            "Brisbane",
            "WA",
            "VIC",
            "QLD",
            "NSW",
            "SA",
            "NT",
            "Australia",
        ]
        # Sort by length descending so multi-word keywords go first
        kw_list = sorted(kw_list, key=lambda x: -len(x))
        for kw in kw_list:
            # Remove at start
            s = re.sub(r"^" + re.escape(kw) + r"\s", "", s, flags=re.IGNORECASE)
            # Remove at end
            s = re.sub(r"\s" + re.escape(kw) + r"$", "", s, flags=re.IGNORECASE)
            # Remove between spaces
            s = re.sub(r"(?<=\s)" + re.escape(kw) + r"(?=\s)", "", s, flags=re.IGNORECASE)
        # Clean up extra spaces after keyword removal
        s = re.sub(r"\s+", " ", s).strip()

        # Replace multiple spaces with single space.
        s = re.sub(r"\s+", " ", s)

        # Remove trailing/leading spaces.
        s = s.strip()

        return s if s else None

    df["address_2_clean"] = df["address_2_clean"].apply(post_clean)

    # 5) Nullify excluded values
    exclusions = [
        "TAS",
        "state",
        "null",
        "unknown",
        "new zealand",
        "germany",
        "azerbaijan",
        "czech republic",
        "china",
        "united states of america",
        "united kingdom",
        "queensland",
        "victoria",
        "perth",
        "brisbane",
        "wa",
        "vic",
        "qld",
        "nsw",
        "sa",
        "nt",
        "australia",
    ]
    exclusions = [e.lower() for e in exclusions]

    def exclude_value(val):
        if pd.isna(val):
            return None
        if val.strip().lower() in exclusions:
            return None
        return val

    df["address_2_clean"] = df["address_2_clean"].apply(exclude_value)

    # Remove everything before and including new keywords at a word boundary
    final_keywords = [
        "street",
        "road",
        "rd",
        "avenue",
        "ave",
        "drive",
        "dr",
        "place",
        "pl",
        "crescent",
        "crs",
        "highway",
        "close",
        "way",
        "crt",
        "circle",
        "court",
    ]
    # Regex: at a word boundary, match any keyword (case-insensitive), keep everything after
    final_pattern = r".*\b(?:" + "|".join(final_keywords) + r")\b[.,]?\s*"

    def remove_before_keyword(val):
        if pd.isna(val):
            return None
        # Only modify if a keyword is present
        match = re.search(final_pattern, val, flags=re.IGNORECASE)
        if match:
            rest = val[match.end() :].strip()
            return rest if rest else None
        return val

    df["address_2_clean"] = df["address_2_clean"].apply(remove_before_keyword)

    # Remove just these keywords at a word boundary
    single_keywords = ["TAS", "WA", "VIC", "QLD", "NSW", "SA", "NT"]
    single_pattern = r"\b(?:" + "|".join(single_keywords) + r")\b"

    def remove_single_keywords(val):
        if pd.isna(val):
            return None
        # Remove all occurrences, collapse multiple spaces
        new_val = re.sub(single_pattern, "", val, flags=re.IGNORECASE)
        return new_val

    df["address_2_clean"] = df["address_2_clean"].apply(remove_single_keywords)

    # 6) Replace multiple spaces with a single space
    df["address_2_clean"] = df["address_2_clean"].str.replace(r"\s+", " ", regex=True)
    # Remove leading and trailing spaces
    df["address_2_clean"] = df["address_2_clean"].str.strip()
    # If the value is now empty, set as None
    df["address_2_clean"] = df["address_2_clean"].replace("", None)

    # Rename and drop columns
    df = df.rename(columns={"address_2_clean": "suburb"})
    df = df.drop(columns=["address_2"])

    return df


# Apply to the dataframe
df_training_data_2 = df_training_data_2A.copy()
df_training_data_2 = process_addresses(df_training_data_2)
df_new_duplicates_exact_2 = df_new_duplicates_exact.copy()
df_new_duplicates_exact_2 = process_addresses(df_new_duplicates_exact_2)

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
        "suburb",
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
matthew b - 2809 comparisons being generated
j smith - 5041 comparisons being generated
1982-01-01 - 324 comparisons being generated
"""
test = n_largest_blocks(  # noqa
    table_or_tables=df_training_data_2,
    blocking_rule=block_on("given_name_1", "substr(surname, 1, 1)"),
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
    unique_id_column_name="id_parent",
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
        ),
        # Compare suburb using a string similarity measure
        DamerauLevenshteinAtThresholds("suburb", distance_threshold_or_thresholds=2).configure(
            term_frequency_adjustments=True
        ),
        # ExactMatch("date_of_birth").configure(term_frequency_adjustments=True),
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
    block_on("given_name_1", "given_name_2", "surname", "date_of_birth"),
    block_on("given_name_1", "given_name_2", "surname", "date_of_birth", "suburb"),
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
    block_on("date_of_birth", "suburb"),
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
field_comparison = linker._settings_obj._get_comparison_by_output_column_name("given_name_1")
else_comparison_level = field_comparison._get_comparison_level_by_comparison_vector_value(1)
else_comparison_level._m_probability = 0.005

field_comparison = linker._settings_obj._get_comparison_by_output_column_name("surname")
else_comparison_level = field_comparison._get_comparison_level_by_comparison_vector_value(1)
else_comparison_level._m_probability = 0.015

field_comparison = linker._settings_obj._get_comparison_by_output_column_name("date_of_birth")
exact_comparison_level = field_comparison._get_comparison_level_by_comparison_vector_value(1)
exact_comparison_level._m_probability = 0.04

linker.visualisations.match_weights_chart()

###########
# Define the model iteration
###########

model_name = "fvms_parent_model_1i"

# Saving the model
# We can save the model, including our estimated parameters, to a .json file, so we can use it next time.
path = f"./temp_db/{model_name}.json"
linker.misc.save_model_to_json(path, overwrite=True)  # noqa

# Loading a pre-trained model
# When using a pre-trained model, you can read in the model from a json and recreate the linker object to make new pairwise predictions.
settings = "./temp_db/fvms_parent_model_1i.json"
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
# 9,276 records (pairwise comparisons above the threshold)
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

SELECT match_weight, match_probability, id_parent_l, id_parent_r, surname_l, surname_r, given_name_1_l, given_name_1_r,
given_name_2_l, given_name_2_r, date_of_birth_l, date_of_birth_r, suburb_l, suburb_r, name_concat_l, name_concat_r
FROM data
WHERE match_weight > 15 AND match_weight < 16
ORDER BY match_weight asc

Look at the below filters to check if we have any incorrect matches at a given match_weight level:

surname_l != surname_r
given_name_1_l != given_name_1_r
date_of_birth_l != date_of_birth_r
ABS(EXTRACT(YEAR FROM age(date_of_birth_l, date_of_birth_r))) > 10
"""

"""
An alternative representation of this result is more useful, where each row is an input record, and where records link, they are assigned to the same cluster.
"""

# Clusters the pairwise match predictions into groups of connected records.
# Records with an estimated match probability at or above threshold_match_probability are considered to be a match (i.e. they represent the same entity).
df_clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(  # noqa
    df_predictions, threshold_match_weight=10.5
)
# Converting to a Pandas dataframe
# 36,107 records (same number of records as df_training_data_2)
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
    COUNT(DISTINCT id_parent) AS unique_id_count
FROM
    data

36,107 - parent of child records original
30,352 - parent of child records after deduplication
5,755 - parent of child records merged duplicates (16% reduction)

334,582 - parent of child records actual
298,475 - parent of child records exact duplicates
304,230 - parent of child records duplicates total (90.9% reduction) - includes parents with NULL in both name fields.
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

    # Find cluster_id in clusters_df for these master_ids using the id_parent column as the link
    master_id_to_cluster_id = clusters_df[clusters_df["id_parent"].isin(unique_master_ids)][["id_parent", "cluster_id"]]

    # Merge master_id to cluster_id mapping back to exact_df
    df_new_duplicates_exact_with_cluster = pd.merge(
        exact_df,
        master_id_to_cluster_id,
        left_on="master_id",
        right_on="id_parent",
        suffixes=("_duplicate", "_cluster"),
        how="left",
    )

    # For master_ids not found in clusters_df, set cluster_id to the original id_parent
    df_new_duplicates_exact_with_cluster["cluster_id"] = df_new_duplicates_exact_with_cluster["cluster_id"].fillna(
        df_new_duplicates_exact_with_cluster["id_parent_duplicate"]
    )

    # Select only the necessary columns for appending
    df_new_duplicates_to_append = df_new_duplicates_exact_with_cluster[
        [
            "cluster_id",
            "id",
            "id_parent_duplicate",
            "address",
            "date_of_birth",
            "indigenous_status_id",
            "given_name_1",
            "given_name_2",
            "given_name_3",
            "surname",
            "parent_type",
            "suburb",
        ]
    ].rename(columns={"id_parent_duplicate": "id_parent"})

    # Append records from df_new_duplicates_to_append to clusters_df
    clusters_df_2 = pd.concat([clusters_df, df_new_duplicates_to_append], ignore_index=True)

    # Sort the dataframe by cluster_id and id_parent in ascending order
    clusters_df_2 = clusters_df_2.sort_values(by=["cluster_id", "id_parent"], ascending=True)

    clusters_df_2 = clusters_df_2[
        [
            "cluster_id",
            "id",
            "id_parent",
            "address",
            "date_of_birth",
            "indigenous_status_id",
            "given_name_1",
            "given_name_2",
            "given_name_3",
            "surname",
            "parent_type",
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
# 334,582 records and 13 columns (same as total record count for cleaned child table)
clusters_pandas_df_2 = process_clusters(df_new_duplicates_exact_2, clusters_pandas_df)

# Unique count of id_parent where both given_name_1 and surname are None: 88,367
# Unique count of id_parent (excluding both names None): 246,215
# Unique count of cluster_id (excluding both names None): 30,490
# Percentage reduction: 87.62%

# Unique count of id_parent
filtered = clusters_pandas_df_2[~(clusters_pandas_df_2["given_name_1"].isna() & clusters_pandas_df_2["surname"].isna())]
unique_id_parent_2 = filtered["id_parent"].nunique()

# Unique count of id_parent where both given_name_1 and surname are None
filtered_2 = clusters_pandas_df_2[
    (clusters_pandas_df_2["given_name_1"].isna() & clusters_pandas_df_2["surname"].isna())
]
unique_id_parent = filtered_2["id_parent"].nunique()

# Unique count of cluster_id
unique_cluster_id = filtered["cluster_id"].nunique()

print()
print(f"Unique count of id_parent where both given_name_1 and surname are None: {unique_id_parent}")
print(f"Unique count of id_parent (excluding both names None): {unique_id_parent_2}")
print(f"Unique count of cluster_id (excluding both names None): {unique_cluster_id}")
print(f"Percentage reduction: {((unique_id_parent_2 - unique_cluster_id) / unique_id_parent_2) * 100:.2f}%")
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
        "gamma_suburb",
        "gamma_given_name_1_surname",
    ]

    # Create a key for easy merging
    predictions_df["key_l"] = (
        predictions_df["id_parent_l"].astype(str) + "_" + predictions_df["id_parent_r"].astype(str)
    )
    predictions_df["key_r"] = (
        predictions_df["id_parent_r"].astype(str) + "_" + predictions_df["id_parent_l"].astype(str)
    )

    clusters_df["key"] = clusters_df["cluster_id"].astype(str) + "_" + clusters_df["id_parent"].astype(str)

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

    # Step 2: Assign default values where cluster_id == id_parent
    condition = merged_df["cluster_id"] == merged_df["id_parent"]
    merged_df.loc[condition, match_columns] = [
        1,  # match_weight
        1,  # match_probability
        3,  # gamma_surname
        3,  # gamma_given_name_1
        3,  # gamma_given_name_2
        5,  # gamma_date_of_birth
        2,  # gamma_suburb
        5,  # gamma_given_name_1_surname
    ]

    return merged_df


# 334,582 records and 21 columns
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


# 334,582 records and 27 columns
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
        "gamma_suburb_desc",
        "gamma_given_name_1_surname_desc",
    ]

    # Identify records where match_weight is NULL
    null_weight_mask = clusters_df["match_weight"].isna()

    # Identify records in exact_df
    exact_match_ids = set(exact_df["id_parent"])

    # Apply 'Association match (Exact)' where id_parent is in exact_df
    clusters_df.loc[null_weight_mask & clusters_df["id_parent"].isin(exact_match_ids), gamma_columns] = (
        "Association match (Exact)"
    )

    # Apply 'Association match' for remaining NULL match_weight records
    clusters_df.loc[null_weight_mask & ~clusters_df["id_parent"].isin(exact_match_ids), gamma_columns] = (
        "Association match"
    )

    return clusters_df


# 334,582 records and 29 columns
# 80 records as "Association match"
# 209,970 records as "Association match (Exact)"
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


# 334,582 records and 30 columns
# 88,487 records with can_exclude = 1
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

# 334,582 records - DO_NOT_MIGRATE_parent_dedup
# 30,490 records - parent of child
# 87.62% reduction

# Note: The clusters that group person records, and consequently their new assigned person IDs, will remain consistent across script reruns, provided:
# 1) the underlying legacy dataset remains unchanged.
# 2) the person cleansing script (`old_person.py`) remains unchanged in a way that does not affect grouping logic
# (e.g., changes to name cleansing or inclusion/exclusion of business/employee records may result in different clusters).
