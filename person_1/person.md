#### **IMPORTANT NOTE when running person table scripts**

The [old_person.py](https://emstas.visualstudio.com/Program%20Unify/_git/unify_2_1_source_to_staging?version=GBdevelop&path=/src/systems/cms_clean/old_person) script is where the main cleansing and transformation (splitting of given_names) of the `person` table is happening.
The [person.py](https://emstas.visualstudio.com/Program%20Unify/_git/unify_2_1_source_to_staging?version=GBdevelop&path=/src/systems/cms_clean/person) script is where the `person` linking and de-duplication work is happening.

If the `cms_clean` tables (in particular person, business, employee, contact or address) **haven't changed** since the last run **and** the script for [old_person.py](https://emstas.visualstudio.com/Program%20Unify/_git/unify_2_1_source_to_staging?version=GBdevelop&path=/src/systems/cms_clean/old_person) **hasn't changed**, the [old_person.py](https://emstas.visualstudio.com/Program%20Unify/_git/unify_2_1_source_to_staging?version=GBdevelop&path=/src/systems/cms_clean/old_person) and [person.py](https://emstas.visualstudio.com/Program%20Unify/_git/unify_2_1_source_to_staging?version=GBdevelop&path=/src/systems/cms_clean/person) can be run independently of each other.
- If they are run together [old_person.py](https://emstas.visualstudio.com/Program%20Unify/_git/unify_2_1_source_to_staging?version=GBdevelop&path=/src/systems/cms_clean/old_person) should always be run **before** [person.py](https://emstas.visualstudio.com/Program%20Unify/_git/unify_2_1_source_to_staging?version=GBdevelop&path=/src/systems/cms_clean/person).

If the `cms_clean` tables (in particular person, business, employee, contact or address) **have changed** since the last run **or** the [old_person.py](https://emstas.visualstudio.com/Program%20Unify/_git/unify_2_1_source_to_staging?version=GBdevelop&path=/src/systems/cms_clean/old_person) script **has changed**, then both [old_person.py](https://emstas.visualstudio.com/Program%20Unify/_git/unify_2_1_source_to_staging?version=GBdevelop&path=/src/systems/cms_clean/old_person) and [person.py](https://emstas.visualstudio.com/Program%20Unify/_git/unify_2_1_source_to_staging?version=GBdevelop&path=/src/systems/cms_clean/person) should be run together, with [old_person.py](https://emstas.visualstudio.com/Program%20Unify/_git/unify_2_1_source_to_staging?version=GBdevelop&path=/src/systems/cms_clean/old_person) always run **before** [person.py](https://emstas.visualstudio.com/Program%20Unify/_git/unify_2_1_source_to_staging?version=GBdevelop&path=/src/systems/cms_clean/person).
- Running [old_person.py](https://emstas.visualstudio.com/Program%20Unify/_git/unify_2_1_source_to_staging?version=GBdevelop&path=/src/systems/cms_clean/old_person) in isolation would prevent the changes (assuming there have been some) to be passed on to the final person table in `cms_staging`, causing downstream issues.

# Transformation

##### DQI:
[32428](https://dev.azure.com/emstas/Program%20Unify/_workitems/edit/32428)
[32377](https://dev.azure.com/emstas/Program%20Unify/_workitems/edit/32377)
[32376](https://dev.azure.com/emstas/Program%20Unify/_workitems/edit/32376)
[32314](https://dev.azure.com/emstas/Program%20Unify/_workitems/edit/32314)
[31661](https://dev.azure.com/emstas/Program%20Unify/_workitems/edit/31661)
[31657](https://dev.azure.com/emstas/Program%20Unify/_workitems/edit/31657)
[31651](https://dev.azure.com/emstas/Program%20Unify/_workitems/edit/31651)
[31650](https://dev.azure.com/emstas/Program%20Unify/_workitems/edit/31650)
[31647](https://dev.azure.com/emstas/Program%20Unify/_workitems/edit/31647)
[31646](https://dev.azure.com/emstas/Program%20Unify/_workitems/edit/31646)
[31643](https://dev.azure.com/emstas/Program%20Unify/_workitems/edit/31643)
[36352](https://dev.azure.com/emstas/Program%20Unify/_workitems/edit/36352)
[31653](https://dev.azure.com/emstas/Program%20Unify/_workitems/edit/31653)
[31584](https://dev.azure.com/emstas/Program%20Unify/_workitems/edit/31584)

##### Note: the following is cleansing and transformation is done by the `old_person.py` script
##### Filtering and Segregating Person Records

- Filters the `person` table to retain only actual person records.
- Excludes employee and business records that are moved to their respective tables.
- Manually updates the `is_business` and `is_employee` columns for business and employee records that remain in the `person` table.

##### Cleaning `given_names` and `surname` Columns

- Removes "ESTATE OF" phrases in both `given_names` and `surname`.
- Sets to NULL any records that start with "REG " and contain numbers.
- Removes quotes from records that start and end with either single (`'`) or double (`"`) quotes if no text follows the closing quote (except spaces or non-alphanumeric).
- Replaces backticks (`) with blanks and en dashes (–) with hyphens (-).
- Removes the following titles if they appear at the beginning of a name: "REV.", "REV", "DR.", "DR", "SIR", "LORD", "MRS", "SISTER", "FATHER".
- Removes the words "DOCTOR" and "REVEREND" anywhere in the column.
- Removes ACN followed by any numbers.
- Replaces care-of abbreviations with blanks.
- Replaces police rank abbreviations with their full form values.
- Moves employee rank terms to the end of the `given_names`.
- Sets `given_names` or `surname` to NULL if they contain specific disqualifying keywords (whole-word matches).

##### Further Cleaning and Processing Aliases in `given_names` and `surname`

- **Handling Special Cases**
  - Extracts and removes A.K.A. references (e.g., "aka", "also known as", "known as", etc.).
  - Moves A.K.A. values to a separate cleaned text column.
  - Removes DOB references starting with "DOB:" or "DOB <number>".
  - Removes age indicators like "25yrs", "30 years", etc.
  - Strips unwanted placeholders (e.g., "U/K", "N/A", "Not", "N/K").

- **Formatting and Standardizing Text**
  - Removes unwanted brackets, quotes, and inner-word apostrophes.
  - Converts `"O "` to `"O'"` (e.g., "O Connor" → "O'Connor").
  - Standardizes spacing around hyphens (e.g., "Smith - Brown" → "Smith-Brown").
  - Converts "Junior" and "Jr" to "Jnr".

- **Extracting Alias Names**
  - Retains only alphabetic and hyphenated names using regex.
  - Removes non-name characters such as numbers and special symbols.
  - Example:
    Input: `"Dr. John (aka Jack) Smith - 25yrs"`
    Output: `"John Smith"`

- **Handling Remaining Text**
  - Stores removed text into a separate `removed_text` column.
  - Extracts extra info into corresponding `_Extra` columns (e.g., "aka", "DOB", "years").

##### Handling Multi-Person and Alias Splits

- **Slashes (/)**
  - Splits on the first slash and updates relevant name columns.

- **Colons ( : )**
  - Splits appropriately and updates relevant fields.

- **Ampersands (&)**
  - Splits on the first `&` and updates accordingly.

- **"nee", "use", "and" Keywords**
  - Ensures these are split and stored in the correct columns.

##### Handling Non-Name Entries

- Maintains a list of keywords to identify non-name records.
- Moves any `Cleaned_Given_Names` or `Cleaned_Surname` with matching keywords to a `Not_A_Name_...` column.

##### Additional Cleanup of `given_names` and `surname`

- Moves "JNR" or "SNR" suffixes from either column to the end of the `surname`.
- Standardizes apostrophes in Irish surnames.
- Creates a `can_exclude` column and sets it to 1 for:
  - Records where both `Cleaned_Given_Names_2` and `Cleaned_Surname_2` are NULL.
  - Records where both `date_of_birth` and `Cleaned_Surname_2` are NULL.
- Identifies and flags records where `surname` starts with "MAC " or "MC ".
- Splitting of given_names into three columns.

##### Finalizing the `person_alias` Table

- Cleans and filters the final `person_alias` table to include only alias entries and not actual new person records.

##### Creating Unique IDs for New Person Records

- Assigns new IDs using the original `id` with suffixes `_1`, `_2`, etc.
- New person records in a group share the same `version`, `date_created`, and `last_updated` as the first row in their group.

##### Note: the following is cleansing and transformation is done by the `person.py` script
##### De-duplication

##### Using `splink` Package to De-duplicate the Person Table

- The `splink` package is used to de-duplicate and cleanse the `person` table.
- This process involves merging duplicate records and extracting alternate names.

- The process results in the following key tables:

  - **`DO_NOT_MIGRATE_person`**
    &nbsp;&nbsp;- The original `person` table after cleansing, prior to de-duplication.

  - **`person_alias`**
    &nbsp;&nbsp;- A new table containing extracted alias records from merged persons.

  - **`DO_NOT_MIGRATE_person_lookup`**
    &nbsp;&nbsp;- A lookup table, with the linking model's matching criteria indicating the matching criteria used by the linking model to group person records. The table maps original person IDs to their corresponding deduplicated person IDs (cluster_id). The linking model’s matching criteria columns are relevant to this clustering logic.
    &nbsp;&nbsp;- This table has a column `updated_cluster_id` with the updated cluster_id values in line with User Story [37973](https://dev.azure.com/emstas/Program%20Unify/_workitems/edit/37973). The logic for selecting master records—originally driven by the linking model (cluster_id)—has been revised to align with current business rules and is reflected in this column.

  - **`person`**
    &nbsp;&nbsp;- The final, cleansed and deduplicated version of the `person` table.


##### Further Updating and Cleansing by `dedup_per.sql`

- Insert only master records into `person` table from `DO_NOT_MIGRATE_person_lookup` table.
- Coalesce missing values from non-master records in descending order of last_updated.
  - Determine the **most recent record** using the `last_updated` field within the `master_id` cluster group. Update the missing `person` table records with the values from these **most recent non-master records**.

- Populate the id column in `person` with new unique id (**starts from 1000000 and incremented by 1 for each row**).
  - **Note**: The clusters that group person records, and consequently their new assigned person IDs, will remain consistent across script reruns, provided:
    - the underlying legacy dataset remains unchanged.
    - the person cleansing script ([old_person.py](https://emstas.visualstudio.com/Program%20Unify/_git/unify_2_1_source_to_staging?version=GBdevelop&path=/src/systems/cms_clean/old_person)) remains unchanged in a way that does not affect grouping logic (e.g., changes to name cleansing or inclusion/exclusion of business/employee records may result in different clusters).

- Updates records with an **invalid `date_of_birth`** using a valid one found in the duplicate cluster group:
  - The script evaluates all duplicate records ranked in **descending order of `last_updated`**.
  - Applies updates based on the following conditions:
    - `date_of_birth` is more than and including **100 years before** `date_created`.
    - `date_of_birth` is **equal to or after** `date_created`.
    - `date_of_birth` is within **12 months before** the `date_created`.
    - `date_of_birth` is more than and including **114 years before** `date_created`.
