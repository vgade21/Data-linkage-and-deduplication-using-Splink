PRINT '';

-- Takes around 20-25 min to run

-- Suppressing default row count output
SET NOCOUNT ON;

SET ANSI_WARNINGS OFF;

-- Updating table DO_NOT_MIGRATE_person_lookup with the original given_names and surname.
-- Update DO_NOT_MIGRATE_person_lookup with data from person using id as the link
-- 612,908 records updated
UPDATE c
SET
    c.given_name_1 = p.given_name_1,
    c.given_name_2 = p.given_name_2,
    c.given_name_3 = p.given_name_3,
    c.surname = p.surname,
    c.name_concat = CONCAT(p.given_name_1, ' ', p.surname)
FROM
    cms_staging.dbo.DO_NOT_MIGRATE_person_lookup c
INNER JOIN
    cms_staging.dbo.DO_NOT_MIGRATE_person p
ON
    c.id = p.id;

PRINT 'Updating table DO_NOT_MIGRATE_person_lookup: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' records updated.';
PRINT '';

GO

-- Check and drop the foreign key if it exists
IF EXISTS (
    SELECT 1
    FROM sys.foreign_keys
    WHERE name = 'pa_per_FK'
    AND parent_object_id = OBJECT_ID('cms_staging.dbo.person_alias')
)
BEGIN
    ALTER TABLE cms_staging.dbo.person_alias DROP CONSTRAINT pa_per_FK;

    PRINT 'Foreign key [pa_per_FK] was successfully dropped.';
END
ELSE
BEGIN
    PRINT 'Foreign key [pa_per_FK] does not exist on person_alias.';
END

PRINT '';

GO

-- We will reassess and redefine how the cluster_id is assigned, in line with User Story 37973.
-- The linking model's matching criteria columns are relevant with the original cluster_id logic.
-- Assigning the master record (updated_cluster_id or File name in Atlas) so that the record containing the most complete set of details in the legacy system.
-- The following fields are considered when assessing the completeness of a record:
    -- given_name_1, given_name_2, given_name_3, surname, sex, and date_of_birth.
-- In cases where multiple records have the same number of populated fields, the tie-breaker will be:
    -- The record with the earliest date_created (i.e. the one with the lowest record id).

-- 612,908 records
-- Add a helper CTE to count non-NULL fields for each record
WITH NonNullCounts AS (
    SELECT
        cluster_id,
		id,
        -- Count the number of non-NULLs in the selected fields
        (CASE WHEN given_name_1 IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN given_name_2 IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN given_name_3 IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN surname IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN sex IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN date_of_birth IS NOT NULL THEN 1 ELSE 0 END) AS non_null_count,
        -- Flag: does id contain an underscore?
        CASE WHEN id LIKE '%[_]%' THEN 1 ELSE 0 END AS has_underscore
    FROM cms_staging.dbo.DO_NOT_MIGRATE_person_lookup
),
-- For each cluster_id, determine if ALL records have '_' in the id
ClusterUnderscoreStatus AS (
    SELECT
        cluster_id,
        MIN(has_underscore) AS all_have_underscore, -- =1 if all have underscore, =0 if any record does not
        SUM(1) AS total_records
    FROM NonNullCounts
    GROUP BY cluster_id
),
-- For each cluster_id, get candidates for new cluster_id
CandidateRecords AS (
    SELECT
        n.*,
        cus.all_have_underscore
    FROM NonNullCounts n
    INNER JOIN ClusterUnderscoreStatus cus ON n.cluster_id = cus.cluster_id
    WHERE
        -- If not all have underscore, only consider ids without underscore
        (cus.all_have_underscore = 0 AND n.has_underscore = 0)
        OR
        -- If all have underscore, consider all records
        (cus.all_have_underscore = 1)
),
-- For each cluster_id, find the candidate record(s) with the most non-NULL fields
RankedCandidates AS (
    SELECT
        cluster_id,
        id AS updated_cluster_id,
        ROW_NUMBER() OVER (
            PARTITION BY cluster_id
            ORDER BY
                non_null_count DESC,
                CASE WHEN TRY_CAST(id AS BIGINT) IS NOT NULL THEN 0 ELSE 1 END ASC,
                TRY_CAST(id AS BIGINT) ASC,
                id ASC
        ) AS rn
    FROM CandidateRecords
),
NewClusterId AS (
    -- Only the top record per cluster_id (the updated master record)
    SELECT cluster_id, updated_cluster_id
    FROM RankedCandidates
    WHERE rn = 1
)
-- Update the main table with the new cluster_id values
UPDATE f
SET f.updated_cluster_id = nci.updated_cluster_id
FROM cms_staging.dbo.DO_NOT_MIGRATE_person_lookup f
INNER JOIN NewClusterId nci
    ON f.cluster_id = nci.cluster_id;

PRINT 'Updating table DO_NOT_MIGRATE_person_lookup with an updated_cluster_id, as per User Story 37973: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' records updated.';
PRINT '';

GO

-- Creating new table person (person_dedup) with Master records with new IDs.
-- Missing values in master records are filled using non-master records by combining partial person information.
-- The DO_NOT_MIGRATE_person_lookup updated to retain a reference (new_id) to the new master record.
-- Create or truncate person

IF OBJECT_ID('cms_staging.dbo.person', 'U') IS NOT NULL
    DROP TABLE cms_staging.dbo.person;

PRINT 'Dropped old person table.';
PRINT '';

GO

CREATE TABLE cms_staging.dbo.person (
    updated_cluster_id VARCHAR(100) NULL,
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
    spi VARCHAR(50) NULL,
    is_business INT NULL,
    is_employee INT NULL,
    can_exclude INT NULL
);

PRINT 'Created new table person.';
PRINT '';

GO

-- Insert master records into person
-- 460,425 records inserted - the unique count of updated_cluster_id
INSERT INTO cms_staging.dbo.person (
    updated_cluster_id, version, country_of_origin_id, date_created, date_of_birth, given_name_1, given_name_2,
    given_name_3, indigenous_status_id, last_updated, ne_reason, not_editable, primary_address_id,
    primary_contact_id, racial_appearance_id, sex_id, surname, care_of_address, spi, is_business,
    is_employee, can_exclude
)
SELECT
    updated_cluster_id, version, country_of_origin_id, date_created, date_of_birth, given_name_1, given_name_2,
    given_name_3, indigenous_status_id, last_updated, ne_reason, not_editable, primary_address_id,
    primary_contact_id, racial_appearance_id, sex_id, surname, care_of_address, spi, is_business,
    is_employee, can_exclude
FROM cms_staging.dbo.DO_NOT_MIGRATE_person_lookup
WHERE id = updated_cluster_id
ORDER BY updated_cluster_id;

PRINT 'Inserting master records to table person: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' records inserted.';
PRINT '';

GO

-- COALESCE missing values from non-master records in DESCENDING ORDER of last_updated
-- 88,337 records updated
-- First CTE: Rank the records in descending order based on last_updated for each updated_cluster_id
WITH RankedData AS (
    SELECT
        updated_cluster_id,
        version,
        country_of_origin_id,
        -- date_of_birth,
        -- given_name_1,
        -- given_name_2,
        -- given_name_3,
        indigenous_status_id,
        last_updated,
        ne_reason,
        not_editable,
        primary_address_id,
        primary_contact_id,
        racial_appearance_id,
        -- sex_id,
        -- surname,
        care_of_address,
        spi,
        is_business,
        is_employee,
        can_exclude,
        ROW_NUMBER() OVER (
            PARTITION BY updated_cluster_id ORDER BY
            CASE WHEN country_of_origin_id IS NOT NULL THEN 0 ELSE 1 END, last_updated DESC
        ) AS rn_country_of_origin_id,
        ROW_NUMBER() OVER (
            PARTITION BY updated_cluster_id ORDER BY
            CASE WHEN version IS NOT NULL THEN 0 ELSE 1 END, last_updated DESC
        ) AS rn_version,
        -- ROW_NUMBER() OVER (
        --     PARTITION BY updated_cluster_id ORDER BY
        --     CASE WHEN date_of_birth IS NOT NULL THEN 0 ELSE 1 END, last_updated DESC
        -- ) AS rn_date_of_birth,
        -- ROW_NUMBER() OVER (
        --     PARTITION BY updated_cluster_id ORDER BY
        --     CASE WHEN given_name_1 IS NOT NULL THEN 0 ELSE 1 END, last_updated DESC
        -- ) AS rn_given_name_1,
        -- ROW_NUMBER() OVER (
        --     PARTITION BY updated_cluster_id ORDER BY
        --     CASE WHEN given_name_2 IS NOT NULL THEN 0 ELSE 1 END, last_updated DESC
        -- ) AS rn_given_name_2,
        -- ROW_NUMBER() OVER (
        --     PARTITION BY updated_cluster_id ORDER BY
        --     CASE WHEN given_name_3 IS NOT NULL THEN 0 ELSE 1 END, last_updated DESC
        -- ) AS rn_given_name_3,
        ROW_NUMBER() OVER (
            PARTITION BY updated_cluster_id ORDER BY
            CASE WHEN indigenous_status_id IS NOT NULL THEN 0 ELSE 1 END, last_updated DESC
        ) AS rn_indigenous_status_id,
        ROW_NUMBER() OVER (
            PARTITION BY updated_cluster_id ORDER BY
            CASE WHEN ne_reason IS NOT NULL THEN 0 ELSE 1 END, last_updated DESC
        ) AS rn_ne_reason,
        ROW_NUMBER() OVER (
            PARTITION BY updated_cluster_id ORDER BY
            CASE WHEN not_editable IS NOT NULL THEN 0 ELSE 1 END, last_updated DESC
        ) AS rn_not_editable,
        ROW_NUMBER() OVER (
            PARTITION BY updated_cluster_id ORDER BY
            CASE WHEN primary_address_id IS NOT NULL THEN 0 ELSE 1 END, last_updated DESC
        ) AS rn_primary_address_id,
        ROW_NUMBER() OVER (
            PARTITION BY updated_cluster_id ORDER BY
            CASE WHEN primary_contact_id IS NOT NULL THEN 0 ELSE 1 END, last_updated DESC
        ) AS rn_primary_contact_id,
        ROW_NUMBER() OVER (
            PARTITION BY updated_cluster_id ORDER BY
            CASE WHEN racial_appearance_id IS NOT NULL THEN 0 ELSE 1 END, last_updated DESC
        ) AS rn_racial_appearance_id,
        -- ROW_NUMBER() OVER (
        --     PARTITION BY updated_cluster_id ORDER BY
        --     CASE WHEN sex_id IS NOT NULL THEN 0 ELSE 1 END, last_updated DESC
        -- ) AS rn_sex_id,
        -- ROW_NUMBER() OVER (
        --     PARTITION BY updated_cluster_id ORDER BY
        --     CASE WHEN surname IS NOT NULL THEN 0 ELSE 1 END, last_updated DESC
        -- ) AS rn_surname,
        ROW_NUMBER() OVER (
            PARTITION BY updated_cluster_id ORDER BY
            CASE WHEN care_of_address IS NOT NULL THEN 0 ELSE 1 END, last_updated DESC
        ) AS rn_care_of_address,
        ROW_NUMBER() OVER (
            PARTITION BY updated_cluster_id ORDER BY
            CASE WHEN spi IS NOT NULL THEN 0 ELSE 1 END, last_updated DESC
        ) AS rn_spi,
        ROW_NUMBER() OVER (
            PARTITION BY updated_cluster_id ORDER BY
            CASE WHEN is_business IS NOT NULL THEN 0 ELSE 1 END, last_updated DESC
        ) AS rn_is_business,
        ROW_NUMBER() OVER (
            PARTITION BY updated_cluster_id ORDER BY
            CASE WHEN is_employee IS NOT NULL THEN 0 ELSE 1 END, last_updated DESC
        ) AS rn_is_employee,
        ROW_NUMBER() OVER (
            PARTITION BY updated_cluster_id ORDER BY
            CASE WHEN can_exclude IS NOT NULL THEN 0 ELSE 1 END, last_updated DESC
        ) AS rn_can_exclude
    FROM cms_staging.dbo.DO_NOT_MIGRATE_person_lookup
    WHERE id <> updated_cluster_id
),
-- Second CTE: Select only the best record for each column within each updated_cluster_id
NonMasterRecords AS (
    SELECT
        updated_cluster_id,
        MAX(CASE WHEN rn_version = 1 THEN version END) AS version,
        MAX(CASE WHEN rn_country_of_origin_id = 1 THEN country_of_origin_id END) AS country_of_origin_id,
        --MAX(CASE WHEN rn_date_of_birth = 1 THEN date_of_birth END) AS date_of_birth,
        --MAX(CASE WHEN rn_given_name_1 = 1 THEN given_name_1 END) AS given_name_1,
        --MAX(CASE WHEN rn_given_name_2 = 1 THEN given_name_2 END) AS given_name_2,
        --MAX(CASE WHEN rn_given_name_3 = 1 THEN given_name_3 END) AS given_name_3,
        MAX(CASE WHEN rn_indigenous_status_id = 1 THEN indigenous_status_id END) AS indigenous_status_id,
        MAX(CASE WHEN rn_ne_reason = 1 THEN ne_reason END) AS ne_reason,
        MAX(CASE WHEN rn_not_editable = 1 THEN not_editable END) AS not_editable,
        MAX(CASE WHEN rn_primary_address_id = 1 THEN primary_address_id END) AS primary_address_id,
        MAX(CASE WHEN rn_primary_contact_id = 1 THEN primary_contact_id END) AS primary_contact_id,
        MAX(CASE WHEN rn_racial_appearance_id = 1 THEN racial_appearance_id END) AS racial_appearance_id,
        --MAX(CASE WHEN rn_sex_id = 1 THEN sex_id END) AS sex_id,
        --MAX(CASE WHEN rn_surname = 1 THEN surname END) AS surname,
        MAX(CASE WHEN rn_care_of_address = 1 THEN care_of_address END) AS care_of_address,
        MAX(CASE WHEN rn_spi = 1 THEN spi END) AS spi,
        MAX(CASE WHEN rn_is_business = 1 THEN is_business END) AS is_business,
        MAX(CASE WHEN rn_is_employee = 1 THEN is_employee END) AS is_employee,
        MAX(CASE WHEN rn_can_exclude = 1 THEN can_exclude END) AS can_exclude
    FROM RankedData
    GROUP BY updated_cluster_id
)
-- Update the master records in person
UPDATE f
SET
    f.version = COALESCE(f.version, n.version),
    f.country_of_origin_id = COALESCE(f.country_of_origin_id, n.country_of_origin_id),
    --f.date_of_birth = COALESCE(f.date_of_birth, n.date_of_birth),
    --f.given_name_1 = COALESCE(f.given_name_1, n.given_name_1),
    --f.given_name_2 = COALESCE(f.given_name_2, n.given_name_2),
    --f.given_name_3 = COALESCE(f.given_name_3, n.given_name_3),
    f.indigenous_status_id = COALESCE(f.indigenous_status_id, n.indigenous_status_id),
    f.ne_reason = COALESCE(f.ne_reason, n.ne_reason),
    f.not_editable = COALESCE(f.not_editable, n.not_editable),
    f.primary_address_id = COALESCE(f.primary_address_id, n.primary_address_id),
    f.primary_contact_id = COALESCE(f.primary_contact_id, n.primary_contact_id),
    f.racial_appearance_id = COALESCE(f.racial_appearance_id, n.racial_appearance_id),
    --f.sex_id = COALESCE(f.sex_id, n.sex_id),
    --f.surname = COALESCE(f.surname, n.surname),
    f.care_of_address = COALESCE(f.care_of_address, n.care_of_address),
    f.spi = COALESCE(f.spi, n.spi),
    f.is_business = COALESCE(f.is_business, n.is_business),
    f.is_employee = COALESCE(f.is_employee, n.is_employee),
    f.can_exclude = COALESCE(f.can_exclude, n.can_exclude)
FROM cms_staging.dbo.person f
JOIN NonMasterRecords n ON n.updated_cluster_id = f.updated_cluster_id;

PRINT 'COALESCE missing values in table person: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' records updated.';
PRINT '';

GO

-- Populate the id column in table person
-- 460,425 records updated
-- Use a CTE to calculate unique row numbers and update original table
-- Note: the generated id starts from 1000000 and incremented by 1 for each row.
WITH CTE AS (
    SELECT id, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) + 999999 AS new_id
    FROM cms_staging.dbo.person
)
UPDATE CTE
SET id = new_id;

PRINT 'Populating the id column in table person: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' records updated.';
PRINT '';

GO

-- Alter column to NOT NULL
ALTER TABLE cms_staging.dbo.person
ALTER COLUMN id INT NOT NULL;

GO

-- Set the primary key
ALTER TABLE cms_staging.dbo.person
ADD CONSTRAINT PK_person_id PRIMARY KEY (id);

PRINT 'Setting the id column in table person as the PK';
PRINT '';

GO

-- 612,908 records updated
UPDATE c
SET c.new_id = f.id
FROM cms_staging.dbo.DO_NOT_MIGRATE_person_lookup c
JOIN cms_staging.dbo.person f ON c.updated_cluster_id = f.updated_cluster_id;

PRINT 'Populating the new_id column in table DO_NOT_MIGRATE_person_lookup: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' records updated.';
PRINT '';

GO

ALTER TABLE cms_staging.dbo.DO_NOT_MIGRATE_person_lookup
ALTER COLUMN new_id INT;

GO

-- Check if table person has unique updated_cluster_id
IF NOT EXISTS (
    SELECT updated_cluster_id
    FROM cms_staging.dbo.person
    GROUP BY updated_cluster_id
    HAVING COUNT(*) > 1
)
    PRINT 'PASS: Table person has unique updated_cluster_id. This column will be deleted later.';
ELSE
    PRINT 'FAIL: Table person DOES NOT have unique updated_cluster_id. This column will be deleted later.';

PRINT '';

GO

-- Updates records with an invalid date_of_birth with a valid date_of_birth if found in the duplicate cluster group.
-- The script checks all duplicate records ranked in descending order of last_updated.
	-- DOB more than and including 100 years before date_created.
    -- DOB equal to or after date_created.
    -- DOB is within 12 months before the date_created.
    -- DOB more than and including 114 years before date_created.

-- Create a temporary table to store records from person that are 100 years or older
WITH OldRecords AS (
    -- 402 records in total
    SELECT updated_cluster_id, date_of_birth
    FROM cms_staging.dbo.person
    WHERE DATEDIFF(YEAR, date_of_birth, date_created) >= 100
),
RankedRecords AS (
    -- Identify records in DO_NOT_MIGRATE_person_lookup linked by updated_cluster_id
    SELECT o.updated_cluster_id, o.date_of_birth AS original_dob,
        pl.new_id, pl.date_of_birth AS lookup_dob, pl.last_updated,
        ROW_NUMBER() OVER (PARTITION BY o.updated_cluster_id ORDER BY pl.last_updated DESC) AS rank
    FROM OldRecords o
    JOIN cms_staging.dbo.DO_NOT_MIGRATE_person_lookup pl
        ON o.updated_cluster_id = pl.updated_cluster_id
-- Handle updates where multiple records exist in DO_NOT_MIGRATE_person_lookup
),
RankedFiltered AS (
    -- 8 records with a non-NULL valid_dob
    SELECT rr.updated_cluster_id,
        (SELECT TOP 1 lookup_dob
            FROM RankedRecords r2
            WHERE r2.updated_cluster_id = rr.updated_cluster_id
            AND r2.lookup_dob IS NOT NULL
            AND r2.lookup_dob > rr.original_dob
            ORDER BY r2.last_updated DESC) AS valid_dob
    FROM RankedRecords rr
    GROUP BY rr.updated_cluster_id, rr.original_dob
)
-- 8 records updated
UPDATE pd
SET pd.date_of_birth = COALESCE(rf.valid_dob, NULL)
FROM cms_staging.dbo.person pd
LEFT JOIN RankedFiltered rf
    ON pd.updated_cluster_id = rf.updated_cluster_id
WHERE pd.date_of_birth IS NOT NULL AND rf.valid_dob IS NOT NULL;

PRINT 'Updating date_of_birth that are 100 years or earlier to the date_created: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' records updated.';
PRINT '';

GO

-- Identify records in person where date_of_birth is equal to or after date_created
WITH InvalidDOBRecords AS (
    -- 26 records in total
    SELECT updated_cluster_id, date_of_birth, date_created
    FROM cms_staging.dbo.person
    WHERE date_of_birth >= date_created
),
RankedDOBRecords AS (
    -- Retrieve records from DO_NOT_MIGRATE_person_lookup that are linked by updated_cluster_id
    SELECT
        i.updated_cluster_id,
        i.date_of_birth AS original_dob,
        i.date_created AS original_date_created,
        pl.new_id,
        pl.date_of_birth AS lookup_dob,
        pl.last_updated,
        ROW_NUMBER() OVER (PARTITION BY i.updated_cluster_id ORDER BY pl.last_updated DESC) AS record_rank
    FROM InvalidDOBRecords i
    JOIN cms_staging.dbo.DO_NOT_MIGRATE_person_lookup pl
        ON i.updated_cluster_id = pl.updated_cluster_id
-- Handle cases where multiple records exist in DO_NOT_MIGRATE_person_lookup
),
RankedFiltered AS (
    -- 1 record with a non-NULL valid_dob
    SELECT rr.updated_cluster_id,
        (SELECT TOP 1 lookup_dob
            FROM RankedDOBRecords r2
            WHERE r2.updated_cluster_id = rr.updated_cluster_id
            AND r2.lookup_dob IS NOT NULL
            AND r2.lookup_dob < rr.original_dob
            AND r2.lookup_dob < rr.original_date_created
            ORDER BY r2.last_updated DESC) AS valid_dob
    FROM RankedDOBRecords rr
    GROUP BY rr.updated_cluster_id, rr.original_dob, rr.original_date_created
)
-- 26 records updated
UPDATE pd
SET pd.date_of_birth = COALESCE(rf.valid_dob, NULL)
FROM cms_staging.dbo.person pd
JOIN RankedFiltered rf
    ON pd.updated_cluster_id = rf.updated_cluster_id
WHERE pd.date_of_birth IS NOT NULL;

PRINT 'Updating date_of_birth that is equal to or after date_created: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' records updated.';
PRINT '';

GO

-- Identify records in person where date_of_birth is within 12 months before the date_created
WITH InvalidDOBRecords AS (
    -- 285 records in total
    SELECT updated_cluster_id, date_of_birth, date_created
    FROM cms_staging.dbo.person
    WHERE date_of_birth >= DATEADD(MONTH, -12, date_created) AND date_of_birth < date_created
),
RankedDOBRecords_2 AS (
    -- Retrieve records from DO_NOT_MIGRATE_person_lookup that are linked by updated_cluster_id
    SELECT
        i.updated_cluster_id,
        i.date_of_birth AS original_dob,
        i.date_created AS original_date_created,
        pl.new_id,
        pl.date_of_birth AS lookup_dob,
        pl.last_updated,
        ROW_NUMBER() OVER (PARTITION BY i.updated_cluster_id ORDER BY pl.last_updated DESC) AS record_rank
    FROM InvalidDOBRecords i
    JOIN cms_staging.dbo.DO_NOT_MIGRATE_person_lookup pl
        ON i.updated_cluster_id = pl.updated_cluster_id
-- Handle cases where multiple records exist in DO_NOT_MIGRATE_person_lookup
),
RankedFiltered AS (
    -- 18 records with a non-NULL valid_dob
    SELECT rr.updated_cluster_id,
        (SELECT TOP 1 lookup_dob
            FROM RankedDOBRecords_2 r2
            WHERE r2.updated_cluster_id = rr.updated_cluster_id
            AND r2.lookup_dob IS NOT NULL
            AND r2.lookup_dob <= DATEADD(MONTH, -12, rr.original_date_created)
            ORDER BY r2.last_updated DESC) AS valid_dob
    FROM RankedDOBRecords_2 rr
    GROUP BY rr.updated_cluster_id, rr.original_date_created
)
-- 18 records updated
UPDATE pd
SET pd.date_of_birth = COALESCE(rf.valid_dob, NULL)
FROM cms_staging.dbo.person pd
LEFT JOIN RankedFiltered rf
    ON pd.updated_cluster_id = rf.updated_cluster_id
WHERE pd.date_of_birth IS NOT NULL AND rf.valid_dob IS NOT NULL;

PRINT 'Updating date_of_birth that is within 12 months before the date_created: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' records updated.';
PRINT '';

GO

-- Update date_of_birth to NULL for records where date_of_birth is 114+ years before date_created
-- 19 records updated
UPDATE cms_staging.dbo.person
SET date_of_birth = NULL
WHERE DATEDIFF(YEAR, date_of_birth, date_created) >= 114;

PRINT 'Updating date_of_birth that are 114 years or earlier to the date_created: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' records updated.';
PRINT '';

GO

-- Dropping column updated_cluster_id
ALTER TABLE cms_staging.dbo.person
DROP COLUMN updated_cluster_id;

PRINT 'Dropping column updated_cluster_id from table person';
PRINT '';

GO

-- 460,425 records - person

-- Re-enable row count messages
SET NOCOUNT OFF;
