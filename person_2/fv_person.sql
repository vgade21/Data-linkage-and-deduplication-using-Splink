-- Script Purpose: Generate the FVMS_staging person table, which stores the de-duplicated person data from legacy FV tables
-- Feature DQI: 24183, 35420

PRINT '';

-- Suppressing default row count output
SET NOCOUNT ON;

SET ANSI_WARNINGS OFF;

-- Create (if it doesn't exist) or truncate table
DROP TABLE IF EXISTS [fvms_staging].[dbo].[person];
CREATE TABLE fvms_staging.dbo.person (
    id BIGINT NOT NULL,
    given_name_1 VARCHAR(255),
    given_name_2 VARCHAR(255),
    given_name_3 VARCHAR(255),
    surname VARCHAR(255),
    sex VARCHAR(255),
    date_of_birth DATETIME,
    cni_number VARCHAR(255),
    indigenous_status_no_involvement_type VARCHAR(255)
);
ALTER TABLE [fvms_staging].[dbo].[person] ADD PRIMARY KEY CLUSTERED
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

PRINT 'Created/Truncated table person';
PRINT '';

GO

-- Person
-- 66,048 records
INSERT INTO fvms_staging.dbo.person (
    id,
    given_name_1,
    given_name_2,
    given_name_3,
    surname,
    sex,
    date_of_birth,
    cni_number,
    indigenous_status_no_involvement_type
)
SELECT
    l.id_new,
    p.given_name_1,
    p.given_name_2,
    p.given_name_3,
    p.surname,
    p.sex,
    p.date_of_birth,
    p.cni_number,
    NULL AS indigenous_status_no_involvement_type
FROM fvms_staging.dbo.DO_NOT_MIGRATE_all_person_lookup l
INNER JOIN fvms_staging.dbo.DO_NOT_MIGRATE_person_dedup p
    ON l.id_old = CAST(p.id AS VARCHAR(255))
WHERE l.type = 'person'
    AND l.id_old = l.Master_id;

PRINT 'Inserting master records from table DO_NOT_MIGRATE_person_dedup into person: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' records inserted.';
PRINT '';

GO

-- Child
-- 53,802 records
INSERT INTO fvms_staging.dbo.person (
    id,
    given_name_1,
    given_name_2,
    given_name_3,
    surname,
    sex,
    date_of_birth,
    cni_number,
    indigenous_status_no_involvement_type
)
SELECT
    l.id_new,
    c.given_name_1,
    c.given_name_2,
    c.given_name_3,
    c.surname,
    c.sex,
    c.date_of_birth,
    c.child_cni_number,
    NULL AS indigenous_status_no_involvement_type
FROM fvms_staging.dbo.DO_NOT_MIGRATE_all_person_lookup l
INNER JOIN fvms_staging.dbo.DO_NOT_MIGRATE_child_dedup c
    ON l.id_old = CAST(c.id AS VARCHAR(255))
WHERE l.type = 'child'
    AND l.id_old = l.Master_id;

PRINT 'Inserting master records from table DO_NOT_MIGRATE_child_dedup into person: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' records inserted.';
PRINT '';

GO

-- Parent of child
-- 30,370 records
INSERT INTO fvms_staging.dbo.person (
    id,
    given_name_1,
    given_name_2,
    given_name_3,
    surname,
    sex,
    date_of_birth,
    cni_number,
    indigenous_status_no_involvement_type
)
SELECT
    l.id_new,
    p.given_name_1,
    p.given_name_2,
    p.given_name_3,
    p.surname,
    NULL AS sex,
    p.date_of_birth,
    NULL AS cni_number,
    p.indigenous_status_id AS indigenous_status_no_involvement_type
FROM fvms_staging.dbo.DO_NOT_MIGRATE_all_person_lookup l
INNER JOIN fvms_staging.dbo.DO_NOT_MIGRATE_parent_dedup p
    ON l.id_old = p.id_parent
WHERE l.type IN ('parent1', 'parent2')
    AND l.id_old = l.Master_id;

PRINT 'Inserting master records from table DO_NOT_MIGRATE_parent_dedup into person: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' records inserted.';
PRINT '';

GO

-- Witness
-- 6,078 records
INSERT INTO fvms_staging.dbo.person (
    id,
    given_name_1,
    given_name_2,
    given_name_3,
    surname,
    sex,
    date_of_birth,
    cni_number,
    indigenous_status_no_involvement_type
)
SELECT
    l.id_new,
    w.given_name_1,
    w.given_name_2,
    w.given_name_3,
    w.surname,
    w.sex,
    w.date_of_birth,
    NULL AS cni_number,
    NULL AS indigenous_status_no_involvement_type
FROM fvms_staging.dbo.DO_NOT_MIGRATE_all_person_lookup l
INNER JOIN fvms_staging.dbo.DO_NOT_MIGRATE_witness_dedup w
    ON l.id_old = CAST(w.id AS VARCHAR(255))
WHERE l.type = 'witness'
    AND l.id_old = l.Master_id;

PRINT 'Inserting master records from table DO_NOT_MIGRATE_witness_dedup into person: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' records inserted.';
PRINT '';

GO

-- Involved_person
-- 30 records
INSERT INTO fvms_staging.dbo.person (
    id,
    given_name_1,
    given_name_2,
    given_name_3,
    surname,
    sex,
    date_of_birth,
    cni_number,
    indigenous_status_no_involvement_type
)
SELECT
    l.id_new,
    CASE
        WHEN i.given_names_dbo_involved_person IS NULL OR i.given_names_dbo_involved_person = '' THEN NULL
        ELSE
            UPPER(LEFT(LTRIM(RTRIM(i.given_names_dbo_involved_person)),1)) +
            LOWER(SUBSTRING(LTRIM(RTRIM(i.given_names_dbo_involved_person)),2,CHARINDEX(' ',LTRIM(RTRIM(i.given_names_dbo_involved_person)) + ' ')-2))
    END AS given_name_1_dbo_involved_person,
    CASE
        WHEN i.given_names_dbo_involved_person IS NULL OR CHARINDEX(' ', LTRIM(RTRIM(i.given_names_dbo_involved_person))) = 0 THEN NULL
        ELSE
            -- Find the first word after first space (sentence case)
            UPPER(LEFT(
                SUBSTRING(
                    LTRIM(RTRIM(i.given_names_dbo_involved_person)),
                    CHARINDEX(' ', LTRIM(RTRIM(i.given_names_dbo_involved_person))) + 1,
                    LEN(LTRIM(RTRIM(i.given_names_dbo_involved_person)))
                ), 1)) +
            LOWER(
                SUBSTRING(
                    SUBSTRING(
                        LTRIM(RTRIM(i.given_names_dbo_involved_person)),
                        CHARINDEX(' ', LTRIM(RTRIM(i.given_names_dbo_involved_person))) + 1,
                        LEN(LTRIM(RTRIM(i.given_names_dbo_involved_person)))
                    ),
                    2,
                    CHARINDEX(' ',
                        SUBSTRING(
                            LTRIM(RTRIM(i.given_names_dbo_involved_person)),
                            CHARINDEX(' ', LTRIM(RTRIM(i.given_names_dbo_involved_person))) + 1,
                            LEN(LTRIM(RTRIM(i.given_names_dbo_involved_person)))
                        ) + ' '
                    ) - 2
                )
            )
    END AS given_name_2_dbo_involved_person,
    CASE
        WHEN i.given_names_dbo_involved_person IS NULL OR
            CHARINDEX(' ', LTRIM(RTRIM(i.given_names_dbo_involved_person))) = 0 OR
            CHARINDEX(' ',
                SUBSTRING(
                    LTRIM(RTRIM(i.given_names_dbo_involved_person)),
                    CHARINDEX(' ', LTRIM(RTRIM(i.given_names_dbo_involved_person))) + 1,
                    LEN(LTRIM(RTRIM(i.given_names_dbo_involved_person)))
                )
            ) = 0
        THEN NULL
        ELSE
            -- All text after the second space (sentence case)
            (
                SELECT
                    CASE
                        WHEN val = '' THEN NULL
                        ELSE UPPER(LEFT(val,1)) + LOWER(SUBSTRING(val,2,LEN(val)))
                    END
                FROM (
                    SELECT
                        LTRIM(
                            SUBSTRING(
                                LTRIM(RTRIM(i.given_names_dbo_involved_person)),
                                CHARINDEX(' ', LTRIM(RTRIM(i.given_names_dbo_involved_person))) +
                                CHARINDEX(' ',
                                    SUBSTRING(
                                        LTRIM(RTRIM(i.given_names_dbo_involved_person)),
                                        CHARINDEX(' ', LTRIM(RTRIM(i.given_names_dbo_involved_person))) + 1,
                                        LEN(LTRIM(RTRIM(i.given_names_dbo_involved_person)))
                                    ) + ' '
                                ),
                                LEN(LTRIM(RTRIM(i.given_names_dbo_involved_person)))
                            )
                        ) AS val
                ) x
            )
    END AS given_name_3_dbo_involved_person,
    UPPER(i.surname_dbo_involved_person) AS surname_dbo_involved_person,
    NULL AS sex,
    NULL AS date_of_birth,
    NULL AS cni_number,
    NULL AS indigenous_status_no_involvement_type
FROM fvms_staging.dbo.DO_NOT_MIGRATE_all_person_lookup l
INNER JOIN fvms_staging.dbo.DO_NOT_MIGRATE_involved_person_DQI24599 i
    ON l.id_old = CAST(i.id_dbo_involved_person AS VARCHAR(255))
WHERE l.type = 'involved_person'
    AND l.id_old = l.Master_id;

PRINT 'Inserting master records from table DO_NOT_MIGRATE_involved_person_DQI24599 into person: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' records inserted.';
PRINT '';

GO

-- COALESCE missing cni_number from non-master records in DESCENDING ORDER of id
-- 527 records updated
-- First CTE: Rank the records in descending order based on id for each updated_cluster_id
WITH NonNullCNI AS (
    SELECT
		--d.id,
        d.updated_cluster_id,
        d.cni_number,
        ROW_NUMBER() OVER (
            PARTITION BY d.updated_cluster_id
            ORDER BY CAST(d.id AS INT) DESC
        ) AS rn
    FROM fvms_staging.dbo.DO_NOT_MIGRATE_person_dedup d
    WHERE d.cni_number IS NOT NULL
        AND d.id <> d.updated_cluster_id
),
FirstCNI AS (
    SELECT updated_cluster_id, cni_number
    FROM NonNullCNI
    WHERE rn = 1
)
UPDATE p
SET p.cni_number = f.cni_number
FROM fvms_staging.dbo.person p
INNER JOIN fvms_staging.dbo.DO_NOT_MIGRATE_person_dedup d
    ON p.id = d.new_id
INNER JOIN FirstCNI f
    ON d.updated_cluster_id = f.updated_cluster_id
WHERE p.cni_number IS NULL;

PRINT 'COALESCE missing cni_number values in table person: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' records updated.';
PRINT '';

GO

-- COALESCE missing parent indigenous_status_no_involvement_type from non-master records in DESCENDING ORDER of id_parent
-- 554 records updated
-- First CTE: Rank the records in descending order based on id_parent for each updated_cluster_id

WITH NonNull_ind_stat AS (
    SELECT
        d.id_parent,
        d.updated_cluster_id,
        d.indigenous_status_id,
        ROW_NUMBER() OVER (
            PARTITION BY d.updated_cluster_id
            ORDER BY
                TRY_CAST(LEFT(d.id_parent, CHARINDEX('_', d.id_parent) - 1) AS INT) DESC
        ) AS rn
    FROM fvms_staging.dbo.DO_NOT_MIGRATE_parent_dedup d
    WHERE d.indigenous_status_id IS NOT NULL
        AND d.id_parent <> d.updated_cluster_id
),
FirstCNI AS (
    SELECT updated_cluster_id, indigenous_status_id
    FROM NonNull_ind_stat
    WHERE rn = 1
)
UPDATE p
SET p.indigenous_status_no_involvement_type = f.indigenous_status_id
FROM fvms_staging.dbo.person p
INNER JOIN fvms_staging.dbo.DO_NOT_MIGRATE_parent_dedup d
    ON p.id = d.new_id
INNER JOIN FirstCNI f
    ON d.updated_cluster_id = f.updated_cluster_id
WHERE p.indigenous_status_no_involvement_type IS NULL;

PRINT 'COALESCE missing indigenous_status_no_involvement_type values in table person: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' records updated.';
PRINT '';

GO

-- Re-enable row count messages
SET NOCOUNT OFF;