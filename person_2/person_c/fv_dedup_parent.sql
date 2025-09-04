PRINT '';

-- Suppressing default row count output
SET NOCOUNT ON;

SET ANSI_WARNINGS OFF;

-- Updating table DO_NOT_MIGRATE_parent_dedup with the original given_names and surname.
-- Update DO_NOT_MIGRATE_parent_dedup with data from involved_person using id as the link
-- 334,582 records updated
UPDATE d
SET
    -- Everything before first space (sentence case)
    given_name_1 =
        CASE
            WHEN d.id_parent LIKE '%_p1' THEN
                CASE
                    WHEN c.parent1given_names IS NULL OR c.parent1given_names = '' THEN NULL
                    ELSE
                        UPPER(LEFT(LTRIM(RTRIM(c.parent1given_names)),1)) +
                        LOWER(SUBSTRING(LTRIM(RTRIM(c.parent1given_names)),2,CHARINDEX(' ',LTRIM(RTRIM(c.parent1given_names)) + ' ')-2))
                END
            ELSE
                CASE
                    WHEN c.parent2given_names IS NULL OR c.parent2given_names = '' THEN NULL
                    ELSE
                        UPPER(LEFT(LTRIM(RTRIM(c.parent2given_names)),1)) +
                        LOWER(SUBSTRING(LTRIM(RTRIM(c.parent2given_names)),2,CHARINDEX(' ',LTRIM(RTRIM(c.parent2given_names)) + ' ')-2))
                END
        END,
    -- Find the first word after first space (sentence case)
    given_name_2 =
        CASE
            WHEN d.id_parent LIKE '%_p1' THEN
                CASE
                    WHEN c.parent1given_names IS NULL OR CHARINDEX(' ', LTRIM(RTRIM(c.parent1given_names))) = 0 THEN NULL
                    ELSE
                        UPPER(LEFT(
                            SUBSTRING(
                                LTRIM(RTRIM(c.parent1given_names)),
                                CHARINDEX(' ', LTRIM(RTRIM(c.parent1given_names))) + 1,
                                LEN(LTRIM(RTRIM(c.parent1given_names)))
                            ), 1)) +
                        LOWER(
                            SUBSTRING(
                                SUBSTRING(
                                    LTRIM(RTRIM(c.parent1given_names)),
                                    CHARINDEX(' ', LTRIM(RTRIM(c.parent1given_names))) + 1,
                                    LEN(LTRIM(RTRIM(c.parent1given_names)))
                                ),
                                2,
                                CHARINDEX(' ',
                                    SUBSTRING(
                                        LTRIM(RTRIM(c.parent1given_names)),
                                        CHARINDEX(' ', LTRIM(RTRIM(c.parent1given_names))) + 1,
                                        LEN(LTRIM(RTRIM(c.parent1given_names)))
                                    ) + ' '
                                ) - 2
                            )
                        )
                END
            ELSE
                CASE
                    WHEN c.parent2given_names IS NULL OR CHARINDEX(' ', LTRIM(RTRIM(c.parent2given_names))) = 0 THEN NULL
                    ELSE
                        UPPER(LEFT(
                            SUBSTRING(
                                LTRIM(RTRIM(c.parent2given_names)),
                                CHARINDEX(' ', LTRIM(RTRIM(c.parent2given_names))) + 1,
                                LEN(LTRIM(RTRIM(c.parent2given_names)))
                            ), 1)) +
                        LOWER(
                            SUBSTRING(
                                SUBSTRING(
                                    LTRIM(RTRIM(c.parent2given_names)),
                                    CHARINDEX(' ', LTRIM(RTRIM(c.parent2given_names))) + 1,
                                    LEN(LTRIM(RTRIM(c.parent2given_names)))
                                ),
                                2,
                                CHARINDEX(' ',
                                    SUBSTRING(
                                        LTRIM(RTRIM(c.parent2given_names)),
                                        CHARINDEX(' ', LTRIM(RTRIM(c.parent2given_names))) + 1,
                                        LEN(LTRIM(RTRIM(c.parent2given_names)))
                                    ) + ' '
                                ) - 2
                            )
                        )
                END
        END,
    -- All text after the second space (sentence case)
    given_name_3 =
        CASE
            WHEN d.id_parent LIKE '%_p1' THEN
                CASE
                    WHEN c.parent1given_names IS NULL OR
                        CHARINDEX(' ', LTRIM(RTRIM(c.parent1given_names))) = 0 OR
                        CHARINDEX(' ',
                            SUBSTRING(
                                LTRIM(RTRIM(c.parent1given_names)),
                                CHARINDEX(' ', LTRIM(RTRIM(c.parent1given_names))) + 1,
                                LEN(LTRIM(RTRIM(c.parent1given_names)))
                            )
                        ) = 0
                    THEN NULL
                    ELSE
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
                                            LTRIM(RTRIM(c.parent1given_names)),
                                            CHARINDEX(' ', LTRIM(RTRIM(c.parent1given_names))) +
                                            CHARINDEX(' ',
                                                SUBSTRING(
                                                    LTRIM(RTRIM(c.parent1given_names)),
                                                    CHARINDEX(' ', LTRIM(RTRIM(c.parent1given_names))) + 1,
                                                    LEN(LTRIM(RTRIM(c.parent1given_names)))
                                                ) + ' '
                                            ),
                                            LEN(LTRIM(RTRIM(c.parent1given_names)))
                                        )
                                    ) AS val
                            ) x
                        )
                END
            ELSE
                CASE
                    WHEN c.parent2given_names IS NULL OR
                        CHARINDEX(' ', LTRIM(RTRIM(c.parent2given_names))) = 0 OR
                        CHARINDEX(' ',
                            SUBSTRING(
                                LTRIM(RTRIM(c.parent2given_names)),
                                CHARINDEX(' ', LTRIM(RTRIM(c.parent2given_names))) + 1,
                                LEN(LTRIM(RTRIM(c.parent2given_names)))
                            )
                        ) = 0
                    THEN NULL
                    ELSE
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
                                            LTRIM(RTRIM(c.parent2given_names)),
                                            CHARINDEX(' ', LTRIM(RTRIM(c.parent2given_names))) +
                                            CHARINDEX(' ',
                                                SUBSTRING(
                                                    LTRIM(RTRIM(c.parent2given_names)),
                                                    CHARINDEX(' ', LTRIM(RTRIM(c.parent2given_names))) + 1,
                                                    LEN(LTRIM(RTRIM(c.parent2given_names)))
                                                ) + ' '
                                            ),
                                            LEN(LTRIM(RTRIM(c.parent2given_names)))
                                        )
                                    ) AS val
                            ) x
                        )
                END
        END,
    -- ALL CAPS for surname
    surname =
        CASE
            WHEN d.id_parent LIKE '%_p1' THEN
                UPPER(c.parent1surname)
            ELSE
                UPPER(c.parent2surname)
        END,
    -- Concatenation with sentence-case given_name_1 and ALL CAPS surname
    name_concat =
        CASE
            WHEN d.id_parent LIKE '%_p1' THEN
                LTRIM(RTRIM(
                    CASE
                        WHEN c.parent1given_names IS NULL OR c.parent1given_names = '' THEN ''
                        ELSE
                            UPPER(LEFT(LTRIM(RTRIM(c.parent1given_names)),1)) +
                            LOWER(SUBSTRING(LTRIM(RTRIM(c.parent1given_names)),2,CHARINDEX(' ',LTRIM(RTRIM(c.parent1given_names)+' '))-2))
                    END
                    + ' ' + UPPER(c.parent1surname)
                ))
            ELSE
                LTRIM(RTRIM(
                    CASE
                        WHEN c.parent2given_names IS NULL OR c.parent2given_names = '' THEN ''
                        ELSE
                            UPPER(LEFT(LTRIM(RTRIM(c.parent2given_names)),1)) +
                            LOWER(SUBSTRING(LTRIM(RTRIM(c.parent2given_names)),2,CHARINDEX(' ',LTRIM(RTRIM(c.parent2given_names)+' '))-2))
                    END
                    + ' ' + UPPER(c.parent2surname)
                ))
        END
FROM fvms_staging.dbo.DO_NOT_MIGRATE_parent_dedup d
INNER JOIN fvms_clean.dbo.child c
    ON d.id = c.id;

PRINT 'Updating table DO_NOT_MIGRATE_parent_dedup: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' records updated.';
PRINT '';

GO

-- We will reassess and redefine how the cluster_id is assigned, in line with User Story 37973.
-- The linking model's matching criteria columns are relevant with the original cluster_id logic.
-- Assigning the master record (updated_cluster_id or File name in Atlas) so that the record containing the most complete set of details in the legacy system.
-- The following fields are considered when assessing the completeness of a record:
    -- given_name_1, given_name_2, given_name_3, surname, sex, and date_of_birth.
-- In cases where multiple records have the same number of populated fields, the tie-breaker will be:
    -- The record with the earliest date_created (i.e. the one with the lowest record id_parent).

-- 334,582 records
-- Add a helper CTE to count non-NULL fields for each record
WITH NonNullCounts AS (
    SELECT
        cluster_id,
		id_parent,
        -- Count the number of non-NULLs in the selected fields
        (CASE WHEN given_name_1 IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN given_name_2 IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN given_name_3 IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN surname IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN date_of_birth IS NOT NULL THEN 1 ELSE 0 END) AS non_null_count
    FROM fvms_staging.dbo.DO_NOT_MIGRATE_parent_dedup
),
-- For each cluster_id, find the candidate record(s) with the most non-NULL fields
RankedCandidates AS (
    SELECT
        cluster_id,
        id_parent AS updated_cluster_id,
        ROW_NUMBER() OVER (
            PARTITION BY cluster_id
            ORDER BY
                non_null_count DESC,
                CASE WHEN id_parent IS NOT NULL THEN 0 ELSE 1 END ASC,
                id_parent ASC
        ) AS rn
    FROM NonNullCounts
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
FROM fvms_staging.dbo.DO_NOT_MIGRATE_parent_dedup f
INNER JOIN NewClusterId nci
    ON f.cluster_id = nci.cluster_id;

PRINT 'Updating table DO_NOT_MIGRATE_parent_dedup with an updated_cluster_id, as per User Story 37973: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' records updated.';
PRINT '';

GO

-- 334,582 records - parent

-- Re-enable row count messages
SET NOCOUNT OFF;