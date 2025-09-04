PRINT '';

-- Suppressing default row count output
SET NOCOUNT ON;

SET ANSI_WARNINGS OFF;

-- Updating table DO_NOT_MIGRATE_child_dedup with the original given_names and surname.
-- Update DO_NOT_MIGRATE_child_dedup with data from involved_person using id as the link
-- 167,291 records updated
UPDATE d
SET
    d.given_name_1 =
        CASE
            WHEN c.given_names IS NULL OR c.given_names = '' THEN NULL
            ELSE
                UPPER(LEFT(LTRIM(RTRIM(c.given_names)),1)) +
                LOWER(SUBSTRING(LTRIM(RTRIM(c.given_names)),2,CHARINDEX(' ',LTRIM(RTRIM(c.given_names)) + ' ')-2))
        END,
    d.given_name_2 =
        CASE
            WHEN c.given_names IS NULL OR CHARINDEX(' ', LTRIM(RTRIM(c.given_names))) = 0 THEN NULL
            ELSE
                -- Find the first word after first space (sentence case)
                UPPER(LEFT(
                    SUBSTRING(
                        LTRIM(RTRIM(c.given_names)),
                        CHARINDEX(' ', LTRIM(RTRIM(c.given_names))) + 1,
                        LEN(LTRIM(RTRIM(c.given_names)))
                    ), 1)) +
                LOWER(
                    SUBSTRING(
                        SUBSTRING(
                            LTRIM(RTRIM(c.given_names)),
                            CHARINDEX(' ', LTRIM(RTRIM(c.given_names))) + 1,
                            LEN(LTRIM(RTRIM(c.given_names)))
                        ),
                        2,
                        CHARINDEX(' ',
                            SUBSTRING(
                                LTRIM(RTRIM(c.given_names)),
                                CHARINDEX(' ', LTRIM(RTRIM(c.given_names))) + 1,
                                LEN(LTRIM(RTRIM(c.given_names)))
                            ) + ' '
                        ) - 2
                    )
                )
        END,
    d.given_name_3 =
        CASE
            WHEN c.given_names IS NULL OR
                CHARINDEX(' ', LTRIM(RTRIM(c.given_names))) = 0 OR
                CHARINDEX(' ',
                    SUBSTRING(
                        LTRIM(RTRIM(c.given_names)),
                        CHARINDEX(' ', LTRIM(RTRIM(c.given_names))) + 1,
                        LEN(LTRIM(RTRIM(c.given_names)))
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
                                    LTRIM(RTRIM(c.given_names)),
                                    CHARINDEX(' ', LTRIM(RTRIM(c.given_names))) +
                                    CHARINDEX(' ',
                                        SUBSTRING(
                                            LTRIM(RTRIM(c.given_names)),
                                            CHARINDEX(' ', LTRIM(RTRIM(c.given_names))) + 1,
                                            LEN(LTRIM(RTRIM(c.given_names)))
                                        ) + ' '
                                    ),
                                    LEN(LTRIM(RTRIM(c.given_names)))
                                )
                            ) AS val
                    ) x
                )
        END,
	-- ALL CAPS for surname
	d.surname = UPPER(c.surname),
	-- Concatenation with sentence-case given_name_1 and ALL CAPS surname
	d.name_concat =
		LTRIM(RTRIM(
			CASE
				WHEN c.given_names IS NULL OR c.given_names = '' THEN ''
				ELSE
					UPPER(LEFT(LTRIM(RTRIM(c.given_names)),1)) +
					LOWER(SUBSTRING(LTRIM(RTRIM(c.given_names)),2,CHARINDEX(' ',LTRIM(RTRIM(c.given_names)+' '))-2))
			END
			+ ' ' + UPPER(c.surname)
		))
FROM
    fvms_staging.dbo.DO_NOT_MIGRATE_child_dedup d
JOIN
    fvms_clean.dbo.involved_person c
    ON d.id = c.id -- (change join as appropriate)

PRINT 'Updating table DO_NOT_MIGRATE_child_dedup: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' records updated.';
PRINT '';

GO

-- We will reassess and redefine how the cluster_id is assigned, in line with User Story 37973.
-- The linking model's matching criteria columns are relevant with the original cluster_id logic.
-- Assigning the master record (updated_cluster_id or File name in Atlas) so that the record containing the most complete set of details in the legacy system.
-- The following fields are considered when assessing the completeness of a record:
    -- given_name_1, given_name_2, given_name_3, surname, sex, and date_of_birth.
-- In cases where multiple records have the same number of populated fields, the tie-breaker will be:
    -- The record with the earliest date_created (i.e. the one with the lowest record id).

-- 167,291 records
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
    FROM fvms_staging.dbo.DO_NOT_MIGRATE_child_dedup
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
FROM fvms_staging.dbo.DO_NOT_MIGRATE_child_dedup f
INNER JOIN NewClusterId nci
    ON f.cluster_id = nci.cluster_id;

PRINT 'Updating table DO_NOT_MIGRATE_child_dedup with an updated_cluster_id, as per User Story 37973: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' records updated.';
PRINT '';

GO

-- 167,291 records - child

-- Re-enable row count messages
SET NOCOUNT OFF;