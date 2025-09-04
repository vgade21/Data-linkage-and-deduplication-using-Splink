-- Script Purpose: Generate the FVMS person lookup table DO_NOT_MIGRATE_all_person_lookup, which is used to link old FV person ids to new FVMS_staging person ids
-- Feature DQI: 24183, 35420

PRINT '';

-- Suppressing default row count output
SET NOCOUNT ON;

SET ANSI_WARNINGS OFF;

-- Create (if it doesn't exist) or truncate table
DROP TABLE IF EXISTS fvms_staging.dbo.DO_NOT_MIGRATE_all_person_lookup;
CREATE TABLE fvms_staging.dbo.DO_NOT_MIGRATE_all_person_lookup (
    id_new INT,
    type VARCHAR(50),
    Master_id VARCHAR(255),
    id_old VARCHAR(255),
    can_exclude BIT
);

PRINT 'Created table DO_NOT_MIGRATE_all_person_lookup';
PRINT '';

GO

-- Insert Data from Source Tables in the Required Order and Mapping

-- PERSON: id_new 1000000 - 1999999
-- 75,651 records
INSERT INTO fvms_staging.dbo.DO_NOT_MIGRATE_all_person_lookup (id_new, type, Master_id, id_old, can_exclude)
SELECT
    1000000 + DENSE_RANK() OVER (ORDER BY updated_cluster_id) - 1 AS id_new,
    'person' AS type,
    CAST(updated_cluster_id AS VARCHAR(255)) AS Master_id,
    CAST(id AS VARCHAR(255)) AS id_old,
    can_exclude
FROM fvms_staging.dbo.DO_NOT_MIGRATE_person_dedup
WHERE can_exclude = 0
ORDER BY updated_cluster_id ASC, id ASC;

PRINT 'Inserting records from table DO_NOT_MIGRATE_person_dedup into table DO_NOT_MIGRATE_all_person_lookup: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' records inserted.';
PRINT '';

GO

-- CHILD: id_new 2000000 - 2999999
-- 167,205 records
INSERT INTO fvms_staging.dbo.DO_NOT_MIGRATE_all_person_lookup (id_new, type, Master_id, id_old, can_exclude)
SELECT
    2000000 + DENSE_RANK() OVER (ORDER BY updated_cluster_id) - 1 AS id_new,
    'child' AS type,
    CAST(updated_cluster_id AS VARCHAR(255)) AS Master_id,
    CAST(id AS VARCHAR(255)) AS id_old,
    can_exclude
FROM fvms_staging.dbo.DO_NOT_MIGRATE_child_dedup
WHERE can_exclude = 0
ORDER BY updated_cluster_id ASC, id ASC;

PRINT 'Inserting records from table DO_NOT_MIGRATE_child_dedup into table DO_NOT_MIGRATE_all_person_lookup: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' records inserted.';
PRINT '';

GO

-- PARENT: id_new 3000000 - 3999999
-- 246,095 records (excluding records where there is no parent information)
INSERT INTO fvms_staging.dbo.DO_NOT_MIGRATE_all_person_lookup (id_new, type, Master_id, id_old, can_exclude)
SELECT
    3000000 + DENSE_RANK() OVER (ORDER BY updated_cluster_id) - 1 AS id_new,
    CASE
        WHEN id_parent LIKE '%_p1' THEN 'parent1'
        WHEN id_parent LIKE '%_p2' THEN 'parent2'
        ELSE 'parent'
    END AS type,
    CAST(updated_cluster_id AS VARCHAR(255)) AS Master_id,
    id_parent,
    can_exclude
FROM fvms_staging.dbo.DO_NOT_MIGRATE_parent_dedup
-- WHERE NOT (given_name_1 IS NULL AND surname IS NULL AND date_of_birth IS NULL AND (address IS NULL OR LTRIM(RTRIM(address)) = '') AND indigenous_status_id IS NULL)
WHERE can_exclude = 0
ORDER BY updated_cluster_id ASC, id_parent ASC;

PRINT 'Inserting records from table DO_NOT_MIGRATE_parent_dedup into table DO_NOT_MIGRATE_all_person_lookup: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' records inserted.';
PRINT '';

GO

-- WITNESS: id_new 4000000 - 4999999
-- 8,428 records
INSERT INTO fvms_staging.dbo.DO_NOT_MIGRATE_all_person_lookup (id_new, type, Master_id, id_old, can_exclude)
SELECT
    4000000 + DENSE_RANK() OVER (ORDER BY updated_cluster_id) - 1 AS id_new,
    'witness' AS type,
    CAST(updated_cluster_id AS VARCHAR(255)) AS Master_id,
    CAST(id AS VARCHAR(255)) AS id_old,
    can_exclude
FROM fvms_staging.dbo.DO_NOT_MIGRATE_witness_dedup
WHERE can_exclude = 0
ORDER BY updated_cluster_id ASC, id ASC;

PRINT 'Inserting records from table DO_NOT_MIGRATE_witness_dedup into table DO_NOT_MIGRATE_all_person_lookup: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' records inserted.';
PRINT '';

GO

-- INVOLVED_PERSON: id_new 5000000 - 5999999
-- 30 records
INSERT INTO fvms_staging.dbo.DO_NOT_MIGRATE_all_person_lookup (id_new, type, Master_id, id_old, can_exclude)
SELECT
    5000000 + ROW_NUMBER() OVER (ORDER BY id_dbo_involved_person ASC) - 1 AS id_new,
    'involved_person' AS type,
    CAST(id_dbo_involved_person AS VARCHAR(255)) AS Master_id,
    CAST(id_dbo_involved_person AS VARCHAR(255)) AS id_old,
    NULL AS can_exclude
FROM fvms_staging.dbo.DO_NOT_MIGRATE_involved_person_DQI24599
ORDER BY id_dbo_involved_person ASC;

PRINT 'Inserting records from table DO_NOT_MIGRATE_involved_person_DQI24599 into table DO_NOT_MIGRATE_all_person_lookup: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' records inserted.';
PRINT '';

GO

-- 1) Count of distinct id_new
DECLARE @cnt_distinct INT;
SELECT @cnt_distinct = COUNT(DISTINCT id_new) FROM fvms_staging.dbo.DO_NOT_MIGRATE_all_person_lookup;
PRINT 'Count of distinct id_new: ' + CAST(@cnt_distinct AS VARCHAR);
PRINT '';

-- 2) First value and last value in column id_new
DECLARE @min_id INT, @max_id INT;
SELECT @min_id = MIN(id_new), @max_id = MAX(id_new) FROM fvms_staging.dbo.DO_NOT_MIGRATE_all_person_lookup;
PRINT 'First (lowest) id_new: ' + CAST(@min_id AS VARCHAR);
PRINT 'Last (highest) id_new: ' + CAST(@max_id AS VARCHAR);
PRINT '';

-- 3) Count of records in each range
DECLARE @cnt_1 INT, @cnt_2 INT, @cnt_3 INT, @cnt_4 INT, @cnt_5 INT;
SELECT
    @cnt_1 = COUNT(*) FROM fvms_staging.dbo.DO_NOT_MIGRATE_all_person_lookup WHERE id_new BETWEEN 1000000 AND 1999999;
SELECT
    @cnt_2 = COUNT(*) FROM fvms_staging.dbo.DO_NOT_MIGRATE_all_person_lookup WHERE id_new BETWEEN 2000000 AND 2999999;
SELECT
    @cnt_3 = COUNT(*) FROM fvms_staging.dbo.DO_NOT_MIGRATE_all_person_lookup WHERE id_new BETWEEN 3000000 AND 3999999;
SELECT
    @cnt_4 = COUNT(*) FROM fvms_staging.dbo.DO_NOT_MIGRATE_all_person_lookup WHERE id_new BETWEEN 4000000 AND 4999999;
SELECT
    @cnt_5 = COUNT(*) FROM fvms_staging.dbo.DO_NOT_MIGRATE_all_person_lookup WHERE id_new BETWEEN 5000000 AND 5999999;
PRINT 'Count in id_new 1000000 - 1999999: ' + CAST(@cnt_1 AS VARCHAR);
PRINT 'Count in id_new 2000000 - 2999999: ' + CAST(@cnt_2 AS VARCHAR);
PRINT 'Count in id_new 3000000 - 3999999: ' + CAST(@cnt_3 AS VARCHAR);
PRINT 'Count in id_new 4000000 - 4999999: ' + CAST(@cnt_4 AS VARCHAR);
PRINT 'Count in id_new 5000000 - 5999999: ' + CAST(@cnt_5 AS VARCHAR);
PRINT '';

-- 4) First and last value in each range
DECLARE @min1 INT, @max1 INT, @min2 INT, @max2 INT, @min3 INT, @max3 INT, @min4 INT, @max4 INT, @min5 INT, @max5 INT;
SELECT @min1 = MIN(id_new), @max1 = MAX(id_new)
FROM fvms_staging.dbo.DO_NOT_MIGRATE_all_person_lookup
WHERE id_new BETWEEN 1000000 AND 1999999;
SELECT @min2 = MIN(id_new), @max2 = MAX(id_new)
FROM fvms_staging.dbo.DO_NOT_MIGRATE_all_person_lookup
WHERE id_new BETWEEN 2000000 AND 2999999;
SELECT @min3 = MIN(id_new), @max3 = MAX(id_new)
FROM fvms_staging.dbo.DO_NOT_MIGRATE_all_person_lookup
WHERE id_new BETWEEN 3000000 AND 3999999;
SELECT @min4 = MIN(id_new), @max4 = MAX(id_new)
FROM fvms_staging.dbo.DO_NOT_MIGRATE_all_person_lookup
WHERE id_new BETWEEN 4000000 AND 4999999;
SELECT @min5 = MIN(id_new), @max5 = MAX(id_new)
FROM fvms_staging.dbo.DO_NOT_MIGRATE_all_person_lookup
WHERE id_new BETWEEN 5000000 AND 5999999;
PRINT 'First and last id_new in 1000000 - 1999999: ' + ISNULL(CAST(@min1 AS VARCHAR), 'None') + ', ' + ISNULL(CAST(@max1 AS VARCHAR), 'None');
PRINT 'First and last id_new in 2000000 - 2999999: ' + ISNULL(CAST(@min2 AS VARCHAR), 'None') + ', ' + ISNULL(CAST(@max2 AS VARCHAR), 'None');
PRINT 'First and last id_new in 3000000 - 3999999: ' + ISNULL(CAST(@min3 AS VARCHAR), 'None') + ', ' + ISNULL(CAST(@max3 AS VARCHAR), 'None');
PRINT 'First and last id_new in 4000000 - 4999999: ' + ISNULL(CAST(@min4 AS VARCHAR), 'None') + ', ' + ISNULL(CAST(@max4 AS VARCHAR), 'None');
PRINT 'First and last id_new in 5000000 - 5999999: ' + ISNULL(CAST(@min5 AS VARCHAR), 'None') + ', ' + ISNULL(CAST(@max5 AS VARCHAR), 'None');
PRINT '';

-- 5) Count of distinct id_new in each range
DECLARE @dcnt_1 INT, @dcnt_2 INT, @dcnt_3 INT, @dcnt_4 INT, @dcnt_5 INT;
SELECT @dcnt_1 = COUNT(DISTINCT id_new)
FROM fvms_staging.dbo.DO_NOT_MIGRATE_all_person_lookup
WHERE id_new BETWEEN 1000000 AND 1999999;
SELECT @dcnt_2 = COUNT(DISTINCT id_new)
FROM fvms_staging.dbo.DO_NOT_MIGRATE_all_person_lookup
WHERE id_new BETWEEN 2000000 AND 2999999;
SELECT @dcnt_3 = COUNT(DISTINCT id_new)
FROM fvms_staging.dbo.DO_NOT_MIGRATE_all_person_lookup
WHERE id_new BETWEEN 3000000 AND 3999999;
SELECT @dcnt_4 = COUNT(DISTINCT id_new)
FROM fvms_staging.dbo.DO_NOT_MIGRATE_all_person_lookup
WHERE id_new BETWEEN 4000000 AND 4999999;
SELECT @dcnt_5 = COUNT(DISTINCT id_new)
FROM fvms_staging.dbo.DO_NOT_MIGRATE_all_person_lookup
WHERE id_new BETWEEN 5000000 AND 5999999;
PRINT 'Distinct count in id_new 1000000 - 1999999: ' + CAST(@dcnt_1 AS VARCHAR);
PRINT 'Distinct count in id_new 2000000 - 2999999: ' + CAST(@dcnt_2 AS VARCHAR);
PRINT 'Distinct count in id_new 3000000 - 3999999: ' + CAST(@dcnt_3 AS VARCHAR);
PRINT 'Distinct count in id_new 4000000 - 4999999: ' + CAST(@dcnt_4 AS VARCHAR);
PRINT 'Distinct count in id_new 5000000 - 5999999: ' + CAST(@dcnt_5 AS VARCHAR);
PRINT '';

/*
Count of distinct id_new: 156328

First (lowest) id_new: 1000000
Last (highest) id_new: 5000029

Count in id_new 1000000 - 1999999: 75651
Count in id_new 2000000 - 2999999: 167205
Count in id_new 3000000 - 3999999: 246095
Count in id_new 4000000 - 4999999: 8428
Count in id_new 5000000 - 5999999: 30

First and last id_new in 1000000 - 1999999: 1000000, 1066047
First and last id_new in 2000000 - 2999999: 2000000, 2053801
First and last id_new in 3000000 - 3999999: 3000000, 3030369
First and last id_new in 4000000 - 4999999: 4000000, 4006077
First and last id_new in 5000000 - 5999999: 5000000, 5000029

Distinct count in id_new 1000000 - 1999999: 66048
Distinct count in id_new 2000000 - 2999999: 53802
Distinct count in id_new 3000000 - 3999999: 30370
Distinct count in id_new 4000000 - 4999999: 6078
Distinct count in id_new 5000000 - 5999999: 30
*/


-- set the new_id column to NULL
UPDATE fvms_staging.dbo.DO_NOT_MIGRATE_person_dedup SET new_id = NULL;
GO
UPDATE fvms_staging.dbo.DO_NOT_MIGRATE_child_dedup SET new_id = NULL;
GO
UPDATE fvms_staging.dbo.DO_NOT_MIGRATE_parent_dedup SET new_id = NULL;
GO
UPDATE fvms_staging.dbo.DO_NOT_MIGRATE_witness_dedup SET new_id = NULL;
GO
UPDATE fvms_staging.dbo.DO_NOT_MIGRATE_involved_person_DQI24599 SET new_id = NULL;
GO

-- Updating the new_id column in each dedup table based on the id_new values in DO_NOT_MIGRATE_all_person_lookup.
-- Update DO_NOT_MIGRATE_person_dedup
-- 75,651 records
UPDATE p
SET p.new_id = l.id_new
FROM fvms_staging.dbo.DO_NOT_MIGRATE_person_dedup p
INNER JOIN fvms_staging.dbo.DO_NOT_MIGRATE_all_person_lookup l
    ON l.type = 'person'
    AND l.id_old = CAST(p.id AS VARCHAR(255));

PRINT 'Updating table DO_NOT_MIGRATE_person_dedup with id_new: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' records updated.';
PRINT '';

GO

-- Update DO_NOT_MIGRATE_child_dedup
-- 167,205 records
UPDATE c
SET c.new_id = l.id_new
FROM fvms_staging.dbo.DO_NOT_MIGRATE_child_dedup c
INNER JOIN fvms_staging.dbo.DO_NOT_MIGRATE_all_person_lookup l
    ON l.type = 'child'
    AND l.id_old = CAST(c.id AS VARCHAR(255));

PRINT 'Updating table DO_NOT_MIGRATE_child_dedup with id_new: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' records updated.';
PRINT '';

GO

-- Update DO_NOT_MIGRATE_parent_dedup for both 'parent1' and 'parent2'
-- 246,095 records
UPDATE pa
SET pa.new_id = l.id_new
FROM fvms_staging.dbo.DO_NOT_MIGRATE_parent_dedup pa
INNER JOIN fvms_staging.dbo.DO_NOT_MIGRATE_all_person_lookup l
    ON (l.type = 'parent1' OR l.type = 'parent2')
    AND l.id_old = pa.id_parent;

PRINT 'Updating table DO_NOT_MIGRATE_parent_dedup with id_new: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' records updated.';
PRINT '';

GO

-- Update DO_NOT_MIGRATE_witness_dedup
-- 8,428 records
UPDATE w
SET w.new_id = l.id_new
FROM fvms_staging.dbo.DO_NOT_MIGRATE_witness_dedup w
INNER JOIN fvms_staging.dbo.DO_NOT_MIGRATE_all_person_lookup l
    ON l.type = 'witness'
    AND l.id_old = CAST(w.id AS VARCHAR(255));

PRINT 'Updating table DO_NOT_MIGRATE_witness_dedup with id_new: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' records updated.';
PRINT '';

GO

-- Update DO_NOT_MIGRATE_involved_person_DQI24599
-- 30 records
UPDATE ip
SET ip.new_id = l.id_new
FROM fvms_staging.dbo.DO_NOT_MIGRATE_involved_person_DQI24599 ip
INNER JOIN fvms_staging.dbo.DO_NOT_MIGRATE_all_person_lookup l
    ON l.type = 'involved_person'
    AND l.id_old = CAST(ip.id_dbo_involved_person AS VARCHAR(255));

PRINT 'Updating table DO_NOT_MIGRATE_involved_person_DQI24599 with id_new: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' records updated.';
PRINT '';

GO

-- Re-enable row count messages
SET NOCOUNT OFF;