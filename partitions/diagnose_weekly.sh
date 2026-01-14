#!/bin/bash
set -e
source .venv/bin/activate

echo "=== Diagnosing wikigrams_weekly partitioning and file issues ==="

duckdb << 'EOF'
SET memory_limit = '30GB';
SET threads = 12;
SET temp_directory = '/netfiles/compethicslab/wikimedia/duckdb_tmp';

ATTACH 'ducklake:./metadata.ducklake' AS wikilake
(DATA_PATH '/netfiles/compethicslab/wikimedia');

USE wikilake;

-- 1. Check table partitioning configuration
.print "=== Partitioning Configuration ==="
SELECT table_name, partition_columns
FROM information_schema.partitions
WHERE table_name = 'wikigrams_weekly';

-- 2. Data distribution by partition
.print ""
.print "=== Data Distribution by Geo ==="
SELECT
    geo,
    COUNT(DISTINCT week) as num_weeks,
    COUNT(*) as total_rows,
    SUM(counts) as total_counts,
    MIN(week) as earliest_week,
    MAX(week) as latest_week
FROM wikigrams_weekly
GROUP BY geo
ORDER BY geo;

-- 3. File statistics (shows small files problem)
.print ""
.print "=== File Statistics ==="
SELECT * FROM wikilake.file_statistics('wikigrams_weekly');

-- 4. Snapshot history
.print ""
.print "=== Snapshot History ==="
SELECT * FROM wikilake.snapshots() WHERE table_name = 'wikigrams_weekly' ORDER BY created_at DESC LIMIT 10;

-- 5. Configuration options
.print ""
.print "=== Current Configuration ==="
SELECT name, value FROM wikilake.options()
WHERE name IN ('target_file_size', 'parquet_compression', 'data_inlining_row_limit');

EOF

echo "=== Diagnosis complete ==="
