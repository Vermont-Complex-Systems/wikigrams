#!/bin/bash
set -e
source .venv/bin/activate

echo "=== Testing weekly aggregation with small data ==="

duckdb << 'EOF'

ATTACH 'ducklake:test_metadata.ducklake' AS testlake
(DATA_PATH './test_data');

USE testlake;

-- Configure settings
CALL testlake.set_option('target_file_size', '512MB');

-- Drop and recreate weekly table
DROP TABLE IF EXISTS wikigrams_weekly_test;

CREATE TABLE wikigrams_weekly_test (
    geo TEXT,
    week DATE,
    types TEXT,
    counts BIGINT
);

ALTER TABLE wikigrams_weekly_test SET PARTITIONED BY (geo, week);

-- Use 1 thread for everything
SET threads = 1;

-- Aggregate and insert in one step
INSERT INTO wikigrams_weekly_test
SELECT
    geo,
    DATE_TRUNC('week', date) as week,
    types,
    SUM(counts)::BIGINT AS counts
FROM wikigrams_test
GROUP BY geo, DATE_TRUNC('week', date), types;

-- Check file counts per partition
.print "Files per partition:"
SELECT
    regexp_extract(file, 'geo=([^/]+)', 1) as geo,
    regexp_extract(file, 'week=([^/]+)', 1) as week,
    COUNT(*) as file_count
FROM glob('./test_data/main/wikigrams_weekly_test/**/*.parquet')
GROUP BY
    regexp_extract(file, 'geo=([^/]+)', 1),
    regexp_extract(file, 'week=([^/]+)', 1)
ORDER BY geo, week;

-- Show total files
.print ""
.print "Total parquet files:"
SELECT COUNT(*) as total_files
FROM glob('./test_data/main/wikigrams_weekly_test/**/*.parquet');

-- Show summary
.print ""
.print "Data summary:"
SELECT
    geo,
    COUNT(DISTINCT week) as weeks,
    COUNT(*) as total_rows
FROM wikigrams_weekly_test
GROUP BY geo
ORDER BY geo;

EOF

echo ""
echo "=== Test complete! ==="
echo "Expected: 1 file per partition (geo+week combination)"
echo "If you see many files per partition, SET threads = 1 isn't working"
