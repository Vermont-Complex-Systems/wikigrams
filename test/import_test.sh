#!/bin/bash
set -e
source .venv/bin/activate

echo "=== Testing import with small data subset ==="

duckdb << 'EOF'

ATTACH 'ducklake:test_metadata.ducklake' AS testlake
(DATA_PATH './test_data');

USE testlake;

-- Create table if it doesn't exist
CREATE TABLE IF NOT EXISTS wikigrams_test (
    geo TEXT,
    date DATE,
    types TEXT,
    counts BIGINT
);

-- Set partitioning
ALTER TABLE wikigrams_test SET PARTITIONED BY (geo, date);

-- Configure settings
CALL testlake.set_option('target_file_size', '512MB');

-- Import 7 days of data for testing (March 3-10, 2025)
CREATE TEMP TABLE csv_import AS
SELECT
    column0 AS geo,
    CAST(regexp_extract(filename, '(\d{4}-\d{2}-\d{2})_', 1) AS DATE) AS date,
    column1 AS types,
    CAST(count AS BIGINT) AS counts
FROM read_csv(
    '/gpfs1/home/m/v/mvarnold/wikipedia-parsing/data/1grams/*_wikipedia_1grams.tsv',
    delim='\t',
    header=true,
    filename=true
)
WHERE column0 IN ('United States', 'Canada', 'Australia', 'United Kingdom')
  AND regexp_extract(filename, '(\d{4}-\d{2}-\d{2})_', 1)
      BETWEEN '2025-03-03' AND '2025-03-10';
-- No LIMIT - use full week of data for realistic test

-- Show what we're importing
.print "Test data summary:"
SELECT
    COUNT(DISTINCT date) as dates,
    COUNT(DISTINCT geo) as geos,
    COUNT(*) as total_rows
FROM csv_import;

-- Use 1 thread for INSERT
SET threads = 1;

-- Insert
INSERT INTO wikigrams_test (geo, date, types, counts)
SELECT * FROM csv_import;

-- Check file counts per partition
.print ""
.print "Files per partition:"
SELECT
    regexp_extract(file, 'geo=([^/]+)', 1) as geo,
    regexp_extract(file, 'date=([^/]+)', 1) as date,
    COUNT(*) as file_count
FROM glob('./test_data/main/wikigrams_test/**/*.parquet')
GROUP BY
    regexp_extract(file, 'geo=([^/]+)', 1),
    regexp_extract(file, 'date=([^/]+)', 1)
ORDER BY geo, date;

-- Show total files
.print ""
.print "Total parquet files:"
SELECT COUNT(*) as total_files
FROM glob('./test_data/main/wikigrams_test/**/*.parquet');

EOF

echo ""
echo "=== Test complete! ==="
echo "Expected: 1-2 files per partition (geo+date combination)"
echo "If you see many files per partition, the fix isn't working"
