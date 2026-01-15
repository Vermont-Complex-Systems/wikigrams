#!/bin/bash
set -e  # Exit on error
source .venv/bin/activate

echo "Starting weekly aggregation (single-threaded approach)..."

duckdb << 'EOF'
SET memory_limit = '30GB';
SET temp_directory = '/netfiles/compethicslab/wikimedia/duckdb_tmp';

ATTACH 'ducklake:./metadata.ducklake' AS wikilake
(DATA_PATH '/netfiles/compethicslab/wikimedia');

USE wikilake;

-- Configure target file size BEFORE creating table
CALL wikilake.set_option('target_file_size', '512MB');

-- Drop and recreate to start fresh (removes small files)
DROP TABLE IF EXISTS wikigrams_weekly;

CREATE TABLE wikigrams_weekly (
    geo TEXT,
    week DATE,
    types TEXT,
    counts BIGINT
);

-- Set partitioning
ALTER TABLE wikigrams_weekly SET PARTITIONED BY (geo, week);

-- CRITICAL: Set threads to 1 BEFORE any aggregation work
SET threads = 1;

-- Single-threaded aggregation and insert in one step
-- This should create 1 file per partition
INSERT INTO wikigrams_weekly
SELECT
    geo,
    DATE_TRUNC('week', date) as week,
    types,
    SUM(counts)::BIGINT AS counts
FROM wikigrams
GROUP BY geo, DATE_TRUNC('week', date), types;

-- Show summary
SELECT
    geo,
    COUNT(DISTINCT week) as weeks,
    COUNT(*) as total_rows,
    SUM(counts) as total_counts,
    MIN(week) as earliest_week,
    MAX(week) as latest_week
FROM wikigrams_weekly
GROUP BY geo
ORDER BY geo;

-- Check specific partition file count
.print ""
.print "File count in sample partition (should be 1-2 files):"
SELECT COUNT(*) as file_count
FROM glob('/netfiles/compethicslab/wikimedia/main/wikigrams_weekly/geo=United%20States/week=2025-03-03/*.parquet');
EOF

echo "Weekly aggregation complete!"
