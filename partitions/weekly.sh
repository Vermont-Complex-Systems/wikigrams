#!/bin/bash
set -e  # Exit on error
source .venv/bin/activate

echo "Starting weekly aggregation..."

duckdb << 'EOF'
SET memory_limit = '30GB';
SET threads = 12;
SET temp_directory = '/netfiles/compethicslab/wikimedia/duckdb_tmp';

ATTACH 'ducklake:./metadata.ducklake' AS wikilake
(DATA_PATH '/netfiles/compethicslab/wikimedia');

USE wikilake;

-- Configure target file size BEFORE creating table
-- This helps DuckDB write larger files per partition
CALL wikilake.set_option('target_file_size', '512MB');

-- Create table if it doesn't exist
CREATE TABLE IF NOT EXISTS wikigrams_weekly (
    geo TEXT,
    week DATE,
    types TEXT,
    counts BIGINT
);

-- Set partitioning (idempotent)
ALTER TABLE wikigrams_weekly SET PARTITIONED BY (geo, week);

-- Use 1 thread for INSERT since weekly aggregates are smaller
-- With smaller data volume per partition, threads=1 prevents fragmentation
SET threads = 4;

-- Increase max open files for partitioned writes
-- Default is 100, which can cause early flushes and small files
-- For full dataset: ~200 weeks × 4 geos = 800 partitions
SET partitioned_write_max_open_files = 1000;

-- For full table refresh, TRUNCATE is more efficient than DELETE
-- Only truncate if table has data (handles both initial setup and rebuild)
DELETE FROM wikigrams_weekly;

-- Materialize aggregation in temp table first (in-memory)
-- This prevents parallel writes from creating many small files per partition
CREATE TEMP TABLE weekly_agg AS
SELECT
    geo,
    DATE_TRUNC('week', date) as week,
    types,
    SUM(counts)::BIGINT AS counts
FROM wikigrams
GROUP BY geo, DATE_TRUNC('week', date), types;

-- Insert from materialized temp table with ORDER BY
-- CRITICAL: ORDER BY partition columns prevents partition switching
-- This writes consolidated files per partition instead of fragmenting
INSERT INTO wikigrams_weekly
SELECT * FROM weekly_agg
ORDER BY geo, week;

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
EOF

echo "Weekly aggregation complete!"
