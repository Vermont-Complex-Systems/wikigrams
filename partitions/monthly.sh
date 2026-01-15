#!/bin/bash
set -e  # Exit on error
source .venv/bin/activate

echo "Starting monthly aggregation..."

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
CREATE TABLE IF NOT EXISTS wikigrams_monthly (
    geo TEXT,
    month DATE,
    types TEXT,
    counts BIGINT
);

-- Set partitioning (idempotent)
ALTER TABLE wikigrams_monthly SET PARTITIONED BY (geo, month);

-- For full table refresh, DELETE works for both empty and populated tables
-- (TRUNCATE would fail on newly created partitioned tables in some cases)
DELETE FROM wikigrams_monthly;

-- Materialize aggregation in temp table first (in-memory)
-- This prevents parallel writes from creating many small files per partition
CREATE TEMP TABLE monthly_agg AS
SELECT
    geo,
    DATE_TRUNC('month', date) as month,
    types,
    SUM(counts)::BIGINT AS counts
FROM wikigrams
GROUP BY geo, DATE_TRUNC('month', date), types;

-- Reduce threads for INSERT to create fewer, larger files
-- threads=4 provides good balance between speed and file count
SET threads = 4;

-- Insert from materialized temp table
-- This writes larger, consolidated files per partition
INSERT INTO wikigrams_monthly
SELECT * FROM monthly_agg;

-- Show summary
SELECT
    geo,
    COUNT(DISTINCT month) as months,
    COUNT(*) as total_rows,
    SUM(counts) as total_counts,
    MIN(month) as earliest_month,
    MAX(month) as latest_month
FROM wikigrams_monthly
GROUP BY geo
ORDER BY geo;
EOF

echo "Monthly aggregation complete!"
