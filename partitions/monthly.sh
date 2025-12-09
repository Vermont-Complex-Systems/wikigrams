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

-- Create table if it doesn't exist
CREATE TABLE IF NOT EXISTS wikigrams_monthly (
    geo TEXT,
    month DATE,
    types TEXT,
    counts BIGINT
);

-- Set partitioning (idempotent)
ALTER TABLE wikigrams_monthly SET PARTITIONED BY (geo, month);

-- Delete all existing data
DELETE FROM wikigrams_monthly;

-- Insert all aggregated data in ONE transaction
INSERT INTO wikigrams_monthly
SELECT
    geo,
    DATE_TRUNC('month', date) as month,
    types,
    SUM(counts)::BIGINT AS counts
FROM wikigrams
GROUP BY geo, DATE_TRUNC('month', date), types;

-- Show summary
SELECT
    geo,
    COUNT(DISTINCT month) as months,
    COUNT(*) as total_rows,
    SUM(counts) as total_counts
FROM wikigrams_monthly
GROUP BY geo
ORDER BY geo;
EOF

echo "Monthly aggregation complete!"
