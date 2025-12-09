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

-- Create table if it doesn't exist
CREATE TABLE IF NOT EXISTS wikigrams_weekly (
    geo TEXT,
    week DATE,
    types TEXT,
    counts BIGINT
);

-- Set partitioning (idempotent)
ALTER TABLE wikigrams_weekly SET PARTITIONED BY (geo, week);

-- Delete all existing data
DELETE FROM wikigrams_weekly;

-- Insert all aggregated data in ONE transaction
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
    SUM(counts) as total_counts
FROM wikigrams_weekly
GROUP BY geo
ORDER BY geo;
EOF

echo "Weekly aggregation complete!"
