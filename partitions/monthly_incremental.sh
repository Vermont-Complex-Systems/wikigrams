#!/bin/bash
set -e
source .venv/bin/activate

echo "Starting incremental monthly aggregation..."

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

-- Find months that need updating (new data or changed data)
CREATE TEMP TABLE months_to_update AS
SELECT DISTINCT
    geo,
    DATE_TRUNC('month', date) as month
FROM wikigrams
WHERE NOT EXISTS (
    SELECT 1 FROM wikigrams_monthly m
    WHERE m.geo = wikigrams.geo
      AND m.month = DATE_TRUNC('month', wikigrams.date)
);

-- Delete old data for months being updated (in case of corrections)
DELETE FROM wikigrams_monthly
WHERE (geo, month) IN (SELECT geo, month FROM months_to_update);

-- Materialize aggregation for only the months being updated
CREATE TEMP TABLE monthly_agg AS
SELECT
    geo,
    DATE_TRUNC('month', date) as month,
    types,
    SUM(counts)::BIGINT AS counts
FROM wikigrams
WHERE (geo, DATE_TRUNC('month', date)) IN (
    SELECT geo, month FROM months_to_update
)
GROUP BY geo, DATE_TRUNC('month', date), types;

-- Insert new monthly aggregates
INSERT INTO wikigrams_monthly
SELECT * FROM monthly_agg;

-- Show what was updated
.print "Months updated:"
SELECT geo, month, COUNT(*) as rows_added
FROM monthly_agg
GROUP BY geo, month
ORDER BY geo, month;

-- Show summary
.print ""
.print "Total table summary:"
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

echo "Incremental monthly aggregation complete!"
