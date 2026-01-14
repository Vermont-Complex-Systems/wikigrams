#!/bin/bash
set -e
source .venv/bin/activate

echo "Starting incremental weekly aggregation..."

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

-- Find weeks that need updating (new data or changed data)
CREATE TEMP TABLE weeks_to_update AS
SELECT DISTINCT
    geo,
    DATE_TRUNC('week', date) as week
FROM wikigrams
WHERE NOT EXISTS (
    SELECT 1 FROM wikigrams_weekly w
    WHERE w.geo = wikigrams.geo
      AND w.week = DATE_TRUNC('week', wikigrams.date)
);

-- Delete old data for weeks being updated (in case of corrections)
DELETE FROM wikigrams_weekly
WHERE (geo, week) IN (SELECT geo, week FROM weeks_to_update);

-- Materialize aggregation for only the weeks being updated
CREATE TEMP TABLE weekly_agg AS
SELECT
    geo,
    DATE_TRUNC('week', date) as week,
    types,
    SUM(counts)::BIGINT AS counts
FROM wikigrams
WHERE (geo, DATE_TRUNC('week', date)) IN (
    SELECT geo, week FROM weeks_to_update
)
GROUP BY geo, DATE_TRUNC('week', date), types;

-- Insert new weekly aggregates
INSERT INTO wikigrams_weekly
SELECT * FROM weekly_agg;

-- Show what was updated
.print "Weeks updated:"
SELECT geo, week, COUNT(*) as rows_added
FROM weekly_agg
GROUP BY geo, week
ORDER BY geo, week;

-- Show summary
.print ""
.print "Total table summary:"
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

echo "Incremental weekly aggregation complete!"
