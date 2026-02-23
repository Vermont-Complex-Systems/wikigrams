#!/bin/bash
set -e  # Exit on error
source .venv/bin/activate

echo "Starting weekly aggregation..."

duckdb << 'EOF'
ATTACH 'ducklake:./metadata.ducklake' AS wikilake
(DATA_PATH '/netfiles/compethicslab/wikimedia');

USE wikilake;

-- Drop and recreate table to actually remove old parquet files
-- DELETE FROM only marks data as deleted but keeps files
DROP TABLE IF EXISTS wikigrams_weekly;

CREATE TABLE wikigrams_weekly (
    geo TEXT,
    week DATE,
    types TEXT,
    counts BIGINT,
    rank BIGINT
);

-- Set partitioning
ALTER TABLE wikigrams_weekly SET PARTITIONED BY (geo, week);

-- Insert all aggregated data in ONE transaction with ORDER BY
-- ORDER BY partition columns prevents partition switching during write
INSERT INTO wikigrams_weekly
WITH agg AS (
    SELECT
        geo,
        DATE_TRUNC('week', date) AS week,
        types,
        SUM(counts)::BIGINT AS counts
    FROM wikigrams
    GROUP BY geo, DATE_TRUNC('week', date), types
)
SELECT geo, week, types, counts,
    RANK() OVER (PARTITION BY geo, week ORDER BY counts DESC) AS rank
FROM agg
ORDER BY geo, week;

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