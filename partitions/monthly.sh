#!/bin/bash
set -e  # Exit on error
source .venv/bin/activate

echo "Starting monthly aggregation..."

duckdb << 'EOF'
ATTACH 'ducklake:./metadata.ducklake' AS wikilake
(DATA_PATH '/netfiles/compethicslab/wikimedia');

USE wikilake;

-- Drop and recreate table to actually remove old parquet files
-- DELETE FROM only marks data as deleted but keeps files
DROP TABLE IF EXISTS wikigrams_monthly;

CREATE TABLE wikigrams_monthly (
    geo TEXT,
    month DATE,
    types TEXT,
    counts BIGINT,
    rank BIGINT
);

-- Set partitioning
ALTER TABLE wikigrams_monthly SET PARTITIONED BY (geo, month);

-- Insert all aggregated data in ONE transaction with ORDER BY
-- ORDER BY partition columns prevents partition switching during write
INSERT INTO wikigrams_monthly
WITH agg AS (
    SELECT
        geo,
        DATE_TRUNC('month', date) AS month,
        types,
        SUM(counts)::BIGINT AS counts
    FROM wikigrams
    GROUP BY geo, DATE_TRUNC('month', date), types
)
SELECT geo, month, types, counts,
    RANK() OVER (PARTITION BY geo, month ORDER BY counts DESC) AS rank
FROM agg
ORDER BY geo, month;

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
