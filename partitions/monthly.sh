#!/bin/bash
set -e  # Exit on error
source .venv/bin/activate

echo "Starting monthly aggregation..."

duckdb << 'EOF'
SET memory_limit = '90GB';
SET threads = 32;
SET enable_progress_bar=true;
SET enable_progress_bar_print=true;
SET progress_bar_time=2000;
SET temp_directory = '/gpfs1/home/j/s/jstonge1/wikigrams/.tmp/duckdb_spill';

-- Materialize aggregation in temp table first
CREATE TEMP TABLE monthly_agg AS
SELECT
    geo,
    DATE_TRUNC('month', date) AS month,
    types,
    SUM(counts)::BIGINT AS counts
FROM read_parquet('/netfiles/compethicslab/wikimedia/wikigrams/**/*.parquet', hive_partitioning=true)
GROUP BY geo, DATE_TRUNC('month', date), types;

-- Show summary
SELECT
    geo,
    COUNT(DISTINCT month) as months,
    COUNT(*) as total_rows,
    SUM(counts) as total_counts,
    MIN(month) as earliest_month,
    MAX(month) as latest_month
FROM monthly_agg
GROUP BY geo
ORDER BY geo;

-- Reduce threads for partitioned write
SET threads = 4;
SET partitioned_write_max_open_files = 500;

-- Write Hive-partitioned parquet (without rank)
COPY (
    SELECT geo, month, types, counts
    FROM monthly_agg
    ORDER BY geo, month
) TO '/netfiles/compethicslab/wikimedia/wikigrams_monthly'
(FORMAT PARQUET, PARTITION_BY (geo, month), OVERWRITE_OR_IGNORE);
EOF

echo "Monthly aggregation complete, computing ranks..."

# Separate DuckDB process for rank — stream directly to staging dir to avoid OOM
duckdb << 'EOF'
SET memory_limit = '90GB';
SET threads = 4;
SET preserve_insertion_order = false;
SET enable_progress_bar=true;
SET enable_progress_bar_print=true;
SET progress_bar_time=2000;
SET temp_directory = '/gpfs1/home/j/s/jstonge1/wikigrams/.tmp/duckdb_spill';
SET partitioned_write_max_open_files = 500;

-- Stream ranked result directly to staging (no temp table, no full materialization)
COPY (
    SELECT
        geo, month, types, counts,
        RANK() OVER (PARTITION BY geo, month ORDER BY counts DESC) AS rank
    FROM read_parquet('/netfiles/compethicslab/wikimedia/wikigrams_monthly/**/*.parquet', hive_partitioning=true)
) TO '/netfiles/compethicslab/wikimedia/wikigrams_monthly_staging'
(FORMAT PARQUET, PARTITION_BY (geo, month), OVERWRITE_OR_IGNORE);
EOF

# Swap staging into place
rm -rf /netfiles/compethicslab/wikimedia/wikigrams_monthly
mv /netfiles/compethicslab/wikimedia/wikigrams_monthly_staging /netfiles/compethicslab/wikimedia/wikigrams_monthly

echo "Monthly aggregation + rank complete!"
