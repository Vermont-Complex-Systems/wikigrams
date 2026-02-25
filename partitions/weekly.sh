#!/bin/bash
set -e  # Exit on error
source .venv/bin/activate

echo "Starting weekly aggregation..."

duckdb << 'EOF'
SET memory_limit = '90GB';
SET threads = 32;
SET enable_progress_bar=true;
SET enable_progress_bar_print=true;
SET progress_bar_time=2000;
SET temp_directory = '/gpfs1/home/j/s/jstonge1/wikigrams/.tmp/duckdb_spill';

-- Materialize aggregation in temp table first
CREATE TEMP TABLE weekly_agg AS
SELECT
    geo,
    DATE_TRUNC('week', date) AS week,
    types,
    SUM(counts)::BIGINT AS counts
FROM read_parquet('/netfiles/compethicslab/wikimedia/wikigrams/**/*.parquet', hive_partitioning=true)
GROUP BY geo, DATE_TRUNC('week', date), types;

-- Show summary
SELECT
    geo,
    COUNT(DISTINCT week) as weeks,
    COUNT(*) as total_rows,
    SUM(counts) as total_counts,
    MIN(week) as earliest_week,
    MAX(week) as latest_week
FROM weekly_agg
GROUP BY geo
ORDER BY geo;

-- Reduce threads for partitioned write
SET threads = 4;
SET partitioned_write_max_open_files = 1000;

-- Write Hive-partitioned parquet (without rank)
COPY (
    SELECT geo, week, types, counts
    FROM weekly_agg
    ORDER BY geo, week
) TO '/netfiles/compethicslab/wikimedia/wikigrams_weekly'
(FORMAT PARQUET, PARTITION_BY (geo, week), OVERWRITE_OR_IGNORE);
EOF

echo "Weekly aggregation complete, computing ranks..."

# Separate DuckDB process for rank — stream directly to staging dir to avoid OOM
duckdb << 'EOF'
SET memory_limit = '90GB';
SET threads = 4;
SET preserve_insertion_order = false;
SET enable_progress_bar=true;
SET enable_progress_bar_print=true;
SET progress_bar_time=2000;
SET temp_directory = '/gpfs1/home/j/s/jstonge1/wikigrams/.tmp/duckdb_spill';
SET partitioned_write_max_open_files = 1000;

-- Stream ranked result directly to staging (no temp table, no full materialization)
COPY (
    SELECT
        geo, week, types, counts,
        RANK() OVER (PARTITION BY geo, week ORDER BY counts DESC) AS rank
    FROM read_parquet('/netfiles/compethicslab/wikimedia/wikigrams_weekly/**/*.parquet', hive_partitioning=true)
) TO '/netfiles/compethicslab/wikimedia/wikigrams_weekly_staging'
(FORMAT PARQUET, PARTITION_BY (geo, week), OVERWRITE_OR_IGNORE);
EOF

# Swap staging into place
rm -rf /netfiles/compethicslab/wikimedia/wikigrams_weekly
mv /netfiles/compethicslab/wikimedia/wikigrams_weekly_staging /netfiles/compethicslab/wikimedia/wikigrams_weekly

echo "Weekly aggregation + rank complete!"
