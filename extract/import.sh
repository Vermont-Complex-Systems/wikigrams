#!/bin/bash
set -e
source .venv/bin/activate

echo "Starting daily data import..."

duckdb << 'EOF'
SET memory_limit = '90GB';
SET threads = 32;
SET enable_progress_bar=true;
SET enable_progress_bar_print=true;
SET progress_bar_time=2000;
SET partitioned_write_max_open_files = 2500;

-- Materialize CSV data in temp table first
CREATE TEMP TABLE csv_import AS
SELECT
    column0 AS geo,
    CAST(regexp_extract(filename, '(\d{4}-\d{2}-\d{2})_', 1) AS DATE) AS date,
    column1 AS types,
    CAST(count AS BIGINT) AS counts
FROM read_csv(
    '/gpfs1/home/m/v/mvarnold/wikipedia-parsing/data/1grams/*_wikipedia_1grams.tsv',
    delim='\t',
    header=true,
    filename=true
)
WHERE column0 IN ('United States', 'Canada', 'Australia', 'United Kingdom');

-- Show import summary
SELECT
    COUNT(DISTINCT date) as dates_imported,
    COUNT(*) as total_rows
FROM csv_import;

-- Write Hive-partitioned parquet (without rank)
COPY (
    SELECT geo, date, types, counts
    FROM csv_import
    ORDER BY geo, date
) TO '/netfiles/compethicslab/wikimedia/wikigrams'
(FORMAT PARQUET, PARTITION_BY (geo, date), OVERWRITE_OR_IGNORE);
EOF

echo "Import complete, computing ranks..."

# Separate DuckDB process for rank — stream directly to staging dir to avoid OOM
duckdb << 'EOF'
SET memory_limit = '90GB';
SET temp_directory = '/gpfs1/home/j/s/jstonge1/wikigrams/.tmp/duckdb_spill';
SET threads = 8;
SET preserve_insertion_order = false;
SET enable_progress_bar=true;
SET enable_progress_bar_print=true;
SET progress_bar_time=2000;
SET partitioned_write_max_open_files = 2500;

-- Stream ranked result directly to staging
COPY (
    SELECT
        geo, date, types, counts,
        RANK() OVER (PARTITION BY geo, date ORDER BY counts DESC) AS rank
    FROM read_parquet('/netfiles/compethicslab/wikimedia/wikigrams/**/*.parquet', hive_partitioning=true)
) TO '/netfiles/compethicslab/wikimedia/wikigrams_staging'
(FORMAT PARQUET, PARTITION_BY (geo, date), OVERWRITE_OR_IGNORE);
EOF

# Swap staging into place
rm -rf /netfiles/compethicslab/wikimedia/wikigrams
mv /netfiles/compethicslab/wikimedia/wikigrams_staging /netfiles/compethicslab/wikimedia/wikigrams

echo "Import + rank complete!"
