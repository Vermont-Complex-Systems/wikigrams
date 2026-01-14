#!/bin/bash
set -e
source .venv/bin/activate

echo "Starting daily data import..."

duckdb << 'EOF'

ATTACH 'ducklake:./metadata.ducklake' AS wikilake 
(DATA_PATH '/netfiles/compethicslab/wikimedia');

USE wikilake;

-- Create table if it doesn't exist
CREATE TABLE IF NOT EXISTS wikigrams (
    geo TEXT,
    date DATE,
    types TEXT,
    counts BIGINT
);

-- Set partitioning (idempotent)
ALTER TABLE wikigrams SET PARTITIONED BY (geo, date);

-- Enable data inlining for small incremental updates
-- Inserts with fewer rows than this limit will be stored in metadata instead of creating new parquet files
-- Periodic CHECKPOINT will flush inlined data to consolidated parquet files
CALL wikilake.set_option('data_inlining_row_limit', 100000, table_name => 'wikigrams');

-- Materialize CSV data in temp table first
-- This prevents parallel reads from creating many small files per partition
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

-- Insert from materialized temp table
-- For incremental updates: If rows < data_inlining_row_limit, data goes to metadata
-- For bulk imports: If rows > limit, creates consolidated parquet files per partition
INSERT INTO wikigrams (geo, date, types, counts)
SELECT * FROM csv_import;

-- Show import summary
SELECT
    COUNT(DISTINCT date) as dates_imported,
    COUNT(*) as total_rows
FROM csv_import;
EOF

echo "Import complete!"