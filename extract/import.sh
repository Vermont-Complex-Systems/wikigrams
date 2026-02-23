#!/bin/bash
set -e
source .venv/bin/activate

echo "Starting daily data import..."

duckdb << 'EOF'
ATTACH 'ducklake:./metadata.ducklake' AS wikilake 
(DATA_PATH '/netfiles/compethicslab/wikimedia');

USE wikilake;

-- Drop and recreate table to actually remove old parquet files
-- DELETE FROM only marks data as deleted but keeps files
DROP TABLE IF EXISTS wikigrams;

CREATE TABLE wikigrams (
    geo TEXT,
    date DATE,
    types TEXT,
    counts BIGINT,
    rank BIGINT
);

-- Set partitioning
ALTER TABLE wikigrams SET PARTITIONED BY (geo, date);

-- Insert data with ORDER BY partition columns
-- This prevents DuckDB from switching between partitions during write
INSERT INTO wikigrams
WITH raw AS (
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
    WHERE column0 IN ('United States', 'Canada', 'Australia', 'United Kingdom')
)
SELECT geo, date, types, counts,
    ROW_NUMBER() OVER (PARTITION BY geo, date ORDER BY counts DESC) AS rank
FROM raw
ORDER BY geo, date;
EOF

echo "Import complete!"