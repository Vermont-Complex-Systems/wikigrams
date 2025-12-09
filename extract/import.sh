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

-- Delete existing data that we're about to re-import
DELETE FROM wikigrams 
WHERE (geo, date) IN (
    SELECT 
        column0 AS geo,
        CAST(regexp_extract(filename, '(\d{4}-\d{2}-\d{2})_', 1) AS DATE) AS date
    FROM read_csv(
        '/gpfs1/home/m/v/mvarnold/wikipedia-parsing/data/1grams/*_wikipedia_1grams.tsv',
        delim='\t',
        header=true,
        filename=true
    )
    WHERE column0 IN ('United States', 'Canada', 'Australia', 'United Kingdom')
);

-- Insert data
INSERT INTO wikigrams (geo, date, types, counts)
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
EOF

echo "Import complete!"