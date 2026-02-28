#!/bin/bash
set -e
source .venv/bin/activate

# Single-pass extract: read TSVs, compute RANK, write sorted parquet.
# Processes day by day (~12s/day, ~100min total for 511 days).
# OVERWRITE_OR_IGNORE preserves existing partitions, enabling resume on failure.

TSV_DIR="/gpfs1/home/m/v/mvarnold/wikipedia-parsing/data/1grams"
OUTPUT="/netfiles/compethicslab/wikimedia/wikigrams"

# Usage: bash extract/extract.sh [YYYY-MM-DD | YYYY-MM]
#   No args: processes only dates missing from output (safe to re-run)
if [ -n "$1" ]; then
  dates="$1"
else
  available=$(ls "$TSV_DIR"/*_wikipedia_1grams.tsv | grep -oP '\d{4}-\d{2}-\d{2}' | sort -u)
  existing=$(ls -d "$OUTPUT"/geo=*/date=*/ 2>/dev/null | grep -oP '\d{4}-\d{2}-\d{2}' | sort -u || true)
  dates=$(comm -23 <(echo "$available") <(echo "$existing"))
  if [ -z "$dates" ]; then
    echo "No new dates to process."
    exit 0
  fi
  echo "$(echo "$dates" | wc -l) new date(s) to process."
fi

for d in $dates; do
  echo "Processing $d..."
  duckdb << EOF
SET memory_limit = '80GB';
SET temp_directory = '/gpfs1/home/j/s/jstonge1/wikigrams/.tmp/duckdb_spill';
SET threads = 32;
SET enable_progress_bar=true;
SET enable_progress_bar_print=true;
SET progress_bar_time=2000;

COPY (
    WITH filtered AS (
        SELECT
            CAST(column0 AS VARCHAR) AS geo,
            CAST(regexp_extract(filename, '(\d{4}-\d{2}-\d{2})_', 1) AS DATE) AS date,
            CAST(column1 AS VARCHAR) AS types,
            CAST(count AS BIGINT) AS counts,
            CAST(top_articles AS VARCHAR) AS top_articles
        FROM read_csv(
            '${TSV_DIR}/${d}*_wikipedia_1grams.tsv',
            delim='\t',
            header=true,
            filename=true,
            quote=''
        )
        WHERE column0 IN ('United States', 'Canada', 'Australia', 'United Kingdom')
    )
    SELECT
        geo, date, types, counts,
        RANK() OVER (PARTITION BY geo, date ORDER BY counts DESC) AS rank,
        top_articles
    FROM filtered
    ORDER BY types
) TO '${OUTPUT}'
(FORMAT PARQUET, PARTITION_BY (geo, date), OVERWRITE_OR_IGNORE);
EOF
  echo "$d done."
done

echo "All days complete!"
