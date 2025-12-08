"""Main import orchestrator - loads data from all locations into DuckDB"""
import duckdb
import argparse

import os
from dotenv import load_dotenv
import duckdb

# Load environment variables
load_dotenv()
from pyprojroot import here

def main():
    """Load data for a specific location"""

    conn = duckdb.connect()
    try:
        conn.execute(f"""
            ATTACH 'ducklake:metadata.ducklake"' AS wikilake
                (DATA_PATH '/netfiles/compethicslab/wikimedia_temp');
        """)

        conn.execute(f"USE wikilake;")

        # Create table
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS wikigrams (
                geo TEXT,
                date DATE,
                types TEXT,
                counts BIGINT
            );
        """)

        # Partition it
        conn.execute(f"""
            ALTER TABLE wikigrams
                SET PARTITIONED BY (geo, date);
        """)

        # Insert data
        conn.execute(f"""
            INSERT INTO wikigrams (geo, date, types, counts)
            SELECT
                column0 AS geo,
                CAST(
                    regexp_extract(filename, '(\\d{4}-\\d{2}-\\d{2})', 1)
                    AS DATE
                ) AS date,
                column1 AS types,
                CAST(count AS BIGINT) AS counts
            FROM read_csv(
                '/gpfs1/home/m/v/mvarnold/wikipedia-parsing/data/1grams/*_wikipedia_1grams.tsv',
                delim='\t',
                header=true,
                filename=true
            );
        """)

    finally:
        conn.close()


if __name__ == '__main__':
    main()