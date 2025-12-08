"""Main import orchestrator - loads data from all locations into DuckDB"""
import duckdb
import argparse

import os
from dotenv import load_dotenv
import duckdb

# Load environment variables
load_dotenv()
from pyprojroot import here

PROJECT_ROOT = here()
LAKE_NAME = os.getenv('LAKE_NAME')
PROJECT_NAME = os.getenv('DATASET_ID')
DATA_PATH =  os.getenv('DATA_PATH')

def main():
    """Load data for a specific location"""

    conn = duckdb.connect()
    try:
        conn.execute(f"""
            ATTACH 'ducklake:{PROJECT_ROOT / "metadata.ducklake"}' AS {LAKE_NAME}
                (DATA_PATH '{DATA_PATH}');
        """)

        conn.execute(f"USE {LAKE_NAME};")
        
        # Create table
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {PROJECT_NAME} (
                geo TEXT,
                date DATE,
                types TEXT,
                counts BIGINT
            );
        """)
        
        # Partition it
        conn.execute(f"""
            ALTER TABLE {PROJECT_NAME}
                SET PARTITIONED BY (geo, date);
        """)

        # Insert data
        conn.execute(f"""
            INSERT INTO {PROJECT_NAME} (geo, date, types, counts)
            SELECT
                column0 AS geo,
                CAST(
                    regexp_extract(filename, '(\\d{4}-\\d{2}-\\d{2})_', 1)
                    AS DATE
                ) AS date,
                column1 AS types,
                CAST(count AS BIGINT) AS counts
            FROM read_parquet(
                '/gpfs1/home/m/v/mvarnold/wikipedia-parsing/data/1grams/*_wikipedia_1grams.tsv',
                delim='\t',
                header=true,
                filename=true
            );
        """)

    finally:
        conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Import baby names data into DuckDB")
    parser.add_argument('location',
                       help="Location to import (e.g., 'united_states', 'quebec')")
    args = parser.parse_args()
    main(args.location)