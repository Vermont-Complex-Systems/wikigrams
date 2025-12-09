"""Main import orchestrator - loads data from all locations into DuckDB"""
import duckdb

#!TODO: something is wrong with the parsing in the Makefile. I had to run it manually in the CLI. 
def main():
    """Load data for a specific location"""

    conn = duckdb.connect()
    try:
        conn.execute("""ATTACH 'ducklake:metadata.ducklake"' AS wikilake
                (DATA_PATH '/netfiles/compethicslab/wikimedia');""")
        conn.execute("USE wikilake;")

        # Create table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS wikigrams (
                geo TEXT,
                date DATE,
                types TEXT,
                counts BIGINT
            );
        """)

        # Partition it
        conn.execute("ALTER TABLE wikigrams SET PARTITIONED BY (geo, date);")

        # Insert data
        conn.execute("""
            INSERT INTO wikigrams (geo, date, types, counts)
            SELECT
                column0 AS geo,
                CAST(
                    regexp_extract(filename, '(\d{4}-\d{2}-\d{2})_', 1)
                    AS DATE
                ) AS date,
                column1 AS types,
                CAST(count AS BIGINT) AS counts
            FROM read_csv(
                '/gpfs1/home/m/v/mvarnold/wikipedia-parsing/data/1grams/*_wikipedia_1grams.tsv',
                delim='\t',
                header=true,
                filename=true
            )
            WHERE geo IN ('United States', 'Canada', 'Australia', 'United Kingdom');
        """)

    finally:
        conn.close()


if __name__ == '__main__':
    main()