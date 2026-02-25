"""
Submit wikigrams dataset registration to datalakes API.
"""

import requests
import os
from dotenv import load_dotenv
import duckdb
from pathlib import Path

# Load environment variables
load_dotenv()

# Table directories to register
TABLE_DIRS = [
    "wikigrams",
    "wikigrams_weekly",
    "wikigrams_monthly",
]

def get_source_raw():
    """Read source URLs from list_urls.csv and group by location.

    Returns dict mapping location names to their source URLs.
    Single URL locations return a string, multiple URLs return a list.
    """
    return {
        'location': '/gpfs1/home/m/v/mvarnold/wikipedia-parsing/data/1grams/*_wikipedia_1grams.tsv'
    }

def get_table_schema(data_path, table_dir):
    """Extract schema from a Hive-partitioned parquet table."""
    parquet_path = Path(data_path) / table_dir / "**" / "*.parquet"
    conn = duckdb.connect()
    schema_result = conn.execute(f"""
        DESCRIBE SELECT * FROM read_parquet('{parquet_path}', hive_partitioning=true)
    """).fetchall()
    conn.close()
    return {row[0]: row[1] for row in schema_result}

def get_table_metadata(data_path):
    """Discover parquet files for each table by globbing the data directory.

    Returns dict mapping table names to lists of parquet file paths.
    """
    data_root = Path(data_path)

    if not data_root.exists():
        raise RuntimeError(f"Data path does not exist: {data_path}")

    tables_metadata = {}
    for table_dir in TABLE_DIRS:
        table_path = data_root / table_dir
        if table_path.exists():
            files = sorted(str(f) for f in table_path.rglob("*.parquet"))
            if files:
                tables_metadata[table_dir] = files

    return tables_metadata

def register_wikigrams_datalake():
    """Register wikigrams dataset with the datalakes API."""

    # Get configuration from environment variables
    dataset_id = os.getenv("DATASET_ID")
    data_location = os.getenv("DATA_PATH")
    api_url = os.getenv("API_URL")

    # Verify adapter parquet exists
    adapter_path = Path(data_location) / "adapter" / "adapter.parquet"
    assert adapter_path.exists(), f"adapter parquet not found at {adapter_path} — run 'make prepare' first"

    # Get current table metadata by globbing parquet files
    file_paths = get_table_metadata(data_location)

    # Get schema from wikigrams table (for reference)
    schema = get_table_schema(data_location, "wikigrams")

    # Get source URLs for validation
    geo_sources = get_source_raw()

    # Dataset metadata for registration
    dataset_metadata = {
        "dataset_id": dataset_id,
        "data_location": data_location,
        "data_format": "parquet_hive",
        "description": "Wikipedia n-grams by frequency, date, and location with entity mappings",
        "tables_metadata": file_paths,

        # Schema (for reference when building queries)
        "data_schema": schema,

        # Entity mapping configuration
        "entity_mapping": {
            "table": "adapter",
            "path": str(adapter_path),
            "local_id_column": "local_id",
            "entity_id_column": "entity_id"
        },

        # Source URLs for validation
        "sources": {
            "geo": geo_sources
        }
    }

    try:
        # Register the datalake
        jwt_token = os.getenv("JWT_TOKEN")

        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {jwt_token}'
        }

        response = requests.post(
            f"{api_url}/admin/datalakes/",
            json=dataset_metadata,
            headers=headers,
        )

        if response.status_code in [200, 201]:
            print(f"{dataset_id} datalake registered successfully!")
            try:
                result = response.json()
                print(f"Response: {result.get('message', 'Success')}")
            except:
                # JSON parsing failed, but registration succeeded
                print(f"Response received (status {response.status_code})")
            return True
        else:
            print(f"Registration failed: {response.status_code}")
            print(f"   Response headers: {dict(response.headers)}")
            print(f"   Response body: {response.text[:500]}")  # First 500 chars
            return False

    except requests.exceptions.ConnectionError:
        print(f"Could not connect to {api_url}")
        print(f"   Is the FastAPI server running?")
        return False
    except Exception as e:
        print(f"Unexpected error: {type(e).__name__}: {e}")
        return False


def main():
    """Run the submitter."""

    # Get default values from environment
    dataset_id = os.getenv("DATASET_ID")
    success = register_wikigrams_datalake()

    if success:
        print(f"\n{dataset_id} datalake is now available!")
    else:
        print(f"\nRegistration failed")


if __name__ == "__main__":
    main()
