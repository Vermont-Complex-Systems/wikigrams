"""
Submit wikigrams dataset registration to datalakes API.
"""

import requests
import os
from dotenv import load_dotenv
import duckdb
import csv
from pathlib import Path
from collections import defaultdict
import json

# Load environment variables
load_dotenv()

def get_source_raw():
    """Read source URLs from list_urls.csv and group by location.

    Returns dict mapping location names to their source URLs.
    Single URL locations return a string, multiple URLs return a list.
    """    
    return {
        'location': '/gpfs1/home/m/v/mvarnold/wikipedia-parsing/data/1grams/*_wikipedia_1grams.tsv'
    }

def get_table_schema(conn, table_name):
    """Extract schema from a table as a simple dict."""
    schema_result = conn.execute(f"DESCRIBE {table_name}").fetchall()
    return {row[0]: row[1] for row in schema_result}

def get_ducklake_table_metadata(conn):
    """Extract table metadata from ducklake for API registration.

    Returns:
        tuple: (tables_metadata, ducklake_info) where:
            - tables_metadata: dict mapping table names to file paths
            - ducklake_info: dict with ducklake name and data_path
    """

    # Get ducklake data path information
    ducklake_data_path = os.getenv("DATA_PATH")

    if Path(ducklake_data_path).exists() == False:
        raise RuntimeError("Could not retrieve ducklake metadata")

    # Get current file metadata for each table
    tables_metadata = {}

    tables = [x[0] for x in conn.execute("SHOW tables").fetchall()]

    assert 'adapter' in tables, "no adapter table"

    for table_name in tables:
        result = conn.execute(f"""
            SELECT df.path
            FROM __ducklake_metadata_wikilake.ducklake_data_file df
            JOIN __ducklake_metadata_wikilake.ducklake_table t ON df.table_id = t.table_id
            WHERE t.table_name = '{table_name}'
              AND df.end_snapshot IS NULL
        """).fetchall()

        if result:
            # Collect all file paths for this table
            tables_metadata[table_name] = [row[0] for row in result]

    return tables_metadata, ducklake_data_path

def register_wikigrams_datalake():
    """Register wikigrams dataset with the datalakes API."""

    # Get configuration from environment variables
    dataset_id = os.getenv("DATASET_ID")
    data_location = os.getenv("DATA_PATH")
    api_url = os.getenv("API_URL")

    conn = duckdb.connect()
    conn.execute(f"ATTACH 'ducklake:metadata.ducklake' AS wikilake (DATA_PATH '{data_location}');")
    conn.execute(f"USE wikilake;")

    # Get current table metadata from ducklake
    file_paths, ducklake_data_path = get_ducklake_table_metadata(conn)

    # Get schema from wikigrams table (for reference)
    schema = get_table_schema(conn, "wikigrams")

    # Get source URLs for validation
    geo_sources = get_source_raw()

    # Dataset metadata for registration
    dataset_metadata = {
        "dataset_id": dataset_id,
        "data_location": data_location,
        "data_format": "ducklake",
        "description": "Wikipedia n-grams by frequency, date, and location with entity mappings",
        "tables_metadata": file_paths,
        "ducklake_data_path": ducklake_data_path,

        # Schema (for reference when building queries)
        "data_schema": schema,

        # Entity mapping configuration
        "entity_mapping": {
            "table": "adapter",
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
            print(f"✅ {dataset_id} datalake registered successfully!")
            try:
                result = response.json()
                print(f"✅ Response: {result.get('message', 'Success')}")
            except:
                # JSON parsing failed, but registration succeeded
                print(f"✅ Response received (status {response.status_code})")
            return True
        else:
            print(f"❌ Registration failed: {response.status_code}")
            print(f"   Response headers: {dict(response.headers)}")
            print(f"   Response body: {response.text[:500]}")  # First 500 chars
            return False

    except requests.exceptions.ConnectionError:
        print(f"❌ Could not connect to {api_url}")
        print(f"   Is the FastAPI server running?")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {type(e).__name__}: {e}")
        return False


def main():
    """Run the submitter."""

    # Get default values from environment
    dataset_id = os.getenv("DATASET_ID")
    success = register_wikigrams_datalake()

    if success:
        print(f"\n🚀 {dataset_id} datalake is now available!")
    else:
        print(f"\n❌ Registration failed")


if __name__ == "__main__":
    main()