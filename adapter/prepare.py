"""
Wikigrams Adapter

Prepares wikigrams data for submission to Storywrangler API.

Type of submission: Pattern 2 - Location entities with pre-computed n-grams
Schema:
  - types: str (n-gram)
  - counts: int (frequency count)
  - geo: str (country)
  - date: date (observation date)
Primary key: countries using wikidata identifier
Dataset metadata:
"""

from pathlib import Path
from typing import Dict
from storywrangler.validation import EntityValidator, EndpointValidator
from pyprojroot import here
import duckdb
import os
from dotenv import load_dotenv

load_dotenv()

class WikigramsAdapter:

    def __init__(self):
        self.project_root = here()
        self.dataset_id = os.getenv("DATASET_ID")
        self.data_path = Path(os.getenv("DATA_PATH"))
        self.entity_validator = EntityValidator()
        self.endpoint_validator = EndpointValidator()
        self.ducklake_path = self.project_root / "metadata.ducklake"
    
    def get_entity_mappings(self) -> Dict[str, Dict]:
        """Map location local_ids to entity identifiers"""
        return {
            "United States": {
                "local_id": "United States",
                "entity_id": "wikidata:Q30",
                "entity_ids": ["iso:US", "local:wikigrams:united_states"],
                "entity_name": "United States",
            },
            "United Kingdom": {
                "local_id": "United Kingdom",
                "entity_id": "wikidata:Q145",
                "entity_ids": ["iso:GB", "local:wikigrams:united_kingdom"],
                "entity_name": "United Kingdom",
            },
            "Canada": {
                "local_id": "Canada",
                "entity_id": "wikidata:Q16",
                "entity_ids": ["iso:CA", "local:wikigrams:canada"],
                "entity_name": "Canada",
            },
            "Australia": {
                "local_id": "Australia",
                "entity_id": "wikidata:Q408",
                "entity_ids": ["iso:AU", "local:wikigrams:australia"],
                "entity_name": "Australia",
            },
        }
    
    def connect_ducklake(self) -> duckdb.DuckDBPyConnection:
        """Connect to the ducklake database"""
        if not self.ducklake_path.exists():
            raise FileNotFoundError(f"Ducklake file not found: {self.ducklake_path}")

        conn = duckdb.connect()
        conn.execute(f"ATTACH 'ducklake:metadata.ducklake' AS wikilake (DATA_PATH '{self.data_path}');")
        conn.execute("USE wikilake;")
        print(f"📊 Connected to ducklake: {self.ducklake_path}")
        print(f"📊 Data path: {self.data_path}")
        return conn

    def create_adapter_table(self, conn: duckdb.DuckDBPyConnection):
        """Create adapter table if it doesn't exist"""
        # Check if adapter table exists
        tables = [x[0] for x in conn.execute("SHOW TABLES").fetchall()]

        if 'adapter' not in tables:
            print("🔧 Creating adapter table...")
            conn.execute("""
                CREATE TABLE adapter (
                    local_id VARCHAR,
                    entity_id VARCHAR,
                    entity_name VARCHAR,
                    entity_ids VARCHAR[]
                )
            """)
        else:
            print("📊 Adapter table already exists")

    def sync_entity_mappings(self, conn: duckdb.DuckDBPyConnection):
        """Insert entity mappings for all locations in wikigrams table"""
        entity_mappings = self.get_entity_mappings()

        # Check if there are any new locations to map
        new_count = conn.execute("""
            SELECT COUNT(DISTINCT w.geo)
            FROM wikigrams w
            LEFT JOIN adapter a ON w.geo = a.local_id
            WHERE a.local_id IS NULL
        """).fetchone()[0]

        if new_count == 0:
            print("✓ All locations already mapped")
            return

        # Get new locations
        locations = conn.execute("""
            SELECT DISTINCT w.geo
            FROM wikigrams w
            LEFT JOIN adapter a ON w.geo = a.local_id
            WHERE a.local_id IS NULL
        """).fetchall()

        # Prepare rows for insertion
        rows = []
        for (location,) in locations:
            if location not in entity_mappings:
                raise ValueError(f"No entity mapping defined for '{location}'")

            mapping = entity_mappings[location]

            # Validate entity ID
            if not self.entity_validator.validate(mapping["entity_id"]):
                raise ValueError(f"Invalid entity_id: {mapping['entity_id']}")

            rows.append((mapping["local_id"], mapping["entity_id"],
                        mapping["entity_name"], mapping["entity_ids"]))

        # Insert new mappings
        conn.executemany(
            "INSERT INTO adapter (local_id, entity_id, entity_name, entity_ids) VALUES (?, ?, ?, ?)",
            rows
        )
        print(f"✓ Inserted {len(rows)} new mapping(s)")

    def validate_wikigrams_schema(self, conn: duckdb.DuckDBPyConnection):
        """Validate that wikigrams data conforms to top-ngrams endpoint schema"""
        print("🔍 Validating wikigrams schema against Storywrangler standards...")

        # Get schema information from wikigrams table
        schema_result = conn.execute("DESCRIBE wikigrams").fetchall()
        columns = {row[0]: {'type': row[1]} for row in schema_result}

        schema = {'columns': columns}

        # Validate against top-ngrams endpoint requirements
        validation = self.endpoint_validator.validate_top_ngrams_schema(schema)

        if not validation['valid']:
            print("❌ Schema validation failed:")
            for error in validation['errors']:
                print(f"   - {error}")
            raise ValueError("Wikigrams schema does not conform to Storywrangler top-ngrams endpoint requirements")

        print("✅ Schema validation passed - wikigrams data conforms to top-ngrams endpoint")
        return validation['column_mapping']

    def prepare(self):
        """Prepare dataset metadata and update DuckDB with entity mappings

        Steps:
        1. Validate wikigrams schema conforms to top-ngrams endpoint
        2. Create adapter table if it doesn't exist
        3. Insert new entity mappings for locations in the database
        """
        print("🔧 Preparing Wikigrams dataset\n")

        # Connect to DuckDB
        conn = self.connect_ducklake()

        try:
            # (i) Validate wikigrams schema against Storywrangler standards
            self.validate_wikigrams_schema(conn)

            # (ii) Create adapter table if it doesn't exist
            self.create_adapter_table(conn)

            # (iii) Insert new entity mappings (existing ones are skipped)
            self.sync_entity_mappings(conn)

            print(f"\n✅ Adapter complete")

        finally:
            conn.close()
    


def main():
    """Run the adapter"""

    # Initialize adapter (reads from .env)
    adapter = WikigramsAdapter()

    print(f"📁 Configuration:")
    print(f"  Dataset ID: {adapter.dataset_id}")
    print(f"  Data path: {adapter.data_path}")
    print(f"  DuckDB: {adapter.ducklake_path}")
    print()

    # Check DuckDB exists
    if not adapter.ducklake_path.exists():
        print(f"❌ DuckDB not found: {adapter.ducklake_path}")
        print("   Run the extract pipeline first!")
        return

    adapter.prepare()


if __name__ == "__main__":
    main()