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
import yaml
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
        self.entities_path = self.project_root / "adapter" / "entities.yaml"
        self.adapter_parquet = self.data_path / "adapter" / "adapter.parquet"

    def get_entity_mappings(self) -> Dict[str, Dict]:
        """Load entity mappings from entities.yaml"""
        with open(self.entities_path) as f:
            mappings = yaml.safe_load(f)
        for local_id, mapping in mappings.items():
            mapping["local_id"] = local_id
        return mappings

    def validate_entities(self):
        """Validate all entity mappings against Storywrangler standards"""
        mappings = self.get_entity_mappings()

        for local_id, mapping in mappings.items():
            if not self.entity_validator.validate(mapping["entity_id"]):
                raise ValueError(f"Invalid entity_id for '{local_id}': {mapping['entity_id']}")

        print(f"Entity validation passed ({len(mappings)} mappings)")
        return mappings

    def validate_geos(self, conn: duckdb.DuckDBPyConnection):
        """Check that all geos in the data have entity mappings"""
        mappings = self.get_entity_mappings()

        parquet_path = self.data_path / "wikigrams" / "**" / "*.parquet"
        geos = [row[0] for row in conn.execute(f"""
            SELECT DISTINCT geo
            FROM read_parquet('{parquet_path}', hive_partitioning=true)
        """).fetchall()]

        unmapped = [g for g in geos if g not in mappings]
        if unmapped:
            raise ValueError(f"No entity mapping for geos: {unmapped}")

        print(f"All {len(geos)} geos have entity mappings")

    def validate_wikigrams_schema(self, conn: duckdb.DuckDBPyConnection):
        """Validate that wikigrams data conforms to top-ngrams endpoint schema"""
        print("Validating wikigrams schema against Storywrangler standards...")

        parquet_path = self.data_path / "wikigrams" / "**" / "*.parquet"
        schema_result = conn.execute(f"""
            DESCRIBE SELECT * FROM read_parquet('{parquet_path}', hive_partitioning=true)
        """).fetchall()
        columns = {row[0]: {'type': row[1]} for row in schema_result}

        schema = {'columns': columns}

        validation = self.endpoint_validator.validate_top_ngrams_schema(schema)

        if not validation['valid']:
            print("Schema validation failed:")
            for error in validation['errors']:
                print(f"   - {error}")
            raise ValueError("Wikigrams schema does not conform to Storywrangler top-ngrams endpoint requirements")

        print("Schema validation passed")
        return validation['column_mapping']

    def write_adapter_parquet(self, conn: duckdb.DuckDBPyConnection):
        """Write entity mappings as a parquet file to the shared data path"""
        mappings = self.get_entity_mappings()

        rows = [
            (m["local_id"], m["entity_id"], m["entity_name"], m["entity_ids"])
            for m in mappings.values()
        ]

        conn.execute("""
            CREATE TEMP TABLE adapter (
                local_id VARCHAR,
                entity_id VARCHAR,
                entity_name VARCHAR,
                entity_ids VARCHAR[]
            )
        """)
        conn.executemany(
            "INSERT INTO adapter VALUES (?, ?, ?, ?)", rows
        )

        self.adapter_parquet.parent.mkdir(parents=True, exist_ok=True)
        conn.execute(f"""
            COPY adapter TO '{self.adapter_parquet}' (FORMAT PARQUET)
        """)
        print(f"Wrote adapter parquet to {self.adapter_parquet}")

    def prepare(self):
        """Prepare and validate dataset

        Steps:
        1. Validate entity mappings in entities.yaml
        2. Validate wikigrams schema conforms to top-ngrams endpoint
        3. Check all geos in data have entity mappings
        4. Write entity mappings as parquet to shared data path
        """
        print("Preparing Wikigrams dataset\n")

        conn = duckdb.connect()

        try:
            self.validate_entities()
            self.validate_wikigrams_schema(conn)
            self.validate_geos(conn)
            self.write_adapter_parquet(conn)
            print(f"\nAdapter complete")
        finally:
            conn.close()


def main():
    """Run the adapter"""

    adapter = WikigramsAdapter()

    print(f"Configuration:")
    print(f"  Dataset ID: {adapter.dataset_id}")
    print(f"  Data path: {adapter.data_path}")
    print(f"  Entities: {adapter.entities_path}")
    print(f"  Adapter output: {adapter.adapter_parquet}")
    print()

    wikigrams_dir = adapter.data_path / "wikigrams"
    if not wikigrams_dir.exists():
        print(f"Wikigrams data not found: {wikigrams_dir}")
        print("   Run the extract pipeline first!")
        return

    adapter.prepare()


if __name__ == "__main__":
    main()
