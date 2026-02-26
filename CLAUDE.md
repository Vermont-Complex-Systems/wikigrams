# BabyNames dataset

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This project processes US baby names data by state and year, transforming it into a format suitable for the Storywrangler API. The pipeline extracts data from government sources, transforms it into normalized tables with location entities, and prepares pre-computed n-grams for API submission.

**For detailed guidance on writing Storywrangler adapters/submitters, see [STORYWRANGLER_ADAPTERS.md](STORYWRANGLER_ADAPTERS.md).**

## Technology Stack

- **Build Tool**: `uv` for dependency management
- **Pipeline Orchestration**: Makefile
- **Python Version**: >=3.11 (specified in pyproject.toml)
- **CLI Tools**: `make` for pipeline execution
- **Data Storage**: DuckDB/Parquet files for efficient n-gram storage

## Setup and Development

### Environment Setup

Using uv (recommended):
```bash
uv sync
source .venv/bin/activate  # macOS/Linux
```

## Architecture

### Pipeline Structure

The project uses a Makefile-based pipeline with the following stages:

**Pipeline Stages:**
- `extract/` - Downloads US baby names data from government sources
  - `import.py` - Load into ducklake

- `transform/` - transform data
  - `partitioned_month.sh` - monthly hive partition

- `adapter/` - Storywrangler submission
  - `prepare.py` - Maps states to location entities, validates data conformance
  - `submit.py` - Submits dataset metadata to Storywrangler API

### Validation Architecture

The project follows a **separation of concerns** for validation:

**Dataset Side (prepare.py)** - Validates data conformance:
- `EntityValidator`: Validates entity IDs (e.g., wikidata:Q30 format)
- `EndpointValidator`: Validates data schema matches Storywrangler endpoint requirements (e.g., top-ngrams schema)
- Purpose: Ensures the **data itself** is correct before submission

**API Backend Side** - Validates registration conformance:
- Pydantic models validate metadata structure (entity_mapping, dimensions, sources)
- FastAPI returns validation errors with HTTP status codes
- Purpose: Ensures the **registration request** has the correct structure

**Pattern**:
```python
# prepare.py - validates DATA conformance
validator.validate_top_ngrams_schema(schema)  # ✓ Does my data fit the endpoint?
validator.validate(entity_id)                  # ✓ Is this a valid entity ID?

# submit.py - just sends metadata
dataset_metadata = {...}  # Construct metadata dict
response = requests.post(...)  # Send to API
# Let API validate and return errors

# API backend (FastAPI + Pydantic) - validates METADATA conformance
class DatasetMetadata(BaseModel):  # ✓ Is the registration request valid?
    dimensions: Dict[str, Dimension]
    entity_mapping: EntityMapping
```

This keeps concerns separated: **prepare.py ensures data quality**, **API ensures registration quality**.

### External Dependencies

- **Storywrangler SDK**: The project depends on a local version of the storywrangler-sdk package (path: `../storywrangler/packages/sdk`)
  - Configured in pyproject.toml under `[tool.uv.sources]`
  - Used by adapter scripts for entity validation and API interaction
  - The pipeline will run without it, but adapter scripts will fail

## DuckLake Usage Guide

### Overview

DuckLake is an ACID-compliant data lakehouse format providing time travel, schema evolution, and transactional guarantees for Parquet data lakes. Built on DuckDB with local file storage.

### Quick Start

```sql
INSTALL ducklake;
ATTACH 'ducklake:my_lake.ducklake' AS my_lake;
USE my_lake;
```

### Core Constraints

**No Primary Keys/Indexes**: DuckLake doesn't support primary keys, indexes, or unique constraints. Use:
- **Partitioning** on frequently-filtered columns: `ALTER TABLE sales SET PARTITIONED BY (region, year(order_date));`
- **File-level statistics** for automatic pruning
- **Query pattern-driven schema design**

### Essential Best Practices

#### Connection Management
- Always use `USE database_name` or qualify table names to avoid accidental operations on in-memory database
- Use relative paths for local storage: `ATTACH 'ducklake:./data/my_lake.ducklake' AS my_lake;`

#### Schema Design
- Plan schema carefully upfront - avoid frequent structural changes
- Use snake_case naming for consistency
- Specify meaningful defaults for new columns
- Design with type promotions in mind (int32 → int64)

#### Maintenance Strategy
- Use `CHECKPOINT` for automated maintenance
- Run `merge_adjacent_files()` for tables with frequent small inserts
- Configure retention: `expire_older_than` and periodic `cleanup_files()`

#### Performance
- Partition strategically on query filter columns
- Configure `target_file_size` (default 512MB) based on usage patterns
- Use `DATA_INLINING_ROW_LIMIT` for small, frequently-updated tables
- Set `parquet_compression = 'zstd'` for balanced speed/size

### Configuration Hierarchy

Settings: Table → Schema → Global → Default

```sql
CALL my_lake.set_option('parquet_compression', 'zstd');                    -- Global
CALL my_lake.set_option('parquet_compression', 'snappy', schema => 'logs'); -- Schema
```

### Common Pitfalls

1. **Schema Evolution**: Frequent structural changes hurt maintainability
2. **File Proliferation**: Monitor small files; use data inlining or batch inserts
3. **Snapshot Accumulation**: Implement retention policies
4. **Connection Leaks**: Always detach properly: `DETACH database_name`

### Cleaning Up Duplicate Imports

If you accidentally import data twice, DuckLake's time travel lets you fix it without restarting:

1. **Delete duplicate data**: `DELETE FROM babynames WHERE geo = 'location_name'`
2. **Re-import correctly**: Run the import again (duplicate prevention now built into loaders)
3. **List snapshots**: `SELECT * FROM babylake.snapshots()` to identify problematic snapshot IDs
4. **Expire bad snapshots**: `CALL ducklake_expire_snapshots('babylake', versions => [6, 7])`
5. **Clean up orphaned files**: `CALL ducklake_delete_orphaned_files('babylake', cleanup_all => true)`
6. **Verify cleanup**: Check `metadata.ducklake.files/` directory - old parquet files should be removed

Note: In some cases, parquet files may persist even after cleanup. This is safe - DuckDB only reads files referenced by active snapshots. If needed, you can manually remove unreferenced parquet files after verifying the correct data is intact.

### Additional DuckLake Usage References

For detailed usage patterns and advanced features, refer to:
- `/users/j/s/jstonge1/ducklake-web/docs/stable/duckdb/usage/` - Core usage patterns (connecting, time travel, schema evolution, etc.)
- `/users/j/s/jstonge1/ducklake-web/docs/stable/duckdb/maintenance/` - Maintenance operations and best practices
- `/users/j/s/jstonge1/ducklake-web/docs/stable/duckdb/advanced_features/` - Advanced features (partitioning, encryption, etc.)