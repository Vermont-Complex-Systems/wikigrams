#!/bin/bash
set -e
source .venv/bin/activate

echo "=== Running DuckLake maintenance (CHECKPOINT) ==="
echo "This will:"
echo "  - Flush inlined data to parquet files"
echo "  - Expire old snapshots"
echo "  - Merge adjacent files"
echo "  - Cleanup orphaned files"
echo ""

duckdb << 'EOF'
SET memory_limit = '30GB';
SET threads = 12;
SET temp_directory = '/netfiles/compethicslab/wikimedia/duckdb_tmp';

ATTACH 'ducklake:./metadata.ducklake' AS wikilake
(DATA_PATH '/netfiles/compethicslab/wikimedia');

USE wikilake;

-- Run full maintenance checkpoint
-- This flushes inlined data and performs other maintenance tasks
CHECKPOINT;

-- Show summary
.print ""
.print "Checkpoint complete. Table summaries:"
SELECT
    'wikigrams' as table_name,
    COUNT(*) as total_rows,
    COUNT(DISTINCT geo) as geos,
    COUNT(DISTINCT date) as dates,
    MIN(date) as earliest_date,
    MAX(date) as latest_date
FROM wikigrams;

SELECT
    'wikigrams_weekly' as table_name,
    COUNT(*) as total_rows,
    COUNT(DISTINCT geo) as geos,
    COUNT(DISTINCT week) as weeks
FROM wikigrams_weekly;

SELECT
    'wikigrams_monthly' as table_name,
    COUNT(*) as total_rows,
    COUNT(DISTINCT geo) as geos,
    COUNT(DISTINCT month) as months
FROM wikigrams_monthly;

EOF

echo ""
echo "=== Maintenance complete ==="
