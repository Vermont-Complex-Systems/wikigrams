#!/bin/bash
set -e
source .venv/bin/activate

echo "=== Merging small files in wikigrams_weekly ==="
echo "Before merge:"

duckdb << 'EOF'
SET memory_limit = '30GB';
SET threads = 12;
SET temp_directory = '/netfiles/compethicslab/wikimedia/duckdb_tmp';

ATTACH 'ducklake:./metadata.ducklake' AS wikilake
(DATA_PATH '/netfiles/compethicslab/wikimedia');

USE wikilake;

-- Show file count before merge (sample partition)
.print "Files in geo=United States/week=2025-03-03 partition:"
SELECT COUNT(*) as parquet_file_count
FROM glob('/netfiles/compethicslab/wikimedia/main/wikigrams_weekly/geo=United%20States/week=2025-03-03/*.parquet');

-- Set compaction target to specific table
CALL wikilake.set_option('compaction_table', 'wikigrams_weekly');

-- Run CHECKPOINT which handles partitioned tables correctly
CHECKPOINT;

-- Show file count after merge (same sample partition)
.print ""
.print "After merge - Files in geo=United States/week=2025-03-03 partition:"
SELECT COUNT(*) as parquet_file_count
FROM glob('/netfiles/compethicslab/wikimedia/main/wikigrams_weekly/geo=United%20States/week=2025-03-03/*.parquet');

EOF

echo "=== File merge complete ==="