#!/bin/bash
set -e  # Exit on error
source .venv/bin/activate

echo "Starting weekly aggregation..."

# Create table and delete old data in one transaction
duckdb << 'EOF'
ATTACH 'ducklake:./metadata.ducklake' AS wikilake
(DATA_PATH '/netfiles/compethicslab/wikimedia');

USE wikilake;

CREATE TABLE IF NOT EXISTS wikigrams_weekly (
    geo TEXT,
    week DATE,
    types TEXT,
    counts BIGINT
);

ALTER TABLE wikigrams_weekly SET PARTITIONED BY (geo, week);
DELETE FROM wikigrams_weekly;
EOF

# Process each geo separately to reduce memory pressure
for geo in "United States" "United Kingdom" "Canada" "Australia"; do
    echo "Processing $geo..."

    duckdb << EOF
SET memory_limit = '30GB';
SET threads = 12;
SET temp_directory = '/netfiles/compethicslab/wikimedia/duckdb_tmp';

ATTACH 'ducklake:./metadata.ducklake' AS wikilake
(DATA_PATH '/netfiles/compethicslab/wikimedia');

USE wikilake;

INSERT INTO wikigrams_weekly
SELECT
    geo,
    DATE_TRUNC('week', date) as week,
    types,
    SUM(counts)::BIGINT AS counts
FROM wikigrams
WHERE geo = '$geo'
GROUP BY geo, DATE_TRUNC('week', date), types;
EOF

    echo "✓ $geo complete"
    echo ""
done

echo "Weekly aggregation complete!"
echo ""
echo "Verifying results..."

# Show summary
duckdb << 'EOF'
ATTACH 'ducklake:./metadata.ducklake' AS wikilake
(DATA_PATH '/netfiles/compethicslab/wikimedia');

USE wikilake;

SELECT
    geo,
    COUNT(DISTINCT week) as weeks,
    COUNT(*) as total_rows,
    SUM(counts) as total_counts
FROM wikigrams_weekly
GROUP BY geo
ORDER BY geo;
EOF
