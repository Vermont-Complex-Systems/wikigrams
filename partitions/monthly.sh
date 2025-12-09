#!/bin/bash
set -e  # Exit on error
source .venv/bin/activate

echo "Starting monthly aggregation..."

# Create table and delete old data in one transaction
duckdb << 'EOF'
ATTACH 'ducklake:./metadata.ducklake' AS wikilake
(DATA_PATH '/netfiles/compethicslab/wikimedia');

USE wikilake;

CREATE TABLE IF NOT EXISTS wikigrams_monthly (
    geo TEXT,
    month DATE,
    types TEXT,
    counts BIGINT
);

ALTER TABLE wikigrams_monthly SET PARTITIONED BY (geo, month);
DELETE FROM wikigrams_monthly;
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

INSERT INTO wikigrams_monthly
SELECT
    geo,
    DATE_TRUNC('month', date) as month,
    types,
    SUM(counts)::BIGINT AS counts
FROM wikigrams
WHERE geo = '$geo'
GROUP BY geo, DATE_TRUNC('month', date), types;
EOF

    echo "✓ $geo complete"
    echo ""
done

echo "Monthly aggregation complete!"
echo ""
echo "Verifying results..."

# Show summary
duckdb << 'EOF'
ATTACH 'ducklake:./metadata.ducklake' AS wikilake
(DATA_PATH '/netfiles/compethicslab/wikimedia');

USE wikilake;

SELECT
    geo,
    COUNT(DISTINCT month) as months,
    COUNT(*) as total_rows,
    SUM(counts) as total_counts
FROM wikigrams_monthly
GROUP BY geo
ORDER BY geo;
EOF
