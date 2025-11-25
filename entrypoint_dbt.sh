#!/bin/bash

set -e
cd /usr/app/dbt

if [ -f "packages.yml" ]; then
    echo "Installing dbt packages..."
    dbt deps
else
    echo "No packages.yml found, skipping dbt deps"
fi

echo "Generating dbt documentation..."
dbt docs generate || echo "Failed to generate docs (this is ok if no models exist yet)"
dbt docs serve --host 0.0.0.0 --port 8001 &
tail -f /dev/null