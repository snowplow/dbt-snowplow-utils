#!/bin/bash

# Expected input:
# -d (database) target database for dbt
# -s (seed) boolean of whether to seed test data. Default true.

SEED_DATA=true

while getopts 'd:s:' opt
do
  case $opt in
    d) DATABASE=$OPTARG ;;
    s) SEED_DATA=$OPTARG ;;
  esac
done


if [ "$SEED_DATA" = true ]; then

  echo "Snowplow-utils integration tests: Seeding data"

  eval "dbt seed --target $DATABASE --full-refresh" || exit 1;

fi

echo "Test materializations: Refresh models"

eval "dbt run --models materializations --target $DATABASE --full-refresh " || exit 1;

echo "Test materializations: Execute models"

eval "dbt run --models materializations --target $DATABASE" || exit 1;

echo "Test materializations: Test models"

eval "dbt test --models materializations --target $DATABASE" || exit 1;

echo "Test materializations: All tests passed"
