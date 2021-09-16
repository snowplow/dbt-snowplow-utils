#!/bin/bash

# Expected input:
# -d (database) target database for dbt. Set to 'all' to test all supported databases.

while getopts 'd:' opt
do
  case $opt in
    d) DATABASE=$OPTARG
  esac
done

declare -a SUPPORTED_DATABASES=("redshift" "bigquery" "snowflake")

# set to lower case
DATABASE="$(echo $DATABASE | tr '[:upper:]' '[:lower:]')"

if [[ $DATABASE == "all" ]]; then
  DATABASES=( "${SUPPORTED_DATABASES[@]}" )
else
  DATABASES=$DATABASE
fi

for db in ${DATABASES[@]}; do

  echo "Snowplow-utils integration tests: Seeding data"

  eval "dbt seed --target $db --full-refresh" || exit 1;

  echo "Snowplow-utils integration tests: Run native dbt based tests"

  echo "Snowplow-utils native dbt tests: Execute models"

  eval "dbt run --exclude tag:requires_script --target $db --full-refresh " || exit 1;

  echo "Snowplow-utils native dbt tests: Test models"

  eval "dbt test --exclude tag:requires_script --target $db" || exit 1;

  echo "Snowplow utils integration tests: Run script based tests"

  echo "Snowplow-utils integration tests: Testing materializations"

  bash test_materializations.sh -d $db -s false || exit 1; # don't re-seed

  echo "Snowplow-utils integration tests: Testing combine_column_versions"

  bash test_combine_column_versions.sh -d $db || exit 1;

  echo "Snowplow-utils integration tests: Testing get_enabled_snowplow_models"

  bash test_get_enabled_snowplow_models.sh -d $db || exit 1;

  echo "Snowplow-utils integration tests: Testing snowplow_delete_from_manifest"

  bash test_snowplow_delete_from_manifest.sh -d $db || exit 1;

  echo "Snowplow-utils integration tests: All tests passed for $db"

done
