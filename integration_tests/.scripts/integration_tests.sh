#!/bin/bash

# Expected input:
# -d (database) target database for dbt. Set to 'all' to test all supported databases.

while getopts 'd:' opt
do
  case $opt in
    d) DATABASE=$OPTARG
  esac
done

declare -a SUPPORTED_DATABASES=("bigquery" "databricks" "postgres" "redshift" "snowflake", "spark_iceberg", "duckdb")

# set to lower case
DATABASE="$(echo $DATABASE | tr '[:upper:]' '[:lower:]')"

if [[ $DATABASE == "all" ]]; then
  DATABASES=( "${SUPPORTED_DATABASES[@]}" )
else
  DATABASES=$DATABASE
fi

# Function to echo text in violet color
echo_violet() {
    echo -e "\033[0;35m$1\033[0m"
}

for db in ${DATABASES[@]}; do

  echo_violet "Snowplow-utils integration tests: Seeding data"

  eval "dbt seed --target $db --full-refresh" || exit 1;

  echo_violet "Snowplow-utils integration tests: Run native dbt based tests"

  echo_violet "Snowplow-utils native dbt tests: Execute models"

  eval "dbt run --exclude tag:requires_script --target $db --full-refresh " || exit 1;

  echo_violet "Snowplow-utils native dbt tests: Test models"

  eval "dbt test --exclude tag:requires_script --target $db --store-failures" || exit 1;

  echo_violet "Snowplow-utils native dbt tests: Run custom base macro models"

  eval "dbt run --select tag:base_macro --target $db --full-refresh --vars 'snowplow__custom_test: true'" || exit 1;

  echo_violet "Snowplow-utils native dbt tests: Test custom base macro models"

  eval "dbt test --select tag:base_macro --target $db --vars 'snowplow__custom_test: true' --store-failures" || exit 1;

  echo_violet "Snowplow-utils native dbt tests: Run custom session sql base macro models"

  eval "dbt run --select tag:base_macro --target $db --full-refresh --vars 'snowplow__session_test: true'" || exit 1;

  echo_violet "Snowplow-utils native dbt tests: Test custom base macro models"

  eval "dbt test --select tag:base_macro --exclude snowplow_base_quarantined_sessions_actual --target $db --vars 'snowplow__session_test: true' --store-failures" || exit 1;

  echo_violet "Snowplow utils integration tests: Run script based tests"

  echo_violet "Snowplow-utils integration tests: Testing get_successful_models"

  source "${BASH_SOURCE%/*}/test_get_successful_models.sh" -d $db || exit 1;

  echo_violet "Snowplow-utils integration tests: Testing materializations"

  source "${BASH_SOURCE%/*}/test_materializations.sh" -d $db -s false || exit 1; # don't re-seed

  echo "Snowplow-utils integration tests: Testing get_enabled_snowplow_models"

  source "${BASH_SOURCE%/*}/test_get_enabled_snowplow_models.sh" -d $db || exit 1;

  echo_violet "Snowplow-utils integration tests: Testing snowplow_delete_from_manifest"

  source "${BASH_SOURCE%/*}/test_snowplow_delete_from_manifest.sh" -d $db || exit 1;

  echo_violet "Snowplow-utils integration tests: Testing return_limits_from_model"

  eval "dbt run-operation test_return_limits_from_models --target $db"  || exit 1;

  echo_violet "Snowplow-utils integration tests: Testing get_sde_or_context"

  eval "dbt run-operation test_get_sde_or_context --target $db"  || exit 1;

  echo_violet "Snowplow-utils integration tests: All tests passed for $db"

done
