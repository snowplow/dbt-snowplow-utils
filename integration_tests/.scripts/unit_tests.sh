#!/bin/bash

# Expected input:
# -d (database) target database for dbt. Set to 'all' to test all supported databases.
# -b (branch) pr branch name, taken from the pr itself, when the pr is opened against main it will take the value release automatically
while getopts "b:d:" opt; do
    case $opt in
        b) BRANCH="$OPTARG" ;;
        d) DATABASE=$OPTARG ;;
        *) echo "Invalid option: -$OPTARG" >&2 ;;
    esac
done


declare -a SUPPORTED_DATABASES=("bigquery" "databricks" "postgres" "redshift" "snowflake", "spark_iceberg")

# set to lower case
DATABASE="$(echo $DATABASE | tr '[:upper:]' '[:lower:]')"
BRANCH="$(echo $BRANCH | tr '[:upper:]' '[:lower:]')"

if [[ $DATABASE == "all" ]]; then
  DATABASES=( "${SUPPORTED_DATABASES[@]}" )
else
  DATABASES=$DATABASE
fi

for db in ${DATABASES[@]}; do

  # Run dbt seed to set up the database, this prepares the ground for int tests that come after unit tests

  echo "Snowplow unified unit tests: Seeding data"
  eval "dbt seed --full-refresh --target $db" || exit 1;

    # In order to test this macro we need a model reference first and also a timestamp column which the macro takes the min and max of
    # We need to make sure that the correct result is returned even if the table is empty and whether they want the output to be a low or a high set date in that case
    # All in the models folder
    
  if [[ $BRANCH == "release" || $BRANCH == "utils_revamp" ]]; then
    echo "Snowplow-utils unit tests: Run test_return_limits_from_model_macro"
    eval "dbt run --select +test_return_limits_from_model_macro expected_return_limits_from_model_macro  --target $db --full-refresh" || exit 1;
    eval "dbt test --select test_return_limits_from_model_macro --store-failures --target $db" || exit 1;
  fi
  
    # This macro returns different queries for different states which will be used to create the base_new_event_limits table
    # We need to make sure that the correct result is returned from this query depending on different inputs
    # Inputs are given based on the get_incremental_manifest_status macro but we can just fake it as it returns an array 
    # Input example: ['9999-01-01 00:00:00', '9999-01-01 00:00:00', 0, false]
    # Inputs are read from a seed file
  
  if [[ $BRANCH == "release" || $BRANCH == "utils_revamp" ]]; then
    echo "Snowplow-utils unit tests: Run test_get_run_limits_macro"
    eval "dbt run --select test_get_run_limits_macro  --target $db --full-refresh" || exit 1;
    eval "dbt test --select test_get_run_limits_macro --store-failures --target $db" || exit 1;
  fi

    # This macro returns different queries for different states which will be used to create the base_new_event_limits table
    # We need to make sure that the correct result is returned from this query depending on different inputs
    # Inputs are given based on the get_incremental_manifest_status macro but we can just fake it as it returns an array 
    # Input example: ['9999-01-01 00:00:00', '9999-01-01 00:00:00', 0, 0, false]
    # Inputs are read from a seed file
  
  if [[ $BRANCH == "release" || $BRANCH == "utils_revamp" ]]; then
    echo "Snowplow-utils unit tests: Run test_get_run_limits_t_macro"
    eval "dbt run --select test_get_run_limits_t_macro --target $db --full-refresh" || exit 1;
    eval "dbt test --select test_get_run_limits_t_macro --store-failures --target $db" || exit 1;
  fi
  
    # This macro returns returns the array: [min_last_success, max_last_success, models_matched_from_manifest, has_matched_all_models]
    # Not too important to test, it is effectively returns a min/max/count from values in the manifest based on the models in the run
    # Inputs are read from a seed file, we can selectively test the different inputs depending on the models in run array so no need for it to contain exact scenarios upfront
  
  if [[ $BRANCH == "release" || $BRANCH == "utils_revamp" ]]; then
    echo "Snowplow-utils unit tests: Run test_get_incremental_manifest_status_macro"
    eval "dbt run --select test_get_incremental_manifest_status_macro --target $db --full-refresh" || exit 1;
    eval "dbt test --select test_get_incremental_manifest_status_macro --store-failures --target $db" || exit 1;
  fi
  
    # This macro returns returns the array: [min_first_success, max_first_success, min_last_success, max_last_success, models_matched_from_manifest, sync_count, has_matched_all_models]
    # Not too important to test, it is effectively returns a min/max/count from values in the manifest based on the models in the run
    # Inputs are read from a seed file, we can selectively test the different inputs depending on the models in run array so no need for it to contain exact scenarios upfront
  
  if [[ $BRANCH == "release" || $BRANCH == "utils_revamp" ]]; then
    echo "Snowplow-utils unit tests: Run test_get_incremental_manifest_status_t_macro"
    eval "dbt run --select test_get_incremental_manifest_status_t_macro --target $db --full-refresh" || exit 1;
    eval "dbt test --select test_get_incremental_manifest_status_t_macro --store-failures --target $db" || exit 1;
  fi

done
