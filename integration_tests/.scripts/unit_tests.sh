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

    # In order to test this macro we need a model reference first and also a timestamp column which the macro takes the min and max of
    # We need to make sure that the correct result is returned even if the table is empty and whether they want the output to be a low or a high set date in that case
    # All in the models folder
    
  if [[ $BRANCH == "release" || $BRANCH == "fix/return_limits" ]]; then
    echo "Snowplow-utils unit tests: Run test_return_limits_from_model_macro"
    eval "dbt run --select +test_return_limits_from_model_macro expected_return_limits_from_model_macro  --target $db --full-refresh" || exit 1;
    eval "dbt test --select +test_return_limits_from_model_macro --store-failures --target $db" || exit 1;
  fi

done
