#!/bin/bash

# Expected input:
# -d (database) target database for dbt

while getopts 'd:' opt
do
  case $opt in
    d) DATABASE=$OPTARG
  esac
done

# set to lower case
DATABASE="$(echo $DATABASE | tr '[:upper:]' '[:lower:]')"

# BQ only macro 
if [[ $DATABASE == 'bigquery' ]]; then
  echo "Test combine_column_versions: Pass renamed_fields arg but not source_fields"
  # Run model + upstream. Upstream needed to 'seed' input data which is supplied via model rather than csv.
  OUTPUT=$(eval "dbt run --models +test_combine_column_versions --vars \"{'renamed_fields': '[\"a\"]', 'source_fields': 'none'}\"")
  EXIT_CODE=$?

  if [[ $OUTPUT == *"Snowplow: To rename fields, pass source_fields arg"* ]]; then
    RENAME_ERROR_RAISED=true
  fi

  if [[ $RENAME_ERROR_RAISED  && $EXIT_CODE==1 ]]; then
    echo "Pass: Compiler error raised"
  else
    echo "Fail: Compiler error not raised"
    exit 1
  fi

  echo "Test combine_column_versions: Pass renamed_fields + source_field arg but mismatch in length"

  OUTPUT=$(eval "dbt run --models test_combine_column_versions --vars \"{'renamed_fields': '[\"a\"]', 'source_fields': '[\"y\", \"x\"]'}\"")
  EXIT_CODE=$?

  if [[ $OUTPUT == *"Snowplow: source_fields and rename_field lists length do not match"* ]]; then
    MISMATCH_ERROR_RAISED=true
  fi

  if [[ $MISMATCH_ERROR_RAISED  && $EXIT_CODE==1 ]]; then
    echo "Pass: Compiler error raised"
  else
    echo "Fail: Compiler error not raised"
    exit 1
  fi

  echo "Test combine_column_versions: Standard test"

  eval "dbt run --models test_combine_column_versions expected_combine_column_versions --target $DATABASE" || exit 1;

  eval "dbt test --models test_combine_column_versions --target $DATABASE" || exit 1;

  echo "Snowplow web integration tests: All tests passed"

else 
  echo "Test combine_column_versions: BQ only. No tests."
fi


