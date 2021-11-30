#!/bin/bash

# Expected input:
# -d (database) target database for dbt

while getopts 'd:' opt
do
  case $opt in
    d) DATABASE=$OPTARG
  esac
done

echo "Test get_successful_models."

OUTPUT=$(eval "dbt run --models incremental_hooks.test_get_successful_models --target $DATABASE --vars '{enabled_test_get_successful_models: true}'") || true

if [[ $OUTPUT == *"Pass: test_get_successful_models()"* ]]; then
  TEST_PASS=true
fi

if [[ $TEST_PASS ]]; then
  echo "Pass: test_get_successful_models identified the correct models"
else
  echo "Fail: test_get_successful_models identified the incorrect models"
  exit 1
fi




