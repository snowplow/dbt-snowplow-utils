#!/bin/bash

# Expected input:
# -d (database) target database for dbt

while getopts 'd:' opt
do
  case $opt in
    d) DATABASE=$OPTARG
  esac
done

eval "dbt seed --select data_snowplow_delete_from_manifest data_snowplow_delete_from_manifest_staging --target $DATABASE --full-refresh" || exit 1;

echo "Test snowplow_delete_from_manifest: Delete subset of models, none exist in the manifest"

# Check correct warning raised if none of the models_to_delete exist in manifest
OUTPUT=$(eval "dbt run --models test_snowplow_delete_from_manifest --target $DATABASE --vars 'models_to_delete: [\"x\",\"y\"]'") || exit 1;

if [[ $OUTPUT == *"Snowplow: None of the supplied models exist in the manifest"* ]]; then
  echo "Pass: Warning raised"
else
  echo "Fail: Warning not raised"
  exit 1
fi

# All models_to_delete exist in manifest. Check they are deleted correctly.
echo "Test snowplow_delete_from_manifest: Delete subset of models, all in manifest"

eval "dbt run --models test_snowplow_delete_from_manifest expected_snowplow_delete_from_manifest --target $DATABASE --vars 'models_to_delete: [\"a\",\"b\"]'" || exit 1;

eval "dbt test --models test_snowplow_delete_from_manifest --target $DATABASE" || exit 1;

# Re-seed data
eval "dbt seed --select data_snowplow_delete_from_manifest data_snowplow_delete_from_manifest_staging --target $DATABASE --full-refresh" || exit 1;

# Some of models_to_delete exist in manifest, some do not. Check they are deleted correctly and warnings raised.
echo "Test snowplow_delete_from_manifest: Delete subset of models, some in the manifest some are not."

OUTPUT=$(eval "dbt run --models test_snowplow_delete_from_manifest expected_snowplow_delete_from_manifest --target $DATABASE --vars 'models_to_delete: [\"a\",\"b\",\"x\",\"y\"]'") || exit 1;

# Check warning for non-existant models. TODO: match whole warning including names of missing models. Quotes messing up string comparison
if [[ $OUTPUT == *"do not exist in the manifest"* ]]; then
  echo "Pass: Warning raised"
else
  echo "Fail: Warning not raised"
  exit 1
fi

# Check pre-existing models deleted
eval "dbt test --models test_snowplow_delete_from_manifest --target $DATABASE" || exit 1;

echo "Test snowplow_delete_from_manifest: All tests passed"
