#!/bin/bash

# Expected input:
# -d (database) target database for dbt

# Regression test for AISP-1714: base_create_snowplow_sessions_lifecycle_manifest's
# previous_sessions CTE must dedupe on session_identifier before joining new events, otherwise a
# pre-existing duplicate manifest row fans out into an incremental MERGE with multiple source rows
# matching one target row.

while getopts 'd:' opt
do
  case $opt in
    d) DATABASE=$OPTARG
  esac
done

echo "Test lifecycle manifest dedupe: build initial manifest state (full-refresh)"

eval "dbt run --select tag:lifecycle_manifest_dedupe_test --target $DATABASE --full-refresh" || exit 1;

echo "Test lifecycle manifest dedupe: seed a pre-existing duplicate session_identifier row into the manifest"

eval "dbt run-operation test_duplicate_lifecycle_manifest_session_identifier --target $DATABASE" || exit 1;

echo "Test lifecycle manifest dedupe: incremental run with a new event reusing the duplicated session_identifier"

# On default__ targets (BigQuery/Snowflake/Databricks/Spark, native MERGE) an undeduped
# previous_sessions fans the duplicate out into the MERGE source and this run errors outright -
# that failure is the assertion. On postgres__ (delete+insert, never errors) the assertion is the
# session_identifier uniqueness test below instead.
eval "dbt run --select tag:lifecycle_manifest_dedupe_test --target $DATABASE" || exit 1;

echo "Test lifecycle manifest dedupe: verify session_identifier uniqueness (postgres/redshift only, see lifecycle_manifest_dedupe.yml)"

eval "dbt test --select test_lifecycle_manifest_dedupe_actual --target $DATABASE" || exit 1;

echo "Test lifecycle manifest dedupe: All tests passed"
