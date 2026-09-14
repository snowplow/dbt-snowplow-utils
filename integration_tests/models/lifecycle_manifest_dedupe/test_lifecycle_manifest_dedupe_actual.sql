{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{#
Regression test for AISP-1714: calls the real base_create_snowplow_sessions_lifecycle_manifest
macro (both default__ and postgres__ dispatch, depending on -d target) with its own dedicated
event/limits/manifest fixtures so it can exercise the is_incremental() branch without depending
on the shared base_macro pipeline's event-limits/manifest state.

Test sequence (see .scripts/test_lifecycle_manifest_dedupe.sh):
  1. dbt run --full-refresh: builds a single, clean manifest row for "dedupe_session_1".
  2. run-operation test_duplicate_lifecycle_manifest_session_identifier: seeds a pre-existing
     duplicate row for that same session_identifier directly into this model's table, simulating
     a manifest already corrupted by session_identifier reuse (the reported production scenario).
  3. dbt run (no --full-refresh): a new event reusing "dedupe_session_1" arrives. Without the
     previous_sessions dedupe fix, previous_sessions returns both duplicate rows, the join fans
     out to two output rows for the same key, and the incremental MERGE errors on
     default__ (BigQuery/Snowflake/Databricks/Spark) - the exact failure reported in AISP-1714.
     With the fix, previous_sessions collapses to one row and the run succeeds.
  4. dbt test: on postgres__ (delete+insert strategy, which never hard-errors) the fix is instead
     verified via the session_identifier uniqueness test below - see lifecycle_manifest_dedupe.yml
     for why that test is scoped to postgres/redshift only.
#}
{{
    config(
        materialized='incremental',
        unique_key='session_identifier',
        tags=['lifecycle_manifest_dedupe_test', 'requires_script']
    )
}}

{% set sessions_lifecycle_manifest_query = snowplow_utils.base_create_snowplow_sessions_lifecycle_manifest(
    session_identifiers=[{"schema": "atomic", "field": "domain_sessionid"}],
    session_timestamp='collector_tstamp',
    user_identifiers=[{"schema": "atomic", "field": "domain_userid"}],
    derived_tstamp_partitioned=false,
    snowplow_events_database=var('snowplow__database', target.database) if target.type not in ['databricks', 'spark'] else var('snowplow__databricks_catalog', 'hive_metastore') if target.type in ['databricks'] else var('snowplow__events_schema', 'snplw_utils_int_tests'),
    snowplow_events_schema=var('snowplow__events_schema', 'snplw_utils_int_tests'),
    snowplow_events_table='data_lifecycle_manifest_dedupe_events',
    event_limits_table='test_lifecycle_manifest_dedupe_event_limits',
    incremental_manifest_table='test_lifecycle_manifest_dedupe_incremental_manifest',
    package_name='lifecycle_manifest_dedupe_test'
) %}

{{ sessions_lifecycle_manifest_query }}
