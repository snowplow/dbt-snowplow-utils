{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{{
    config(
        tags=['base_macro']
    )
}}

{% set snowplow_session_identifiers = var("snowplow__session_identifiers") %}
{% set snowplow_user_identifiers = var("snowplow__user_identifiers") %}
{% set snowplow_session_sql = '' %}

{% if var('snowplow__custom_test', false) %}
    {% set snowplow_session_identifiers = snowplow_utils.get_value_by_target_type(
        bigquery_val=var("snowplow__bigquery_session_identifiers"),
        snowflake_val=var("snowplow__snowflake_session_identifiers"),
        databricks_val=var("snowplow__databricks_session_identifiers"),
        spark_val=var("snowplow__spark_session_identifiers"),
        postgres_val=var("snowplow__postgres_session_identifiers"),
        redshift_val=var("snowplow__postgres_session_identifiers"))%}
    {% set snowplow_user_identifiers = snowplow_utils.get_value_by_target_type(
        bigquery_val=var("snowplow__bigquery_user_identifiers"),
        snowflake_val=var("snowplow__snowflake_user_identifiers"),
        databricks_val=var("snowplow__databricks_user_identifiers"),
        spark_val=var("snowplow__spark_user_identifiers"),
        postgres_val=var("snowplow__postgres_user_identifiers"),
        redshift_val=var("snowplow__postgres_user_identifiers"))%}
{% elif var('snowplow__session_test', false) %}
    {% set snowplow_session_sql = var("snowplow__custom_session_sql") %}
{% endif %}

{% set sessions_lifecycle_manifest_query = snowplow_utils.base_create_snowplow_sessions_lifecycle_manifest(
    session_identifiers=snowplow_session_identifiers,
    session_sql=snowplow_session_sql,
    session_timestamp=var('snowplow__session_timestamp', 'collector_tstamp'),
    user_identifiers=snowplow_user_identifiers,
    quarantined_sessions=var('snowplow__quarantined_sessions', 'snowplow_base_quarantined_sessions_actual'),
    derived_tstamp_partitioned=var('snowplow__derived_tstamp_partitioned', true),
    days_late_allowed=var('snowplow__days_late_allowed', 3),
    max_session_days=var('snowplow__max_session_days', 3),
    app_ids=var('snowplow__app_ids', []),
    snowplow_events_database=var('snowplow__database', target.database) if target.type not in ['databricks', 'spark'] else var('snowplow__databricks_catalog', 'hive_metastore') if target.type in ['databricks'] else var('snowplow__events_schema', 'snplw_utils_int_tests'),
    snowplow_events_schema=var('snowplow__events_schema', 'snplw_utils_int_tests'),
    snowplow_events_table=var('snowplow__events_table', 'snowplow_events_stg'),
    event_limits_table=var('snowplow__event_limits', 'snowplow_base_new_event_limits_actual'),
    incremental_manifest_table=var('snowplow__incremental_manifest', 'snowplow_incremental_manifest_actual')
 ) %}

{{ sessions_lifecycle_manifest_query }}
