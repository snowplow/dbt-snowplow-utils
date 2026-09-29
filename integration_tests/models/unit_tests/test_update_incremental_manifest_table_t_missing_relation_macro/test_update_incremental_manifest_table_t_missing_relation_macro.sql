{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{#
  Simulates a scoped/selective dbt build where the base events table was never materialized.
  api.Relation.create() is used (rather than ref()) so this relation is never built by the
  integration test suite, which is required to exercise the "does not exist" branch of
  update_incremental_manifest_table_t - anything ref()'d would get built in a full test run.
#}
{{ config(
    post_hook="{{ snowplow_utils.update_incremental_manifest_table_t(this,
                                                                       api.Relation.create(database=this.database, schema=this.schema, identifier='snowplow_utils_missing_base_events_this_run_t_for_test'),
                                                                      ['a','b','c']) }}",
    materialized="table"
   )
}}

select 'a' as model, cast('2021-01-01 00:00:00' as {{ dbt.type_timestamp() }}) as last_success, cast('2021-01-01 00:00:00' as {{ dbt.type_timestamp() }}) as first_success
union all
select 'b' as model, cast('2021-01-02 00:00:00' as {{ dbt.type_timestamp() }}) as last_success, cast('2021-01-02 00:00:00' as {{ dbt.type_timestamp() }}) as first_success
