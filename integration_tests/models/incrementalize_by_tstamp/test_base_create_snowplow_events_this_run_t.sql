{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{{
    config(
        materialized="table"
    )
}}

{#
Verifies the load_tstamp boundary handling in base_create_snowplow_events_this_run_t: the event exactly
at lower_limit must be excluded (already accounted for by whichever run set that checkpoint), and the
event exactly at upper_limit must be included (otherwise, if it's the only new row available, the
incremental manifest can never advance and the run gets stuck out-of-sync forever).
#}
{% set base_events_query = snowplow_utils.base_create_snowplow_events_this_run_t(
    run_limits_table='data_base_create_snowplow_events_this_run_t_limits',
    app_ids=[],
    snowplow_events_database=target.database,
    snowplow_events_schema=var('snowplow__events_schema', 'snplw_utils_int_tests'),
    snowplow_events_table='data_base_create_snowplow_events_this_run_t_events',
    event_names=[]
) %}

select
    event_id,
    load_tstamp

from ( {{ base_events_query }} )
