{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{# Tests boundary semantics of base_create_snowplow_events_this_run_t:
   - event at lower_limit is excluded (lower bound is exclusive: load_tstamp > lower_limit)
   - event at upper_limit is included (upper bound is inclusive: load_tstamp <= upper_limit)
   - event before lower_limit is excluded
   - event inside the window is included
#}

{% set boundary_events_query = snowplow_utils.base_create_snowplow_events_this_run_t(
    run_limits_table='data_run_limits_boundary_t',
    app_ids=[],
    snowplow_events_database=var('snowplow__database', target.database) if target.type not in ['databricks', 'spark'] else var('snowplow__databricks_catalog', 'hive_metastore') if target.type in ['databricks'] else var('snowplow__events_schema', 'snplw_utils_int_tests'),
    snowplow_events_schema=var('snowplow__events_schema', 'snplw_utils_int_tests'),
    snowplow_events_table='data_base_events_boundary_t',
    event_names=none,
    custom_filter=none
) %}

with events_this_run as (
    {{ boundary_events_query }}
)

select event_id, load_tstamp
from events_this_run
