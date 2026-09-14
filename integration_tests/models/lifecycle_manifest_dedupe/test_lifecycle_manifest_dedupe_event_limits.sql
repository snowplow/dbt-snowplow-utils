{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{#
Drives the event window for test_lifecycle_manifest_dedupe_actual across its two test runs:
run 1 (full-refresh) only sees the first fixture event, run 2 (incremental) only sees the second,
which reuses the same session_identifier. Materialized incremental with a constant unique_key so
each run replaces the single row rather than accumulating one per invocation.
#}
{{
    config(
        materialized='incremental',
        unique_key='id',
        tags=['lifecycle_manifest_dedupe_test', 'requires_script']
    )
}}

select
    1 as id
    {% if is_incremental() %}
    , cast('2024-01-10 00:00:00' as timestamp) as lower_limit
    , cast('2024-01-10 23:59:59' as timestamp) as upper_limit
    {% else %}
    , cast('2024-01-01 00:00:00' as timestamp) as lower_limit
    , cast('2024-01-01 23:59:59' as timestamp) as upper_limit
    {% endif %}
