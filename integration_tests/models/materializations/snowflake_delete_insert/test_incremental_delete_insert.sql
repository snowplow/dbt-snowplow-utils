{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{# Tests both RS, PG (delete/insert) and BQ/Snowflake (merge) incremental materialization
   upsert_date_key: RS, PG only. Key used to limit the table scan
   partition_by: BQ only. Key used to limit table scan
   TODO: Add tests that change the granularity of the partition #}

{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['id', 'id2'],
    upsert_date_key='start_tstamp',
    tags=["requires_script"],
    snowplow_optimize=true
  )
}}

with data as (
  select * from {{ ref('data_incremental') }}
)

{% if is_incremental() %}

  select
    id,
    id2,
    start_tstamp

  from data
  where run = 2

{% else %}

  select
    id,
    id2,
    start_tstamp

  from data
  where run = 1

{% endif %}
