{# Tests both RS, PG (delete/insert) and BQ/Snowflake (merge) incremental materialization
   upsert_date_key: RS, PG only. Key used to limit the table scan
   partition_by: BQ only. Key used to limit table scan
   TODO: Add tests that change the granularity of the partition #}

{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='id',
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
    start_tstamp

  from data
  where run = 2

{% else %}

  select
    id,
    start_tstamp

  from data
  where run = 1

{% endif %}
