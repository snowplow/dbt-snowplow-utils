{# Tests both RS (delete/insert) and BQ/Snowflake (merge) snowplow_incremental materialization with lookback disabled.
   upsert_date_key: RS only. Key used to limit the table scan
   partition_by: BQ only. Key used to limit table scan #}

{{ 
  config(
    materialized='snowplow_incremental',
    incremental_strategy='delete+insert',
    unique_key='id',
    upsert_date_key='start_tstamp',
    disable_upsert_lookback=true,
    tags=["requires_script"]
  ) 
}}

with data as (
  select * from {{ ref('data_snowplow_incremental') }}
)

{% if snowplow_utils.snowplow_is_incremental() %}

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


