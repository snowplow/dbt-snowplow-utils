{# Tests both RS/PG (delete/insert) and BQ/Snowflake/Databricks (merge) snowplow_incremental materialization with lookback disabled.
   upsert_date_key: RS/PG/Databricks only. Key used to limit the table scan
   partition_by: BQ only. Key used to limit table scan #}

{{
  config(
    materialized='snowplow_incremental',
    unique_key='id',
    upsert_date_key='start_tstamp',
    disable_upsert_lookback=true,
    partition_by = snowplow_utils.get_partition_by(bigquery_partition_by={
      "field": "start_tstamp",
      "data_type": "timestamp"
    }),
    tags=["requires_script"]
  )
}}

with data as (
  select * from {{ ref('data_snowplow_incremental') }}
  {% if target.type == 'snowflake' %}
    -- data set intentionally contains dupes.
    -- Snowflake merge will error if dupes occur. Removing for test
    where not (run = 1 and id = 2 and start_tstamp = '2021-03-03 00:00:00')
  {% endif %}
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
