
{# 
 datediff/dateadd functions in dbt_utils cast tstamps to datetimes for BQ.
 This results in mismatching dtypes when filtering on tstamp fields. Overriding with timestamp func.
 #}

{% macro timestamp_diff(first_tstamp, second_tstamp, datepart) %}
    {{ return(adapter.dispatch('timestamp_diff', ['snowplow_utils'])(first_tstamp, second_tstamp, datepart)) }}
{% endmacro %}


{% macro default__timestamp_diff(first_tstamp, second_tstamp, datepart) %}
    {{ return(dbt_utils.datediff(first_tstamp, second_tstamp, datepart)) }}
{% endmacro %}


{% macro bigquery__timestamp_diff(first_tstamp, second_tstamp, datepart) %}
    timestamp_diff({{second_tstamp}}, {{first_tstamp}}, {{datepart}})
{% endmacro %}


{% macro timestamp_add(datepart, interval, tstamp) %}
    {{ return(adapter.dispatch('timestamp_add', ['snowplow_utils'])(datepart, interval, tstamp)) }}
{% endmacro %}


{% macro default__timestamp_add(datepart, interval, tstamp) %}
    {{ return(dbt_utils.dateadd(datepart, interval, tstamp)) }}
{% endmacro %}


{% macro bigquery__timestamp_add(datepart, interval, tstamp) %}
    timestamp_add({{tstamp}}, interval {{interval}} {{datepart}})
{% endmacro %}


{% macro cast_to_tstamp(tstamp_literal) -%}
  {% if tstamp_literal is none or tstamp_literal|lower in ['null',''] %}
    cast(null as {{dbt_utils.type_timestamp()}})
  {% else %}
    cast('{{tstamp_literal}}' as {{dbt_utils.type_timestamp()}})
  {% endif %}
{%- endmacro %}


{%- macro to_unixtstamp(tstamp) -%}
    {{ adapter.dispatch('to_unixtstamp', ['snowplow_utils']) (tstamp) }}
{%- endmacro %}


{%- macro default__to_unixtstamp(tstamp) -%}
    date_part('epoch', {{ tstamp }})
{%- endmacro %}


{%- macro snowflake__to_unixtstamp(tstamp) -%}
    date_part('epoch_seconds', {{ tstamp }})
{%- endmacro %}


{%- macro bigquery__to_unixtstamp(tstamp) -%}
    unix_seconds({{ tstamp }})
{%- endmacro %}
