
{#
 datediff/dateadd functions in dbt_utils cast tstamps to datetimes for BQ.
 This results in mismatching dtypes when filtering on tstamp fields. Overriding with timestamp func.
 #}

{% macro timestamp_diff(first_tstamp, second_tstamp, datepart) %}
    {{ return(adapter.dispatch('timestamp_diff', 'snowplow_utils')(first_tstamp, second_tstamp, datepart)) }}
{% endmacro %}


{% macro default__timestamp_diff(first_tstamp, second_tstamp, datepart) %}
    {{ return(datediff(first_tstamp, second_tstamp, datepart)) }}
{% endmacro %}


{% macro bigquery__timestamp_diff(first_tstamp, second_tstamp, datepart) %}
    timestamp_diff({{second_tstamp}}, {{first_tstamp}}, {{datepart}})
{% endmacro %}


{% macro timestamp_add(datepart, interval, tstamp) %}
    {{ return(adapter.dispatch('timestamp_add', 'snowplow_utils')(datepart, interval, tstamp)) }}
{% endmacro %}


{% macro default__timestamp_add(datepart, interval, tstamp) %}
    {{ return(dateadd(datepart, interval, tstamp)) }}
{% endmacro %}


{% macro bigquery__timestamp_add(datepart, interval, tstamp) %}
    timestamp_add({{tstamp}}, interval {{interval}} {{datepart}})
{% endmacro %}


{% macro databricks__timestamp_add(datepart, interval, tstamp) %}
    timestampadd({{datepart}}, {{interval}}, {{tstamp}})
{% endmacro %}


{% macro cast_to_tstamp(tstamp_literal) -%}
  {% if tstamp_literal is none or tstamp_literal|lower in ['null',''] %}
    cast(null as {{type_timestamp()}})
  {% else %}
    cast('{{tstamp_literal}}' as {{type_timestamp()}})
  {% endif %}
{%- endmacro %}


{%- macro to_unixtstamp(tstamp) -%}
    {{ adapter.dispatch('to_unixtstamp', 'snowplow_utils') (tstamp) }}
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

{%- macro databricks__to_unixtstamp(tstamp) -%}
    unix_timestamp({{ tstamp }})
{%- endmacro %}

{%- macro spark__to_unixtstamp(tstamp) -%}
    {{ return(snowplow_utils.databricks__to_unixtstamp(tstamp)) }}
{%- endmacro %}

{% macro current_timestamp_in_utc() -%}
  {{ return(adapter.dispatch('current_timestamp_in_utc', 'snowplow_utils')()) }}
{%- endmacro %}

{% macro default__current_timestamp_in_utc() %}
    {{current_timestamp()}}
{% endmacro %}

{% macro snowflake__current_timestamp_in_utc() %}
    convert_timezone('UTC', {{current_timestamp()}})::{{type_timestamp()}}
{% endmacro %}

{% macro postgres__current_timestamp_in_utc() %}
    (current_timestamp at time zone 'utc')::{{type_timestamp()}}
{% endmacro %}

{# redshift should use default instead of postgres #}
{% macro redshift__current_timestamp_in_utc() %}
    {{ return(snowplow_utils.default__current_timestamp_in_utc()) }}
{% endmacro %}
