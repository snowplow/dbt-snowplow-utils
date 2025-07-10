{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

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

{% macro databricks__timestamp_diff(first_tstamp, second_tstamp, datepart) %}
    {{ return(datediff(first_tstamp, second_tstamp, datepart)) }}
{% endmacro %}

{% macro spark__timestamp_diff(first_tstamp, second_tstamp, datepart) %}
    {% if datepart|lower == 'week' %}
        cast((unix_timestamp(cast({{second_tstamp}} as timestamp)) - unix_timestamp(cast({{first_tstamp}} as timestamp))) / (3600 * 24 * 7) as bigint)
    {% elif datepart|lower == 'day' %}
        cast((unix_timestamp(cast({{second_tstamp}} as timestamp)) - unix_timestamp(cast({{first_tstamp}} as timestamp))) / (3600 * 24) as bigint)
    {% elif datepart|lower == 'hour' %}
        cast((unix_timestamp(cast({{second_tstamp}} as timestamp)) - unix_timestamp(cast({{first_tstamp}} as timestamp))) / 3600 as bigint)
    {% elif datepart|lower == 'minute' %}
        cast((unix_timestamp(cast({{second_tstamp}} as timestamp)) - unix_timestamp(cast({{first_tstamp}} as timestamp))) / 60 as bigint)
    {% elif datepart|lower == 'second' %}
        cast(unix_timestamp(cast({{second_tstamp}} as timestamp)) - unix_timestamp(cast({{first_tstamp}} as timestamp)) as bigint)
    {% elif datepart|lower == 'millisecond' %}
        cast((unix_timestamp(cast({{second_tstamp}} as timestamp)) - unix_timestamp(cast({{first_tstamp}} as timestamp))) * 1000 as bigint)
    {% else %}
        {{ exceptions.raise_compiler_error("Unsupported datepart for Spark: " ~ datepart) }}
    {% endif %}
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

{% macro spark__timestamp_add(datepart, interval, tstamp) %}
    {% if datepart|lower == 'week' %}
        timestamp_millis(cast(cast(unix_millis({{tstamp}}) as bigint) + (cast({{interval}} as bigint) * cast(3600 as bigint) * cast(24 as bigint) * cast(7 as bigint) * cast(1000 as bigint)) as bigint))
    {% elif datepart|lower == 'day' %}
        timestamp_millis(cast(cast(unix_millis({{tstamp}}) as bigint) + (cast({{interval}} as bigint) * cast(3600 as bigint) * cast(24 as bigint) * cast(1000 as bigint)) as bigint))
    {% elif datepart|lower == 'hour' %}
        timestamp_millis(cast(cast(unix_millis({{tstamp}}) as bigint) + (cast({{interval}} as bigint) * cast(3600 as bigint) * cast(1000 as bigint)) as bigint))
    {% elif datepart|lower == 'minute' %}
        timestamp_millis(cast(cast(unix_millis({{tstamp}}) as bigint) + (cast({{interval}} as bigint) * cast(60 as bigint) * cast(1000 as bigint)) as bigint))
    {% elif datepart|lower == 'second' %}
        timestamp_millis(cast(cast(unix_millis({{tstamp}}) as bigint) + cast({{interval}} as bigint) * cast(1000 as bigint) as bigint))
    {% elif datepart|lower == 'millisecond' %}
        timestamp_millis(cast(cast(unix_millis({{tstamp}}) as bigint) + cast({{interval}} as bigint) as bigint))
    {% else %}
        {{ exceptions.raise_compiler_error("Unsupported datepart for Spark: " ~ datepart) }}
    {% endif %}
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

{%- macro spark__to_unixtstamp(tstamp) -%}
    unix_timestamp({{ tstamp }})
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


{% macro deduct_days_from_current_tstamp_utc(interval) %} 
    {{ return(adapter.dispatch('deduct_days_from_current_tstamp_utc', 'snowplow_utils')(interval)) }}
{% endmacro %}
    
{% macro default__deduct_days_from_current_tstamp_utc(interval) %}
    date_trunc('day', convert_timezone('UTC', current_timestamp) - interval '{{ interval }} day')
{% endmacro %}

{% macro add_days_to_date(interval, base_date) %}
    {{ return(adapter.dispatch('add_days_to_date', 'snowplow_utils')(interval, base_date)) }}
{% endmacro %}


{% macro default__add_days_to_date(interval, base_date) %}
    DATEADD('DAY', {{interval}}, {{base_date}})
{% endmacro %}


{% macro bigquery__add_days_to_date(interval, base_date) %}
    DATE_ADD({{base_date}}, interval {{interval}} day)
{% endmacro %}
