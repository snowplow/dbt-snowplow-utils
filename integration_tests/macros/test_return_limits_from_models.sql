{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{% macro test_return_limits_from_models() %}
    {{ return(adapter.dispatch('test_return_limits_from_models', 'snowplow_utils')()) }}
{% endmacro %}
{% macro default__test_return_limits_from_models() %}
    {% set expected = ["cast('2023-01-25 09:58:02' as TIMESTAMP)", "cast('2023-01-26 09:58:02' as TIMESTAMP)"] %}
    {% set results = snowplow_utils.return_limits_from_model(ref('data_return_limits_from_models'), 'start_tstamp', 'end_tstamp')
    %}

    {% for exp, res in zip(expected, results) %}
        {% if res.strip()|replace('+00:00','')|lower == exp.strip()|lower %} {# Because formatting across warehouses #}
            {% do log("SUCCESS") %}
        {% else %}
            {% do exceptions.raise_compiler_error("FAILED: " ~ res.strip()|lower ~ " is not equal to " ~ exp.strip()|lower ~ ".") %}
        {% endif %}
    {% endfor %}

    {% set expected = ["cast('9999-01-01 00:00:00' as TIMESTAMP)", "cast('9999-01-02 00:00:00' as TIMESTAMP)"] %}
    {% set results = snowplow_utils.return_limits_from_model(ref('data_return_limits_from_models_nulls'), 'start_tstamp', 'end_tstamp')
    %}

    {% for exp, res in zip(expected, results) %}
        {% if res.strip()|replace('+00:00','')|lower == exp.strip()|lower %}
            {% do log("SUCCESS") %}
        {% else %}
            {% do exceptions.raise_compiler_error("FAILED: " ~ res.strip()|lower ~ " is not equal to " ~ exp.strip()|lower ~ ".") %}
        {% endif %}
    {% endfor %}

{% endmacro %}
