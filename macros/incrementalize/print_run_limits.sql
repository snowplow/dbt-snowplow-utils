{# Prints the run limits for the run to the console #}
{% macro print_run_limits(run_limits_relation, package= none) -%}

  {# When used on its own, it will print out the previous runs limits to the console #}

  {% set run_limits_query %}
    select lower_limit, upper_limit from {{ run_limits_relation }}
  {% endset %}

  {# Derive limits from manifest instead of selecting from limits table since run_query executes during 2nd parse the limits table is yet to be updated. #}
  {% set results = run_query(run_limits_query) %}

  {% if execute %}

    {% set lower_limit = snowplow_utils.tstamp_to_str(results.columns[0].values()[0]) %}
    {% set upper_limit = snowplow_utils.tstamp_to_str(results.columns[1].values()[0]) %}
    {% set run_limits_message = "Snowplow: Processing data between " + lower_limit + " and " + upper_limit %}
    {% if package %}
        {% set run_limits_message = run_limits_message +  " (" + package + ")" %}
    {% endif %}

    {% do snowplow_utils.log_message(run_limits_message) %}

  {% endif %}

{%- endmacro %}
