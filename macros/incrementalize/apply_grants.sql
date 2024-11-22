{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{# Note this does not work for bigquery due to the role/IAM type approach they have to grants, so BQ users should not supply values to this var #}
{% macro default__apply_grants(relation, grant_config={}, should_revoke=True) %}
    {# 
        We only want to enforce this if the package user is managing grants this way - if they are doing it in database we should 
        pass {} so that it's a no-op 
    #}
    {% if (grant_config.get('select', []) or var('snowplow__grant_select_to', [])) and target.type != 'bigquery' %}
        {# Add our config to the grants from our variable #}
        {% do grant_config.update({'select': grant_config.get('select', []) + var('snowplow__grant_select_to', [])}) %}
    {% endif %}
    {# Call the original macro so we don't have to keep this in sync ourselves #}
    {{ dbt.default__apply_grants(relation, grant_config, should_revoke=True) }}
{% endmacro %}

{% macro grant_usage_on_schemas_built_into(enabled=false) -%}

  {{ return(adapter.dispatch('grant_usage_on_schemas_built_into', 'snowplow_utils')(enabled)) }}

{% endmacro %}

{# Grants usage on _any_ schema that has been part of the run, only runnable as an on-run-end #}
{% macro default__grant_usage_on_schemas_built_into(enabled=true) %}
    {% if enabled %}
        {% if execute %}
            {% set grant_list %}
                {% for schema in schemas %}
                    {% for role in var('snowplow__grant_select_to', []) %}
                        grant usage on schema {{ schema }} to {% if target.type == 'databricks' %}`{% else %}"{% endif %}{{ role }}{% if target.type == 'databricks' %}`{% else %}"{% endif %};
                    {% endfor %}
                {% endfor %}
            {% endset %}
            {{ return(grant_list) }}
        {% endif %}
    {% endif %}
    {{ return("") }}
{% endmacro %}

{% macro bigquery__grant_usage_on_schemas_built_into(enabled=false) %}
    {# Bigquery doesn't need usage granted on schemas #}
    {{ return("") }}
{% endmacro %}
