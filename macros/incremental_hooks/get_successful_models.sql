{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{# Returns an array of successfully executed models by name #}
{% macro get_successful_models(models=[], run_results=results) -%}

  {% set successful_models = [] %}
  {# Remove the patch version from dbt version #}
  {% set dbt_version_trunc = dbt_version.split('.')[0:2]|join('.')|float %}

  {% if execute %}

    {% for res in run_results -%}
      {# Filter for models #}
      {% if res.node.unique_id.startswith('model.') %}

        {% set is_model_to_include = true if not models|length or res.node.name in models else false %}

        {# run_results schema changed between dbt v0.18 and v0.19 so different methods to define success #}
        {% if dbt_version_trunc <= 0.18 %}
          {% set skipped = true if res.status is none and res.skip else false %}
          {% set errored = true if res.status == 'ERROR' else false %}
          {% set success = true if not (skipped or errored) else false %}
        {% else %}
          {% set success = true if res.status == 'success' else false %}
        {% endif %}

        {% if success and is_model_to_include %}

          {%- do successful_models.append(res.node.name) -%}

        {% endif %}

      {% endif %}

    {% endfor %}

    {{ return(successful_models) }}

  {% endif %}

{%- endmacro %}
