{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{# Deletes specified models from the incremental_manifest table #}
{% macro licence_check() %}

  {% if not var('snowplow__license_accepted') %}
          {{ exceptions.raise_compiler_error(
            "🚫 Snowplow Error: License not accepted. Please set dbt_project.yml variable 'snowplow__license_accepted' to true in snowplow-utils to proceed."
      ) }}
  {% endif %}

{% endmacro %}
