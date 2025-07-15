{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{# Checks that user has accepted the Snowplow license terms #}

{% macro license_check(license_acceptance) %}

  {% if not license_acceptance %}
          {{ exceptions.raise_compiler_error(
            "🚫 Snowplow Error: To use this dbt package, you must accept the Snowplow Personal and Academic License or have a commercial agreement with Snowplow. See: https://docs.snowplow.io/personal-and-academic-license-1.0. To accept, set the  dbt_project.yml variable 'snowplow__license_accepted' to true."
      ) }}

  {% endif %}

{% endmacro %}
