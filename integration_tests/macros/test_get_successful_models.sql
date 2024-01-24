{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{# get_successful_models() macro requires the run_results object. This is only generated at the end of the run.
  This macro is intended to be run on a 'on-run-end' hook, so the run_results object is present.
  It is possible to hardcode a run_results object, however the schema for this object can evolve with time.
  By letting dbt generate the object, we ensure we are testing against the correct schema.
#}

{% macro test_get_successful_models(enabled) -%}

  {% if enabled and execute %}
    {% set actual_successful_models = snowplow_utils.get_successful_models(models=['successful_model_1', 'fail_model', 'skip_model']) %}
    {% set expected_successful_models = ['successful_model_1'] %}

    {% if actual_successful_models == expected_successful_models %}
      {%- do log("Pass: test_get_successful_models()", info=true) -%}
    {% else %}
      {%- do log("Fail: Actual successful models: "~actual_successful_models~", Expected successful models: "~expected_successful_models, info=true) -%}
    {% endif %}

  {% endif %}

{% endmacro %}
