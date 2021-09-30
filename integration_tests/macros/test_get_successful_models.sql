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
