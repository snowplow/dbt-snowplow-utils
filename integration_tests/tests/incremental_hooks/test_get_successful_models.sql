
{% set run_results = [{"status":"success", "node":{"name":"a"}},
                      {"status":"failure", "node":{"name":"b"}}, 
                      {"status":"success", "node":{"name":"c"}},
                      {"status":"success", "node":{"name":"d"}},] %}

{% set models_test_cases = [["a", "b", "d"], []] %}

{% set expected_successful_models_cases = [["a", "d"],["a", "c", "d"]] %}

{% set test_results = [] %}

{% for models in models_test_cases %}

  {% set expected_successful_models = expected_successful_models_cases[loop.index0] %}

  {% set actual_successful_models = snowplow_utils.get_successful_models(models, run_results) %}

  {% if expected_successful_models == actual_successful_models %}
    {% do test_results.append(true) %}
  {% else %}
    {% do test_results.append(false) %}
  {% endif %}

{% endfor %}

{% set num_tests_passed = test_results|sum() %}

{% set all_tests_passed = true if num_tests_passed == models_test_cases|length else false %}

{% if all_tests_passed %}
  select 1 limit 0 --test passes if no rows returned
{% else %}
  select 1 --test fails if rows returned
{% endif %}
