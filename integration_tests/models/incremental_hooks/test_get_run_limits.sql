{%- set data_query -%} 
  select * from {{ ref('data_get_run_limits') }}
{%- endset -%}

{# fetch test data set as dict. dict form {column_name: (tuple_of_results) #}
{%- set test_data = dbt_utils.get_query_results_as_dict(data_query) -%}

{% for i in range(test_data.min_last_success|length) %}

  {# iteratively pass each row of test data into get_run_limits() and execute returned query #}
  {%- set results = run_query(snowplow_utils.get_run_limits(test_data.min_last_success[i],
                                                           test_data.max_last_success[i], 
                                                           test_data.models_matched_from_manifest[i], 
                                                           test_data.has_matched_all_models[i], 
                                                           test_data.start_date[i])) -%}

  {# expected limits taken from test data #}
  {%- set expected_lower_limit = test_data.lower_limit[i] -%}
  {%- set expected_upper_limit = test_data.upper_limit[i] -%}

  {# actual limits taken from get_run_limits() results #}
  {%- if execute -%}
    {%- set actual_lower_limit = results.columns[0].values()[0] -%}
    {%- set actual_upper_limit = results.columns[1].values()[0] -%}
  {%- else -%}
    {%- set actual_lower_limit = none -%}
    {%- set actual_upper_limit = none -%}
  {%- endif -%}

  {# union expected vs. actual for each test case #}
  select
    {{snowplow_utils.cast_to_tstamp(expected_lower_limit)}} as expected_lower_limit,
    {{snowplow_utils.cast_to_tstamp(expected_upper_limit)}} as expected_upper_limit,
    {{snowplow_utils.cast_to_tstamp(actual_lower_limit)}} as actual_lower_limit,
    {{snowplow_utils.cast_to_tstamp(actual_upper_limit)}} as actual_upper_limit
  {% if not loop.last %} union all {% endif %}

{% endfor %}


