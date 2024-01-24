{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{# Tests:
   1 - Find all enabled models with the snowplow_web_incremental tag
   2 - Find subset of enabled and tagged models using the dbt ls command. Performed via bash script
   3 - Test error message for untagged models. Performed by changing 'tag_var' via bash script. #}

{{ config(tags=["requires_script"]) }}

{% set dummy_graph={
    'nodes':
    {
        'model.snowplow.enabled_model_w_dependency':
        {
            'config': {'enabled': true},
            'depends_on':{'nodes':['model.snowplow_utils_integration_tests.snowplow_base_events_this_run']},
            'resource_type': 'model',
            'tags':[var("tag_var",'snowplow_incremental')],
            "name": "enabled_model_w_dependency"
        },
        'model.snowplow.enabled_model_wo_dependency':
        {
            'config': {'enabled': true},
            'depends_on':{'nodes':['model.snowplow_web.snowplow_web_sessions']},
            'resource_type': 'model',
            'tags':[var("tag_var",'snowplow_incremental')],
            "name": "enabled_model_wo_dependency"
        },
        'model.snowplow.disabled_model':
        {
            'config': {'enabled': false},
            'depends_on':{'nodes':['model.snowplow_utils_integration_tests.snowplow_base_events_this_run']},
            'resource_type': 'model',
            'tags':[var("tag_var",'snowplow_incremental')],
            "name": "disabled_model"
        },
        'model.non_snowplow_model':
        {
            'config': {'enabled': true},
            'depends_on':{'nodes':['model.dummy']},
            'resource_type': 'model',
            'tags':['dummy'],
            "name": "non_snowplow_model"
        },
        'test.non_model':
        {
            'config': {'enabled': true},
            'depends_on':{'nodes':['model.snowplow_utils_integration_tests.snowplow_base_events_this_run']},
            'resource_type': 'test',
            "name": "non_model"
        }
    }
} %}

{% set actual_enabled_models = snowplow_utils.get_enabled_snowplow_models('snowplow', graph_object=dummy_graph) %}

{% if var("models_to_run","")|length %}
    {# Test 2 #}
    {% set expected_enabled_models = ['enabled_model_w_dependency'] %}
{% else %}
    {# Test 1 #}
    {% set expected_enabled_models = ['enabled_model_w_dependency', 'enabled_model_wo_dependency'] %}
{% endif %}


{% if actual_enabled_models == expected_enabled_models %}
    select 1 as result limit 0 {# returns no rows therefore test passes #}
{% else %}
    select 1 as result {# returns rows therefore test fails #}
{% endif %}
