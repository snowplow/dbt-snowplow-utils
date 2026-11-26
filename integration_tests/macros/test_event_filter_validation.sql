{#
Copyright (c) 2026-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{# Called by integration_tests.sh: the valid case must pass, each invalid case must fail to compile. #}
{% macro test_event_filter_validation(case) %}
  {% set cases = {
    'valid': {'event_names': ['link_click'], 'event_vendor': 'com.acme', 'vendor_event_names': ['a']},
    'names_string': {'event_names': 'link_click'},
    'vendor_names_string': {'event_names': ['link_click'], 'event_vendor': 'com.acme', 'vendor_event_names': 'a'},
    'names_not_strings': {'event_names': [1, 2]},
    'vendor_list': {'event_names': ['link_click'], 'event_vendor': ['com.acme']},
    'not_mapping': ['link_click']
  } %}
  {% do snowplow_utils.validate_event_filter(cases[case]) %}
  {% do log("event_filter case '" ~ case ~ "' passed validation", info=true) %}
{% endmacro %}
