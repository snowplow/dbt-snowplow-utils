{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{# TODO: add test for compiler error if no columns matched #}

{% set actual_matched_columns = snowplow_utils.get_columns_in_relation_by_column_prefix(relation=ref('data_get_columns_in_relation_by_column_prefix'),
																																												column_prefix='a') %}

{% set actual_match_column_names = [] %}

{# actual_matched_columns is an array of column objects. These vary between warehouses so extract name #}
{% for col in actual_matched_columns %}
	{% do actual_match_column_names.append(col.name) %}
{% endfor %}

{# Snowflake cols returned as uppercase #}
{% if target.type == 'snowflake' %}
	{% set expected_matched_column_names = ['A_1','A_2','A_3'] %}
{% else %}
	{% set expected_matched_column_names = ['a_1','a_2','a_3'] %}
{% endif %}

{% if actual_match_column_names == expected_matched_column_names %}
	select 1 as result limit 0 -- Test passes if no rows returned
{% else %}
	select 1 as result
{% endif %}
