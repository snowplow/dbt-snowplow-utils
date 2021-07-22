{# TODO: add test for compiler error if no columns matched #}

{% set actual_matched_columns = snowplow_utils.get_columns_in_relation_by_column_prefix(relation=ref('data_get_columns_in_relation_by_column_prefix'),
																																												column_prefix='a') %}

{% set actual_match_column_names = [] %}

{# actual_matched_columns is an array of column objects. These vary between warehouses so extract name #}
{% for col in actual_matched_columns %}
	{% do actual_match_column_names.append(col.name) %}
{% endfor %}

{% set expected_matched_column_names = ['a_1','a_2','a_3'] %}

{% if actual_match_column_names == expected_matched_column_names %}
	select 1 limit 0 -- Test passes if no rows returned
{% else %}
	select 1
{% endif %}
