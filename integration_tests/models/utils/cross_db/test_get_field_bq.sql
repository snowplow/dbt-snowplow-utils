{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{{
  config(
    tags = ['get_field'],
    )
}}

{% set test_1_actual = snowplow_utils.combine_column_versions(relation=ref('data_combine_column_versions'),
                                                              column_prefix='staff_v',
                                                              array_index=1) %}
{% if target.type == 'bigquery' %}
select
    {{snowplow_utils.get_field('staff_v_*', 'first_name', 'a', dbt.type_string(), 0, relation = ref('data_combine_column_versions'))}} as combined_column,
from
    {{ref('data_combine_column_versions')}} a
{% else %}
select 'John'  as combined_column
{% endif %}
