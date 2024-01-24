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

select
    {{snowplow_utils.get_field('non_array_structure', 'col1')}} as nas_col1,
    {{snowplow_utils.get_field('non_array_structure', 'col3')}} as nas_col3,
    {{snowplow_utils.get_field('non_array_structure', 'col1', table_alias = 'a')}} as nas_ta_col1,
    {{snowplow_utils.get_field('non_array_structure', 'col3', table_alias = 'a')}} as nas_ta_col3,
    {{snowplow_utils.get_field('non_array_structure', 'col1', type = 'string')}} as nas_ty_col1,
    {{snowplow_utils.get_field('non_array_structure', 'col3', type = 'string')}} as nas_ty_col3,
    {{snowplow_utils.get_field('non_array_structure', 'col1', table_alias = 'a', type = 'string')}} as nas_ta_ty_col1,
    {{snowplow_utils.get_field('non_array_structure', 'col3', table_alias = 'a', type = 'string')}} as nas_ta_ty_col3,

    {{snowplow_utils.get_field('array_structure', 'col1', array_index = 0)}} as as_col1_ind0,
    {{snowplow_utils.get_field('array_structure', 'col3', array_index = 0)}} as as_col3_ind0,
    {{snowplow_utils.get_field('array_structure', 'col1', table_alias = 'a', array_index = 0)}} as as_ta_col1_ind0,
    {{snowplow_utils.get_field('array_structure', 'col3', table_alias = 'a', array_index = 0)}} as as_ta_col3_ind0,
    {{snowplow_utils.get_field('array_structure', 'col1', table_alias = 'a', type = 'string', array_index = 0)}} as as_ta_ty_col1_ind0,
    {{snowplow_utils.get_field('array_structure', 'col3', table_alias = 'a', type = 'string', array_index = 0)}} as as_ta_ty_col3_ind0,
    {{snowplow_utils.get_field('array_structure', 'col1', type = 'string', array_index = 0)}} as as_ty_col1_ind0,
    {{snowplow_utils.get_field('array_structure', 'col3', type = 'string', array_index = 0)}} as as_ty_col3_ind0,
    {{snowplow_utils.get_field('array_structure', 'col1', table_alias = 'a', type = 'string', array_index = 1)}} as as_ta_ty_col1_ind1,
    {{snowplow_utils.get_field('array_structure', 'col3', table_alias = 'a', type = 'string', array_index = 1)}} as as_ta_ty_col3_ind1
from
    {{ ref('data_get_field') }} a
