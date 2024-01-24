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
{% if target.type in ['snowflake'] %}
        object_construct('col1', 'alice', 'col2', 'bob', 'col3', 5) as non_array_structure,
        ARRAY_CONSTRUCT(object_construct('col1', 'alice', 'col2', 'bob', 'col3', 5), object_construct('col1', 'charlie', 'col2', 'doris', 'col3', 9)) as array_structure
{% elif target.type in ['bigquery'] %}
        struct('alice' as col1, 'bob' as col2, 5 as col3) as non_array_structure,
        [struct('alice' as col1, 'bob' as col2, 5 as col3), struct('charlie' as col1, 'doris' as col2, 9 as col3)] as array_structure
{% elif target.type in ['spark', 'databricks'] %}
        struct('alice' as col1, 'bob' as col2, 5 as col3) as non_array_structure,
        array(struct('alice' as col1, 'bob' as col2, 5 as col3), struct('charlie' as col1, 'doris' as col2, 9 as col3)) as array_structure
{% endif %}
