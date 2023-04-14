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
