with data as (
  select 
    cast(null as {{ dbt_utils.type_string() }}) as col_a3_field_1,
    cast('a' as {{ dbt_utils.type_string() }}) as col_a2_field_1,
    cast('b' as {{ dbt_utils.type_string() }}) as col_a1_field_1 --required to cast for RS.
)

, prep as (
  select
    {{ snowplow_utils.coalesce_field_paths(field_name='actual',
                                           field_paths_array=['col_a3_field_1','col_a2_field_1','col_a1_field_1']) }},
    cast('a' as {{ dbt_utils.type_string() }}) as expected
  from data
)

select *
from prep
where actual != expected
