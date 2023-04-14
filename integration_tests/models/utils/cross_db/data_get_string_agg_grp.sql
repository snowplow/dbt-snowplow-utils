with data as (

  select
    'a' as string_col,
    'y' as group_col

  union all

  select
    'b' as string_col,
    'y' as group_col

  union all

  select
    'c' as string_col,
    'z' as group_col

  union all

  select
    'd' as string_col,
    'z' as group_col

)

select * from data
