with data as (

  select
    'b' as string_col,
    'a' as order_by_col

  union all

  select
    'c' as string_col,
    'c' as order_by_col

  union all

  select
    'a' as string_col,
    'b' as order_by_col

)

select * from data
