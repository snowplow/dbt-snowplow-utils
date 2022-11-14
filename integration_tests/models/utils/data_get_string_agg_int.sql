with data as (

  select
    10 as int_col,
    30 as order_by_col

  union all

  select
    3 as int_col,
    100 as order_by_col

  union all

  select
    1 as int_col,
    10 as order_by_col

)

select * from data
