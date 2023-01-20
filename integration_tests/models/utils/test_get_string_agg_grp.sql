with data as (
select * from {{ ref('data_get_string_agg_grp') }}
)

select{{ snowplow_utils.get_string_agg('string_col', 'd') }} as result, group_col as grp from data d
group by group_col
