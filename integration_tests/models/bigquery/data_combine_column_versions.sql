-- Breaking convention and using a model to seed data rather than csv. 
-- Easier to construct RECORDs in sql than string in csv.
-- BQ Only 

{{ config(enabled=(target.type == 'bigquery' | as_bool()),
          tags=["requires_script"] )}}


with data as (
  select 1 as x, 5 as y, 2 as z
)

, prep as (
select 
  x, 
  array_agg(struct(y, z)) as array_of_structs_1,
  array_agg(struct(y+1 as y, z+1 as z)) as array_of_structs_2
from data 
group by x
)

select 
    x,
    array_of_structs_1,
    array_of_structs_2,
    array_of_structs_1[safe_offset(0)] as simple_struct_1,
    array_of_structs_2[safe_offset(0)] as simple_struct_2,

from prep
