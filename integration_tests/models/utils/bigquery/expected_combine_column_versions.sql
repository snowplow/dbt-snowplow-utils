
with prep as (
  select   
    -- Test 1
    coalesce(staff_v2[safe_offset(1)].first_name, staff_v1[safe_offset(1)].first_name) as first_name,
    coalesce(staff_v2[safe_offset(1)].last_name) as last_name,
    coalesce(staff_v1[safe_offset(1)].age) as age,
    -- Test 2
    coalesce(product_v2.name, product_v1.name) as name,
    coalesce(product_v2.specs.power_rating, product_v1.specs.power_rating) as product_power_rating,
    coalesce(product_v2.specs.accessories, product_v1.specs.accessories) as specs_accessories,
    -- Test 3
    coalesce(product_v2.specs.power_rating, product_v1.specs.power_rating) as specs_power_rating,
    coalesce(product_v2.specs.volume) as specs_volume,
    coalesce(product_v2.specs.accessories, product_v1.specs.accessories) as specs_accessories,
    -- Test 4
    coalesce(product_v2.specs.volume) as product_volume,
    -- Test 5
    coalesce(address_v2.status, address_v1.status) as status,
    coalesce(address_v2.address, address_v1.address) as address,
    coalesce(address_v2.city, address_v1.city) as city,
    coalesce(address_v2.state, address_v1.state) as state,
    coalesce(address_v2.zip, address_v1.zip) as zip,
    coalesce(address_v2.numberOfYears, address_v1.numberOfYears) as numberOfYears

  from {{ ref('data_combine_column_versions') }}
)

select 
   concat("[", string_agg(to_json_string(p), ","), "]") as summary

from prep as p
