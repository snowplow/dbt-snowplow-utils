{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
-- Breaking convention and using a model to seed data rather than csv.
-- Easier to construct RECORDs in sql than string in csv.
-- BQ Only

with data as (
  select
    1 as id,
    [
      struct('John' as first_name, 'Scott' as last_name),
      struct('Jane' as first_name, 'Scott' as last_name)
    ] as staff_v2,
    [
      struct('Bill' as first_name, 30 as age),
      struct('Ben' as first_name, 55 as age)
    ] as staff_v1,
    struct(
        'Bookshelf Speaker' as name,
        struct(
            '100W' as power_rating,
            '25dB' as volume,
            [
              struct('stand' as name, 22 as price),
              struct('cable' as name, 10 as price)
            ] as accessories
        ) as specs
    ) as product_v2,
    struct(
        'Floorstanding Speaker' as name,
        struct(
            '50W' as power_rating,
            [
              struct('remote' as name, 22 as price)
            ] as accessories
        ) as specs
    ) as product_v1,
    struct('Alice' as f_name, 29 as numeric_col) as person_1_0_0,
    struct('Bob' as f_name, 30 as numeric_col) as person_1_0_1,
    struct('Charlie' as f_name, 31 as numeric_col) as person_1_0_2,
    struct('Dog the bounty' as f_name, 50 as numeric_col) as person_1_1_0,
    struct('Dog the bounty' as f_name, 50 as numeric_col) as person_1_1_30,
    struct('Dog the bounty' as f_name, 50 as numeric_col) as person_1_1_31,
    struct('a' as field_1, 2 as field_2) as test_1_0_0,
    struct('b' as field_1, 3 as field_2) as test_1_1_0,
    struct('b' as field_1, 3 as field_2) as test_1_1_15,
    struct('c' as field_1, 4 as field_2) as test_new_1_0_0

)

select * from data
