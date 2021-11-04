-- Breaking convention and using a model to seed data rather than csv. 
-- Easier to construct RECORDs in sql than string in csv.
-- BQ Only 

{{ config(enabled=(target.type == 'bigquery' | as_bool()) )}}


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
    ) as product_v1

)

select * from data
