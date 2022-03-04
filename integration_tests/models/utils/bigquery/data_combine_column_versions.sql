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
    [struct(
        'current' as status,
        '123 First Avenue' as address,
        'Seattle' as city,
        'WA' as state,
        '11111' as zip,
        '1' as numberOfYears
    )] as address_v2,
    [struct(
        'previous' as status,
        '321 Main Street' as address,
        'Hoboken' as city,
        'NJ' as state,
        '44444' as zip,
        '3' as numberOfYears
    )] as address_v1

)

select * from data
