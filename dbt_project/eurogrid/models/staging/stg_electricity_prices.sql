-- models/staging/stg_electricity_prices.sql
with source as (
    -- We now reference our "Base" model instead of a raw source
    select * from {{ ref('base_silver_prices') }}
),

renamed as (
    select
        timestamp as price_timestamp,
        country_code,
        country_name,
        price_eur_per_mwh,
        is_negative_price,
        is_suspicious_price,
        year,
        month,
        day,
        hour
    from source
    where is_suspicious_price = false
)

select * from renamed