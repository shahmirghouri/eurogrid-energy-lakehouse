-- Compare prices across countries at the same hour
-- Useful for spotting market imbalances

with prices as (
    select * from {{ ref('stg_electricity_prices') }}
),

pivoted as (
    select
        price_timestamp,
        year, month, day, hour,
        
        -- One column per country (FILTER is DuckDB's pivot syntax)
        avg(price_eur_per_mwh) filter (where country_code = 'BE')    as price_be,
        avg(price_eur_per_mwh) filter (where country_code = 'DE_LU') as price_de,
        avg(price_eur_per_mwh) filter (where country_code = 'FR')    as price_fr,
        avg(price_eur_per_mwh) filter (where country_code = 'NL')    as price_nl

    from prices
    group by 1, 2, 3, 4, 5
)

select
    *,
    -- Highest and lowest price country at each hour
    greatest(price_be, price_de, price_fr, price_nl) as max_country_price,
    least(price_be, price_de, price_fr, price_nl)    as min_country_price,
    
    -- Price spread between most and least expensive country
    round(
        greatest(price_be, price_de, price_fr, price_nl) - 
        least(price_be, price_de, price_fr, price_nl), 2
    ) as price_spread

from pivoted
order by price_timestamp