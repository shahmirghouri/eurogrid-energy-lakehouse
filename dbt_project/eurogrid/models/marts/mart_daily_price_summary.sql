

with prices as (
    select * from {{ ref('stg_electricity_prices') }}

)

select
    country_code,
    country_name,
    year,
    month,
    day,
    

    round(avg(price_eur_per_mwh), 2)    as avg_price_eur_mwh,
    round(min(price_eur_per_mwh), 2)    as min_price_eur_mwh,
    round(max(price_eur_per_mwh), 2)    as max_price_eur_mwh,
    

    round(stddev(price_eur_per_mwh), 2) as price_volatility,
    

    sum(case when is_negative_price then 1 else 0 end) as negative_price_hours,
    count(*) as total_hours_with_data

from prices
group by 1, 2, 3, 4, 5
order by year, month, day, country_code