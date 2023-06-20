{{ config(
    materialized="view",
    schema="kpi_test"
) }}
select customerid,
        segment,
        country,
        sum(orderprofit) as profit
from {{ ref('stage_orders') }}
group by 
    customerid,
    segment,
    country