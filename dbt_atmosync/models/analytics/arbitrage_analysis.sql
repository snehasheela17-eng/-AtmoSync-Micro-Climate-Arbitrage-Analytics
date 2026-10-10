{{ config(materialized='view') }}

SELECT
    m.DATE,
    m.CITY AS SOURCE_CITY,
    m.PRODUCT,
    m.CATEGORY,
    m.AVG_PRICE_INR AS SOURCE_PRICE,
    r.DESTINATION_CITY,
    r.DISTANCE_KM,
    t.TRANSPORT_COST_PER_KM,
    r.DISTANCE_KM * t.TRANSPORT_COST_PER_KM AS TRANSPORT_COST,
    d.AVG_DEMAND_LEVEL,
    d.AVG_SUPPLY_LEVEL,

    m.AVG_PRICE_INR
        - (r.DISTANCE_KM * t.TRANSPORT_COST_PER_KM)
        AS NET_PRICE_AFTER_TRANSPORT,

    CASE
        WHEN d.AVG_DEMAND_LEVEL >= 70
             AND d.AVG_SUPPLY_LEVEL <= 50
             AND r.DISTANCE_KM <= 400
            THEN 'HIGH'

        WHEN d.AVG_DEMAND_LEVEL >= 50
             AND d.AVG_SUPPLY_LEVEL <= 70
            THEN 'MODERATE'

        ELSE 'LOW'
    END AS ARBITRAGE_OPPORTUNITY

FROM {{ ref('daily_market_summary') }} m

JOIN {{ ref('route_distance') }} r
    ON m.CITY = r.SOURCE_CITY

CROSS JOIN {{ ref('transport_cost_config') }} t

LEFT JOIN {{ ref('daily_market_summary') }} d
    ON d.DATE = m.DATE
    AND d.PRODUCT = m.PRODUCT
    AND d.CITY = r.DESTINATION_CITY

WHERE m.AVG_PRICE_INR IS NOT NULL
