SELECT
    iot.timestamp,
    iot.city,
    iot.state,

    -- Environmental data
    iot.temperature_c,
    iot.humidity_pct,
    iot.wind_speed_kmh,
    iot.rainfall_mm,
    iot.overall_environmental_risk,

    -- Market data
    market.product,
    market.category,
    market.supply_level,
    market.demand_level,
    market.price_inr,
    market.price_change_pct,
    market.storage_risk,
    market.market_status

FROM {{ ref('iot_environment_risk') }} AS iot

INNER JOIN {{ ref('market_product_cleaned') }} AS market
    ON CAST(iot.timestamp AS DATE) = market.date
    AND LOWER(iot.city) = LOWER(market.city)