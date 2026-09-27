SELECT
    timestamp,
    city,
    state,

    -- Environmental conditions
    temperature_c,
    humidity_pct,
    wind_speed_kmh,
    rainfall_mm,
    overall_environmental_risk,

    -- Market information
    product,
    category,
    supply_level,
    demand_level,
    price_inr,
    price_change_pct,
    storage_risk,
    market_status,

    -- Demand vs supply gap
    ROUND(demand_level - supply_level, 2) AS demand_supply_gap,

    -- Price movement indicator
    CASE
        WHEN price_change_pct >= 5 THEN 'STRONG_PRICE_INCREASE'
        WHEN price_change_pct > 0 THEN 'PRICE_INCREASE'
        WHEN price_change_pct = 0 THEN 'NO_PRICE_CHANGE'
        ELSE 'PRICE_DECREASE'
    END AS price_movement,

    -- Arbitrage opportunity indicator
    CASE
        WHEN demand_level >= 80
             AND supply_level <= 30
             AND price_change_pct > 0
             AND overall_environmental_risk IN ('HIGH', 'MEDIUM')
        THEN 'HIGH_OPPORTUNITY'

        WHEN demand_level >= 70
             AND supply_level <= 50
             AND price_change_pct > 0
        THEN 'MODERATE_OPPORTUNITY'

        ELSE 'LOW_OPPORTUNITY'
    END AS arbitrage_opportunity

FROM {{ ref('microclimate_market_analysis') }}