WITH source_data AS (

    SELECT
        date,
        city,
        state,
        product,
        category,
        supply_level,
        demand_level,
        price_inr,
        price_change_pct,
        storage_risk,
        market_status

    FROM ATMOSYNC_DB.RAW.MARKET_PRODUCT_DATA

),

cleaned_data AS (

    SELECT
        date,
        city,
        state,
        product,
        category,

        ROUND(supply_level, 2) AS supply_level,
        ROUND(demand_level, 2) AS demand_level,
        ROUND(price_inr, 2) AS price_inr,
        ROUND(price_change_pct, 2) AS price_change_pct,

        storage_risk,
        market_status

    FROM source_data

    WHERE date IS NOT NULL
      AND city IS NOT NULL
      AND product IS NOT NULL
      AND price_inr IS NOT NULL

)

SELECT *
FROM cleaned_data
