{{ config(materialized='view') }}

WITH commodity_risk AS (

    SELECT
        DATE,
        CITY,
        PRODUCT,

        CASE
            WHEN MAX(
                CASE
                    WHEN STORAGE_RISK = 'HIGH' THEN 3
                    WHEN STORAGE_RISK = 'MEDIUM' THEN 2
                    WHEN STORAGE_RISK = 'LOW' THEN 1
                    ELSE 0
                END
            ) = 3 THEN 'HIGH'

            WHEN MAX(
                CASE
                    WHEN STORAGE_RISK = 'HIGH' THEN 3
                    WHEN STORAGE_RISK = 'MEDIUM' THEN 2
                    WHEN STORAGE_RISK = 'LOW' THEN 1
                    ELSE 0
                END
            ) = 2 THEN 'MEDIUM'

            ELSE 'LOW'
        END AS STORAGE_RISK

    FROM {{ ref('stg_commodity') }}

    GROUP BY
        DATE,
        CITY,
        PRODUCT
),

spoilage_analysis AS (

    SELECT
        a.DATE,
        a.SOURCE_CITY,
        a.DESTINATION_CITY,
        a.PRODUCT,
        a.CATEGORY,
        a.SOURCE_PRICE,
        a.DISTANCE_KM,

        c.STORAGE_RISK,

        s.ESTIMATED_SPOILAGE_HOURS,

        a.DISTANCE_KM / 50.0 AS ESTIMATED_TRAVEL_HOURS,

        a.SOURCE_PRICE - a.TRANSPORT_COST
            AS NET_VALUE_AFTER_TRANSPORT,

        a.ARBITRAGE_OPPORTUNITY

    FROM {{ ref('arbitrage_analysis') }} a

    LEFT JOIN commodity_risk c
        ON c.DATE = a.DATE
        AND c.CITY = a.SOURCE_CITY
        AND c.PRODUCT = a.PRODUCT

    LEFT JOIN {{ ref('spoilage_config') }} s
        ON c.STORAGE_RISK = s.STORAGE_RISK
)

SELECT
    DATE,
    SOURCE_CITY,
    DESTINATION_CITY,
    PRODUCT,
    CATEGORY,
    SOURCE_PRICE,
    DISTANCE_KM,
    STORAGE_RISK,
    ESTIMATED_SPOILAGE_HOURS,
    ESTIMATED_TRAVEL_HOURS,
    NET_VALUE_AFTER_TRANSPORT,

    CASE
        WHEN ESTIMATED_TRAVEL_HOURS <= ESTIMATED_SPOILAGE_HOURS
            THEN 'WITHIN_SPOILAGE_WINDOW'
        ELSE 'EXCEEDS_SPOILAGE_WINDOW'
    END AS SPOILAGE_FEASIBILITY,

    CASE
        WHEN ESTIMATED_TRAVEL_HOURS <= ESTIMATED_SPOILAGE_HOURS
             AND ARBITRAGE_OPPORTUNITY = 'HIGH'
            THEN 'HIGH_SPOILAGE_ARBITRAGE'

        WHEN ESTIMATED_TRAVEL_HOURS <= ESTIMATED_SPOILAGE_HOURS
             AND ARBITRAGE_OPPORTUNITY = 'MODERATE'
            THEN 'MODERATE_SPOILAGE_ARBITRAGE'

        ELSE 'LOW_SPOILAGE_ARBITRAGE'
    END AS SPOILAGE_ARBITRAGE_OPPORTUNITY

FROM spoilage_analysis