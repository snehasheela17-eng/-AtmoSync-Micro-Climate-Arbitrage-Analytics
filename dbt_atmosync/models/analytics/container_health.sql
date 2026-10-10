{{ config(materialized='view') }}

SELECT
    CONTAINER_ID,
    TIMESTAMP,
    TEMPERATURE,
    HUMIDITY,
    VIBRATION,

    CASE
        WHEN TEMPERATURE >= 35
          OR HUMIDITY >= 90
          OR VIBRATION >= 0.40
            THEN 'HIGH_RISK'

        WHEN TEMPERATURE >= 30
          OR HUMIDITY >= 80
          OR VIBRATION >= 0.25
            THEN 'MEDIUM_RISK'

        ELSE 'LOW_RISK'
    END AS ENVIRONMENT_RISK

FROM {{ ref('stg_iot') }}