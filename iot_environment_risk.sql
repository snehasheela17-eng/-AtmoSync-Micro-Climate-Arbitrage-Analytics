WITH cleaned_data AS (

    SELECT
        *
    FROM {{ ref('iot_sensor_cleaned') }}

),

risk_calculation AS (

    SELECT
        timestamp,
        city,
        state,
        latitude,
        longitude,

        temperature_c,
        humidity_pct,
        soil_moisture_pct,
        leaf_wetness_pct,
        co2_ppm,
        light_intensity_lux,
        wind_speed_kmh,
        rainfall_mm,
        sensor_status,

        CASE
            WHEN temperature_c >= 35 THEN 'HIGH'
            WHEN temperature_c >= 30 THEN 'MEDIUM'
            ELSE 'LOW'
        END AS temperature_risk,

        CASE
            WHEN humidity_pct >= 85 THEN 'HIGH'
            WHEN humidity_pct >= 70 THEN 'MEDIUM'
            ELSE 'LOW'
        END AS humidity_risk,

        CASE
            WHEN soil_moisture_pct < 30 THEN 'HIGH'
            WHEN soil_moisture_pct < 50 THEN 'MEDIUM'
            ELSE 'LOW'
        END AS soil_moisture_risk,

        CASE
            WHEN leaf_wetness_pct >= 80 THEN 'HIGH'
            WHEN leaf_wetness_pct >= 60 THEN 'MEDIUM'
            ELSE 'LOW'
        END AS leaf_wetness_risk,

        CASE
            WHEN rainfall_mm >= 10 THEN 'HIGH'
            WHEN rainfall_mm >= 5 THEN 'MEDIUM'
            ELSE 'LOW'
        END AS rainfall_risk

    FROM cleaned_data

)

SELECT
    *,
    
    CASE
        WHEN temperature_risk = 'HIGH'
          OR humidity_risk = 'HIGH'
          OR soil_moisture_risk = 'HIGH'
          OR leaf_wetness_risk = 'HIGH'
          OR rainfall_risk = 'HIGH'
            THEN 'HIGH'

        WHEN temperature_risk = 'MEDIUM'
          OR humidity_risk = 'MEDIUM'
          OR soil_moisture_risk = 'MEDIUM'
          OR leaf_wetness_risk = 'MEDIUM'
          OR rainfall_risk = 'MEDIUM'
            THEN 'MEDIUM'

        ELSE 'LOW'
    END AS overall_environmental_risk

FROM risk_calculation