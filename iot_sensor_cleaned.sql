WITH source_data AS (

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
        sensor_status

    FROM ATMOSYNC_DB.RAW.IOT_SENSOR_DATA

),

cleaned_data AS (

    SELECT
        timestamp,
        city,
        state,
        latitude,
        longitude,

        ROUND(temperature_c, 2) AS temperature_c,
        ROUND(humidity_pct, 2) AS humidity_pct,
        ROUND(soil_moisture_pct, 2) AS soil_moisture_pct,
        ROUND(leaf_wetness_pct, 2) AS leaf_wetness_pct,
        ROUND(co2_ppm, 2) AS co2_ppm,
        ROUND(light_intensity_lux, 2) AS light_intensity_lux,
        ROUND(wind_speed_kmh, 2) AS wind_speed_kmh,
        ROUND(rainfall_mm, 2) AS rainfall_mm,

        sensor_status

    FROM source_data

    WHERE timestamp IS NOT NULL
      AND city IS NOT NULL
      AND temperature_c IS NOT NULL
      AND humidity_pct IS NOT NULL

)

SELECT *
FROM cleaned_data