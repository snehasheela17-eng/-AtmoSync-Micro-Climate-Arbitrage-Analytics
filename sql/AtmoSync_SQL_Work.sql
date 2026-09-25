-- =====================================================
-- ATMOSYNC SQL WORK FILE
-- Project: AtmoSync - Micro-Climate Arbitrage Analytics
-- Database: ATMOSYNC_DB
-- Schema: RAW
-- =====================================================


-- =====================================================
-- 1. DATABASE & TABLE SETUP
-- =====================================================

SHOW SCHEMAS IN DATABASE ATMOSYNC_DB;

SHOW TABLES IN SCHEMA ATMOSYNC_DB.RAW;


-- =====================================================
-- 2. RAW DATA EXPLORATION
-- =====================================================

-- Check total number of rows

SELECT COUNT(*)
FROM ATMOSYNC_DB.RAW.WEATHER_DATA;


-- View sample records

SELECT *
FROM ATMOSYNC_DB.RAW.WEATHER_DATA
LIMIT 10;


-- =====================================================
-- 3. INITIAL DATA QUALITY CHECK
-- =====================================================

SELECT
    COUNT(*) AS total_rows,
    COUNT(temperature) AS temperature_filled,
    COUNT(humidity) AS humidity_filled,
    COUNT(precipitation) AS precipitation_filled,
    COUNT(wind_speed) AS wind_speed_filled,
    COUNT(weather) AS weather_filled
FROM ATMOSYNC_DB.RAW.WEATHER_DATA;


-- =====================================================
-- 4. CREATE CLEANED TABLE
-- =====================================================

CREATE OR REPLACE TABLE ATMOSYNC_DB.RAW.WEATHER_DATA_CLEANED AS
SELECT DISTINCT
    city,
    latitude,
    longitude,
    temperature,
    humidity,
    precipitation,
    wind_speed,
    weather,
    timestamp
FROM ATMOSYNC_DB.RAW.WEATHER_DATA;


-- =====================================================
-- 5. TEMPERATURE CLEANING
-- =====================================================

-- Check invalid temperature values

SELECT
    timestamp,
    temperature
FROM ATMOSYNC_DB.RAW.WEATHER_DATA_FINAL
WHERE temperature IS NOT NULL
  AND (temperature >= 100 OR temperature <= -50)
ORDER BY timestamp;


-- Check missing temperature values
-- and their neighboring values

SELECT
    timestamp,
    previous_temperature,
    next_temperature
FROM (
    SELECT
        timestamp,
        temperature,
        LAG(temperature) IGNORE NULLS
            OVER (ORDER BY timestamp) AS previous_temperature,
        LEAD(temperature) IGNORE NULLS
            OVER (ORDER BY timestamp) AS next_temperature
    FROM ATMOSYNC_DB.RAW.WEATHER_DATA_FINAL
)
WHERE temperature IS NULL
ORDER BY timestamp;


-- Check suspicious -45 temperature values

SELECT
    timestamp,
    temperature
FROM ATMOSYNC_DB.RAW.WEATHER_DATA_FINAL
WHERE temperature = -45
ORDER BY timestamp;


-- Verify suspicious temperature values in cleaned table

SELECT
    timestamp,
    temperature
FROM ATMOSYNC_DB.RAW.WEATHER_DATA_CLEANED
WHERE temperature = -45
ORDER BY timestamp;


-- FINAL METHOD USED:
-- Replace -45 with NULL and fill using neighboring values

CREATE OR REPLACE TABLE ATMOSYNC_DB.RAW.WEATHER_DATA_FINAL AS
WITH base AS (
    SELECT
        city,
        latitude,
        longitude,
        NULLIF(temperature, -45) AS temperature,
        humidity,
        precipitation,
        wind_speed,
        weather,
        timestamp
    FROM ATMOSYNC_DB.RAW.WEATHER_DATA_FINAL
),
filled AS (
    SELECT
        city,
        latitude,
        longitude,
        COALESCE(
            temperature,
            (
                LAG(temperature) IGNORE NULLS
                    OVER (ORDER BY timestamp)
                +
                LEAD(temperature) IGNORE NULLS
                    OVER (ORDER BY timestamp)
            ) / 2,
            LEAD(temperature) IGNORE NULLS
                OVER (ORDER BY timestamp)
        ) AS temperature,
        humidity,
        precipitation,
        wind_speed,
        weather,
        timestamp
    FROM base
)
SELECT *
FROM filled;


-- Verify no invalid -45 values remain

SELECT
    timestamp,
    temperature
FROM ATMOSYNC_DB.RAW.WEATHER_DATA_FINAL
WHERE temperature = -45
ORDER BY timestamp;


-- Verify missing temperature count

SELECT
    COUNT_IF(temperature IS NULL) AS missing_temperature
FROM ATMOSYNC_DB.RAW.WEATHER_DATA_FINAL;


-- =====================================================
-- 6. HUMIDITY CLEANING
-- =====================================================

-- Check invalid humidity values

SELECT
    timestamp,
    humidity
FROM ATMOSYNC_DB.RAW.WEATHER_DATA_FINAL
WHERE humidity IS NOT NULL
  AND (humidity < 0 OR humidity > 100)
ORDER BY timestamp;


-- Check missing humidity values
-- and neighboring values

SELECT
    timestamp,
    previous_humidity,
    next_humidity
FROM (
    SELECT
        timestamp,
        humidity,
        LAG(humidity) IGNORE NULLS
            OVER (ORDER BY timestamp) AS previous_humidity,
        LEAD(humidity) IGNORE NULLS
            OVER (ORDER BY timestamp) AS next_humidity
    FROM ATMOSYNC_DB.RAW.WEATHER_DATA_FINAL
)
WHERE humidity IS NULL
ORDER BY timestamp;


-- Check very low humidity values

SELECT
    timestamp,
    humidity
FROM ATMOSYNC_DB.RAW.WEATHER_DATA_FINAL
WHERE humidity IS NOT NULL
  AND humidity < 1
ORDER BY timestamp;


-- FINAL METHOD USED:
-- Convert fraction-style humidity values to percentage

CREATE OR REPLACE TABLE ATMOSYNC_DB.RAW.WEATHER_DATA_FINAL AS
SELECT
    city,
    latitude,
    longitude,
    temperature,
    CASE
        WHEN humidity IS NOT NULL
             AND humidity < 1
        THEN humidity * 100
        ELSE humidity
    END AS humidity,
    precipitation,
    wind_speed,
    weather,
    timestamp
FROM ATMOSYNC_DB.RAW.WEATHER_DATA_FINAL;


-- Fill missing humidity values

CREATE OR REPLACE TABLE ATMOSYNC_DB.RAW.WEATHER_DATA_FINAL AS
WITH filled AS (
    SELECT
        city,
        latitude,
        longitude,
        temperature,
        COALESCE(
            humidity,
            (
                LAG(humidity) IGNORE NULLS
                    OVER (ORDER BY timestamp)
                +
                LEAD(humidity) IGNORE NULLS
                    OVER (ORDER BY timestamp)
            ) / 2,
            LEAD(humidity) IGNORE NULLS
                OVER (ORDER BY timestamp)
        ) AS humidity,
        precipitation,
        wind_speed,
        weather,
        timestamp
    FROM ATMOSYNC_DB.RAW.WEATHER_DATA_FINAL
)
SELECT *
FROM filled;


-- Verify missing humidity values

SELECT
    COUNT_IF(humidity IS NULL) AS missing_humidity
FROM ATMOSYNC_DB.RAW.WEATHER_DATA_FINAL;


-- =====================================================
-- 7. PRECIPITATION CLEANING
-- =====================================================

-- Check suspicious precipitation values

SELECT
    timestamp,
    precipitation
FROM ATMOSYNC_DB.RAW.WEATHER_DATA_FINAL
WHERE precipitation >= 100
ORDER BY timestamp;


-- Verify suspicious precipitation values in cleaned table

SELECT
    timestamp,
    precipitation
FROM ATMOSYNC_DB.RAW.WEATHER_DATA_CLEANED
WHERE precipitation >= 100
ORDER BY timestamp;


-- FINAL METHOD USED:
-- Replace 500 and 999 with NULL and fill using neighbors

CREATE OR REPLACE TABLE ATMOSYNC_DB.RAW.WEATHER_DATA_FINAL AS
WITH base AS (
    SELECT
        city,
        latitude,
        longitude,
        temperature,
        humidity,
        NULLIF(NULLIF(precipitation, 500), 999)
            AS precipitation,
        wind_speed,
        weather,
        timestamp
    FROM ATMOSYNC_DB.RAW.WEATHER_DATA_FINAL
),
filled AS (
    SELECT
        city,
        latitude,
        longitude,
        temperature,
        humidity,
        COALESCE(
            precipitation,
            (
                LAG(precipitation) IGNORE NULLS
                    OVER (ORDER BY timestamp)
                +
                LEAD(precipitation) IGNORE NULLS
                    OVER (ORDER BY timestamp)
            ) / 2,
            LEAD(precipitation) IGNORE NULLS
                OVER (ORDER BY timestamp)
        ) AS precipitation,
        wind_speed,
        weather,
        timestamp
    FROM base
)
SELECT *
FROM filled;


-- Verify no suspicious precipitation values remain

SELECT
    timestamp,
    precipitation
FROM ATMOSYNC_DB.RAW.WEATHER_DATA_FINAL
WHERE precipitation >= 100
ORDER BY timestamp;


-- =====================================================
-- 8. WIND SPEED CLEANING
-- =====================================================

-- Check suspicious wind speed values

SELECT
    timestamp,
    wind_speed
FROM ATMOSYNC_DB.RAW.WEATHER_DATA_FINAL
WHERE wind_speed >= 100
ORDER BY timestamp;


-- Verify suspicious wind values in cleaned table

SELECT
    timestamp,
    wind_speed
FROM ATMOSYNC_DB.RAW.WEATHER_DATA_CLEANED
WHERE wind_speed >= 100
ORDER BY timestamp;


-- FINAL METHOD USED:
-- Replace 300 and 999 with NULL and fill using neighbors

CREATE OR REPLACE TABLE ATMOSYNC_DB.RAW.WEATHER_DATA_FINAL AS
WITH base AS (
    SELECT
        city,
        latitude,
        longitude,
        temperature,
        humidity,
        precipitation,
        NULLIF(NULLIF(wind_speed, 300), 999)
            AS wind_speed,
        weather,
        timestamp
    FROM ATMOSYNC_DB.RAW.WEATHER_DATA_CLEANED
),
filled AS (
    SELECT
        city,
        latitude,
        longitude,
        temperature,
        humidity,
        precipitation,
        COALESCE(
            wind_speed,
            (
                LAG(wind_speed) IGNORE NULLS
                    OVER (ORDER BY timestamp)
                +
                LEAD(wind_speed) IGNORE NULLS
                    OVER (ORDER BY timestamp)
            ) / 2,
            LEAD(wind_speed) IGNORE NULLS
                OVER (ORDER BY timestamp)
        ) AS wind_speed,
        weather,
        timestamp
    FROM base
)
SELECT *
FROM filled;


-- Verify no suspicious wind values remain

SELECT
    timestamp,
    wind_speed
FROM ATMOSYNC_DB.RAW.WEATHER_DATA_FINAL
WHERE wind_speed >= 100
ORDER BY timestamp;


-- =====================================================
-- 9. WEATHER CODE CLEANING
-- =====================================================

-- Check weather code distribution

SELECT
    weather,
    COUNT(*) AS count
FROM ATMOSYNC_DB.RAW.WEATHER_DATA_FINAL
GROUP BY weather
ORDER BY count DESC;


-- Check missing weather codes

SELECT
    timestamp,
    weather
FROM ATMOSYNC_DB.RAW.WEATHER_DATA_FINAL
WHERE weather IS NULL
ORDER BY timestamp;


-- Preview neighboring weather codes

SELECT
    timestamp,
    weather,
    previous_weather,
    next_weather,
    CASE
        WHEN previous_weather = next_weather
            THEN previous_weather

        WHEN previous_weather IS NULL
            THEN next_weather

        WHEN next_weather IS NULL
            THEN previous_weather

        ELSE
            CASE
                WHEN DATEDIFF(
                    'second',
                    previous_timestamp,
                    timestamp
                )
                <=
                DATEDIFF(
                    'second',
                    timestamp,
                    next_timestamp
                )
                THEN previous_weather
                ELSE next_weather
            END
    END AS suggested_weather
FROM (
    SELECT
        timestamp,
        weather,

        LAG(weather) IGNORE NULLS
            OVER (ORDER BY timestamp) AS previous_weather,

        LEAD(weather) IGNORE NULLS
            OVER (ORDER BY timestamp) AS next_weather,

        LAG(timestamp) IGNORE NULLS
            OVER (ORDER BY timestamp) AS previous_timestamp,

        LEAD(timestamp) IGNORE NULLS
            OVER (ORDER BY timestamp) AS next_timestamp

    FROM ATMOSYNC_DB.RAW.WEATHER_DATA_FINAL
)
WHERE weather IS NULL
ORDER BY timestamp;


-- FINAL METHOD USED:
-- If previous and next codes are same, use that.
-- Otherwise use the nearest timestamp.

CREATE OR REPLACE TABLE ATMOSYNC_DB.RAW.WEATHER_DATA_FINAL AS
SELECT
    city,
    latitude,
    longitude,
    temperature,
    humidity,
    precipitation,
    wind_speed,

    COALESCE(
        weather,

        CASE
            WHEN previous_weather = next_weather
                THEN previous_weather

            WHEN previous_weather IS NULL
                THEN next_weather

            WHEN next_weather IS NULL
                THEN previous_weather

            ELSE
                CASE
                    WHEN DATEDIFF(
                        'second',
                        previous_timestamp,
                        timestamp
                    )
                    <=
                    DATEDIFF(
                        'second',
                        timestamp,
                        next_timestamp
                    )
                    THEN previous_weather
                    ELSE next_weather
                END
        END
    ) AS weather,

    timestamp

FROM (
    SELECT
        *,

        LAG(weather) IGNORE NULLS
            OVER (ORDER BY timestamp) AS previous_weather,

        LEAD(weather) IGNORE NULLS
            OVER (ORDER BY timestamp) AS next_weather,

        LAG(timestamp) IGNORE NULLS
            OVER (ORDER BY timestamp) AS previous_timestamp,

        LEAD(timestamp) IGNORE NULLS
            OVER (ORDER BY timestamp) AS next_timestamp

    FROM ATMOSYNC_DB.RAW.WEATHER_DATA_FINAL
);


-- Verify no missing weather codes remain

SELECT
    COUNT_IF(weather IS NULL) AS missing_weather
FROM ATMOSYNC_DB.RAW.WEATHER_DATA_FINAL;


-- Check final weather-code distribution

SELECT
    weather,
    COUNT(*) AS count
FROM ATMOSYNC_DB.RAW.WEATHER_DATA_FINAL
GROUP BY weather
ORDER BY weather;


-- =====================================================
-- 10. FINAL DATA VALIDATION
-- =====================================================

SELECT
    COUNT(*) AS total_rows,

    COUNT_IF(city IS NULL)
        AS missing_city,

    COUNT_IF(latitude IS NULL)
        AS missing_latitude,

    COUNT_IF(longitude IS NULL)
        AS missing_longitude,

    COUNT_IF(temperature IS NULL)
        AS missing_temperature,

    COUNT_IF(humidity IS NULL)
        AS missing_humidity,

    COUNT_IF(precipitation IS NULL)
        AS missing_precipitation,

    COUNT_IF(wind_speed IS NULL)
        AS missing_wind_speed,

    COUNT_IF(weather IS NULL)
        AS missing_weather,

    COUNT_IF(timestamp IS NULL)
        AS missing_timestamp

FROM ATMOSYNC_DB.RAW.WEATHER_DATA_FINAL;


-- =====================================================
-- 11. WEATHER DATA ANALYSIS
-- =====================================================

-- Overall statistics

SELECT
    COUNT(*) AS total_rows,

    AVG(temperature) AS avg_temperature,
    MIN(temperature) AS min_temperature,
    MAX(temperature) AS max_temperature,

    AVG(humidity) AS avg_humidity,
    MIN(humidity) AS min_humidity,
    MAX(humidity) AS max_humidity,

    AVG(precipitation) AS avg_precipitation,
    MIN(precipitation) AS min_precipitation,
    MAX(precipitation) AS max_precipitation,

    AVG(wind_speed) AS avg_wind_speed,
    MIN(wind_speed) AS min_wind_speed,
    MAX(wind_speed) AS max_wind_speed

FROM ATMOSYNC_DB.RAW.WEATHER_DATA_FINAL;


-- Time coverage

SELECT
    MIN(timestamp) AS start_timestamp,
    MAX(timestamp) AS end_timestamp,
    COUNT(DISTINCT CAST(timestamp AS DATE))
        AS distinct_dates,
    COUNT(DISTINCT EXTRACT(HOUR FROM timestamp))
        AS distinct_hours
FROM ATMOSYNC_DB.RAW.WEATHER_DATA_FINAL;


-- Daily weather summary

SELECT
    CAST(timestamp AS DATE) AS date,

    AVG(temperature) AS avg_temperature,

    AVG(humidity) AS avg_humidity,

    SUM(precipitation) AS total_precipitation,

    AVG(wind_speed) AS avg_wind_speed

FROM ATMOSYNC_DB.RAW.WEATHER_DATA_FINAL

GROUP BY CAST(timestamp AS DATE)

ORDER BY date;


-- Hourly coverage

SELECT
    CAST(timestamp AS DATE) AS date,
    EXTRACT(HOUR FROM timestamp) AS hour,
    COUNT(*) AS observations
FROM ATMOSYNC_DB.RAW.WEATHER_DATA_FINAL
GROUP BY
    CAST(timestamp AS DATE),
    EXTRACT(HOUR FROM timestamp)
ORDER BY
    date,
    hour;


-- =====================================================
-- 12. MISSING TIMESTAMP ANALYSIS
-- =====================================================

-- Find missing hourly timestamps

WITH time_range AS (
    SELECT
        MIN(timestamp) AS start_time,
        MAX(timestamp) AS end_time
    FROM ATMOSYNC_DB.RAW.WEATHER_DATA_FINAL
),

expected_hours AS (
    SELECT
        DATEADD(
            'hour',
            SEQ4(),
            start_time
        ) AS expected_timestamp
    FROM time_range,
    TABLE(
        GENERATOR(
            ROWCOUNT => 1000
        )
    )
    WHERE DATEADD(
        'hour',
        SEQ4(),
        start_time
    ) <= end_time
)

SELECT
    expected_timestamp
FROM expected_hours

EXCEPT

SELECT
    timestamp
FROM ATMOSYNC_DB.RAW.WEATHER_DATA_FINAL

ORDER BY expected_timestamp;


-- Count missing hourly timestamps

WITH time_range AS (
    SELECT
        MIN(timestamp) AS start_time,
        MAX(timestamp) AS end_time
    FROM ATMOSYNC_DB.RAW.WEATHER_DATA_FINAL
),

expected_hours AS (
    SELECT
        DATEADD(
            'hour',
            SEQ4(),
            start_time
        ) AS expected_timestamp
    FROM time_range,
    TABLE(
        GENERATOR(
            ROWCOUNT => 1000
        )
    )
    WHERE DATEADD(
        'hour',
        SEQ4(),
        start_time
    ) <= end_time
)

SELECT
    COUNT(*) AS missing_hourly_timestamps
FROM expected_hours

WHERE expected_timestamp NOT IN (
    SELECT timestamp
    FROM ATMOSYNC_DB.RAW.WEATHER_DATA_FINAL
);


-- =====================================================
-- 13. CORRELATION ANALYSIS
-- =====================================================

SELECT
    CORR(temperature, humidity)
        AS temp_humidity_corr,

    CORR(temperature, precipitation)
        AS temp_precipitation_corr,

    CORR(temperature, wind_speed)
        AS temp_wind_corr,

    CORR(humidity, precipitation)
        AS humidity_precipitation_corr,

    CORR(humidity, wind_speed)
        AS humidity_wind_corr,

    CORR(precipitation, wind_speed)
        AS precipitation_wind_corr

FROM ATMOSYNC_DB.RAW.WEATHER_DATA_FINAL;


-- =====================================================
-- 14. ML FEATURE ENGINEERING
-- =====================================================

CREATE OR REPLACE TABLE ATMOSYNC_DB.RAW.WEATHER_ML_FEATURES AS
SELECT
    city,
    latitude,
    longitude,
    temperature,
    humidity,
    precipitation,
    wind_speed,
    weather,
    timestamp,

    EXTRACT(HOUR FROM timestamp)
        AS hour,

    EXTRACT(DAY FROM timestamp)
        AS day_of_month,

    EXTRACT(DAYOFWEEK FROM timestamp)
        AS day_of_week,

    EXTRACT(MONTH FROM timestamp)
        AS month

FROM ATMOSYNC_DB.RAW.WEATHER_DATA_FINAL;


-- =====================================================
-- 15. ML FEATURE VALIDATION
-- =====================================================

-- Preview ML features

SELECT *
FROM ATMOSYNC_DB.RAW.WEATHER_ML_FEATURES
ORDER BY timestamp
LIMIT 10;


-- Check ML feature table schema

SELECT
    column_name,
    data_type
FROM ATMOSYNC_DB.INFORMATION_SCHEMA.COLUMNS
WHERE table_schema = 'RAW'
  AND table_name = 'WEATHER_ML_FEATURES'
ORDER BY ordinal_position;


-- Check number of ML feature rows

SELECT COUNT(*) AS total_rows
FROM ATMOSYNC_DB.RAW.WEATHER_ML_FEATURES;


-- Check missing values in ML features

SELECT
    COUNT_IF(temperature IS NULL)
        AS missing_temperature,

    COUNT_IF(humidity IS NULL)
        AS missing_humidity,

    COUNT_IF(precipitation IS NULL)
        AS missing_precipitation,

    COUNT_IF(wind_speed IS NULL)
        AS missing_wind_speed,

    COUNT_IF(weather IS NULL)
        AS missing_weather,

    COUNT_IF(timestamp IS NULL)
        AS missing_timestamp

FROM ATMOSYNC_DB.RAW.WEATHER_ML_FEATURES;


-- =====================================================
-- END OF CURRENT SQL WORK
-- =====================================================