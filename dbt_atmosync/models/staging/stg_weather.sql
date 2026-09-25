{{ config(materialized='view') }}

SELECT
    CITY,
    YEAR,
    MONTH,
    DAY,
    HOUR,
    TEMP,
    RHUM,
    PRCP,
    WDIR,
    WSPD,
    PRES,
    CLDC,
    COCO
FROM ATMOSYNC_DB.RAW.HISTORICAL_WEATHER_RAW
WHERE CITY IS NOT NULL