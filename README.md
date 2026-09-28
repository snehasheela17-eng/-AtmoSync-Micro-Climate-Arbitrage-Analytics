 AtmoSync – Micro-Climate Analytics & Weather Streaming Platform

 1. Project Overview

“Our project is AtmoSync, a Micro-Climate Arbitrage Analytics system. We combine historical weather, live weather API data, simulated IoT environmental data, and market/product data. Kafka handles the streaming layer, and Snowflake acts as our central warehouse. We use dbt to clean and transform the IoT and market data, calculate environmental risk, combine environmental and market conditions by city and date, and finally classify potential arbitrage opportunities as high, moderate, or low. The final analytical view is then used for dashboard visualisation.”

 2. Objectives

- Build a reproducible weather-data pipeline.
- Profile raw weather data before cleaning.
- Identify missing values, inconsistent formats, invalid values, and invalid timestamps.
- Create a validated historical weather dataset without inventing missing observations.
- Stream historical weather records through Apache Kafka.
- Store weather observations in Snowflake.
- Prevent duplicate records during repeated ingestion.
- Prepare the architecture for live weather ingestion.
- Enable city/time-based weather analysis and micro-climate comparisons.

 3. Problem Statement

Weather datasets can contain missing observations, inconsistent representations, invalid date/time values, placeholder values, incorrect ranges, and duplicate records. If such data is sent directly into a streaming or analytics system, downstream databases and dashboards can produce misleading results.
AtmoSync introduces data profiling and validation before downstream ingestion and uses a streaming architecture to move weather records into Snowflake.

 4. Scope

- Historical weather-data profiling
- Data-quality validation
- Data cleaning
- Timestamp validation
- Kafka streaming
- Snowflake ingestion
- Duplicate detection and prevention
- Pipeline documentation
- Preparation for live weather API ingestion
- Analytics-ready storage

 Planned expansion
- Multiple Indian cities/states
- Open-Meteo live weather ingestion
- Continuous live streaming

5. High-Level Architecture

ATMOSYNC
     Architecture

                              ATMOSYNC
                                  │
             ┌────────────────────┼────────────────────┐
             │                    │                    │
             ▼                    ▼                    ▼
      Historical Weather    Live Weather API      IoT Sensors
           Data                                      Simulator
             │                    │                    │
             └────────────────────┼────────────────────┘
                                  │
                                  ▼
                         KAFKA (Real-time Streaming)
                                  │
             ┌────────────────────┼────────────────────┐
             │                    │                    │
             ▼                    ▼                    ▼
      Historical Consumer   Live Weather Consumer   IoT Data Consumer
             │                    │                    │
             └────────────────────┼────────────────────┘
                                  │
                                  ▼
                            SNOWFLAKE
                          ATMOSYNC_DB
                               RAW
                                  │
                         ┌────────┴────────┐
                         │                 │
                         ▼                 ▼
                    Market Data        IoT Data
                         │                 │
                         └────────┬────────┘
                                  ▼
                                DBT
                                  │
                                  ▼
                    Micro-Climate + Market Analysis
                                  │
                                  ▼
                    Arbitrage Opportunity Analysis
                                  │
                                  ▼
                         Dashboard / Superset
                         

 Planned live path
     text
Open-Meteo Weather API
       |
       v
Python Producer
       |
       v
Apache Kafka
       |
       v
Python Consumer
       |
       v
Snowflake
       |
       v
Power BI / Analytics

Historical and live data are treated as separate ingestion paths so historical data can be validated independently from continuously changing live data.

 6. Technologies

| Technology | Purpose |
| Python | Data processing, cleaning, Kafka producer/consumer |
| Pandas | Data profiling and transformation |
| OpenPyXL | Excel input/output |
| Apache Kafka | Streaming weather records |
| kafka-python | Python/Kafka integration |
| Java 21 | Runtime for current Kafka installation |
| Snowflake | Cloud data warehouse |
| SQL | Database, validation and deduplication operations |
| Open-Meteo | Planned live weather API source |
| Power BI | Planned analytics and visualization |
| Git/GitHub | Version control and collaboration |
| VS Code | Development environment |

 7. Historical Dataset

The historical dataset contains 8,632 rows and 20 columns representing hourly weather observations for 2024.

Columns:
`year, month, day, hour, temp, temp_source, rhum, rhum_source, prcp, prcp_source, wdir, wdir_source, wspd, wspd_source, pres, pres_source, cldc, cldc_source, coco, coco_source`

Key meanings:
- temp: temperature
- rhum: relative humidity
- prcp: precipitation
- wdir: wind direction
- wspd: wind speed
- pres: atmospheric pressure
- cldc: cloud cover
- coco: weather condition code
- *_source: source associated with the corresponding weather field

 8. Data Profiling

The raw Excel dataset was profiled before cleaning.

Results:
- Rows: 8,632
- Columns: 20
- Duplicate rows: 0
- Total null cells: 6,637

Problems identified included missing date/time components, missing measurements and source fields, `-9999` placeholders, invalid ranges, inconsistent formats, invalid calendar dates, and invalid date/time combinations.

 9. Data Cleaning

Cleaning is implemented in:

clean_weather_dataset.py

Cleaning includes:
- Converting known -9999 placeholders to missing values
- Numeric conversion of measurements
- Humidity range validation: 0–100
- Precipitation unit/decimal normalisation
- Precipitation validation
- Wind-direction validation: 0–360
- Wind-speed validation
- Pressure validation
- Cloud-cover conversion to a 0–8 scale
- Weather-condition-code normalization
- Year/month/day/hour validation
- Calendar-date validation
- Creation of a combined datetime field

 10. Final Historical Dataset

The final historical time-series dataset is:

data/weather_dataset_final.xlsx

 11. Kafka

Current Kafka setup:
- Kafka: 4.3.1
- Java: 21
- Broker: `localhost:9092
- Historical topic: historical_weather
- Test topic: weather_data

 Producer:
Reads validated records and publishes JSON messages to Kafka.

 Consumer:
Reads Kafka messages and sends the relevant weather fields to Snowflake.

 12. Snowflake
 Snowflake Structure:

ATMOSYNC_DB
└── RAW
    └── WEATHER_DATA

Current table fields:

| Column | Type |
| CITY | VARCHAR |
| LATITUDE | FLOAT |
| LONGITUDE | FLOAT |
| TEMPERATURE | FLOAT |
| HUMIDITY | FLOAT |
| PRECIPITATION | FLOAT |
| WIND_SPEED | FLOAT |
| WEATHER | VARCHAR |
| TIMESTAMP | TIMESTAMP |

Snowflake is the centralised storage layer for weather observations.

 13. Duplicate Handling

Repeated producer runs can publish the same historical observations more than once. Therefore, the final ingestion process must be duplicate-safe.

The final ingestion should use duplicate-safe insert/upsert logic instead of blindly inserting every Kafka message.

 14. Snowflake Validation Finding

The current Snowflake test table was inspected for duplicates.
This test result demonstrates why duplicate protection is required. 

 15. Conclusion:
AtmoSync demonstrates an end-to-end weather-data engineering workflow:
 Project Workflow


GitHub treats everything inside the code block as **preformatted text**, so it will preserve:

- line breaks
- spaces
- indentation
- arrows
- numbering

Your GitHub README will therefore show it as:

1. Python Environment
        ↓
2. Kafka/KRaft Setup
        ↓
3. Historical Weather Cleaning
        ↓
4. Historical Weather → Kafka
        ↓
5. Historical Data → Snowflake
        ↓
6. Live Weather API
        ↓
7. Live Weather → Kafka
        ↓
8. Live Weather → Snowflake
        ↓
9. IoT Simulator
        ↓
10. IoT → Kafka
        ↓
11. IoT → Snowflake
        ↓
12. Market Data Generation
        ↓
13. Market Data → Snowflake
        ↓
14. dbt Transformations
        ↓
15. Final AtmoSync Analysis
        ↓
16. End-to-End Testing
        ↓
17. Snowflake Cleanup
        ↓
18. GitHub
        ↓
19. Dashboard / Superset
        ↓
20. README + Documentation

The project emphasises data quality, reproducibility, streaming architecture, cloud storage, and team collaboration. The repository is designed so that any team member can clone the `main` branch, install the documented dependencies, configure required local services and credentials, and execute the pipeline without relying on the team lead's personal development environment.
GitHub main branch sync test.
