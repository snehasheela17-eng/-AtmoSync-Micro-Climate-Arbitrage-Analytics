# AtmoSync — Micro-Climate Arbitrage Analytics

## 1. Project Overview

**AtmoSync** is a data analytics project that combines weather, IoT container telemetry, and market data to identify environmental risks, potential arbitrage opportunities, and spoilage risks during transportation.

The project demonstrates an end-to-end data pipeline using **Python, Apache Kafka, Snowflake, dbt, and Apache Superset**.

The main objective is to transform raw real-time and historical data into meaningful analytical insights and dashboards.

---

## 2. Project Objectives

The main objectives of AtmoSync are:

* Collect historical and live weather data.
* Stream real-time data using Apache Kafka.
* Store raw data in Snowflake.
* Simulate IoT container telemetry.
* Analyze temperature, humidity, and vibration risks.
* Generate and analyze market data.
* Use dbt to transform raw data into analytical models.
* Identify potential market arbitrage opportunities.
* Analyze spoilage risk based on environmental conditions and transportation time.
* Build an interactive dashboard using Apache Superset.

---

## 3. Technology Stack

| Technology      | Purpose                                            |
| --------------- | -------------------------------------------------- |
| Python          | Data collection, cleaning, simulation and analysis |
| Pandas          | Data cleaning and preparation                      |
| Apache Kafka    | Real-time data streaming                           |
| Snowflake       | Cloud data warehouse                               |
| dbt             | Data transformation and testing                    |
| Apache Superset | Data visualization and dashboard                   |
| GitHub          | Source-code management                             |

---

## 4. Overall Architecture

```text
Historical Weather Data
          │
          ▼
       Pandas
          │
          ▼
    Kafka Producer
          │
          ▼
        Kafka
          │
          ▼
    Kafka Consumer
          │
          ▼
      Snowflake
          │
    ┌─────┼───────────────┐
    │     │               │
    ▼     ▼               ▼
Live    IoT             Market
Weather Simulator        Data
 API      │                │
  │       ▼                │
  └──► Kafka ◄─────────────┘
          │
          ▼
      Snowflake
          │
          ▼
          dbt
          │
    ┌─────┼─────────────────────┐
    ▼     ▼                     ▼
Container  Market              Spoilage /
Risk       Analysis &          Arbitrage
Analysis   Arbitrage           Analysis
    │        │                   │
    └────────┼───────────────────┘
             ▼
          Superset
             │
             ▼
         Dashboard
```

---

## 5. Data Sources

### Historical Weather Data

Historical weather observations are cleaned using Pandas and streamed through Kafka.

The current master historical dataset contains:

* **209,103 weather records**
* **24 cities**
* Hourly weather observations

Important fields include:

* City
* Temperature
* Relative humidity
* Precipitation
* Wind speed
* Pressure
* Cloud cover
* Weather condition
* Timestamp

### Live Weather Data

Live weather information is collected from a weather API and continuously streamed through Kafka.

The live pipeline captures information such as:

* City
* Latitude
* Longitude
* Temperature
* Humidity
* Precipitation
* Wind speed
* Weather condition
* Timestamp

### IoT Container Telemetry

An IoT simulator generates container telemetry containing:

* Container ID
* Temperature
* Humidity
* Vibration
* Timestamp

The current container telemetry dataset contains approximately **43,460 unique records**.

### Market Data

Market data is generated for different cities and products and stored in Snowflake for market and arbitrage analysis.

---

## 6. Kafka Streaming

Apache Kafka is used as the real-time streaming layer.

Kafka separates data producers from consumers and allows data to be streamed continuously.

Main Kafka streams include:

* `weather_data`
* `live_weather`
* `iot_container_telemetry`

The historical weather pipeline successfully processed **209,103 records**.

---

## 7. Snowflake Data Warehouse

Snowflake is used as the central cloud data warehouse.

### Database

```text
ATMOSYNC_DB
```

### Schema

```text
RAW
```

The RAW layer stores incoming data before analytical transformations.

Important tables include:

* `HISTORICAL_WEATHER_RAW`
* `WEATHER_DATA`
* `IOT_TELEMETRY_STAGE`
* `CONTAINER_TELEMETRY`
* `CONTAINER_TELEMETRY_ANALYSIS`
* `CONTAINER_RISK_SUMMARY`

Market and analytical data are also stored in Snowflake.

---

## 8. Container Environmental Risk Analysis

Container conditions are analyzed using:

* Temperature
* Humidity
* Vibration

The project classifies environmental conditions into:

```text
HIGH_RISK
MEDIUM_RISK
LOW_RISK
```

The risk thresholds are project-defined rules used for analytical demonstration.

Risk analysis helps identify containers that may require attention because of unfavorable environmental conditions.

---

## 9. dbt Transformations

dbt is used to transform raw Snowflake data into analytical models.

### Staging Models

Examples include:

* `stg_weather`
* `stg_iot`
* `stg_commodity`

### Analytical Models

Examples include:

* `container_health`
* `daily_market_summary`
* `arbitrage_analysis`
* `spoilage_arbitrage`
* `spoilage_config`
* `transport_cost_config`

dbt also provides automated data-quality testing.

The project successfully passed **6/6 dbt tests**.

---

## 10. Market and Arbitrage Analysis

The project compares product prices between different cities.

The analysis considers:

* Source city
* Destination city
* Product
* Source price
* Destination price
* Route distance
* Transportation cost
* Net opportunity

Transportation cost is calculated using the project-defined route-cost configuration.

An arbitrage opportunity represents a **potential opportunity for investigation**, not a guaranteed profit.

---

## 11. Spoilage Analysis

Spoilage analysis combines:

* Product
* Environmental risk
* Route distance
* Estimated transportation time
* Product storage-risk configuration

Example project-defined storage windows:

| Risk Level | Storage Window |
| ---------- | -------------: |
| HIGH       |       24 hours |
| MEDIUM     |       48 hours |
| LOW        |       72 hours |

Estimated travel time is calculated from route distance and the configured transportation speed.

This helps determine whether transportation time could exceed the assumed spoilage window.

---

## 12. Superset Dashboard

The final Superset dashboard provides a high-level view of environmental risk.

### KPI Cards

The dashboard contains:

1. **Total Records** — 43.2K
2. **High-Risk Records** — 10.7K
3. **Moderate-Risk Records** — 13.6K
4. **Low-Risk Records** — 18.8K

### Charts

The dashboard also contains:

* **Average Temperature Over Time** — Line Chart
* **Environmental Risk Distribution** — Pie Chart

The dashboard provides a quick overview of the environmental conditions and risk distribution in the processed data.

---

## 13. Data Quality and Validation

The project includes multiple validation steps:

* Duplicate prevention during Snowflake loading
* Record-count verification
* Kafka producer/consumer verification
* Snowflake connection testing
* dbt model testing
* dbt data-quality tests
* End-to-end pipeline verification

Key validation results:

```text
Historical Weather Records: 209,103
Container Telemetry Records: 43,460
dbt Tests Passed: 6/6
```

---

## 14. Project Workflow

The complete AtmoSync workflow is:

```text
1. Collect Data
        ↓
2. Clean Historical Data
        ↓
3. Stream Data Using Kafka
        ↓
4. Load Raw Data into Snowflake
        ↓
5. Simulate IoT Container Data
        ↓
6. Generate Market Data
        ↓
7. Transform Data Using dbt
        ↓
8. Perform Risk Analysis
        ↓
9. Perform Arbitrage Analysis
        ↓
10. Perform Spoilage Analysis
        ↓
11. Validate Results
        ↓
12. Visualize Results Using Superset
```

---

## 15. Key Outcomes

AtmoSync demonstrates how multiple data sources can be integrated into a single analytics pipeline.

The project provides:

* Historical weather analysis
* Live weather streaming
* IoT container monitoring
* Environmental risk classification
* Market analysis
* Potential arbitrage analysis
* Spoilage-risk analysis
* Data transformation using dbt
* Data-quality testing
* Business-oriented dashboard visualization

---

## 16. Important Notes

The risk thresholds, transportation cost, storage windows, and travel-speed assumptions used in this project are **project-defined analytical assumptions**.

Arbitrage results represent **potential opportunities for further investigation** and should not be interpreted as guaranteed financial profit.

The analysis identifies relationships and patterns in the available data; it does not establish scientific causation.

---

## 17. How to Run

### Start Kafka

Start the Kafka server and required services.

### Run Data Producers

Run the required Python producers for:

* Historical weather
* Live weather
* IoT telemetry
* Market data

### Run Consumers

Run the Kafka consumers that load data into Snowflake.

### Run dbt

From the dbt project directory:

```bash
dbt debug
dbt run
dbt test
```

### Start Superset

From the Superset directory:

```powershell
cd C:\superset-6.0.0
docker compose -f docker-compose-image-tag.yml up
```

Open:

```text
http://localhost:8088
```

Then open the AtmoSync dashboard.

---

## 18. Conclusion

AtmoSync demonstrates an end-to-end modern data analytics workflow that combines **streaming data, cloud data warehousing, transformation, analytical modeling, and business intelligence**.

The project transforms raw weather, IoT, and market data into actionable environmental-risk, arbitrage, and spoilage insights through a unified analytics pipeline.
