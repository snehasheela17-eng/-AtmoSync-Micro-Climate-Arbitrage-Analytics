# AtmoSync – Micro-Climate Arbitrage Analytics

## 1. Project Overview

AtmoSync is a data engineering and analytics project that combines weather observations, IoT environmental telemetry, and agricultural market data to identify environmental risks and potential spoilage-arbitrage opportunities.

The project uses Python for data processing and simulation, Apache Kafka for streaming, Snowflake as the central data warehouse, dbt for SQL transformations, and Apache Superset for analytics dashboards.

The objective is to turn multiple data sources into useful insights about environmental conditions, container risk, market prices, supply and demand, and potential product-spoilage opportunities.

## 2. Business Problem

Weather conditions and storage environments can affect agricultural products during transportation and distribution. Market prices, supply, and demand can also vary by product, city, and date.

AtmoSync brings these datasets together to help answer questions such as:

- Which containers show elevated environmental risk?
- How do temperature and humidity relate to container conditions?
- How do market prices vary across products, cities, and dates?
- Where might environmental and market conditions indicate a potential spoilage-arbitrage opportunity?
- Which cities or conditions require further investigation?

The analytics support decision-making; identified opportunities and risk classifications should be validated against operational data before action is taken.

## 3. Architecture

```text
Historical Weather ─┐
Live Weather Feed ──┼──> Python Producers / Data Loaders
IoT Sensor Data ────┘                |
                                     v
                              Apache Kafka
                                     |
                       Python Consumers / Loaders
                                     |
                                     v
                                Snowflake
                               ATMOSYNC_DB
                                   RAW
                                     |
                                     v
                              dbt Transformations
                                     |
                                     v
                          Analytics and Risk Models
                                     |
                                     v
                         Apache Superset Dashboard

Market Data Generation ───────> Snowflake
```

The exact ingestion route depends on the source-specific producer, consumer, and loader scripts. Historical weather, live weather, IoT telemetry, and market data can follow different ingestion paths before their analytical outputs are combined.

## 4. Technology Stack

| Technology | Purpose |
| --- | --- |
| Python | Data processing, producers, consumers, and simulators |
| Pandas | Data profiling and transformation where used |
| Apache Kafka | Streaming and messaging |
| Kafka Python libraries | Python integration with Kafka |
| Java 21 | Runtime for the current Kafka installation |
| Snowflake | Central data warehouse |
| SQL | Data validation, transformations, and analytics |
| dbt | Staging and analytical data models |
| Apache Superset | Interactive analytics dashboard |
| Git and GitHub | Version control and collaboration |
| VS Code | Development environment |

## 5. Data Sources

### Historical weather

The repository includes historical weather datasets and a Kafka-ready historical weather CSV.

The original historical dataset was documented as containing 8,632 rows and 20 columns. A cleaned version was also generated. Row counts can differ between raw, cleaned, and Kafka-ready datasets because of cleaning and validation.

Weather attributes include:

- Temperature
- Relative humidity
- Precipitation
- Wind direction and speed
- Atmospheric pressure
- Cloud cover
- Weather condition codes
- Date and time components

### Live weather

Python scripts support the live weather ingestion workflow. Weather data can be published through Kafka and processed by consumers for storage and analysis.

The sample `data/live_weather.csv` is a saved sample, not a continuously refreshed live feed.

### IoT environmental telemetry

The IoT components simulate or process environmental readings, including measurements such as temperature, humidity, and vibration.

These readings are used to assess container conditions and derive environmental-risk indicators.

### Agricultural market data

The market components generate or process product-level market data and load it into Snowflake.

The analytics use attributes such as city, product, date, supply, demand, and price where available in the source tables.

## 6. Data Quality and Cleaning

Data quality is important because missing or invalid measurements can distort risk calculations and market analysis.

The historical weather workflow includes checks for:

- Missing values and known placeholder values such as `-9999`
- Numeric conversion and invalid measurement ranges
- Humidity, precipitation, wind, pressure, and cloud-cover values
- Invalid dates, hours, and calendar combinations
- Duplicate observations and consistent date/time representation

A missing value is different from an abnormal value. A missing value has no usable measurement, while an abnormal value is present but may be invalid, implausible, or unusually different from expected behaviour.

Missing values should be handled according to the field and business context. Abnormal readings should be validated against domain limits and source data before correction, exclusion, or retention.

## 7. Snowflake Data Warehouse

Snowflake is the central storage layer for the project's weather, IoT, and market datasets.

The configured database is:

- Database: `ATMOSYNC_DB`
- Raw schema: `RAW`

The repository includes Snowflake connection tests, data loaders, and SQL scripts. Actual tables and row counts depend on which ingestion scripts have been run and the current warehouse state.

Credentials and account-specific settings must be supplied through a secure local configuration. Never commit passwords, private keys, access tokens, or populated `.env` files to GitHub.

## 8. dbt Transformations and Analytics

The repository contains dbt models and standalone SQL scripts for transforming raw data into analytical datasets.

The available SQL model names include:

- `iot_environment_risk`
- `iot_sensor_cleaned`
- `market_product_cleaned`
- `microclimate_market_analysis`
- `atmosync_arbitrage_analysis`

Related analytics include container health, daily market summaries, and spoilage-arbitrage analysis.

These transformations support the following analytics perspectives:

- **Descriptive:** What happened in the weather, market, and container data?
- **Diagnostic:** Which environmental or market conditions may explain an observed result?
- **Predictive:** What risk or spoilage outcomes might occur? Predictive claims require a validated predictive model.
- **Prescriptive:** What action might be considered based on the evidence, constraints, and calculated opportunity?

The repository currently contains two dbt project directories. Confirm the intended project and profile configuration before running dbt commands. Generated `target` and log files are not source models and should not be committed.

## 9. Superset Dashboard

The project includes an AtmoSync Micro-Climate Analytics dashboard built with Apache Superset.

The dashboard brings together market trends, container environmental risk, and spoilage-arbitrage information.

The configured dashboard visualizations cover topics such as:

- Supply versus demand by city
- Average market price by product
- Market price trends over time
- Average container high-risk percentage
- Temperature versus humidity
- Spoilage-arbitrage details and opportunities
- Product and city comparisons

Dashboard results depend on the selected Snowflake datasets, query definitions, and available data.

## 10. Repository Structure

```text
.
├── data/
│   ├── kafka_historical_weather.csv
│   └── live_weather.csv
├── IOT/
│   ├── iot_kafka_producer.py
│   ├── iot_sensor_simulator.py
│   └── iot_snowflake_loader.py
├── market/
│   ├── market_data_generator.py
│   ├── market_product_data.csv
│   └── market_snowflake_loader.py
├── kafka/
│   ├── historical_producer.py
│   └── historical_consumer.py
├── python/
├── sql/
├── dbt_atmosync/
├── Historical Producer.py
├── live_weather_producer.py
├── live_weather_consumer.py
├── historical_consumer.py
├── clean_historical_data.py
├── generate_container_telemetry.py
├── load_container_data.py
├── market_data_generator.py
├── snowflake_test.py
├── test_snowflake_connection.py
├── schema.yml
└── README.md
```

This is a high-level view of the main source areas. Additional SQL files and supporting scripts are present in the repository.

## 11. Setup Overview

### Prerequisites

Install and configure the tools required by the components you intend to run:

- Python and the project's required packages
- Java and Apache Kafka
- A Snowflake account with suitable database permissions
- dbt with the appropriate Snowflake adapter
- Apache Superset and its project-specific configuration
- Git and VS Code

### General workflow

1. Clone the repository.
2. Review the project structure and Python dependency requirements.
3. Configure local credentials and environment variables securely.
4. Configure Kafka and confirm that the broker is available.
5. Validate historical weather data before ingestion.
6. Run the relevant weather or IoT producer and consumer workflows.
7. Load market data into Snowflake using the appropriate loader.
8. Validate the raw Snowflake tables and data quality.
9. Configure the intended dbt project and profile, then execute and test its models.
10. Verify the analytical datasets and open the Superset dashboard.

The exact commands and startup order depend on the selected workflow and local configuration. Do not run all producers and consumers indiscriminately; follow the requirements of each script.

## 12. Data Validation and Duplicate Handling

Repeated producer or loader runs can cause duplicate records if the ingestion process does not use an appropriate record key or deduplication strategy.

Validation should include:

- Row counts and expected schema
- Null and invalid-value checks
- Timestamp and city consistency
- Duplicate checks using appropriate business keys
- Source-to-target reconciliation
- Validation of transformed model outputs

Use idempotent loading or suitable deduplication logic where required. A successful test-table insert alone does not prove that the entire pipeline is duplicate-safe.

## 13. Security and Reproducibility

- Keep credentials and secrets out of source control.
- Use `.env` or another secure local configuration mechanism, and provide only sanitized examples when documenting settings.
- Exclude virtual environments, generated dbt output, logs, caches, and local database files.
- Validate dependencies and service configurations before running the pipeline.
- Document any manual steps needed to reproduce the results.

## 14. Conclusion

AtmoSync demonstrates an integrated data engineering and analytics workflow that connects weather observations, IoT environmental readings, and agricultural market data.

By combining Python, Kafka, Snowflake, dbt, SQL, and Superset, the project turns source data into analytical datasets and dashboard views that help investigate container risk, market behaviour, and potential spoilage-arbitrage opportunities.

The project emphasizes data quality, traceable transformations, streaming architecture, warehouse-based analytics, and reproducible collaboration through GitHub.
