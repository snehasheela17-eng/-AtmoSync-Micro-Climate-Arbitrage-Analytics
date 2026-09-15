# AtmoSync — Micro-Climate Arbitrage Analytics

Real-time streaming pipeline that detects hyper-local micro-climate shifts inside shipping containers and calculates "Spoilage Arbitrage" opportunities for commodities traders.

**Stack:** Apache Kafka → Snowflake → dbt → Apache Superset

---

## Implementation Roadmap: Next 5 Steps

### 1. Build the IoT Telemetry Simulator
Stand up a Python script that generates realistic container sensor data (temperature, humidity, vibration) as JSON events, on a timer/loop.
- Model multiple containers, each with a baseline "healthy" profile plus injectable drift/anomaly scenarios (e.g., slow temperature climb, humidity spike)
- Include a `container_id`, `commodity_type` (e.g., avocados), `timestamp`, and sensor readings in the payload
- Package it so it can run continuously (`while True` + `sleep`) or in burst mode for testing
- **Output:** a script that prints/streams synthetic telemetry you can pipe into Kafka

### 2. Stand Up Kafka and Wire Up the Producer
Get a local (or Docker-based) Kafka broker running, create topics, and connect the simulator as a producer.
- `docker-compose` with Kafka + Zookeeper (or KRaft mode) is the fastest path
- Create a topic per data type or a single `container-telemetry` topic partitioned by `container_id`
- Modify the simulator to serialize events and publish them via a Kafka producer client (`confluent-kafka` or `kafka-python`)
- Write a simple consumer script to sanity-check messages are flowing before moving to Snowflake
- **Output:** verified end-to-end message flow, simulator → Kafka topic → console consumer

### 3. Connect Kafka to Snowflake (Raw Landing Layer)
Get streaming data persisted into Snowflake raw tables.
- Easiest path: Snowflake Kafka Connector (Kafka Connect + Snowflake sink connector) auto-creates raw variant tables from topic data
- Alternative (lighter-weight for a learning project): a Python consumer that batches messages and loads them via Snowflake's Python connector or `COPY INTO` from staged files
- Design the raw table as a landing zone: minimal transformation, JSON stored in a `VARIANT` column plus ingestion metadata (load timestamp, topic, partition/offset)
- **Output:** raw container telemetry queryable in Snowflake within seconds of being produced

### 4. Set Up dbt and Build Staging Models
Initialize dbt against the Snowflake warehouse and write the first transformation layer.
- `dbt init`, configure `profiles.yml` for the Snowflake connection
- Build staging models (`stg_container_telemetry`) that flatten the JSON variant into typed columns
- Add a second staging model for mock historical commodity pricing data (can be seeded via `dbt seed` with a CSV for now)
- Add basic dbt tests (not-null, accepted values) so bad simulator data surfaces early
- **Output:** clean, typed staging tables ready for the arbitrage logic in Week 3

### 5. Deploy Superset and Connect It to Snowflake
Get the BI layer online so you can visually confirm data is landing correctly before building real dashboards.
- Deploy Superset (Docker is fastest) and add a Snowflake database connection
- Configure a role/user for Superset with read access scoped to the relevant schemas
- Build one throwaway chart (e.g., raw temperature over time for one container) purely to validate the connection — this satisfies the Week 1 "BI Foundations" deliverable
- **Output:** Superset successfully querying live Snowflake data, ready for real dashboard work in Week 2–3

---

## Suggested Order of Operations
Simulator → Kafka → Snowflake raw → dbt staging → Superset connectivity check. Each step is independently testable — don't move to the next until you can verify data landed correctly at the current stage.

