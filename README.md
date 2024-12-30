# SmartTraffic_Lakehouse_for_HCMC

> **WARNING:** This project only runs on ARM64 chips.

# Table of Contents

1. **[Project Objective](#project-objective)**
2. **[Datasets Selection](#datasets-selection)**
3. **[System Architecture](#system-architecture)**
   - [System Overview](#system-overview)
   - [Data Processing Stages](#data-processing-stages)
   - [Data Warehouse Architecture in the Gold Layer](#data-warehouse-architecture-in-the-gold-layer)
4. **[Technologies Used](#technologies-used)**
5. **[Deployment](#deployment)**
   - [System Requirements](#system-requirements)
   - [Running the Project](#running-the-project)
6. **[Result](#result)**
7. **[Authors](#authors)**

# Project Objective

The project aims to build a `Smart Traffic Management System` that leverages real-time data processing, advanced visualization, and monitoring to optimize traffic flow, enhance safety, and support sustainable urban mobility. Key objectives include:

- **Real-Time Insights**: Deliver actionable traffic data on congestion, accidents, and vehicle movement.
- **Data Integration**: Process and store high-volume traffic data from IoT devices and sensors.
- **Visualization**: Provide intuitive dashboards for traffic trends, weather, and parking analysis.
- **Monitoring**: Ensure system reliability with real-time infrastructure monitoring.
- **User Applications**: Offer real-time traffic updates through user-friendly apps.

This system transforms raw traffic data into valuable insights, enabling smarter, safer, and greener cities.

# Datasets Selection

## 1. Parking transactions data in HCM City on PostgreSQL

- Source: [ParkingDB_HCMCity_PostgreSQL](https://www.kaggle.com/datasets/ren294/parkingdb-hcmcity-postgres)

This database manages operations for a parking lot system in **Ho Chi Minh City, Vietnam**, tracking everything from parking records to customer feedback. The database contains operational data for managing parking facilities, including vehicle tracking, payment processing, customer management, and staff scheduling.

<center>
      <img src="image/ERD/GasStation1.png" width="900" />
  </center>

## 2. Gas Station data in HCM City on PostgreSQL

- Source: [GasStationDB_HCMCity_PostgreSQL](https://www.kaggle.com/datasets/ren294/gasstationdb-hcmcity-postgres)

This database manages operations for a chain of gas stations in **Ho Chi Minh city, Viet Nam**, tracking everything from fuel sales to inventory management. The database contains operational data for managing gas stations, including sales transactions, inventory tracking, customer management, and employee records.

<center>
      <img src="image/ERD/ParkingTransaction1.png" width="900" />
  </center>

## 3. IOT data road transport in HCM City

- Source: [IOT_RoadTransport_HCMCity](https://www.kaggle.com/datasets/ren294/iot-car-hcmcity)

The dataset provided describes information about a vehicle (in this case, a motorbike) moving along a specific road in Ho Chi Minh City. It includes various details about the vehicle, its owner, weather conditions, traffic status, and alerts related to the vehicle during its journey. This data can be used in traffic monitoring systems, vehicle operation analysis, or smart transportation services.

Here is the schema of the provided `JSON`, described in a hierarchical structure:

```json
{
  "vehicle_id": "string",
  "owner": {
    "name": "string",
    "license_number": "string",
    "contact_info": {
      "phone": "string",
      "email": "string"
    }
  },
  "speed_kmph": "float",
  "road": {
    "street": "string",
    "district": "string",
    "city": "string"
  },
  "timestamp": "string",
  "vehicle_size": {
    "length_meters": "float",
    "width_meters": "float",
    "height_meters": "float"
  },
  "vehicle_type": "string",
  "vehicle_classification": "string",
  "coordinates": {
    "latitude": "float",
    "longitude": "float"
  },
  "engine_status": {
    "is_running": "boolean",
    "rpm": "int",
    "oil_pressure": "string"
  },
  "fuel_level_percentage": "int",
  "passenger_count": "int",
  "internal_temperature_celsius": "float",
  "weather_condition": {
    "temperature_celsius": "float",
    "humidity_percentage": "float",
    "condition": "string"
  },
  "estimated_time_of_arrival": {
    "destination": {
      "street": "string",
      "district": "string",
      "city": "string"
    },
    "eta": "string"
  },
  "traffic_status": {
    "congestion_level": "string",
    "estimated_delay_minutes": "int"
  },
  "alerts": [
    {
      "type": "string",
      "description": "string",
      "severity": "string",
      "timestamp": "string"
    }
  ]
}
```

## 4. Traffic accidents data in HCM City

- Source: [Traffic_Accidents_HCMCity](https://www.kaggle.com/datasets/ren294/traffic-accidents-hcmcity)

The dataset provided contains information about a road accident that took place on various roads in **Ho Chi Minh City, Viet Nam**. It includes details about the accident, such as the vehicles involved, severity, accident time, recovery time, and traffic congestion caused by the accident. This data can be useful for traffic management systems, accident reporting, and analyzing traffic patterns.

Here is the schema of the provided `JSON`, described in a hierarchical structure:

```json
{
  "road_name": "string",
  "district": "string",
  "city": "string",
  "vehicles_involved": [
    {
      "vehicle_type": "string",
      "vehicle_id": "string"
    }
  ],
  "accident_severity": "int",
  "accident_time": "string",
  "number_of_vehicles": "int",
  "estimated_recovery_time": "string",
  "congestion_km": "float",
  "description": "string"
}
```

## 5. Weather data from API in HCM City

- Source: [VisualCrossing](https://www.visualcrossing.com/weather-api)

The dataset provided contains detailed weather information for **Ho Chi Minh City, Viet Nam**, including temperature, humidity, wind conditions, precipitation, and other meteorological measurements. This data is collected hourly and aggregated daily, useful for weather forecasting, climate analysis, and urban planning applications.

Here is the schema of the provided `JSON`, described in a hierarchical structure:

```json
{
  "latitude": "number",
  "longitude": "number",
  "resolvedAddress": "string",
  "address": "string",
  "timezone": "string",
  "tzoffset": "number",
  "days": [
    {
      "datetime": "string",
      "datetimeEpoch": "number",
      "tempmax": "number",
      "tempmin": "number",
      "temp": "number",
      "feelslike": "number",
      "humidity": "number",
      "precip": "number",
      "windspeed": "number",
      "winddir": "number",
      "pressure": "number",
      "cloudcover": "number",
      "visibility": "number",
      "uvindex": "number",
      "sunrise": "string",
      "sunset": "string",
      "conditions": "string",
      "hours": [
        {
          "datetime": "string",
          "temp": "number",
          "feelslike": "number",
          "humidity": "number",
          "precip": "number",
          "windspeed": "number",
          "winddir": "number",
          "conditions": "string"
        }
      ]
    }
  ]
}
```

# System Architecture

The **Data Lakehouse** architecture implemented in this project is meticulously designed to accommodate both batch and real-time streaming data, seamlessly integrating multiple data sources into a cohesive analytics platform. This architecture follows the Me dallion Architecture pattern, which organizes data into **Bronze**, **Silver**, and **Gold** layers, each serving specific roles in the data lifecycle.

## System Overview

The system is divided into several components, each responsible for specific tasks within the data process:

<center>
      <img src="image/Operate/Overview.png" width="900" />
  </center>

The architecture consists of several key components:

### 1. Data Ingestion Layer

- **Seatunnel**: Handles CDC (Change Data Capture) using Debezium format
- **Apache NiFi**: Manages data routing and transformation

### 2. Data Collection Layer

- **Apache Kafka & Zookeeper**: Handles real-time data streaming
- **Redpanda UI**: Provides Kafka management interface

### 3. Processing Layer

- **Apache Spark**: Batch processing
- **Apache Flink**: Stream processing
- **Apache Airflow**: Workflow orchestration
- **Redis**: Data caching

### 4. Storage Layer

- **MinIO**: Object storage
- **lakeFS**: Data versioning
- **Apache Hudi**: Storage format for Silver layer
- **Apache Hive**: Data warehouse for Gold layer
- **PostgreSQL**: Metastore backend

### 5. Serving Layer

- **ClickHouse**: OLAP database
- **Prometheus**: Metrics storage
- **Trino**: Distributed SQL query engine

### 6. Visualization Layer

- **Streamlit**: Interactive dashboards
- **Metabase**: Business analytics
- **Apache Superset**: Data exploration
- **Grafana**: Metrics visualization

## Data Processing Stages

### Bronze Stage: Raw Data Acquisition & Validation

<center>
      <img src="image/Operate/BronzeStage.png" width="900" />
  </center>

### a. Data Ingestion Layer

The **Bronze stage** serves as the initial landing zone for raw data, implementing a robust ingestion and validation pipeline:

#### Real-time Data Sources

- **Weather API Data**: Live weather metrics and forecasts
- **Traffic Data**: Real-time traffic conditions and events
- **Vehicle IOT Data**: Streaming vehicle telemetry and sensor data

#### Ingestion Process

- **Initial Collection**
  - `Apache NiFi` orchestrates data collection from all sources
  - Implements initial data validation and formatting
  - Ensures data completeness and basic quality checks
- **Stream Processing**
  - Data streams are directed to `Kafka` (managed via `Redpanda UI`)
  - Implements message validation and schema verification
  - Maintains data lineage and source tracking

### b. Storage Management

The **Bronze stage** implements a sophisticated version-controlled storage strategy:

#### Version Control Process

- **Branch Management**
  - Creates temporary branches from main **Bronze** repository
  - Implements atomic commits for data consistency
  - Maintains data versioning through `lakeFS`
- **Commit Workflow**
  - Automated validation triggers on commit
  - `Airflow` DAGs orchestrate `Spark` jobs for data verification
  - Successful validation merges changes to main branch

### c. Monitoring & Performance

- **Caching Layer**: `Redis` implementation for frequently accessed data
- **Monitoring Stack**:
  - `Prometheus` metrics collection
  - `Grafana` dashboards for real-time monitoring
  - Performance metrics and SLA tracking

### 2. Silver Stage: Data Transformation & Enrichment

<center>
        <img src="image/Operate/SilverStage.png" width="900" />
  </center>

### a. Data Sources Integration

The **Silver stage** combines multiple data streams and implements advanced processing:

#### Change Data Capture (CDC)

- **PostgreSQL Integration**:
  - Real-time monitoring of `ParkingLot` and `StorageTank` tables
  - `Seatunnel` implementation with `Debezium` format
  - Maintains data consistency and transaction order

#### Stream Processing Architecture

- **Real-time Processing Pipeline**

  - `Flink` processes incoming `Kafka` streams
  - Implements business logic and data enrichment
  - Maintains stateful processing for complex operations
- **Data Quality Management**

  - `Spark Streaming` validates processed data
  - Implements data quality rules and business constraints
  - Maintains data consistency across branches

### b. Multi-Branch Processing Strategy

The **Silver stage** implements a sophisticated branching strategy:

#### Staging Branch Processing

- **Change Detection**

  - Continuous monitoring of data changes
  - `Spark` jobs compare incoming data with staging
  - Implements delta detection algorithms
- **Branch Management**

  - Creates temporary branches for changes
  - Implements validation before commit
  - Maintains data lineage and audit trails

#### Main Branch Updates

- **Hourly Synchronization:**
  - `Airflow` DAGs orchestrate main branch updates
  - Implements merge conflict resolution
  - Maintains data consistency and quality

#### c. Data Serving Layer

#### Analytics Processing

- `ClickHouse` collects processed `Kafka` data
- `Superset` provides visualization capabilities
- Implements real-time analytics queries

#### Application Integration

- `Streamlit` applications consume processed data
- Combines `Redis` cache with `PostgreSQL` data
- Provides real-time user interfaces

### 3. Gold Stage Processing

<center>
      <img src="image/Operate/GoldStage.png" width="900" />
  </center>

#### Dimensional Modeling

The **Gold stage** implements a robust dimensional model:

#### Dimension Tables Management

- **Daily Processing**
  - Scheduled `Airflow` DAGs process **Silver layer** data
  - Implements slowly changing dimension logic
  - Maintains historical accuracy and tracking

- **Aggregation Pipeline**
  - `Spark` jobs perform complex aggregations
  - Implements business rules and calculations
  - Maintains data consistency and accuracy

#### b. Fact Table Processing

- **Real-time Updates**
  - Triggered by **Silver layer** changes
  - Implements upsert operations for fact tables
  - Maintains transactional consistency

- **Batch Processing**
  - Daily scheduled updates from **Silver layer**
  - Implements full refresh of fact tables
  - Maintains historical accuracy

#### c. Process Orchestration

- **Workflow Management:**
  - `Airflow` orchestrates all `Spark` jobs
  - Implements dependency management
  - Maintains process monitoring and logging

#### d. Performance Optimization

- **Caching Strategy:**
  - `Redis` caches frequently accessed data
  - Implements cache invalidation rules
  - Maintains performance SLAs

#### e. Monitoring & Governance

- **Quality Assurance:**
  - `Prometheus` metrics collection
  - `Grafana` dashboards for monitoring
  - Implements SLA monitoring and alerting

## Data Warehouse Architecture in the Gold Layer

The **Gold Layer** in a data lakehouse architecture represents the final, refined stage where clean, processed, and analytics-ready data is stored. This layer is specifically designed for consumption by business intelligence tools, data scientists, and analysts. In the context of an *Traffic Data Warehouse*, the **Gold Layer** is where critical business metrics, aggregated datasets, and key insights are stored in an optimized format.

The schema DataWarehouse:

<center>
      <img src="image/ERD/WarehouseGold.png" width="900" />
  </center>

# Deployment

## System Requirements

To deploy and run this *Data Lakehouse* project, the following system requirements are necessary:

### Hardware

- **Processor**: `ARM64` chip with at least 12 CPU cores.
- **Memory**: 32 GB of RAM.
- **Storage**: 50 GB of free storage space.

### Software

- **Operating System**: A `Linux-based` OS supporting `ARM64` architecture.
- **Docker**: Pre-installed to run all components in isolated containers.
- **Docker Compose**: Pre-installed for orchestrating multi-container Docker applications.
- **Git**: For version control and project deployment

## Running the Project

### 1. Clone the Project Repository

- Run the following command to clone the project repository:

  ```bash
  git clone https://github.com/Ren294/SmartTraffic_Lakehouse_for_HCMC.git
  ```
- Navigate to the project directory:

  ```bash
  cd SmartTraffic_Lakehouse_for_HCMC
  ```

### 2. Running the Project

**2.1. Grant Execution Permissions for Shell Scripts**

- Once connected, ensure all shell scripts have the correct execution permissions:

  ```bash
  chmod -R +x ./*
  ```

  - This command grants execute permissions to all .sh files in the project directory, allowing them to be run without any issues.

**2.2. Start Docker Services**

```bash
  docker-compose up -d
```

**2.3. Update LakeFS Initial Credentials**

- Watch the `LakeFS` container logs to get the initial credentials:

  ```bash
  docker logs lakefs | grep -A 2 "Login"
  ```

  - You should see output similar to this:

    ```bash
      Login at http://127.0.0.1:8000/
      Access Key ID    :  XXXXXXXXXXXXXXXXXXX
      Secret Access Key: XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    ```

  - Edit the [init.sh](./init.sh) file and update these lines with your `LakeFS` credentials:

    ```bash
      username='AKIAJC5AQUW4OXQYCRAQ' # Replace with your Access Key ID
      password='iYf4H8GSjY+HMfN70AMquj0+PRpYcgUl0uN01G7Z' # Replace with your Secret Access Key
    ```

**2.4. Initialize the Project Environment**

- Run the initialization script:

  ```bash
  ./init.sh
  ```
- This script will perform the following tasks:

  - Download required data from Kaggle
  - Update `LakeFS` credentials across all necessary files
  - Initialize storage (`Minio` buckets and `LakeFS` repositories)
  - Set up `Airflow` connector
  - Create `Debezium` connector
  - Upload `Flink` jobs
  - Submit S `park Streaming` jobs

**2.5. Configure and Run Apache NiFi**
To kick off your data processing workflows, follow these steps to configure and run `Apache NiFi`:

- Open the `NiFi Web UI` by navigating to `http://localhost:8443/nifi` in your browser.
- Add the [template](nifi/template/NifiTrafficTemplateRen294.xml) for the `NiFi` workflow:

  <center>
      <img src="image/Nifi/Home.png" width="900" />
  </center>
- You will now see several key components in your NiFi workflow:

  - **`TrafficDataIngestion`**: Collects and integrates raw traffic data.

    <center>
        <img src="image/Nifi/TrafficDataIngestion.png" width="900" />
    </center>
  - **`TrafficAccidentsIngestion`**: Collects and integrates raw traffic accident data.

    <center>
        <img src="image/Nifi/TrafficAccidentsIngestion.png" width="900" />
    </center>
  - **`WeatherIngestion`**: Collects and integrates raw weather data.

    <center>
        <img src="image/Nifi/WeatherIngestion.png" width="900" />
    </center>
  - **`ProcessFromIngestion`**: Sends data from ingestion pipelines to `Kafka` for downstream consumption.

    <center>
        <img src="image/Nifi/ProcessFromIngestion.png" width="900" />
    </center>
  - **`TrafficDataCommitToBronzeLayer`**: Transfers traffic data into the **Bronze layer** for storage.

    <center>
        <img src="image/Nifi/TrafficDataCommitToBronzeLayer.png" width="900" />
    </center>
  - **`TrafficAccidentCommitToBronzeLayer`**: Loads traffic accident data into the **Bronze layer** for storage.

    <center>
        <img src="image/Nifi/TrafficAccidentCommitToBronzeLayer.png" width="900" />
    </center>
  - **`WeatherCommitToBronzeLayer`**: Stores ingested weather data into the **Bronze layer** for foundational analytics.

    <center>
        <img src="image/Nifi/WeatherCommitToBronzeLayer.png" width="900" />
    </center>

**2.6. Run Apache Airflow DAGs**

- Open the `Airflow Web UI` by navigating to `http://localhost:6060` in your browser
- Login with

  - **Username**: *ren294*
  - **Password**: *ren294*

- After login, it's time to activate the **DAGs (Directed Acyclic Graphs)** that control the data workflows. The project consists of ten pre-configured `DAGs`, which you can activate from the `Airflow` dashboard:

  <center>
      <img src="image/Airflow/AirflowDAG.png" width="900" />
  </center>

Here are the ten DAGs and their respective functions:

- **`Bronze_Traffic_accident_Validation_DAG`**: Validate traffic accident data and merge to main.

  <center>
      <img src="image/Airflow/Bronze_Traffic_accident_Validation_DAG.png" width="900" />
  </center>
- **`Bronze_Traffic_data_Validation_DAG`**: Validates traffic data and integrates it into the main branch.

  <center>
      <img src="image/Airflow/Bronze_Traffic_accident_Validation_DAG.png" width="900" />
  </center>
- **`Bronze_Weather_Validation_DAG`**: Validates weather data and integrates it into the main branch.

  <center>
      <img src="image/Airflow/Bronze_Weather_Validation_DAG.png" width="900" />
  </center>
- **`Silver_Staging_Gasstation_Validate_DAG`**: Handles the synchronization of gasstation data from PostgreSQL to Hudi tables.

  <center>
      <img src="image/Airflow/Silver_Staging_Gasstation_Validate_DAG.png" width="900" />
  </center>
- **`Silver_Staging_Parking_Validate_DAG`**: Handles the synchronization of parking data from PostgreSQL to Hudi tables.

  <center>
      <img src="image/Airflow/Silver_Staging_Parking_Validate_DAG.png" width="900" />
  </center>
- **`Silver_to_Staging_Accidents_Merge_DAG`**: Merges traffic accident data from a temporary branch into the staging branch.

  <center>
      <img src="image/Airflow/Silver_to_Staging_Accidents_Merge_DAG.png" width="900" />
  </center>
- **`Silver_to_Staging_Parkinglot_Merge_DAG`**: Merges parking lot data from a temporary branch into the staging branch.

  <center>
      <img src="image/Airflow/Silver_to_Staging_Parkinglot_Merge_DAG.png" width="900" />
  </center>
- **`Silver_to_Staging_Storagetank_Merge_DAG`**: Merges storage tank data from a temporary branch into the staging branch.

  <center>
      <img src="image/Airflow/Silver_to_Staging_Storagetank_Merge_DAG.png" width="900" />
  </center>
- **`Silver_to_Staging_Traffic_Merge_DAG`**: Merges traffic data from a temporary branch into the staging branch.

  <center>
      <img src="image/Airflow/Silver_to_Staging_Traffic_Merge_DAG.png" width="900" />
  </center>
- **`Silver_to_Staging_Weather_Merge_DAG`**: Merges weather data from a temporary branch into the staging branch.

  <center>
      <img src="image/Airflow/Silver_to_Staging_Weather_Merge_DAG.png" width="900" />
  </center>
- **`Silver_Main_Gasstation_Sync_DAG`**: Merges gasstation data from the staging branch into the main branch.

  <center>
      <img src="image/Airflow/Silver_Main_Gasstation_Sync_DAG.png" width="900" />
  </center>
- **`Silver_Main_Parking_Sync_DAG`**: Merges parking data from the staging branch into the main branch.

  <center>
      <img src="image/Airflow/Silver_Main_Parking_Sync_DAG.png" width="900" />
  </center>
- **`Staging_to_Main_Accidents_Merge_DAG`**: Merges traffic accident data from the staging branch into the main branch.

  <center>
      <img src="image/Airflow/Staging_to_Main_Accidents_Merge_DAG.png" width="900" />
  </center>
- **`Staging_to_Main_Traffic_Merge_DAG`**: Merges traffic data from the staging branch into the main branch.

  <center>
      <img src="image/Airflow/Staging_to_Main_Traffic_Merge_DAG.png" width="900" />
  </center>
- **`Staging_to_Main_Weather_Merge_DAG`**: Merge weather data from staging to main branch

  <center>
      <img src="image/Airflow/Staging_to_Main_Weather_Merge_DAG.png" width="900" />
  </center>
- **`Gold_Dimension_Tables_Load_DAG`**: Create dimension tables and commits them to the main branch.

  <center>
      <img src="image/Airflow/Gold_Dimension_Tables_Load_DAG.png" width="900" />
  </center>
- **`Gold_Fact_Tables_WithUpsert_Load_DAG`**: To update and commit fact tables with upsert functionality into the main branch.

  <center>
      <img src="image/Airflow/Gold_Fact_Tables_WithUpsert_Load_DAG.png" width="900" />
  </center>
- **`Gold_Fact_Tables_WithOverwrite_Load_DAG`**: To update and commit fact tables with overwrite functionality into the main branch.

  <center>
      <img src="image/Airflow/Gold_Fact_Tables_WithOverwrite_Load_DAG.png" width="900" />
  </center>

**2.7. Apache Flink**

- Open the `Apache Flink` Web UI by navigating to `http://localhost:8085` in your browser. You can view active jobs, completed jobs, and failed jobs, along with their associated details such as execution plans and performance metrics.

  <center>
      <img src="image/Flink/Flink.png" width="900" />
  </center>

**2.8. Kafka via Redpanda UI**

- Access the `Redpanda` Web UI by navigating to `http://localhost:1010` in your browser. This intuitive interface provides an efficient way to manage and monitor your Kafka infrastructure.

  <center>
      <img src="image/Redpanda/Overview.png" width="900" />
  </center>

  - **View Topics**: Explore the complete list of `Kafka` topics and monitor their performance.

    <center>
        <img src="image/Redpanda/Redpanda.png" width="900" />
    </center>

  - **Consumer Groups**: Analyze the list of active consumer groups and monitor their consumption patterns.

    <center>
        <img src="image/Redpanda/Consumer.png" width="900" />
    </center>

  - **Broker Configuration**: Inspect and manage the broker settings to optimize your `Kafka` cluster’s performance.

    <center>
        <img src="image/Redpanda/Broker.png" width="900" />
    </center>

**2.9. Redis**

- Access the `Redis Insight` dashboard by visiting `http://localhost:6379` in your browser. `Redis Insight` allows you to inspect keys, monitor performance metrics, and visualize data structures like hashes, sets, and lists.

  <center>
      <img src="image/Redis/Redis.png" width="900" />
  </center>

  - Use this dashboard to perform administrative tasks, such as analyzing key usage and monitoring latency, to ensure `Redis` is performing optimally for your application. The intuitive UI helps streamline debugging and data analysis.

**2.10. Clikhouse via Dbeaver**

- To connect to the `ClickHouse` database through `DBeaver`, configure the connection using the following settings:

  - **Host**: *localhost*
  - **Port**: *18123*
  - **Database/Schema**: *smart_traffic_db*
  - **Username**: *ren294*
  - **Password**: *trungnghia294*

  <center>
      <img src="image/Dbeaver/CH_conn.png" width="400" />
  </center>
- Once the connection is established, the `DBeaver` interface will display the connected database, as shown below:

  <center>
      <img src="image/Dbeaver/Ch.png" width="300" />
  </center>
- Explore the schema and table structures within the **smart_traffic_db** database to understand the relationships and data stored:

  <center>
      <img src="image/Dbeaver/smart_traffic_db.png" width="900" />
  </center>
- Use `DBeaver`’s SQL editor to execute queries, analyze data, and visualize query results directly within the tool.

**2.11. Trino via Dbeaver**

- Connect to the `Trino` query engine via `DBeaver` by setting up a connection with the following details:

  - **Host**: *localhost*
  - **Port**: *8080*
  - **Database/Schema**: *default*
  - **Username**: *admin*
  - **Password**: (leave blank)

  <center>
      <img src="image/Dbeaver/Trino_conn.png" width="400" />
  </center>
- After successfully connecting, you can view the Trino instance in DBeaver as shown below:

  <center>
      <img src="image/Dbeaver/Trino.png" width="300" />
  </center>
- The database schema will appear in a graphical representation for easy navigation and exploration:

  <center>
      <img src="image/Dbeaver/hudi-default.png" width="900" />
  </center>

**2.12. Promotheus**

- Launch the `Prometheus` Web UI by navigating to `http://localhost:9090` in your browser.

  <center>
      <img src="image/Promotheus/Home.png" width="900" />
  </center>
- Use the `Prometheus` query interface to explore metrics and analyze performance trends. The metrics browser allows you to filter and visualize time-series data based on specific query expressions:

  <center>
      <img src="image/Promotheus/Metrics.png" width="400" />
  </center>

### 3. Visualization

#### 3.1. Visualization with Superset

- Access `Superset` by navigating to `http://localhost:8081` in your web browser.
- Log in using the following credentials:

  - **Username**: *ren294*
  - **Password**: *trungnghia294*

- Import the pre-configured templates for charts, dashboards, datasets, and queries from the folder [superset/Template](superset/Template/). These templates are designed to provide seamless insights and accelerate your visualization setup.
- After successfully importing, you will have access to a comprehensive collection of visualizations, including:

  - **Dashboards:**

  <center>
      <img src="image/superset/Dashboard.png" width="800" />
  </center>

  - **Charts:**

  <center>
      <img src="image/superset/Charts.png" width="800" />
  </center>

  - **Datasets:**

  <center>
      <img src="image/superset/Datasets.png" width="800" />
  </center>
- Superset’s interactive interface allows you to drill down into data, create new visualizations, and customize dashboards to meet your analytical needs.

#### 3.2. Visualization with Metabase

- Open the `Metabase` by navigating to `http://localhost:3000` in your browser
- Connect to the `Trino` query engine by setting up a connection with the following details:

  - **Host**: *trino-coordinator*
  - **Port**: *8080*
  - **Database/Schema**: *hive*
  - **Username**: *admin*
  - **Password**: (leave blank)

  <center>
      <img src="image/metabase/Conn.png" width="800" />
  </center>

  - After configuring the connection, you will see the available `Trino` tables in the `Browse Data` section of `Metabase`:
    <center>
        <img src="image/metabase/Trino.png" width="800" />
    </center>
- Utilize the SQL query files located in the [metabase/dashboard](metabase/dashboard/) folder to create visually appealing and interactive dashboards. These pre-written queries are optimized for extracting meaningful insights, making it easier to visualize complex datasets:

  <center>
      <img src="image/metabase/home.png" width="800" />
  </center>
- With `Metabase`, you can effortlessly build and customize dashboards, explore data trends, and share insights with your team.

### 4. Monitoring

- Access `Grafana` for monitoring by navigating to `http://localhost:3001` in your browser.
- Log in using the following credentials:

  - **Username**: *ren294*
  - **Password**: *trungnghia294*

- Import the pre-built monitoring dashboards from the [grafana/Dashboard](grafana/Dashboard/) folder to instantly visualize critical system metrics and performance indicators.

  <center>
      <img src="image/Grafana/home.png" width="800" />
  </center>

- `Grafana` enables real-time monitoring with customizable dashboards, allowing you to track system health, detect anomalies, and ensure smooth operation. The intuitive interface provides detailed insights into your infrastructure and application performance.

# Result

## 1. Visualization Real-time with Superset

### Traffic Dashboard

Gain instant insights into real-time traffic conditions with this comprehensive dashboard, offering detailed analytics on road usage and congestion patterns.

<center>
      <img src="image/superset/TrafficDashboard.png" width="900" />
  </center>

### Accident Dashboard

Monitor and analyze accident data in real-time to identify trends and enhance road safety measures effectively.

<center>
      <img src="image/superset/AccidentDashboard.png" width="900" />
  </center>

### Weather Dashboard

Stay updated on real-time weather conditions, helping to correlate traffic and safety insights with environmental factors.

<center>
      <img src="image/superset/WeatherDashboard.png" width="900" />
  </center>

## 2. Visualization Warehouse with Metabase

### Parking Transaction Dashboard

Dive into detailed parking transaction data, enabling better resource planning and operational insights.

<center>
      <img src="image/metabase/parkingtransaction.png" width="900" />
  </center>

### Vehicle Movement Dashboard

Track vehicle movements across key areas to understand traffic flow and optimize transport infrastructure.

<center>
      <img src="image/metabase/vehiclemovement.png" width="900" />
  </center>

### Traffic Incident Dashboard

Visualize and explore traffic incident patterns to improve emergency response and preventive measures.

<center>
      <img src="image/metabase/trafficincident.png" width="900" />
  </center>

### Gas Station Transaction Dashboard

Analyze fuel consumption patterns and gas station transactions to better understand travel behavior and fuel efficiency.

<center>
      <img src="image/metabase/gasstationtransaction.png" width="900" />
  </center>

## 3. Application for External User with Streamlit

- Open the `Streamlit` Web UI by navigating to `http://localhost:8501` in your browser.

### Current Road Traffic Information

Access live updates on traffic conditions for the current road, providing essential details for drivers.

<center>
      <img src="image/streamlit/CurrentRoad.png" width="900" />
  </center>

### Nearby Road Traffic Information

Stay informed about real-time traffic on three nearby roads, empowering better route planning and decision-making.

<center>
      <img src="image/streamlit/3Road.png" width="900" />
  </center>

## 4. Monitoring with Grafana

### Monitoring Airflow

Visualize and track the performance and health of your Airflow workflows with a dedicated monitoring dashboard.

<center>
      <img src="image/Grafana/Airflow/Airflow.png" width="900" />
  </center>

### Monitoring ClickHouse

Keep a close eye on ClickHouse database performance metrics to ensure efficient query processing.

<center>
      <img src="image/Grafana/ClickHouse/ClickHouse.png" width="900" />
  </center>

### Monitoring Flink

Track the performance and stability of Apache Flink jobs, ensuring optimal real-time data processing.

<center>
      <img src="image/Grafana/Flink/Flink.png" width="900" />
  </center>

### Monitoring Kafka

Monitor Kafka brokers and topics in real-time to maintain smooth message streaming and processing pipelines.

<center>
      <img src="image/Grafana/Kafka/Kafka.png" width="900" />
  </center>

### Monitoring Minio

Oversee MinIO storage performance, ensuring reliability and availability for data storage.

<center>
      <img src="image/Grafana/Minio/Minio.png" width="900" />
  </center>

### Monitoring Nifi

Gain insights into the performance of Apache NiFi workflows, tracking throughput and data processing efficiency.

<center>
      <img src="image/Grafana/Nifi/nifi.png" width="900" />
  </center>

### Monitoring Redis

Monitor Redis key-value store performance and optimize caching and session management tasks.

<center>
      <img src="image/Grafana/Redis/Redis.png" width="900" />
  </center>

### Monitoring Spark

Track Apache Spark job performance, resource utilization, and system health to maintain efficient batch and streaming processing.

<center>
      <img src="image/Grafana/Spark/Spark.png" width="900" />
  </center>

### Monitoring Docker

Ensure the stability of your containerized environment by monitoring Docker performance metrics in real-time.

<center>
      <img src="image/Grafana/Docker/Docker.png" width="900" />
  </center>

# Authors
Nguyen Trung Nghia
- Contact: trungnghia294@gmail.com
- GitHub: [Ren294](https://github.com/Ren294)
- Linkedln: [tnghia294](https://www.linkedin.com/in/tnghia294/)