CREATE DATABASE smart_traffic_db;
USE smart_traffic_db;

CREATE TABLE IF NOT EXISTS weather_queue
(
    datetime String,
    datetimeEpoch UInt64,
    temp Float32,
    feelslike Float32,
    humidity Float32,
    dew Float32,
    precip Float32,
    precipprob Float32,
    snow Float32,
    snowdepth Float32,
    preciptype String,
    windgust Float32,
    windspeed Float32,
    winddir Float32,
    pressure Float32,
    visibility Float32,
    cloudcover Float32,
    solarradiation Float32,
    solarenergy Float32,
    uvindex UInt8,
    severerisk UInt8,
    conditions String,
    icon String,
    source String,
    timezone String,
    name String,
    latitude Float32,
    longitude Float32,
    resolvedAddress String,
    date String,
    address String,
    tzoffset Int8,
    day_visibility Float32,
    day_cloudcover Float32,
    day_uvindex UInt8,
    day_description String,
    day_tempmin Float32,
    day_windspeed Float32,
    day_icon String,
    day_precip Float32,
    day_tempmax Float32,
    day_precipcover Float32,
    day_pressure Float32,
    day_preciptype String,
    day_humidity Float32,
    day_conditions String,
    day_feelslike Float32,
    day_dew Float32,
    day_sunrise String,
    day_sunriseEpoch UInt64,
    day_feelslikemax Float32,
    day_windgust Float32,
    day_solarenergy Float32,
    day_sunset String,
    day_snowdepth Float32,
    day_sunsetEpoch UInt64,
    day_severerisk UInt8,
    day_solarradiation Float32,
    day_precipprob Float32,
    day_temp Float32,
    day_winddir Float32,
    day_moonphase Float32,
    day_feelslikemin Float32,
    day_snow Float32
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'broker:29092',
         kafka_topic_list = 'weatherHCMC_out',
         kafka_group_name = 'weather_ch',
         kafka_format = 'CSV';

CREATE TABLE smart_traffic_db.weather_data
(
    datetime String,
    datetimeEpoch UInt64,
    temp Float32,
    feelslike Float32,
    humidity Float32,
    dew Float32,
    precip Float32,
    precipprob Float32,
    snow Float32,
    snowdepth Float32,
    preciptype String,
    windgust Float32,
    windspeed Float32,
    winddir Float32,
    pressure Float32,
    visibility Float32,
    cloudcover Float32,
    solarradiation Float32,
    solarenergy Float32,
    uvindex UInt8,
    severerisk UInt8,
    conditions String,
    icon String,
    source String,
    timezone String,
    name String,
    latitude Float32,
    longitude Float32,
    resolvedAddress String,
    date String,
    address String,
    tzoffset Int8,
    day_visibility Float32,
    day_cloudcover Float32,
    day_uvindex UInt8,
    day_description String,
    day_tempmin Float32,
    day_windspeed Float32,
    day_icon String,
    day_precip Float32,
    day_tempmax Float32,
    day_precipcover Float32,
    day_pressure Float32,
    day_preciptype String,
    day_humidity Float32,
    day_conditions String,
    day_feelslike Float32,
    day_dew Float32,
    day_sunrise String,
    day_sunriseEpoch UInt64,
    day_feelslikemax Float32,
    day_windgust Float32,
    day_solarenergy Float32,
    day_sunset String,
    day_snowdepth Float32,
    day_sunsetEpoch UInt64,
    day_severerisk UInt8,
    day_solarradiation Float32,
    day_precipprob Float32,
    day_temp Float32,
    day_winddir Float32,
    day_moonphase Float32,
    day_feelslikemin Float32,
    day_snow Float32
)
ENGINE = MergeTree()
ORDER BY (datetime, datetimeEpoch);


CREATE MATERIALIZED VIEW smart_traffic_db.weather_consumer
TO smart_traffic_db.weather_data
AS SELECT *
FROM weather_queue;

-- ====================================================================================
CREATE TABLE IF NOT EXISTS accident_queue
(
    road_name String,
    district String,
    city String,
    car_involved Int32,
    motobike_involved Int32,
    other_involved Int32,
    accident_severity Int32,
    accident_time DateTime,
    number_of_vehicles Int32,
    estimated_recovery_time DateTime,
    congestion_km Float32,
    description String
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'broker:29092',
         kafka_topic_list = 'accidentsHCMC_out',
         kafka_group_name = 'accident_ch',
         kafka_format = 'CSV';

CREATE TABLE smart_traffic_db.accident_data
(
    road_name String,
    district String,
    city String,
    car_involved Int32,
    motobike_involved Int32,
    other_involved Int32,
    accident_severity Int32,
    accident_time DateTime,
    number_of_vehicles Int32,
    estimated_recovery_time DateTime,
    congestion_km Float32,
    description String
)
ENGINE = MergeTree()
ORDER BY (accident_time, road_name, district);

CREATE MATERIALIZED VIEW smart_traffic_db.accident_consumer
TO smart_traffic_db.accident_data
AS SELECT *
FROM accident_queue;
-- ====================================================================================
CREATE TABLE IF NOT EXISTS traffic_queue
(
    vehicle_id String,
    owner_name String,
    license_number String,
    phone String,
    email String,
    speed_kmph Float32,
    street String,
    district String,
    city String,
    timestamp DateTime,
    length_meters Float32,
    width_meters Float32,
    height_meters Float32,
    vehicle_type String,
    vehicle_classification String,
    latitude Float32,
    longitude Float32,
    is_running String,
    rpm UInt32,
    oil_pressure String,
    fuel_level_percentage UInt8,
    passenger_count UInt8,
    internal_temperature_celsius Float32,
    destination_street String,
    destination_district String,
    destination_city String,
    eta DateTime
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'broker:29092',
         kafka_topic_list = 'trafficHCMC_out',
         kafka_group_name = 'traffic_ch',
         kafka_format = 'CSV';

CREATE TABLE smart_traffic_db.traffic_data
(
    vehicle_id String,
    owner_name String,
    license_number String,
    phone String,
    email String,
    speed_kmph Float32,
    street String,
    district String,
    city String,
    timestamp DateTime,
    length_meters Float32,
    width_meters Float32,
    height_meters Float32,
    vehicle_type String,
    vehicle_classification String,
    latitude Float32,
    longitude Float32,
    is_running String,
    rpm UInt32,
    oil_pressure String,
    fuel_level_percentage UInt8,
    passenger_count UInt8,
    internal_temperature_celsius Float32,
    destination_street String,
    destination_district String,
    destination_city String,
    eta DateTime
)
ENGINE = MergeTree()
ORDER BY (timestamp, vehicle_id, city);

CREATE MATERIALIZED VIEW smart_traffic_db.traffic_consumer
TO smart_traffic_db.traffic_data
AS SELECT *
FROM traffic_queue;