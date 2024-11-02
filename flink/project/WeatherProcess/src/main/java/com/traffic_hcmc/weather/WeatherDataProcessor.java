package com.traffic_hcmc.weather;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class WeatherDataProcessor {
    private static final DecimalFormat df = new DecimalFormat("#.##");
    private static final JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), "redis", 6379);
    private static String cleanValue(String value) {
        return value == null ? "" : value.replace(",", "").trim();
    }
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Kafka Consumer Configuration
        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", "broker:29092");
        consumerProps.setProperty("group.id", "weather-processor");
        consumerProps.setProperty("auto.offset.reset", "earliest");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "weatherHCMC_in",
                new SimpleStringSchema(),
                consumerProps
        );
        consumer.setStartFromEarliest();

        // Kafka Producer Configuration
        Properties producerProps = new Properties();
        producerProps.setProperty("bootstrap.servers", "broker:29092");
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>(
                "weatherHCMC_out",
                new SimpleStringSchema(),
                producerProps
        );

        DataStream<String> processedStream = env
                .addSource(consumer)
                .process(new ProcessFunction<String, String>() {
                    private final ObjectMapper mapper = new ObjectMapper();

                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                        try {
                            JsonNode root = mapper.readTree(value);
                            StringBuilder csv = new StringBuilder();

                            // Prepare data for Redis
                            Map<String, String> weatherData = new HashMap<>();
                            weatherData.put("datetime", root.get("datetime").asText());
                            weatherData.put("datetimeEpoch", String.valueOf(root.get("datetimeEpoch").asLong()));
                            weatherData.put("temp", df.format(root.get("temp").asDouble()));
                            weatherData.put("feelslike", df.format(root.get("feelslike").asDouble()));
                            weatherData.put("humidity", df.format(root.get("humidity").asDouble()));
                            weatherData.put("dew", df.format(root.get("dew").asDouble()));
                            weatherData.put("precip", df.format(root.get("precip").asDouble()));
                            weatherData.put("precipprob", df.format(root.get("precipprob").asDouble()));
                            weatherData.put("snow", df.format(root.get("snow").asDouble()));
                            weatherData.put("snowdepth", df.format(root.get("snowdepth").asDouble()));

                            // Handle preciptype
                            JsonNode preciptypea = root.get("preciptype");
                            if (preciptypea.isNull()) {
                                weatherData.put("preciptype", "");
                            } else if (preciptypea.isArray() && preciptypea.size() > 0) {
                                weatherData.put("preciptype", preciptypea.get(0).asText());
                            } else if (preciptypea.isTextual()) {
                                weatherData.put("preciptype", preciptypea.asText());
                            } else {
                                weatherData.put("preciptype", "");
                            }

                            weatherData.put("windgust", df.format(root.get("windgust").asDouble()));
                            weatherData.put("windspeed", df.format(root.get("windspeed").asDouble()));
                            weatherData.put("winddir", df.format(root.get("winddir").asDouble()));
                            weatherData.put("pressure", df.format(root.get("pressure").asDouble()));
                            weatherData.put("visibility", df.format(root.get("visibility").asDouble()));
                            weatherData.put("cloudcover", df.format(root.get("cloudcover").asDouble()));
                            weatherData.put("solarradiation", df.format(root.get("solarradiation").asDouble()));
                            weatherData.put("solarenergy", df.format(root.get("solarenergy").asDouble()));
                            weatherData.put("uvindex", String.valueOf(root.get("uvindex").asInt()));
                            weatherData.put("severerisk", String.valueOf(root.get("severerisk").asInt()));
                            weatherData.put("conditions", root.get("conditions").asText());
                            weatherData.put("icon", root.get("icon").asText());
                            weatherData.put("source", root.get("source").asText());
                            weatherData.put("timezone", root.get("timezone").asText());
                            weatherData.put("name", root.get("name").asText());
                            weatherData.put("latitude", df.format(root.get("latitude").asDouble()));
                            weatherData.put("longitude", df.format(root.get("longitude").asDouble()));
                            weatherData.put("resolvedAddress", root.get("resolvedAddress").asText());
                            weatherData.put("date", root.get("date").asText());
                            weatherData.put("address", root.get("address").asText());
                            weatherData.put("tzoffset", String.valueOf(root.get("tzoffset").asInt()));

                            // Store in Redis with fixed key "weather"
                            try (Jedis jedis = jedisPool.getResource()) {
                                jedis.hmset("weather", weatherData);
                            }

                            // Original CSV processing for Kafka
                            csv.append(cleanValue(root.get("datetime").asText())).append(",");
                            csv.append(root.get("datetimeEpoch").asLong()).append(",");
                            csv.append(df.format(root.get("temp").asDouble())).append(",");
                            csv.append(df.format(root.get("feelslike").asDouble())).append(",");
                            csv.append(df.format(root.get("humidity").asDouble())).append(",");
                            csv.append(df.format(root.get("dew").asDouble())).append(",");
                            csv.append(df.format(root.get("precip").asDouble())).append(",");
                            csv.append(df.format(root.get("precipprob").asDouble())).append(",");
                            csv.append(df.format(root.get("snow").asDouble())).append(",");
                            csv.append(df.format(root.get("snowdepth").asDouble())).append(",");

                            // Handle preciptype with comma removal
                            JsonNode preciptype = root.get("preciptype");
                            if (preciptype.isNull()) {
                                csv.append("").append(",");
                            } else if (preciptype.isArray() && preciptype.size() > 0) {
                                csv.append(cleanValue(preciptype.get(0).asText())).append(",");
                            } else if (preciptype.isTextual()) {
                                csv.append(cleanValue(preciptype.asText())).append(",");
                            } else {
                                csv.append("").append(",");
                            }

                            // Continue with remaining fields
                            csv.append(df.format(root.get("windgust").asDouble())).append(",");
                            csv.append(df.format(root.get("windspeed").asDouble())).append(",");
                            csv.append(df.format(root.get("winddir").asDouble())).append(",");
                            csv.append(df.format(root.get("pressure").asDouble())).append(",");
                            csv.append(df.format(root.get("visibility").asDouble())).append(",");
                            csv.append(df.format(root.get("cloudcover").asDouble())).append(",");
                            csv.append(df.format(root.get("solarradiation").asDouble())).append(",");
                            csv.append(df.format(root.get("solarenergy").asDouble())).append(",");
                            csv.append(root.get("uvindex").asInt()).append(",");
                            csv.append(root.get("severerisk").asInt()).append(",");
                            csv.append(cleanValue(root.get("conditions").asText())).append(",");
                            csv.append(cleanValue(root.get("icon").asText())).append(",");
                            csv.append(cleanValue(root.get("source").asText())).append(",");
                            csv.append(cleanValue(root.get("timezone").asText())).append(",");
                            csv.append(cleanValue(root.get("name").asText())).append(",");
                            csv.append(df.format(root.get("latitude").asDouble())).append(",");
                            csv.append(df.format(root.get("longitude").asDouble())).append(",");
                            csv.append(cleanValue(root.get("resolvedAddress").asText())).append(",");
                            csv.append(cleanValue(root.get("date").asText())).append(",");
                            csv.append(cleanValue(root.get("address").asText())).append(",");
                            csv.append(root.get("tzoffset").asInt()).append(",");

                            // Process day fields with comma removal
                            JsonNode day = root.get("day");
                            csv.append(df.format(day.get("visibility").asDouble())).append(",");
                            csv.append(df.format(day.get("cloudcover").asDouble())).append(",");
                            csv.append(day.get("uvindex").asInt()).append(",");
                            csv.append(cleanValue(day.get("description").asText())).append(",");
                            csv.append(df.format(day.get("tempmin").asDouble())).append(",");
                            csv.append(df.format(day.get("windspeed").asDouble())).append(",");
                            csv.append(cleanValue(day.get("icon").asText())).append(",");
                            csv.append(df.format(day.get("precip").asDouble())).append(",");
                            csv.append(df.format(day.get("tempmax").asDouble())).append(",");
                            csv.append(df.format(day.get("precipcover").asDouble())).append(",");
                            csv.append(df.format(day.get("pressure").asDouble())).append(",");
                            csv.append(cleanValue(day.get("preciptype").asText())).append(",");
                            csv.append(df.format(day.get("humidity").asDouble())).append(",");
                            csv.append(cleanValue(day.get("conditions").asText())).append(",");
                            csv.append(df.format(day.get("feelslike").asDouble())).append(",");
                            csv.append(df.format(day.get("dew").asDouble())).append(",");
                            csv.append(cleanValue(day.get("sunrise").asText())).append(",");
                            csv.append(day.get("sunriseEpoch").asLong()).append(",");
                            csv.append(df.format(day.get("feelslikemax").asDouble())).append(",");
                            csv.append(df.format(day.get("windgust").asDouble())).append(",");
                            csv.append(df.format(day.get("solarenergy").asDouble())).append(",");
                            csv.append(cleanValue(day.get("sunset").asText())).append(",");
                            csv.append(df.format(day.get("snowdepth").asDouble())).append(",");
                            csv.append(day.get("sunsetEpoch").asLong()).append(",");
                            csv.append(day.get("severerisk").asInt()).append(",");
                            csv.append(df.format(day.get("solarradiation").asDouble())).append(",");
                            csv.append(df.format(day.get("precipprob").asDouble())).append(",");
                            csv.append(df.format(day.get("temp").asDouble())).append(",");
                            csv.append(df.format(day.get("winddir").asDouble())).append(",");
                            csv.append(df.format(day.get("moonphase").asDouble())).append(",");
                            csv.append(df.format(day.get("feelslikemin").asDouble())).append(",");
                            csv.append(df.format(day.get("snow").asDouble()));

                            out.collect(csv.toString());
                        } catch (Exception e) {
                            System.err.println("Error processing record: " + e.getMessage());
                            e.printStackTrace();
                        }
                    }
                });

        processedStream.addSink(producer);
        env.execute("Weather Data Processor");
    }
}
