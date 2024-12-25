//////////////////////////////////////////////////////////////////////////////
// Project: SmartTraffic_Lakehouse_for_HCMC
// Author: Nguyen Trung Nghia (ren294)
// Contact: trungnghia294@gmail.com
// GitHub: Ren294
//////////////////////////////////////////////////////////////////////////////
package com.traffic_hcmc.accident;

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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class TrafficAccidentDataProcessor {
    private static final JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), "redis", 6379);
    private static String cleanValue(String value) {
        return value == null ? "" : value.replace(",", "").trim();
    }

    private static String normalizeRoadName(String roadName) {
        return roadName.replaceAll("\\s+", "")
                .replaceAll("Duong", "")
                .replaceAll("duong", "");
    }
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", "broker:29092");
        consumerProps.setProperty("group.id", "accident-processor");
        consumerProps.setProperty("auto.offset.reset", "earliest");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "accidentsHCMC_in",
                new SimpleStringSchema(),
                consumerProps
        );
        consumer.setStartFromEarliest();

        Properties producerProps = new Properties();
        producerProps.setProperty("bootstrap.servers", "broker:29092");
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>(
                "accidentsHCMC_out",
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

                            int carCount = 0;
                            int motobikeCount = 0;
                            int otherCount = 0;

                            JsonNode vehicles = root.get("vehicles_involved");
                            for (JsonNode vehicle : vehicles) {
                                String vehicleType = vehicle.get("vehicle_type").asText();
                                switch (vehicleType) {
                                    case "Car":
                                        carCount++;
                                        break;
                                    case "Motorbike":
                                        motobikeCount++;
                                        break;
                                    default:
                                        otherCount++;
                                        break;
                                }
                            }

                            String roadName = normalizeRoadName(root.get("road_name").asText());
                            Map<String, String> accidentData = new HashMap<>();
                            accidentData.put("road_name", root.get("road_name").asText());
                            accidentData.put("district", root.get("district").asText());
                            accidentData.put("city", root.get("city").asText());
                            accidentData.put("car_involved", String.valueOf(carCount));
                            accidentData.put("motobike_involved", String.valueOf(motobikeCount));
                            accidentData.put("other_involved", String.valueOf(otherCount));
                            accidentData.put("accident_severity", String.valueOf(root.get("accident_severity").asInt()));
                            accidentData.put("accident_time", root.get("accident_time").asText());
                            accidentData.put("number_of_vehicles", String.valueOf(root.get("number_of_vehicles").asInt()));
                            accidentData.put("estimated_recovery_time", root.get("estimated_recovery_time").asText());
                            accidentData.put("congestion_km", String.valueOf(root.get("congestion_km").asDouble()));
                            accidentData.put("description", root.get("description").asText());

                            try (Jedis jedis = jedisPool.getResource()) {
                                jedis.hmset("accident_" + roadName, accidentData);
                            }

                            csv.append(cleanValue(root.get("road_name").asText())).append(",");
                            csv.append(cleanValue(root.get("district").asText())).append(",");
                            csv.append(cleanValue(root.get("city").asText())).append(",");
                            csv.append(carCount).append(",");
                            csv.append(motobikeCount).append(",");
                            csv.append(otherCount).append(",");
                            csv.append(root.get("accident_severity").asInt()).append(",");
                            csv.append(cleanValue(root.get("accident_time").asText())).append(",");
                            csv.append(root.get("number_of_vehicles").asInt()).append(",");
                            csv.append(cleanValue(root.get("estimated_recovery_time").asText())).append(",");
                            csv.append(root.get("congestion_km").asDouble()).append(",");
                            csv.append(cleanValue(root.get("description").asText()));

                            out.collect(csv.toString());
                        } catch (Exception e) {
                            System.err.println("Error processing record: " + e.getMessage());
                            e.printStackTrace();
                        }
                    }
                });

        processedStream.addSink(producer);
        env.execute("Accident Data Processor");
    }
}
