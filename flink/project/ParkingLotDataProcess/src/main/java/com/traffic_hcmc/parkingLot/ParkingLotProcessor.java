package com.traffic_hcmc.parkingLot;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ParkingLotProcessor {
    private static final JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), "redis", 6379);

    private static class ParkingLotData {
        Integer parkingLotId;
        String name;
        String location;
        Integer totalSpaces;
        Integer availableSpaces;
        Integer carSpaces;
        Integer motorbikeSpaces;
        Integer bicycleSpaces;
        String type;
        String hourlyRate;

        public ParkingLotData(JsonNode payload) {
            JsonNode after = payload.get("after");
            this.parkingLotId = after.get("parkinglotid").asInt();
            this.name = after.get("name").asText();
            this.location = after.get("location").asText();
            this.totalSpaces = after.get("totalspaces").asInt();
            this.availableSpaces = after.get("availablespaces").asInt();
            this.carSpaces = after.get("carspaces").asInt();
            this.motorbikeSpaces = after.get("motorbikespaces").asInt();
            this.bicycleSpaces = after.get("bicyclespaces").asInt();
            this.type = after.get("type").asText();
            this.hourlyRate = after.get("hourlyrate").asText();
        }
    }

    private static String normalizeLocationName(String location) {
        return location.replaceAll("\\s+", "_")
                .replaceAll(",", "_")
                .replaceAll("TP", "");
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Kafka Consumer Configuration
        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", "broker:29092");
        consumerProps.setProperty("group.id", "parking-lot-processor");
        consumerProps.setProperty("auto.offset.reset", "earliest");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "parkinglot",
                new SimpleStringSchema(),
                consumerProps
        );
        consumer.setStartFromEarliest();

        DataStream<ParkingLotData> parkingLotStream = env
                .addSource(consumer)
                .map(new MapFunction<String, ParkingLotData>() {
                    private final ObjectMapper mapper = new ObjectMapper();

                    @Override
                    public ParkingLotData map(String value) throws Exception {
                        JsonNode root = mapper.readTree(value);
                        JsonNode payload = root.get("payload");

                        // Only process 'create' or 'update' operations
                        if (payload.has("op") && (payload.get("op").asText().equals("c") ||
                                payload.get("op").asText().equals("u"))) {
                            return new ParkingLotData(payload);
                        }
//                        return null;
                        return new ParkingLotData(payload);
                    }
                })
                // Filter out null values (for delete or other operations)
                .filter(data -> data != null);

        // Process the stream and store in Redis
        parkingLotStream.map(data -> {
            try (Jedis jedis = jedisPool.getResource()) {
                String redisKey = "parking_lot_" + normalizeLocationName(data.location);
                System.out.println(redisKey);
                Map<String, String> redisData = new HashMap<>();
                redisData.put("parkinglotid", String.valueOf(data.parkingLotId));
                redisData.put("name", data.name);
                redisData.put("location", data.location);
                redisData.put("totalspaces", String.valueOf(data.totalSpaces));
                redisData.put("availablespaces", String.valueOf(data.availableSpaces));
                redisData.put("carspaces", String.valueOf(data.carSpaces));
                redisData.put("motorbikespaces", String.valueOf(data.motorbikeSpaces));
                redisData.put("bicyclespaces", String.valueOf(data.bicycleSpaces));
                redisData.put("type", data.type);
                redisData.put("hourlyrate", data.hourlyRate);
                System.out.println(redisData);
                jedis.hmset(redisKey, redisData);
            }
            return data;
        }).print(); // Optional: print for logging

        env.execute("Parking Lot Processor");
    }
}