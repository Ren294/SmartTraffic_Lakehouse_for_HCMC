//////////////////////////////////////////////////////////////////////////////
// Project: SmartTraffic_Lakehouse_for_HCMC
// Author: Nguyen Trung Nghia (ren294)
// Contact: trungnghia294@gmail.com
// GitHub: Ren294
//////////////////////////////////////////////////////////////////////////////
package com.traffic_hcmc.storageTank;

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

public class StorageTankProcessor {
    private static final JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), "redis", 6379);
    private static final ObjectMapper mapper = new ObjectMapper();
    private static class StorageTankData {
        Integer tankId;
        Integer gasStationId;
        String tankName;
        Integer capacity;
        String materialType;
        String currentQuantity;

        public StorageTankData(JsonNode payload) {
            JsonNode after = payload.get("after");
            this.tankId = after.get("tankid").asInt();
            this.gasStationId = after.get("gasstationid").asInt();
            this.tankName = after.get("tankname").asText();
            this.capacity = after.get("capacity").asInt();
            this.materialType = after.get("materialtype").asText();

            // Handle potential byte conversion for currentquantity
            JsonNode currentQuantityNode = after.get("currentquantity");
            this.currentQuantity = currentQuantityNode != null ? currentQuantityNode.asText() : "N/A";
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Kafka Consumer Configuration
        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", "broker:29092");
        consumerProps.setProperty("group.id", "storage-tank-processor");
        consumerProps.setProperty("auto.offset.reset", "earliest");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "storagetank",
                new SimpleStringSchema(),
                consumerProps
        );
        consumer.setStartFromEarliest();

        DataStream<StorageTankData> storageTankStream = env
                .addSource(consumer)
                .map(new MapFunction<String, StorageTankData>() {
                    private final ObjectMapper mapper = new ObjectMapper();

                    @Override
                    public StorageTankData map(String value) throws Exception {
                        JsonNode root = mapper.readTree(value);
                        JsonNode payload = root.get("payload");

                        // Process 'create', 'update', or 'read' operations
                        if (payload.has("op") &&
                                (payload.get("op").asText().equals("c") ||
                                        payload.get("op").asText().equals("u") ||
                                        payload.get("op").asText().equals("r"))) {
                            return new StorageTankData(payload);
                        }
                        return null;
                    }
                })
                // Filter out null values
                .filter(data -> data != null);

        // Process the stream and store in Redis
        storageTankStream.map(data -> {
            try (Jedis jedis = jedisPool.getResource()) {
                // Create Redis key based on gas station ID
                String redisKey = "storage_tank_" + data.gasStationId;

                // Create a unique hash key for each tank within the gas station
                String tankHashKey = "tank_" + data.tankId;

                // Prepare Redis hash data for this specific tank
                Map<String, String> redisData = new HashMap<>();
                redisData.put("tankid", String.valueOf(data.tankId));
                redisData.put("tankname", data.tankName);
                redisData.put("capacity", String.valueOf(data.capacity));
                redisData.put("materialtype", data.materialType);
                redisData.put("currentquantity", data.currentQuantity);

                // Store the tank data as a nested hash in Redis
                jedis.hset(redisKey, tankHashKey, mapper.writeValueAsString(redisData));

                System.out.println("Processed tank: " + redisKey + " - " + tankHashKey);
            }
            return data;
        }).print(); // Optional: print for logging

        env.execute("Storage Tank Processor");
    }
}