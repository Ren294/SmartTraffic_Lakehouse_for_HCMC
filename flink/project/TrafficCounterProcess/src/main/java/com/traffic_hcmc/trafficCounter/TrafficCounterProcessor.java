package com.traffic_hcmc.trafficCounter;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class TrafficCounterProcessor {
    private static final JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), "redis", 6379);

    private static class VehicleData {
        String timestamp;
        String vehicleType;
        String road;
        String district;

        public VehicleData(String timestamp, String vehicleType, String road, String district) {
            this.timestamp = timestamp;
            this.vehicleType = vehicleType;
            this.road = road;
            this.district = district;
        }
    }

    private static String normalizeRoadName(String roadName) {
        return roadName.replaceAll("\\s+", "")
                .replaceAll("Duong", "")
                .replaceAll("duong", "");
    }
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Kafka Consumer Configuration
        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", "broker:29092");
        consumerProps.setProperty("group.id", "vehicle-processor");
        consumerProps.setProperty("auto.offset.reset", "earliest");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "trafficHCMC_in",
                new SimpleStringSchema(),
                consumerProps
        );

        DataStream<VehicleData> vehicleStream = env
                .addSource(consumer)
                .map(new MapFunction<String, VehicleData>() {
                    private final ObjectMapper mapper = new ObjectMapper();
                    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                    @Override
                    public VehicleData map(String value) throws Exception {
                        JsonNode root = mapper.readTree(value);

                        String timestamp = root.get("timestamp").asText();
                        String vehicleType = root.get("vehicle_type").asText();
                        String road = root.get("road").get("street").asText();
                        String district = root.get("road").get("district").asText();

                        return new VehicleData(timestamp, vehicleType, road, district);
                    }
                });

        // Process the stream with sliding windows
        vehicleStream
                .keyBy(data -> normalizeRoadName(data.road) + "_" + data.district)
                .window(SlidingProcessingTimeWindows.of(Time.minutes(5), Time.minutes(1)))
                .process(new ProcessWindowFunction<VehicleData, Object, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<VehicleData> elements, Collector<Object> out) {
                        Map<String, Integer> vehicleCounts = new HashMap<>();
                        vehicleCounts.put("Motorbike", 0);
                        vehicleCounts.put("Car", 0);
                        vehicleCounts.put("Truck", 0);
                        vehicleCounts.put("Bus", 0);
                        vehicleCounts.put("Bicycle", 0);

                        // Count vehicles by type
                        for (VehicleData data : elements) {
                            vehicleCounts.merge(data.vehicleType, 1, Integer::sum);
                        }

                        // Store in Redis
                        try (Jedis jedis = jedisPool.getResource()) {
                            String redisKey = "traffic_" + key;
                            Map<String, String> redisData = new HashMap<>();

                            // Convert counts to strings for Redis
                            for (Map.Entry<String, Integer> entry : vehicleCounts.entrySet()) {
                                redisData.put(entry.getKey(), String.valueOf(entry.getValue()));
                            }

                            jedis.hmset(redisKey, redisData);
                            // Set expiry time to 10 minutes (twice the window size)
                            jedis.expire(redisKey, 600);
                        }
                    }
                });

        env.execute("Vehicle Count Processor");
    }
    }
