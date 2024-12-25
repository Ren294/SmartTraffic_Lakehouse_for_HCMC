//////////////////////////////////////////////////////////////////////////////
// Project: SmartTraffic_Lakehouse_for_HCMC
// Author: Nguyen Trung Nghia (ren294)
// Contact: trungnghia294@gmail.com
// GitHub: Ren294
//////////////////////////////////////////////////////////////////////////////
package com.traffic_hcmc.vehicleData;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

import java.text.DecimalFormat;
import java.util.Properties;

public class VehicleDataProcessor {
    private static final DecimalFormat df = new DecimalFormat("#.##");

    private static String cleanValue(String value) {
        return value == null ? "" : value.replace(",", "").trim();
    }
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", "broker:29092");
        consumerProps.setProperty("group.id", "vehicle-processor");
        consumerProps.setProperty("auto.offset.reset", "earliest");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "trafficHCMC_in",
                new SimpleStringSchema(),
                consumerProps
        );
        consumer.setStartFromEarliest();

        Properties producerProps = new Properties();
        producerProps.setProperty("bootstrap.servers", "broker:29092");
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>(
                "trafficHCMC_out",
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

                            csv.append(cleanValue(root.get("vehicle_id").asText())).append(",");
                            csv.append(cleanValue(root.path("owner").path("name").asText())).append(",");
                            csv.append(cleanValue(root.path("owner").path("license_number").asText())).append(",");
                            csv.append(cleanValue(root.path("owner").path("contact_info").path("phone").asText())).append(",");
                            csv.append(cleanValue(root.path("owner").path("contact_info").path("email").asText())).append(",");
                            csv.append(df.format(root.get("speed_kmph").asDouble())).append(",");
                            csv.append(cleanValue(root.path("road").path("street").asText())).append(",");
                            csv.append(cleanValue(root.path("road").path("district").asText())).append(",");
                            csv.append(cleanValue(root.path("road").path("city").asText())).append(",");
                            csv.append(cleanValue(root.get("timestamp").asText())).append(",");
                            csv.append(df.format(root.path("vehicle_size").path("length_meters").asDouble())).append(",");
                            csv.append(df.format(root.path("vehicle_size").path("width_meters").asDouble())).append(",");
                            csv.append(df.format(root.path("vehicle_size").path("height_meters").asDouble())).append(",");
                            csv.append(cleanValue(root.get("vehicle_type").asText())).append(",");
                            csv.append(cleanValue(root.get("vehicle_classification").asText())).append(",");
                            csv.append(df.format(root.path("coordinates").path("latitude").asDouble())).append(",");
                            csv.append(df.format(root.path("coordinates").path("longitude").asDouble())).append(",");
                            csv.append(root.path("engine_status").path("is_running").asBoolean()).append(",");
                            csv.append(root.path("engine_status").path("rpm").asInt()).append(",");
                            csv.append(cleanValue(root.path("engine_status").path("oil_pressure").asText())).append(",");
                            csv.append(root.get("fuel_level_percentage").asInt()).append(",");
                            csv.append(root.get("passenger_count").asInt()).append(",");
                            csv.append(df.format(root.get("internal_temperature_celsius").asDouble())).append(",");
                            csv.append(cleanValue(root.path("estimated_time_of_arrival").path("destination").path("street").asText())).append(",");
                            csv.append(cleanValue(root.path("estimated_time_of_arrival").path("destination").path("district").asText())).append(",");
                            csv.append(cleanValue(root.path("estimated_time_of_arrival").path("destination").path("city").asText())).append(",");
                            csv.append(cleanValue(root.path("estimated_time_of_arrival").path("eta").asText()));

                            out.collect(csv.toString());
                        } catch (Exception e) {
                            System.err.println("Error processing record: " + e.getMessage());
                            e.printStackTrace();
                        }
                    }
                });

        processedStream.addSink(producer);
        env.execute("Traffic Data Processor");
    }
}
