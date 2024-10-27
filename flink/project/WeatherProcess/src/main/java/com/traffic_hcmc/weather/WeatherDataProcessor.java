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

import java.text.DecimalFormat;
import java.util.Properties;

public class WeatherDataProcessor {
    private static final DecimalFormat df = new DecimalFormat("#.##");
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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

                            // Xử lý các trường cơ bản
                            csv.append(root.get("datetime").asText()).append(",");
                            csv.append(root.get("datetimeEpoch").asLong()).append(",");
                            csv.append(df.format(root.get("temp").asDouble())).append(",");
                            csv.append(df.format(root.get("feelslike").asDouble())).append(",");
                            csv.append(df.format(root.get("humidity").asDouble())).append(",");
                            csv.append(df.format(root.get("dew").asDouble())).append(",");
                            csv.append(df.format(root.get("precip").asDouble())).append(",");
                            csv.append(df.format(root.get("precipprob").asDouble())).append(",");
                            csv.append(df.format(root.get("snow").asDouble())).append(",");
                            csv.append(df.format(root.get("snowdepth").asDouble())).append(",");

                            // Xử lý preciptype
                            JsonNode preciptype = root.get("preciptype");
                            if (preciptype.isNull()) {
                                csv.append("").append(",");
                            } else if (preciptype.isArray() && preciptype.size() > 0) {
                                csv.append(preciptype.get(0).asText()).append(",");
                            } else if (preciptype.isTextual()) {
                                csv.append(preciptype.asText()).append(",");
                            } else {
                                csv.append("").append(",");
                            }

                            // Tiếp tục với các trường còn lại
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
                            csv.append(root.get("conditions").asText()).append(",");
                            csv.append(root.get("icon").asText()).append(",");
                            csv.append(root.get("source").asText()).append(",");
                            csv.append(root.get("timezone").asText()).append(",");
                            csv.append(root.get("name").asText()).append(",");
                            csv.append(df.format(root.get("latitude").asDouble())).append(",");
                            csv.append(df.format(root.get("longitude").asDouble())).append(",");
                            csv.append(root.get("resolvedAddress").asText()).append(",");
                            csv.append(root.get("date").asText()).append(",");
                            csv.append(root.get("address").asText()).append(",");
                            csv.append(root.get("tzoffset").asInt()).append(",");

                            // Xử lý các trường trong day
                            JsonNode day = root.get("day");
                            csv.append(df.format(day.get("visibility").asDouble())).append(",");
                            csv.append(df.format(day.get("cloudcover").asDouble())).append(",");
                            csv.append(day.get("uvindex").asInt()).append(",");
                            csv.append(day.get("description").asText()).append(",");
                            csv.append(df.format(day.get("tempmin").asDouble())).append(",");
                            csv.append(df.format(day.get("windspeed").asDouble())).append(",");
                            csv.append(day.get("icon").asText()).append(",");
                            csv.append(df.format(day.get("precip").asDouble())).append(",");
                            csv.append(df.format(day.get("tempmax").asDouble())).append(",");
                            csv.append(df.format(day.get("precipcover").asDouble())).append(",");
                            csv.append(df.format(day.get("pressure").asDouble())).append(",");
                            csv.append(day.get("preciptype").asText()).append(",");
                            csv.append(df.format(day.get("humidity").asDouble())).append(",");
                            csv.append(day.get("conditions").asText()).append(",");
                            csv.append(df.format(day.get("feelslike").asDouble())).append(",");
                            csv.append(df.format(day.get("dew").asDouble())).append(",");
                            csv.append(day.get("sunrise").asText()).append(",");
                            csv.append(day.get("sunriseEpoch").asLong()).append(",");
                            csv.append(df.format(day.get("feelslikemax").asDouble())).append(",");
                            csv.append(df.format(day.get("windgust").asDouble())).append(",");
                            csv.append(df.format(day.get("solarenergy").asDouble())).append(",");
                            csv.append(day.get("sunset").asText()).append(",");
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
                            // Log lỗi nếu cần
                            System.err.println("Error processing record: " + e.getMessage());
                        }

                    }
                });
        processedStream.addSink(producer);
        env.execute("Weather Data Processor");

    }
}
