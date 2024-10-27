package com.traffic_hcmc.weather;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class example {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "broker:29092");
        properties.setProperty("auto.offset.reset", "earliest");

        properties.setProperty("group.id", "test-group");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "weatherHCMC_in",
                new SimpleStringSchema(),
                properties
        );
        consumer.setStartFromEarliest();
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>(
                "weatherHCMC_out",
                new SimpleStringSchema(),
                properties
        );
        DataStream<String> stream = env.addSource(consumer);
        DataStream<String> processedStream = stream.map(String::toUpperCase);
        processedStream.addSink(producer);
        env.execute("Simple Kafka Example");

    }
}
