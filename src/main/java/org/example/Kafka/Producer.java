package org.example.Kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.example.Main.topic;

public class Producer {
    static String bootstrapServers = "localhost:29092";
    static KafkaProducer producer;

    public Producer() {
        Properties properties = getKafkaProperties();
        producer = new KafkaProducer<String, String>(properties);
    }

    private static Properties getKafkaProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //props.put("partitioner.class", "org.apache.kafka.clients.producer.RoundRobinPartitioner");
        return props;
    }

    public void sendMessage(String data, int partition) {
        producer.send(new ProducerRecord<>(topic, partition+"", data));
    }
}
