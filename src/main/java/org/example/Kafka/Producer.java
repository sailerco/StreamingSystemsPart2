package org.example.Kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.Properties;

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
        return props;
    }

    public void sendMessage(String data) {
        String a = "hi";
        ProducerRecord<String, String> message = new ProducerRecord<>(topic, a, data);
        producer.send(message);
    }

    public void sendMessageList(ArrayList<String> data) {
        String a = "test";
        for(String entry : data) {
            ProducerRecord<String, String> message = new ProducerRecord<>(topic, a, entry);
            producer.send(message);
            /*Future<RecordMetadata> future = producer.send(message);
            RecordMetadata metadata = future.get();
            System.out.println(String.valueOf(metadata.partition()));*/
        }
        producer.flush();
    }
}
