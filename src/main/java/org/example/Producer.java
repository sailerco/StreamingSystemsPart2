package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.Properties;

import static org.example.Main.env;

//import static org.example.Main.env;

public class Producer {
    //static String bootstrapServers = "localhost:29092";
    static String bootstrapServers = "localhost:" + env.getBrokers().getFirst().getPort();
    static String topic = "Measurements";
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
        //todo:
        String a = "hi";
        ProducerRecord<String, String> message = new ProducerRecord<>(topic, a, data);
        producer.send(message);
    }

    public void sendMessageList(ArrayList<String> data) {
        String a = "test";
        for(String entry : data) {
            ProducerRecord<String, String> message = new ProducerRecord<>(topic, a, entry);
            producer.send(message);
        }
        producer.flush();
    }
}
