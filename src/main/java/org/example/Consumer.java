package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.StreamSupport;

import static org.example.Main.env;

public class Consumer {
    //static String bootstrapServers = "localhost:29092";
    static String bootstrapServers = "localhost:" + env.getBrokers().getFirst().getPort();
    static String topicName = "Measurements";
    KafkaConsumer<Integer, String> consumer;
    ConsumerRecords<Integer, String> records;

    public Consumer() {
        consumer = new KafkaConsumer<>(getKafkaProperties());
        consumer.subscribe(Collections.singletonList(topicName));
    }

    private Properties getKafkaProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        return props;
    }

    public List<String> getData(int time) {
        records = consumer.poll(time);
        //System.out.println("size of records polled is " + records.count() + " ");
        for (ConsumerRecord<Integer, String> record : records) {
            System.out.println(record.value() + " was received at " + record.offset());
            //            System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
        }

        return StreamSupport.stream(records.spliterator(), false).toList().stream().map(record -> record.value()).toList();
    }
}
