package com.github.yepez1997.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args){
        // create producer properties
        Properties properties = new Properties();
        String kafkaIP = "127.0.0.1:9092";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaIP);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // create a producer record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello world");
        ProducerRecord<String, String> record2 = new ProducerRecord<String, String>("first_topic", "hello world 2");
        ProducerRecord<String, String> record3 = new ProducerRecord<String, String>("first_topic", "hello world 3");


        ArrayList<ProducerRecord> valuesToSend = new ArrayList<ProducerRecord>();
        valuesToSend.add(record);
        valuesToSend.add(record2);
        valuesToSend.add(record3);
        // send data
        for (int i = 0; i < valuesToSend.size(); i++) {
            producer.send(valuesToSend.get(i));
        }
        // producer.send(record);

        // flush data
        producer.flush();

        // flush and close producer
        producer.close();
    }
}
