package com.github.yepez1997.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys{

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        // logger properties; create a logger for my class
        String topic = "first_topic";
        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        // create producer properties
        Properties properties = new Properties();
        String kafkaIP = "127.0.0.1:9092";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaIP);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // by operating on the same key the key goes to the same partition
        // formulate ordering

        // send data with producer record
        for (int i = 0; i <  10; i++) {
            String key = "id_" + i;
            String value = "hello world " + i;
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
            logger.info("Key: " + key);
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // same keys should go to the same partition
                    if (e == null) {
                        logger.info(("Received new metadata \n." + "Topic: " + recordMetadata.topic() + "\n"
                                + "Partition: " + recordMetadata.partition() + "\n"  + "Offset: " + recordMetadata.offset() + "\n"
                                + "Timestamp: " + recordMetadata.timestamp() + "\n"
                        ));
                    } else {
                        // something went wrong
                        logger.error("Error while producing", e);

                    }
                }
                // block send to make it sync -- bad practice; do not do in production
            }).get();
        }
        // producer.send(record);

        // partitions remain the same for all keys

        // flush data
        producer.flush();

        // flush and close producer
        producer.close();
    }
}
