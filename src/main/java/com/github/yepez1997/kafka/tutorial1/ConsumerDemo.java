package com.github.yepez1997.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {
    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

        String groupId = "my-fourth-application";
        // create consumer config
        Properties properties = new Properties();
        String topic = "first_topic";
        // bootstrap servers;
        String kafkaAPI = "127.0.0.1:9092";
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAPI);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // subscribe to a consumer topic
        consumer.subscribe(Collections.singleton(topic));

        // poll for new data
        while(true) {
            // use duration in kafka 2.0
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            // once a list of records is received iterate over them
            for (ConsumerRecord<String, String> record: records) {
                logger.info("key: " + record.key() + '\n' + "value: " + record.value() + "\n");
                logger.info("partition: " + record.partition() + '\n' + "offset: " + record.offset() + "\n");
            }
        }

    }
}
