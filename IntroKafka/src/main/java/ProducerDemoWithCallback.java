import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Properties;

public class ProducerDemoWithCallback {

    public static void main(String[] args){

        // logger properties; create a logger for my class
        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        // create producer properties
        Properties properties = new Properties();
        String kafkaIP = "127.0.0.1:9092";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaIP);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // create a producer record
        final ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello world 11");
        ProducerRecord<String, String> record2 = new ProducerRecord<String, String>("first_topic", "hello world 12");
        ProducerRecord<String, String> record3 = new ProducerRecord<String, String>("first_topic", "hello world 13");


        ArrayList<ProducerRecord> valuesToSend = new ArrayList<ProducerRecord>();
        valuesToSend.add(record);
        valuesToSend.add(record2);
        valuesToSend.add(record3);

        // send data
        for (int i = 0; i < valuesToSend.size(); i++) {
//            final ProducerRecord currentRecord = valuesToSend.get(i);
            producer.send(valuesToSend.get(i), new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
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
            });
        }
        // producer.send(record);

        // flush data
        producer.flush();

        // flush and close producer
        producer.close();
    }
}
