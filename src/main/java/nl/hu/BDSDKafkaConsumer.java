package nl.hu;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Hello world!
 *
 */
public class BDSDKafkaConsumer extends Thread {
    private static Logger log = LoggerFactory.getLogger("BDSDKafkaConsumer");
    private final String topic;
    private final Boolean isAsync;
    private final KafkaConsumer<Integer, String> kafkaConsumer;
    private static final String KAFKA_SERVER_URL = "localhost";
    private static final int KAFKA_SERVER_PORT = 9092;
    private static final String CLIENT_ID = "BDSDKafkaProducer";

    public BDSDKafkaConsumer(String topic, boolean isAsync) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", KAFKA_SERVER_URL + ":" + KAFKA_SERVER_PORT);
        properties.put("client.id", CLIENT_ID);
        properties.put("group.id", "mygroup");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaConsumer = new KafkaConsumer <Integer, String>(properties);
        this.topic = topic;
        this.isAsync = isAsync;
    }

    /**
     * This method will be run, because this Java file extends the Thread class.
     */
    public void run() {
        Set topics = new HashSet<String>();
        /* first subscribe to the topic */
        topics.add(topic);
        kafkaConsumer.subscribe(topics);

        pollForNewRecords(kafkaConsumer);

    }

    private void pollForNewRecords(KafkaConsumer consumer) {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records)
                {
                    log.info("topic = %s, partition = %d, offset = %d, customer = %s, country = %s\n",
                            record.topic(), record.partition(), record.offset(),
                            record.key(), record.value());
                    System.out.println("topic = " + record.topic()  + " partition = "+record.partition() +", offset = "+ record.offset()+ ", customer = "+record.key()+", country = " + record.value()+ "\n");
                    int updatedCount = 1;
                }
            }
        } finally {
            consumer.close();
        }
    }


}
