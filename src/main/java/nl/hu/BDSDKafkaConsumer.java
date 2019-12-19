package nl.hu;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Hello world!
 *
 */
public class KafkaConsumer extends Thread {
        private final String topic = "";
        private final Boolean isAsync = false;
        private static final String KAFKA_SERVER_URL = "localhost";
        private static final int KAFKA_SERVER_PORT = 9092;
        private static final String CLIENT_ID = "BDSDKafkaProducer";
        private static final Set<String> filenames = new HashSet<>();

    public KafkaConsumer(String topic, boolean isAsync) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", KAFKA_SERVER_URL + ":" + KAFKA_SERVER_PORT);
        properties.put("client.id", CLIENT_ID);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        private final KafkaConsumer<Integer, String> consumer = KafkaConsumer<Integer, String>();
        this.topic = topic;
        this.isAsync = isAsync;
        this.filenames.addAll(filenames);

    }


}
