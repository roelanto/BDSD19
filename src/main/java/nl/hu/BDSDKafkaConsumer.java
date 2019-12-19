package nl.hu;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Hello world!
 *
 */
public class BDSDKafkaConsumer extends Thread {
    private static Logger log = LoggerFactory.getLogger("BDSDKafkaConsumer");
    private final String topic;
    private final Boolean isAsync;
    private final KafkaConsumer<String, String> kafkaConsumer;
    private static final String KAFKA_SERVER_URL = "localhost";
    private static final int KAFKA_SERVER_PORT = 9092;
    private static final String CLIENT_ID = "BDSDKafkaProducer";
    private static final Transactions transactions = new Transactions();

    public BDSDKafkaConsumer(String topic, boolean isAsync) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", KAFKA_SERVER_URL + ":" + KAFKA_SERVER_PORT);
        properties.put("client.id", CLIENT_ID);
        properties.put("group.id", "mygroup");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaConsumer = new KafkaConsumer <String, String>(properties);
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


    private int resolve_query_1(Transaction transaction, int threshold) {
        Set<Integer> allProductsForCustomer = transactions.productsByCustomer(transaction.getCustomerId());
        Map<Integer, Integer> customers = transactions.customersWithProduct(allProductsForCustomer);
        Iterator iter = customers.entrySet().iterator();
        int resultval = 0;
        while (iter.hasNext()) {
            Map.Entry<Integer, Integer> customerCountPair = (Map.Entry<Integer, Integer>) iter.next();
            if (customerCountPair.getKey() != transaction.getCustomerId()) {
                if (customerCountPair.getValue() > threshold) {
                    log.info("Customer " + customerCountPair.getKey() + "  shares " + customerCountPair.getValue() + " identical products with customer " +
                            transaction.getCustomerId());
                    resultval ++;
                }
            }
        }
        return resultval;
    }

    /**
     * This method runs a poll job. It continuously asks for new data from Kafka.
     * @param consumer
     */
    private void pollForNewRecords(KafkaConsumer consumer) {
        int threshold = 4;
        try {
            while (true) {
                // for Java versions 1.8 and higher, use Duration.
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records)
                {
                    log.info("topic = "+record.topic() + " partition = "+record.partition()+", offset = %d, customer = "+record.key()+", productid = "+record.value()+"\n");
                    Transaction t = new Transaction(Integer.parseInt(record.key()), Integer.parseInt(record.value()));
                    // add transaction to the list of known transactions
                    transactions.add(t);
                    // run query 1
                    log.info("# of customers with threshold > " + threshold + ": " + resolve_query_1(t, threshold));

                    int updatedCount = 1;
                }
            }
        } finally {
            consumer.close();
        }
    }


}
