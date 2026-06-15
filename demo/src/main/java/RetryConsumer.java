import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class RetryConsumer {
    private static final int MAX_RETRY = 3;

    public static void main(String[] args) {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "retry-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList("orders-retry"));

        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", "localhost:9092");
        producerProps.put("key.serializer", StringSerializer.class.getName());
        producerProps.put("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        System.out.println("[RETRY] Listening to orders-retry...");

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                String value = record.value();
                int retryCount = getRetryCount(value);
                String order = getOrderValue(value);

                if (retryCount < MAX_RETRY) {
                    int nextRetry = retryCount + 1;
                    String retryMessage = order + "|retry=" + nextRetry;

                    System.out.println("[RETRY] " + order + " attempt=" + nextRetry + " -> FAILED, retry again");
                    EventLogger.log("RETRY: " + order + " attempt=" + nextRetry);
                    producer.send(new ProducerRecord<>("orders-retry", retryMessage));
                } else {
                    String dlqMessage = order + "|reason=max retry exceeded|retry=" + retryCount;

                    System.out.println("[RETRY] " + order + " exceeded max retry -> send to DLQ");
                    EventLogger.log("DLQ: " + order + " exceeded max retry");
                    producer.send(new ProducerRecord<>("orders-dlq", dlqMessage));
                }
            }
        }
    }

    private static int getRetryCount(String value) {
        if (!value.contains("|retry=")) {
            return 0;
        }

        String[] parts = value.split("\\|retry=");
        return Integer.parseInt(parts[1]);
    }

    private static String getOrderValue(String value) {
        if (!value.contains("|retry=")) {
            return value;
        }

        return value.split("\\|retry=")[0];
    }
}
