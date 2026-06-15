import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;

public class OrderConsumer {
    public static void main(String[] args) {
        String name = args[0];

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "order-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", "localhost:9092");
        producerProps.put("key.serializer", StringSerializer.class.getName());
        producerProps.put("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        consumer.subscribe(
                Collections.singletonList("orders"),
                new ConsumerMonitor()
        );

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                String value = record.value();

                try {
                    if (value.equals("Order-5") || value.equals("Order-10") || value.equals("Order-15")) {
                        throw new RuntimeException("Simulated processing error");
                    }

                    System.out.println("[" + name + "] SUCCESS: " + value);
                    EventLogger.log("SUCCESS: " + value);

                } catch (Exception e) {
                    System.out.println("[" + name + "] FAILED: " + value + " -> send to RETRY");
                    EventLogger.log("FAILED: " + value);
                    producer.send(new ProducerRecord<>("orders-retry", value));
                }
            }
        }
    }
}
