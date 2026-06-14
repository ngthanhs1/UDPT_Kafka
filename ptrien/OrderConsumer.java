package ptrien;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OrderConsumer {

    public static void main(String[] args)
            throws Exception {

        String name = args[0];

        Properties props =
                new Properties();

        props.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092"
        );

        props.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                "order-group"
        );

        props.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName()
        );

        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName()
        );

        props.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest"
        );

        KafkaConsumer<String,String> consumer =
                new KafkaConsumer<>(props);

        consumer.subscribe(
                Collections.singletonList(
                        "orders"
                ),
                new ConsumerMonitor()
        );

        System.out.println(
                "[" + name + "] Started..."
        );

        while(true){

            ConsumerRecords<String,String>
                    records =
                    consumer.poll(
                            Duration.ofMillis(100)
                    );

            for(
                    ConsumerRecord<String,String>
                            record :
                    records
            ){

                System.out.println(
                        "\n[" + name + "] Dang xu ly:"
                );

                System.out.println(
                        record.value()
                );

                Thread.sleep(2000);

                System.out.println(
                        "[" + name + "] Hoan thanh!"
                );
            }
        }
    }
}