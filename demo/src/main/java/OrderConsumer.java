import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

public class OrderConsumer {

    public static void main(String[] args) {

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

        KafkaConsumer<String,String> consumer =
                new KafkaConsumer<>(props);

        consumer.subscribe(
            Collections.singletonList(
                "orders"
        ),
        new ConsumerMonitor()
        );

        while(true){

            ConsumerRecords<String,String>
                    records =
                    consumer.poll(
                            Duration.ofMillis(
                                    100
                            )
                    );

            for(
                    ConsumerRecord<String,String>
                            record :
                    records
            ){

                System.out.println(
                        "[" + name + "] "
                                + record.value()
                );
            }
        }
    }
}