import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class OrderProducer {

    public static void main(String[] args)
            throws Exception {

        Properties props =
                new Properties();

        props.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092"
        );

        props.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName()
        );

        props.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName()
        );

        KafkaProducer<String,String> producer =
                new KafkaProducer<>(props);

        for(int i=1;i<=100;i++){

            String order =
                    "Order-" + i;

            producer.send(
                    new ProducerRecord<>(
                            "orders",
                            order
                    )
            );

            System.out.println(
                    "Sent: " + order
            );

            Thread.sleep(200);
        }

        producer.close();
    }
}