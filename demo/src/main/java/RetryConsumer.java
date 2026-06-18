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

        consumerProps.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092"
        );

        consumerProps.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                "retry-group"
        );

        consumerProps.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName()
        );

        consumerProps.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName()
        );

        consumerProps.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest"
        );

        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(consumerProps);

        consumer.subscribe(
                Collections.singletonList("orders-retry")
        );

        Properties producerProps = new Properties();

        producerProps.put(
                "bootstrap.servers",
                "localhost:9092"
        );

        producerProps.put(
                "key.serializer",
                StringSerializer.class.getName()
        );

        producerProps.put(
                "value.serializer",
                StringSerializer.class.getName()
        );

        KafkaProducer<String, String> producer =
                new KafkaProducer<>(producerProps);

        System.out.println(
                "\n========================================" +
                "\n KHỞI ĐỘNG RETRY CONSUMER" +
                "\n Topic đang theo dõi : orders-retry" +
                "\n Retry tối đa       : " + MAX_RETRY +
                "\n========================================\n"
        );

        while (true) {

            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {

                String value = record.value();

                int retryCount =
                        getRetryCount(value);

                String order =
                        getOrderValue(value);

                /*
                 * CASE 1:
                 * Order-5 sẽ thành công sau lần Retry đầu tiên
                 */
                if (order.equals("Order-5") || order.contains("fail-transient")) {

                    if (retryCount < 1) {

                        int nextRetry = retryCount + 1;

                        System.out.println(
                                "\n----------------------------------------" +
                                "\n [RETRY CONSUMER]" +
                                "\n Đơn hàng      : " + order +
                                "\n Retry lần     : " + nextRetry +
                                "\n Kết quả       : THẤT BẠI TẠM THỜI" +
                                "\n Hành động     : Retry lại" +
                                "\n----------------------------------------"
                        );

                        EventLogger.log(
                                "RETRY: " + order
                        );

                        producer.send(
                                new ProducerRecord<>(
                                        "orders-retry",
                                        order + "|retry=" + nextRetry
                                )
                        );

                    } else {

                        System.out.println(
                                "\n========================================" +
                                "\n [RETRY THÀNH CÔNG]" +
                                "\n Đơn hàng      : " + order +
                                "\n Retry lần     : " + retryCount +
                                "\n Trạng thái    : THÀNH CÔNG" +
                                "\n========================================"
                        );

                        EventLogger.log(
                                "SUCCESS AFTER RETRY: " + order
                        );
                    }

                    continue;
                }

                /*
                 * CASE 2:
                 * Order-10 và Order-15 sẽ luôn lỗi
                 * để minh họa DLQ
                 */
                if (retryCount < MAX_RETRY) {

                    int nextRetry = retryCount + 1;

                    String retryMessage =
                            order + "|retry=" + nextRetry;

                    System.out.println(
                            "\n----------------------------------------" +
                            "\n [RETRY CONSUMER]" +
                            "\n Đơn hàng      : " + order +
                            "\n Retry lần     : " +
                            nextRetry + "/" + MAX_RETRY +
                            "\n Kết quả       : THẤT BẠI" +
                            "\n Hành động     : Retry tiếp" +
                            "\n----------------------------------------"
                    );

                    EventLogger.log(
                            "RETRY: " +
                            order +
                            " lan=" +
                            nextRetry
                    );

                    producer.send(
                            new ProducerRecord<>(
                                    "orders-retry",
                                    retryMessage
                            )
                    );

                } else {

                    String dlqMessage =
                            order +
                            "|reason=max retry exceeded|retry=" +
                            retryCount;

                    System.out.println(
                            "\n****************************************" +
                            "\n [CHUYỂN VÀO DLQ]" +
                            "\n Đơn hàng      : " + order +
                            "\n Retry tối đa  : " + retryCount +
                            "\n Trạng thái    : THẤT BẠI HOÀN TOÀN" +
                            "\n****************************************"
                    );

                    EventLogger.log(
                            "DLQ: " +
                            order +
                            " vuot qua gioi han retry"
                    );

                    producer.send(
                            new ProducerRecord<>(
                                    "orders-dlq",
                                    dlqMessage
                            )
                    );
                }
            }
        }
    }

    private static int getRetryCount(String value) {

        if (!value.contains("|retry=")) {
            return 0;
        }

        String[] parts =
                value.split("\\|retry=");

        return Integer.parseInt(parts[1]);
    }

    private static String getOrderValue(String value) {

        if (!value.contains("|retry=")) {
            return value;
        }

        return value.split("\\|retry=")[0];
    }
}
