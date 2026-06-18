import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/*
 * =====================================================
 * ORDER CONSUMER
 * =====================================================
 *
 * Chức năng:
 * - Nhận đơn hàng từ Topic orders
 * - Thực hiện xử lý đơn hàng
 * - Nếu lỗi => chuyển sang Topic orders-retry
 *
 * Tính năng liên quan:
 * ✓ Consumer Group
 * ✓ Load Balancing
 * ✓ Retry
 * ✓ Fault Tolerance
 *
 * CÂU HỎI GIẢNG VIÊN:
 *
 * 1. Tại sao cần Consumer Group?
 * -> Để nhiều Consumer cùng xử lý dữ liệu.
 *
 * 2. Tại sao cần Retry?
 * -> Tránh mất dữ liệu khi lỗi tạm thời.
 *
 * 3. Tại sao Order-5,10,15 luôn lỗi?
 * -> Dữ liệu mô phỏng để kiểm thử Retry và DLQ.
 *
 * =====================================================
 */

public class OrderConsumer {

    public static void main(String[] args) {

        String name = args[0];

        Properties props = new Properties();

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

        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(props);

        /*
         * Producer được sử dụng để
         * gửi message lỗi sang Retry Topic
         */
        Properties producerProps =
                new Properties();

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

        /*
         * ConsumerMonitor theo dõi Rebalance
         * để minh họa Load Balancing
         * và Fault Tolerance
         */
        consumer.subscribe(
                Collections.singletonList("orders"),
                new ConsumerMonitor(name)
        );

        System.out.println(
                "\n========================================" +
                "\n KHỞI ĐỘNG ORDER CONSUMER : " + name +
                "\n Consumer Group           : order-group" +
                "\n Topic                    : orders" +
                "\n========================================\n"
        );

        while (true) {

            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {

                String value = record.value();

                try {

                    /*
                     * Mô phỏng lỗi để test Retry và DLQ
                     */
                    if (
                            value.equals("Order-5")
                                    || value.equals("Order-10")
                                    || value.equals("Order-15")
                                    || value.contains("fail-transient")
                                    || value.contains("fail-persistent")
                    ) {

                        throw new RuntimeException(
                                "Lỗi mô phỏng"
                        );
                    }

                    System.out.println(
                            "\n========================================" +
                            "\n [CONSUMER " + name + "]" +
                            "\n Đơn hàng       : " + value +
                            "\n Trạng thái     : THÀNH CÔNG" +
                            "\n Hành động      : Hoàn tất xử lý" +
                            "\n========================================"
                    );

                    EventLogger.log(
                            "SUCCESS: " + value
                    );

                }
                catch (Exception e) {

                    System.out.println(
                            "\n****************************************" +
                            "\n [CONSUMER " + name + "]" +
                            "\n Đơn hàng       : " + value +
                            "\n Trạng thái     : THẤT BẠI" +
                            "\n Nguyên nhân    : Lỗi xử lý dữ liệu" +
                            "\n Hành động      : Chuyển sang orders-retry" +
                            "\n****************************************"
                    );

                    EventLogger.log(
                            "FAILED: " + value
                    );

                    producer.send(
                            new ProducerRecord<>(
                                    "orders-retry",
                                    value
                            )
                    );
                }
            }
        }
    }
}