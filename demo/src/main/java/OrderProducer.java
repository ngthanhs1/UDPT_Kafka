import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/*
 * =====================================================
 * ORDER PRODUCER
 * =====================================================
 *
 * Chức năng:
 * - Sinh dữ liệu đơn hàng giả lập
 * - Gửi dữ liệu vào Kafka Topic "orders"
 *
 * Vai trò:
 * Producer là điểm bắt đầu của toàn bộ hệ thống.
 *
 * Luồng dữ liệu:
 *
 * OrderProducer
 *      ↓
 * orders
 *      ↓
 * OrderConsumer
 *      ↓
 * RetryConsumer
 *      ↓
 * orders-dlq
 *      ↓
 * DlqConsumer
 *
 * =====================================================
 *
 * CÂU HỎI GIẢNG VIÊN THƯỜNG HỎI
 *
 * Producer là gì?
 *
 * Producer là thành phần chịu trách nhiệm
 * tạo và gửi dữ liệu vào Kafka Topic.
 *
 * Tại sao dùng KafkaProducer?
 *
 * Vì KafkaProducer hỗ trợ gửi dữ liệu
 * theo thời gian thực với hiệu năng cao.
 *
 * =====================================================
 */

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

        KafkaProducer<String, String> producer =
                new KafkaProducer<>(props);

        System.out.println(
                "\n==================================================" +
                "\n KHỞI ĐỘNG ORDER PRODUCER" +
                "\n--------------------------------------------------" +
                "\n Topic đích      : orders" +
                "\n Tổng đơn hàng   : 100" +
                "\n Kafka Server    : localhost:9092" +
                "\n==================================================\n"
        );

        int totalSent = 0;

        for (int i = 1; i <= 100; i++) {

            String order =
                    "Order-" + i;

            ProducerRecord<String, String> record =
                    new ProducerRecord<>(
                            "orders",
                            String.valueOf(i), // key
                            order
                    );

            producer.send(
                    record,
                    (metadata, exception) -> {

                        if (exception == null) {

                            System.out.println(
                                    "\n----------------------------------------" +
                                    "\n ĐANG GỬI ĐƠN HÀNG" +
                                    "\n Mã đơn hàng : " + order +
                                    "\n Topic       : " + metadata.topic() +
                                    "\n Partition   : " + metadata.partition() +
                                    "\n Offset      : " + metadata.offset() +
                                    "\n Trạng thái  : GỬI THÀNH CÔNG" +
                                    "\n----------------------------------------"
                            );

                        } else {

                            System.out.println(
                                    "\n[ERROR] Không gửi được: "
                                            + order
                            );

                            exception.printStackTrace();
                        }
                    }
            );

            EventLogger.log(
                    "PRODUCER SENT: " + order
            );

            totalSent++;

            Thread.sleep(200);
        }

        System.out.println(
                "\n==================================================" +
                "\n TỔNG KẾT PRODUCER" +
                "\n--------------------------------------------------" +
                "\n Đã gửi thành công : " + totalSent +
                "\n Topic             : orders" +
                "\n Trạng thái        : HOÀN TẤT" +
                "\n=================================================="
        );

        producer.flush();

        producer.close();
    }
}
