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
 * - Gửi dữ liệu vào Topic orders
 *
 * Vai trò:
 * Producer là điểm bắt đầu của hệ thống.
 *
 * Luồng:
 *
 * OrderProducer
 *      ↓
 * orders
 *      ↓
 * OrderConsumer
 *      ↓
 * RetryConsumer
 *      ↓
 * DLQ Consumer
 *
 * =====================================================
 *
 * CÂU HỎI GIẢNG VIÊN:
 *
 * Producer là gì?
 *
 * Trả lời:
 * Producer là thành phần tạo và gửi dữ liệu
 * vào Kafka Topic.
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

        KafkaProducer<String,String> producer =
                new KafkaProducer<>(props);

        System.out.println(
                "\n========================================" +
                "\n KHỞI ĐỘNG ORDER PRODUCER" +
                "\n Topic đích : orders" +
                "\n Số đơn hàng : 100" +
                "\n========================================\n"
        );

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
                    "\n----------------------------------------" +
                    "\n ĐANG GỬI ĐƠN HÀNG" +
                    "\n Mã đơn hàng : " + order +
                    "\n Topic       : orders" +
                    "\n Trạng thái  : ĐÃ GỬI" +
                    "\n----------------------------------------"
            );

            EventLogger.log(
                    "PRODUCER SENT: " + order
            );

            Thread.sleep(200);
        }

        System.out.println(
                "\n========================================" +
                "\n HOÀN THÀNH GỬI DỮ LIỆU" +
                "\n Tổng số đơn hàng : 100" +
                "\n========================================"
        );

        producer.close();
    }
}