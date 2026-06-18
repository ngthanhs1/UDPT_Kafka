import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/*
 * RetryConsumer
 *
 * Chức năng:
 * - Lắng nghe Topic orders-retry.
 * - Thực hiện xử lý lại (Retry) các message bị lỗi.
 * - Retry tối đa 3 lần.
 * - Nếu vẫn lỗi sẽ chuyển sang Topic orders-dlq.
 *
 * Ý nghĩa thực tế:
 * Trong hệ thống thực tế, lỗi có thể chỉ là tạm thời:
 * + Mất kết nối mạng
 * + Database quá tải
 * + Dịch vụ bên ngoài phản hồi chậm
 *
 * Vì vậy không nên loại bỏ message ngay lập tức.
 *
 * CÂU HỎI GIẢNG VIÊN:
 * Tại sao phải có Retry?
 *
 * TRẢ LỜI:
 * Retry giúp giảm nguy cơ mất dữ liệu khi lỗi chỉ mang
 * tính tạm thời. Hệ thống có cơ hội xử lý lại trước khi
 * quyết định đưa message vào DLQ.
 */

public class RetryConsumer {

    // Số lần Retry tối đa
    private static final int MAX_RETRY = 3;

    public static void main(String[] args) {

        /*
         * Cấu hình Consumer
         */
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

        /*
         * Cấu hình Producer
         *
         * Producer này được sử dụng để:
         * - Gửi lại message vào orders-retry
         * - Hoặc gửi sang orders-dlq
         */
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
                "\n Topic đang theo dõi: orders-retry" +
                "\n Retry tối đa      : " + MAX_RETRY +
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
                 * Chưa vượt quá số lần Retry
                 */
                if (retryCount < MAX_RETRY) {

                    int nextRetry = retryCount + 1;

                    String retryMessage =
                            order + "|retry=" + nextRetry;

                    System.out.println(
                            "\n----------------------------------------" +
                            "\n [RETRY CONSUMER]" +
                            "\n Đơn hàng      : " + order +
                            "\n Lần thử lại   : " +
                            nextRetry + "/" + MAX_RETRY +
                            "\n Kết quả       : THẤT BẠI" +
                            "\n Hành động     : Tiếp tục Retry" +
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

                    /*
                     * Đã vượt quá số lần Retry
                     * => chuyển sang DLQ
                     */

                    String dlqMessage =
                            order +
                            "|reason=max retry exceeded|retry=" +
                            retryCount;

                    System.out.println(
                            "\n****************************************" +
                            "\n [RETRY THẤT BẠI HOÀN TOÀN]" +
                            "\n Đơn hàng      : " + order +
                            "\n Số lần Retry  : " + retryCount +
                            "\n Kết quả       : VƯỢT QUÁ GIỚI HẠN" +
                            "\n Hành động     : Chuyển sang DLQ" +
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

    /*
     * Lấy số lần Retry từ message
     *
     * Ví dụ:
     * Order-5|retry=2
     *
     * => trả về 2
     */
    private static int getRetryCount(String value) {

        if (!value.contains("|retry=")) {
            return 0;
        }

        String[] parts =
                value.split("\\|retry=");

        return Integer.parseInt(parts[1]);
    }

    /*
     * Lấy mã đơn hàng
     *
     * Ví dụ:
     * Order-5|retry=2
     *
     * => Order-5
     */
    private static String getOrderValue(String value) {

        if (!value.contains("|retry=")) {
            return value;
        }

        return value.split("\\|retry=")[0];
    }
}