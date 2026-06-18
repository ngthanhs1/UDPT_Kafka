import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/*
 * =====================================================
 * DLQ CONSUMER (DEAD LETTER QUEUE)
 * =====================================================
 *
 * Chức năng:
 * - Lắng nghe Topic orders-dlq
 * - Nhận các message đã Retry thất bại
 * - Lưu vết và hỗ trợ kiểm tra lỗi
 *
 * Ý nghĩa:
 * Trong hệ thống thực tế không nên xóa message lỗi.
 * Các message này cần được lưu lại để:
 *  + Điều tra nguyên nhân
 *  + Khắc phục thủ công
 *  + Phân tích dữ liệu lỗi
 *
 * CÂU HỎI GIẢNG VIÊN:
 *
 * Tại sao cần DLQ?
 *
 * Trả lời:
 * DLQ giúp tránh mất dữ liệu.
 * Những message không xử lý được sẽ được
 * lưu riêng để quản trị viên kiểm tra sau.
 *
 * =====================================================
 */

public class DlqConsumer {

    public static void main(String[] args) {

        Properties props =
                new Properties();

        props.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092"
        );

        props.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                "dlq-group"
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
                        "orders-dlq"
                )
        );

        System.out.println(
                "\n========================================" +
                "\n KHỞI ĐỘNG DEAD LETTER QUEUE" +
                "\n Topic đang theo dõi : orders-dlq" +
                "\n Chức năng           : Lưu message lỗi" +
                "\n========================================\n"
        );

        EventLogger.log(
                "DLQ CONSUMER STARTED"
        );

        while(true){

            ConsumerRecords<String,String>
                    records =
                    consumer.poll(
                            Duration.ofMillis(100)
                    );

            for(
                    ConsumerRecord<String,String>
                            record
                    : records
            ){

                String value =
                        record.value();

                System.out.println(
                        "\n****************************************" +
                        "\n DEAD LETTER QUEUE" +
                        "\n----------------------------------------" +
                        "\n Message lỗi      : " + value +
                        "\n Trạng thái       : Không thể xử lý" +
                        "\n Nguyên nhân      : Vượt quá số lần Retry" +
                        "\n Hành động        : Lưu để kiểm tra thủ công" +
                        "\n****************************************"
                );

                EventLogger.log(
                        "DLQ: " + value
                );
            }
        }
    }
}