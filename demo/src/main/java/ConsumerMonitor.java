import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

/*
 * =====================================================
 * CONSUMER MONITOR
 * =====================================================
 *
 * Chức năng:
 * Theo dõi sự kiện Rebalance trong Consumer Group.
 *
 * Rebalance xảy ra khi:
 *
 * 1. Consumer mới tham gia nhóm
 * 2. Consumer bị tắt hoặc lỗi
 * 3. Partition thay đổi
 *
 * Mục đích:
 * Chứng minh Load Balancing và Fault Tolerance.
 *
 * CÂU HỎI GIẢNG VIÊN:
 *
 * Rebalance là gì?
 *
 * Trả lời:
 * Rebalance là quá trình Kafka tự động phân phối lại
 * Partition giữa các Consumer trong cùng Consumer Group.
 *
 * =====================================================
 */

public class ConsumerMonitor
        implements ConsumerRebalanceListener {

    private String consumerName = "Unknown-Consumer";

    public ConsumerMonitor() {}

    public ConsumerMonitor(String consumerName) {
        this.consumerName = consumerName;
    }

    /*
     * Partition bị thu hồi
     *
     * Thường xảy ra khi:
     * - Có Consumer mới tham gia
     * - Consumer hiện tại mất quyền xử lý
     */
    @Override
    public void onPartitionsRevoked(
            Collection<TopicPartition> partitions
    ) {

        String msg =
                "\n========================================" +
                "\n REBALANCE - THU HỒI PARTITION (" + consumerName + ")" +
                "\n Partition bị thu hồi: " +
                partitions +
                "\n Kafka đang phân phối lại tải..." +
                "\n========================================";

        System.out.println(msg);

        EventLogger.log(
                "REBALANCE REMOVE: " + consumerName + " -> " + partitions
        );
    }

    /*
     * Partition được cấp phát
     *
     * Đây là bằng chứng của:
     * - Load Balancing
     * - Fault Tolerance
     */
    @Override
    public void onPartitionsAssigned(
            Collection<TopicPartition> partitions
    ) {

        String msg =
                "\n****************************************" +
                "\n REBALANCE - PHÂN CÔNG PARTITION (" + consumerName + ")" +
                "\n Partition được giao: " +
                partitions +
                "\n Consumer bắt đầu xử lý dữ liệu" +
                "\n****************************************";

        System.out.println(msg);

        EventLogger.log(
                "REBALANCE ASSIGN: " + consumerName + " -> " + partitions
        );
    }
}