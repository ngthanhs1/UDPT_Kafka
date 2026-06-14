package ptrien;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

public class ConsumerMonitor
        implements ConsumerRebalanceListener {

    @Override
    public void onPartitionsRevoked(
            Collection<TopicPartition> partitions
    ) {

        System.out.println(
                "\n===== REBALANCE ====="
        );

        System.out.println(
                "[INFO] Partition removed:"
        );

        for (TopicPartition p : partitions) {

            System.out.println(
                    p.topic()
                            + "-"
                            + p.partition()
            );
        }
    }

    @Override
    public void onPartitionsAssigned(
            Collection<TopicPartition> partitions
    ) {

        System.out.println(
                "\n===== REBALANCE ====="
        );

        System.out.println(
                "[INFO] Partition assigned:"
        );

        for (TopicPartition p : partitions) {

            System.out.println(
                    p.topic()
                            + "-"
                            + p.partition()
            );
        }
    }
}