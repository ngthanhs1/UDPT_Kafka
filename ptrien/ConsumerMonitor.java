import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

public class ConsumerMonitor
        implements ConsumerRebalanceListener {

    @Override
    public void onPartitionsRevoked(
            Collection<TopicPartition>
                    partitions
    ) {

        System.out.println(
                "Partition removed: "
                        + partitions
        );
    }

    @Override
    public void onPartitionsAssigned(
            Collection<TopicPartition>
                    partitions
    ) {

        System.out.println(
                "Partition assigned: "
                        + partitions
        );
    }
}