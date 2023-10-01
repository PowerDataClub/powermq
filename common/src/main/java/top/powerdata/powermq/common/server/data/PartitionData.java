package top.powerdata.powermq.common.server.data;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@EqualsAndHashCode(callSuper = true)
@Data
@NoArgsConstructor
public class PartitionData extends TopicData{
    private int partitionId;

    public PartitionData(String tenant, String namespace, String topicName, int partitionId) {
        super(tenant, namespace, topicName);
        this.partitionId = partitionId;
    }
}
