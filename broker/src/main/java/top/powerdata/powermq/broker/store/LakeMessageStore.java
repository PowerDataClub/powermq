package top.powerdata.powermq.broker.store;

import top.powerdata.powermq.common.schema.MessageSchema;
import top.powerdata.powermq.common.server.data.PartitionData;
import top.powerdata.powermq.common.server.data.TopicData;

public abstract class LakeMessageStore extends AbstractMessageStore {
    public LakeMessageStore(TopicData topicData, PartitionData partition, MessageSchema schema) {
        super(topicData, partition, schema);
    }

    @Override
    public void start() throws Exception {
        super.start();
        buildLakeSchema(getMessageSchema());
        createDataLakeTable();
    }

    public abstract void buildLakeSchema(MessageSchema schema);

    public abstract void createDataLakeTable();
}
