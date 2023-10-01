package top.powerdata.powermq.broker.store;

import top.powerdata.powermq.common.schema.MessageSchema;
import top.powerdata.powermq.common.server.data.PartitionData;
import top.powerdata.powermq.common.server.data.TopicData;

public abstract class LakeMessageStore extends AbstractMessageStore {
    public LakeMessageStore(PartitionData partition, MessageSchema schema) {
        super(partition, schema);
    }

    @Override
    public void start() throws Exception {
        super.start();
        buildLakeSchema(getMessageSchema());
        createDataLakeTable();
        tryRecover();
    }

    public abstract void tryRecover();

    public abstract void buildLakeSchema(MessageSchema schema);

    public abstract void createDataLakeTable();
}
