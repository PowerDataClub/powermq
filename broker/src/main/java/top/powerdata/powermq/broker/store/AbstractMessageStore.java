package top.powerdata.powermq.broker.store;

import lombok.Getter;
import top.powerdata.powermq.common.schema.MessageSchema;
import top.powerdata.powermq.common.server.data.PartitionData;

import java.util.concurrent.CompletableFuture;

public abstract class AbstractMessageStore implements MessageStore {

    @Getter
    private MessageSchema messageSchema;
    @Getter
    private PartitionData partition;
    public AbstractMessageStore(PartitionData partition, MessageSchema messageSchema) {
        this.partition = partition;
        this.messageSchema = messageSchema;
    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void close() throws Exception {

    }
}
