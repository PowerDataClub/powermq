package top.powerdata.powermq.broker.store;

import lombok.Getter;
import org.apache.rocketmq.common.message.Message;
import top.powerdata.powermq.common.schema.MessageSchema;
import top.powerdata.powermq.common.server.data.PartitionData;
import top.powerdata.powermq.common.server.data.TopicData;

import java.util.concurrent.CompletableFuture;

public abstract class AbstractMessageStore implements MessageStore {

    @Getter
    private MessageSchema messageSchema;
    @Getter
    private TopicData topicData;
    private PartitionData partition;
    public AbstractMessageStore(TopicData topicData, PartitionData partition, MessageSchema messageSchema) {
        this.topicData = topicData;
        this.partition = partition;
        this.messageSchema = messageSchema;
    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public CompletableFuture<AddMessageResult> addMessage(Message message) {
        return null;
    }

    @Override
    public CompletableFuture<GetMessageResult> getMessage(long offset) {
        return null;
    }

    @Override
    public long getMinOffset() {
        return 0;
    }

    @Override
    public long getMaxOffset() {
        return 0;
    }
}
