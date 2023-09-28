package top.powerdata.powermq.broker.store;

import org.apache.rocketmq.common.message.Message;
import top.powerdata.powermq.common.PowerMqService;

import java.util.concurrent.CompletableFuture;

public interface MessageStore extends PowerMqService {

    CompletableFuture<AddMessageResult> addMessage(Message message);

    CompletableFuture<GetMessageResult> getMessage(final long offset);
    long getMinOffset();
    long getMaxOffset();
}
