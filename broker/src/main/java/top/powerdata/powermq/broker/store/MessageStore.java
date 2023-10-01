package top.powerdata.powermq.broker.store;

import io.openmessaging.api.Message;
import top.powerdata.powermq.common.PowerMqService;
import top.powerdata.powermq.common.SchemaMessage;

import java.util.concurrent.CompletableFuture;

public interface MessageStore extends PowerMqService {

    void asyncAddMessage(SchemaMessage message) throws Exception;

    CompletableFuture<GetMessageResult> getMessage(final long offset);
    long getMinOffset();
    long getMaxOffset();
}
