package top.powerdata.powermq.broker.store;

import org.apache.iceberg.data.Record;
import top.powerdata.powermq.common.PowerMqService;
import top.powerdata.powermq.common.SchemaMessage;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface MessageStore extends PowerMqService {

    void asyncAddMessage(SchemaMessage message) throws Exception;
    void asyncAddMessage(Map<String, Object> message) throws Exception;

    Map<String, Object> getMessage(final int partitionId, final long offset) throws IOException;
    long getMinOffset(int partitionId);
    long getMaxOffset(int partitionId);
    Map<Integer, Long> getMinOffsets();
    Map<Integer, Long> getMaxOffsets();
}
