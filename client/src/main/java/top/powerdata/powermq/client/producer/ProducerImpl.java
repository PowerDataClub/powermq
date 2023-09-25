package top.powerdata.powermq.client.producer;

import io.openmessaging.api.Message;
import io.openmessaging.api.MessageBuilder;
import io.openmessaging.api.SendCallback;
import io.openmessaging.api.SendResult;

import java.util.Properties;
import java.util.concurrent.ExecutorService;

public class ProducerImpl extends AbstractOMSProducer {


    @Override
    public SendResult send(Message message) {
        return null;
    }

    @Override
    public void sendOneway(Message message) {

    }

    @Override
    public void sendAsync(Message message, SendCallback sendCallback) {

    }

    @Override
    public void setCallbackExecutor(ExecutorService executorService) {

    }

    @Override
    public void updateCredential(Properties properties) {

    }

    @Override
    public boolean isStarted() {
        return false;
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public void start() {

    }

    @Override
    public void shutdown() {

    }

    @Override
    public <T> MessageBuilder<T> messageBuilder() {
        return null;
    }
}
