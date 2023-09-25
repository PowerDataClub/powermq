package top.powerdata.powermq.client;


import io.openmessaging.api.Consumer;
import io.openmessaging.api.MessagingAccessPoint;
import io.openmessaging.api.Producer;
import io.openmessaging.api.PullConsumer;
import io.openmessaging.api.batch.BatchConsumer;
import io.openmessaging.api.order.OrderConsumer;
import io.openmessaging.api.order.OrderProducer;
import io.openmessaging.api.transaction.LocalTransactionChecker;
import io.openmessaging.api.transaction.TransactionProducer;

import java.util.Properties;

public class MessagingAccessPointImpl implements MessagingAccessPoint {


    @Override
    public String version() {
        return null;
    }

    @Override
    public Properties attributes() {
        return null;
    }

    @Override
    public Producer createProducer(Properties properties) {
        return null;
    }

    @Override
    public OrderProducer createOrderProducer(Properties properties) {
        return null;
    }

    @Override
    public TransactionProducer createTransactionProducer(Properties properties, LocalTransactionChecker localTransactionChecker) {
        return null;
    }

    @Override
    public TransactionProducer createTransactionProducer(Properties properties) {
        return null;
    }

    @Override
    public Consumer createConsumer(Properties properties) {
        return null;
    }

    @Override
    public PullConsumer createPullConsumer(Properties properties) {
        return null;
    }

    @Override
    public BatchConsumer createBatchConsumer(Properties properties) {
        return null;
    }

    @Override
    public OrderConsumer createOrderedConsumer(Properties properties) {
        return null;
    }
}
