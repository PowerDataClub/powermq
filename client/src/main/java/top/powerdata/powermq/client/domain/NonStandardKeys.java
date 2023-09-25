
package top.powerdata.powermq.client.domain;

public interface NonStandardKeys {
    String CONSUMER_GROUP = "rmq.consumer.group";
    String PRODUCER_GROUP = "rmq.producer.group";
    String MAX_REDELIVERY_TIMES = "rmq.max.redelivery.times";
    String MESSAGE_CONSUME_TIMEOUT = "rmq.message.consume.timeout";
    String MAX_CONSUME_THREAD_NUMS = "rmq.max.consume.thread.nums";
    String MIN_CONSUME_THREAD_NUMS = "rmq.min.consume.thread.nums";
    String MESSAGE_CONSUME_STATUS = "rmq.message.consume.status";
    String MESSAGE_DESTINATION = "rmq.message.destination";
    String PULL_MESSAGE_BATCH_NUMS = "rmq.pull.message.batch.nums";
    String PULL_MESSAGE_CACHE_CAPACITY = "rmq.pull.message.cache.capacity";
}
