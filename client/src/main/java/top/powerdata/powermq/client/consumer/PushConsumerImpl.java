package top.powerdata.powermq.client.consumer;


import io.openmessaging.api.Message;
import io.openmessaging.api.MessageSelector;
import io.openmessaging.api.PullConsumer;
import io.openmessaging.api.TopicPartition;

import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class PushConsumerImpl implements PullConsumer {

    @Override
    public Set<TopicPartition> topicPartitions(String topic) {
        return null;
    }

    @Override
    public void subscribe(String topic, String subExpression) {

    }

    @Override
    public void subscribe(String topic, MessageSelector selector) {

    }

    @Override
    public void assign(Collection<TopicPartition> topicPartitions) {

    }

    @Override
    public void registerTopicPartitionChangedListener(String topic, TopicPartitionChangeListener callback) {

    }

    @Override
    public void registerAssignedTopicPartitionChangedListener(String topic, AssignedTopicPartitionChangeListener callback) {

    }

    @Override
    public List<Message> poll(long timeout) {
        return null;
    }

    @Override
    public void seek(TopicPartition topicPartition, long offset) {

    }

    @Override
    public void seekToBeginning(TopicPartition topicPartition) {

    }

    @Override
    public void seekToEnd(TopicPartition topicPartition) {

    }

    @Override
    public void pause(Collection<TopicPartition> topicPartitions) {

    }

    @Override
    public void resume(Collection<TopicPartition> topicPartitions) {

    }

    @Override
    public Long offsetForTimestamp(TopicPartition topicPartition, Long timestamp) {
        return null;
    }

    @Override
    public Long committed(TopicPartition topicPartition) {
        return null;
    }

    @Override
    public void commitSync() {

    }

    @Override
    public void updateCredential(Properties credentialProperties) {

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
}
