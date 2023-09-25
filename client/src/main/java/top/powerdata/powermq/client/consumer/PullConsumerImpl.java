package top.powerdata.powermq.client.consumer;

import io.openmessaging.api.Message;
import io.openmessaging.api.MessageSelector;
import io.openmessaging.api.PullConsumer;
import io.openmessaging.api.TopicPartition;

import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class PullConsumerImpl implements PullConsumer {

    @Override
    public Set<TopicPartition> topicPartitions(String s) {
        return null;
    }

    @Override
    public void subscribe(String s, String s1) {

    }

    @Override
    public void subscribe(String s, MessageSelector messageSelector) {

    }

    @Override
    public void assign(Collection<TopicPartition> collection) {

    }

    @Override
    public void registerTopicPartitionChangedListener(String s, TopicPartitionChangeListener topicPartitionChangeListener) {

    }

    @Override
    public void registerAssignedTopicPartitionChangedListener(String s, AssignedTopicPartitionChangeListener assignedTopicPartitionChangeListener) {

    }

    @Override
    public List<Message> poll(long l) {
        return null;
    }

    @Override
    public void seek(TopicPartition topicPartition, long l) {

    }

    @Override
    public void seekToBeginning(TopicPartition topicPartition) {

    }

    @Override
    public void seekToEnd(TopicPartition topicPartition) {

    }

    @Override
    public void pause(Collection<TopicPartition> collection) {

    }

    @Override
    public void resume(Collection<TopicPartition> collection) {

    }

    @Override
    public Long offsetForTimestamp(TopicPartition topicPartition, Long aLong) {
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
}
