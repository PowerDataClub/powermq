package top.powerdata.powermq.common;

import io.openmessaging.api.Message;
import lombok.SneakyThrows;
import top.powerdata.powermq.common.schema.MessageSchema;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import static top.powerdata.powermq.common.Consts.DEFAULT_FIELD_EVENT_TIME;
import static top.powerdata.powermq.common.Consts.DEFAULT_FIELD_PARTITION_ID;


public class SchemaMessage<T> extends Message {
    private MessageSchema<T> messageSchema;
    private T msgWithSchema;

    public SchemaMessage(String topic, String tag, String key, byte[] body, MessageSchema<T> messageSchema) {
        super(topic, tag, key, body);
        this.messageSchema = messageSchema;
    }

    public SchemaMessage(String topic, Map<String, String> properties, T instance, MessageSchema<T> messageSchema) {
        super(topic, null, messageSchema.encode(instance));
        this.messageSchema = messageSchema;
        this.msgWithSchema = instance;
        setTag(SchemaMessage.getPropertiesTag(properties));
        setKey(SchemaMessage.getPropertiesKey(properties));
    }

    public SchemaMessage(String topic, String tag, String key, T instance, MessageSchema<T> messageSchema) {
        super(topic, tag, key, messageSchema.encode(instance));
        this.messageSchema = messageSchema;
        this.msgWithSchema = instance;
    }

    public SchemaMessage(String topic, String tags, byte[] body, MessageSchema<T> messageSchema) {
        super(topic, tags, body);
        this.messageSchema = messageSchema;
    }

    public SchemaMessage(String topic, String tags, T instance, MessageSchema<T> messageSchema) {
        super(topic, tags, messageSchema.encode(instance));
        this.messageSchema = messageSchema;
        this.msgWithSchema = instance;
    }

    @SneakyThrows
    public Object getField(String fieldName) {
        Field field = getMsgWithSchema().getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(msgWithSchema);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SchemaMessage<?> that = (SchemaMessage<?>) o;
        return Objects.equals(messageSchema, that.messageSchema) && Objects.equals(msgWithSchema, that.msgWithSchema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(messageSchema, msgWithSchema);
    }

    public MessageSchema<T> getMessageSchema() {
        return messageSchema;
    }

    public void setMessageSchema(MessageSchema<T> messageSchema) {
        this.messageSchema = messageSchema;
    }

    public T getMsgWithSchema() {
        if (msgWithSchema == null) {
            msgWithSchema = messageSchema.decode(getBody());
        }
        return msgWithSchema;
    }

    public void setMsgWithSchema(T msgWithSchema) {
        this.msgWithSchema = msgWithSchema;
    }

    public long getEventTimestamp() {
        String pro = this.getSystemProperties(DEFAULT_FIELD_EVENT_TIME);
        if (pro != null) {
            return Long.parseLong(pro);
        }

        return getBornTimestamp();
    }

    public void setEventTimestamp(final long value) {
        putSystemProperties(DEFAULT_FIELD_EVENT_TIME, String.valueOf(value));
    }

    public int getPartitionId() {
        String pro = this.getSystemProperties(DEFAULT_FIELD_PARTITION_ID);
        if (pro != null) {
            return Integer.parseInt(pro);
        }

        return 0;
    }

    public void setPartitionId(final int value) {
        putSystemProperties(DEFAULT_FIELD_PARTITION_ID, String.valueOf(value));
    }

    public static long getEventTimestamp(Map<String, Object> message) {
        Map<String, String> systemProperties = (Map) message.getOrDefault("systemProperties", Collections.emptyMap());
        String pro = systemProperties.get(DEFAULT_FIELD_EVENT_TIME);
        if (pro != null) {
            return Long.parseLong(pro);
        }

        return getBornTimestamp(message);
    }

    public static long getBornTimestamp(Map<String, Object> message) {
        Map<String, String> systemProperties = (Map) message.getOrDefault("systemProperties", Collections.emptyMap());
        String pro = systemProperties.get(SystemPropKey.BORNTIMESTAMP);
        if (pro != null) {
            return Long.parseLong(pro);
        }

        return 0L;
    }

    public static int getPartitionId(Map<String, Object> message) {
        Map<String, String> systemProperties = (Map) message.getOrDefault("systemProperties", Collections.emptyMap());
        String pro = systemProperties.get(DEFAULT_FIELD_PARTITION_ID);
        if (pro != null) {
            return Integer.parseInt(pro);
        }

        return 0;
    }

    public static String getTag(Map<String, Object> message) {
        Map<String, String> systemProperties = (Map) message.getOrDefault("systemProperties", Collections.emptyMap());
        return systemProperties.getOrDefault(SystemPropKey.TAG, "");
    }
    public static String getPropertiesTag(Map<String, String> systemProperties) {
        return systemProperties.getOrDefault(SystemPropKey.TAG, "");
    }

    public static String getKey(Map<String, Object> message) {
        Map<String, String> systemProperties = (Map) message.getOrDefault("systemProperties", Collections.emptyMap());
        return systemProperties.getOrDefault(SystemPropKey.KEY, "");
    }

    public static String getPropertiesKey(Map<String, String> systemProperties) {
        return systemProperties.getOrDefault(SystemPropKey.KEY, "");
    }
}
