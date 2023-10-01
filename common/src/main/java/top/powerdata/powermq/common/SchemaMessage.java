package top.powerdata.powermq.common;

import io.openmessaging.api.Message;
import lombok.SneakyThrows;
import top.powerdata.powermq.common.schema.MessageSchema;

import java.lang.reflect.Field;

import static top.powerdata.powermq.common.Consts.DEFAULT_FIELD_EVENT_TIME;
import static top.powerdata.powermq.common.Consts.DEFAULT_FIELD_PARTITION_ID;


public class SchemaMessage<T> extends Message {
    private MessageSchema<T> messageSchema;
    private T msgWithSchema;

    public SchemaMessage(String topic, String tag, String key, byte[] body, MessageSchema<T> messageSchema) {
        super(topic, tag, key, body);
        this.messageSchema = messageSchema;
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
}
