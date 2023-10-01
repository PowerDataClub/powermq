package top.powerdata.powermq.common.schema;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.internal.DefaultImplementation;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Date;
import java.util.List;

public abstract class MessageSchema<T> implements Schema<T> {

    public abstract List<FieldInfo> getSchemaFields();

    @Override
    public Schema<T> clone() {
        return null;
    }

    final static public MessageSchemaWrapper<String> STRING = new MessageSchemaWrapper<>(Schema.STRING);
    final static public MessageSchemaWrapper<Byte> INT8 = new MessageSchemaWrapper<>(Schema.INT8);
    final static public MessageSchemaWrapper<Short> INT16 = new MessageSchemaWrapper<>(Schema.INT16);
    final static public MessageSchemaWrapper<Integer> INT32 = new MessageSchemaWrapper<>(Schema.INT32);
    final static public MessageSchemaWrapper<Long> INT64 = new MessageSchemaWrapper<>(Schema.INT64);
    final static public MessageSchemaWrapper<Boolean> BOOL = new MessageSchemaWrapper<>(Schema.BOOL);
    final static public MessageSchemaWrapper<Float> FLOAT = new MessageSchemaWrapper<>(Schema.FLOAT);
    final static public MessageSchemaWrapper<Double> DOUBLE = new MessageSchemaWrapper<>(Schema.DOUBLE);
    final static public MessageSchemaWrapper<Date> DATE = new MessageSchemaWrapper<>(Schema.DATE);
    final static public MessageSchemaWrapper<Time> TIME = new MessageSchemaWrapper<>(Schema.TIME);
    final static public MessageSchemaWrapper<Timestamp> TIMESTAMP = new MessageSchemaWrapper<>(Schema.TIMESTAMP);
    final static public MessageSchemaWrapper<Instant> INSTANT = new MessageSchemaWrapper<>(Schema.INSTANT);
    final static public MessageSchemaWrapper<LocalDate> LOCAL_DATE = new MessageSchemaWrapper<>(Schema.LOCAL_DATE);
    final static public MessageSchemaWrapper<LocalTime> LOCAL_TIME = new MessageSchemaWrapper<>(Schema.LOCAL_TIME);
    final static public MessageSchemaWrapper<LocalDateTime> LOCAL_DATE_TIME = new MessageSchemaWrapper<>(Schema.LOCAL_DATE_TIME);

    public static <T extends com.google.protobuf.GeneratedMessageV3> MessageSchema<T> PROTOBUF(SchemaDefinition<T> schemaDefinition) {
        return new MessageSchemaWrapper<>(Schema.PROTOBUF(schemaDefinition));
    }


    public static <T extends com.google.protobuf.GeneratedMessageV3> MessageSchema<T> PROTOBUF_NATIVE(Class<T> clazz) {
        return new MessageSchemaWrapper<>(DefaultImplementation.getDefaultImplementation()
                .newProtobufNativeSchema(SchemaDefinition.builder().withPojo(clazz).build()));
    }

    public static <T extends com.google.protobuf.GeneratedMessageV3> MessageSchema<T> PROTOBUF_NATIVE(
            SchemaDefinition<T> schemaDefinition) {
        return new MessageSchemaWrapper<>(DefaultImplementation.getDefaultImplementation().newProtobufNativeSchema(schemaDefinition));
    }

    public static <T> MessageSchema<T> AVRO(Class<T> pojo) {
        return new MessageSchemaWrapper<>(DefaultImplementation.getDefaultImplementation()
                .newAvroSchema(SchemaDefinition.builder().withPojo(pojo).build()));
    }

    public static <T> MessageSchema<T> AVRO(SchemaDefinition<T> schemaDefinition) {
        return new MessageSchemaWrapper<>(DefaultImplementation.getDefaultImplementation().newAvroSchema(schemaDefinition));
    }

    public static <T> MessageSchema<T> JSON(Class<T> pojo) {
        return new MessageSchemaWrapper<>(DefaultImplementation.getDefaultImplementation()
                .newJSONSchema(SchemaDefinition.builder().withPojo(pojo).build()));
    }

    public static <T> MessageSchema<T> JSON(SchemaDefinition schemaDefinition) {
        return new MessageSchemaWrapper<>(DefaultImplementation.getDefaultImplementation().newJSONSchema(schemaDefinition));
    }

    public static <K, V> MessageSchema<KeyValue<K, V>> KeyValue(Class<K> key, Class<V> value, SchemaType type) {
        return new MessageSchemaWrapper<>(DefaultImplementation.getDefaultImplementation().newKeyValueSchema(key, value, type));
    }

    public static MessageSchema<KeyValue<byte[], byte[]>> KV_BYTES() {
        return new MessageSchemaWrapper<>(DefaultImplementation.getDefaultImplementation().newKeyValueBytesSchema());
    }

    public static <K, V> MessageSchema<KeyValue<K, V>> KeyValue(Class<K> key, Class<V> value) {
        return new MessageSchemaWrapper<>(DefaultImplementation.getDefaultImplementation().newKeyValueSchema(key, value, SchemaType.JSON));
    }

    public static <K, V> MessageSchema<KeyValue<K, V>> KeyValue(Schema<K> key, Schema<V> value) {
        return new MessageSchemaWrapper<>(KeyValue(key, value, KeyValueEncodingType.INLINE));
    }

    public static <K, V> MessageSchema<KeyValue<K, V>> KeyValue(Schema<K> key, Schema<V> value,
                                                                KeyValueEncodingType keyValueEncodingType) {
        return new MessageSchemaWrapper<>(DefaultImplementation.getDefaultImplementation().newKeyValueSchema(key, value, keyValueEncodingType));
    }

    @Deprecated
    public static MessageSchema<GenericRecord> AUTO() {
        return AUTO_CONSUME();
    }

    public static MessageSchema<GenericRecord> AUTO_CONSUME() {
        return new MessageSchemaWrapper<>(DefaultImplementation.getDefaultImplementation().newAutoConsumeSchema());
    }

    public static MessageSchema<byte[]> AUTO_PRODUCE_BYTES() {
        return new MessageSchemaWrapper<>(DefaultImplementation.getDefaultImplementation().newAutoProduceSchema());
    }

    public static MessageSchema<byte[]> AUTO_PRODUCE_BYTES(Schema<?> schema) {
        return new MessageSchemaWrapper<>(DefaultImplementation.getDefaultImplementation().newAutoProduceSchema(schema));
    }

    public static MessageSchema<byte[]> NATIVE_AVRO(Object schema) {
        return new MessageSchemaWrapper<>(DefaultImplementation.getDefaultImplementation().newAutoProduceValidatedAvroSchema(schema));
    }

    public static MessageSchema<?> getSchema(SchemaInfo schemaInfo) {
        return new MessageSchemaWrapper<>(DefaultImplementation.getDefaultImplementation().getSchema(schemaInfo));
    }
}
