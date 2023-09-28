package top.powerdata.powermq.common.schema;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericRecordBuilder;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.api.schema.SchemaInfoProvider;
import org.apache.pulsar.client.internal.DefaultImplementation;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.nio.ByteBuffer;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Date;
import java.util.List;
import java.util.Optional;

public class MessageSchemaWrapper<T> implements MessageSchema<T>{
    private Schema<T> schema;
    private GenericSchema genericSchema;
    public MessageSchemaWrapper(Schema<T> schema) {
        this.schema = schema;
    }
    public MessageSchemaWrapper(GenericSchema genericSchema) {
        this.genericSchema = genericSchema;
    }

    public MessageSchemaWrapper() {
        this((Schema<T>) Schema.BYTES);
    }

    @Override
    public void validate(byte[] message) {
        schema.validate(message);
    }

    @Override
    public byte[] encode(T message) {
        return schema.encode(message);
    }

    @Override
    public boolean supportSchemaVersioning() {
        return schema.supportSchemaVersioning();
    }

    @Override
    public void setSchemaInfoProvider(SchemaInfoProvider schemaInfoProvider) {
        schema.setSchemaInfoProvider(schemaInfoProvider);
    }

    @Override
    public T decode(byte[] bytes) {
        return schema.decode(bytes);
    }

    @Override
    public T decode(byte[] bytes, byte[] schemaVersion) {
        return schema.decode(bytes, schemaVersion);
    }

    @Override
    public T decode(ByteBuffer data) {
        return MessageSchema.super.decode(data);
    }

    @Override
    public T decode(ByteBuffer data, byte[] schemaVersion) {
        return schema.decode(data, schemaVersion);
    }

    @Override
    public boolean requireFetchingSchemaInfo() {
        return schema.requireFetchingSchemaInfo();
    }

    @Override
    public void configureSchemaInfo(String topic, String componentName, SchemaInfo schemaInfo) {
        schema.configureSchemaInfo(topic, componentName, schemaInfo);
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return schema.getSchemaInfo();
    }

    @Override
    public Schema<T> clone() {
        return schema.clone();
    }

    @Override
    public Optional<Object> getNativeSchema() {
        return MessageSchema.super.getNativeSchema();
    }

    public static <T extends com.google.protobuf.GeneratedMessageV3> MessageSchema<T> PROTOBUF(SchemaDefinition<T> schemaDefinition) {
        return new MessageSchemaWrapper<>(Schema.PROTOBUF(schemaDefinition));
    }


    static <T extends com.google.protobuf.GeneratedMessageV3> MessageSchema<T> PROTOBUF_NATIVE(Class<T> clazz) {
        return new MessageSchemaWrapper<>(DefaultImplementation.getDefaultImplementation()
                .newProtobufNativeSchema(SchemaDefinition.builder().withPojo(clazz).build()));
    }

    static <T extends com.google.protobuf.GeneratedMessageV3> MessageSchema<T> PROTOBUF_NATIVE(
            SchemaDefinition<T> schemaDefinition) {
        return new MessageSchemaWrapper<>(DefaultImplementation.getDefaultImplementation().newProtobufNativeSchema(schemaDefinition));
    }

    static <T> MessageSchema<T> AVRO(Class<T> pojo) {
        return new MessageSchemaWrapper<>(DefaultImplementation.getDefaultImplementation()
                .newAvroSchema(SchemaDefinition.builder().withPojo(pojo).build()));
    }

    static <T> MessageSchema<T> AVRO(SchemaDefinition<T> schemaDefinition) {
        return new MessageSchemaWrapper<>(DefaultImplementation.getDefaultImplementation().newAvroSchema(schemaDefinition));
    }

    static <T> MessageSchema<T> JSON(Class<T> pojo) {
        return new MessageSchemaWrapper<>(DefaultImplementation.getDefaultImplementation()
                .newJSONSchema(SchemaDefinition.builder().withPojo(pojo).build()));
    }

    static <T> MessageSchema<T> JSON(SchemaDefinition schemaDefinition) {
        return new MessageSchemaWrapper<>(DefaultImplementation.getDefaultImplementation().newJSONSchema(schemaDefinition));
    }

    static <K, V> MessageSchema<KeyValue<K, V>> KeyValue(Class<K> key, Class<V> value, SchemaType type) {
        return new MessageSchemaWrapper<>(DefaultImplementation.getDefaultImplementation().newKeyValueSchema(key, value, type));
    }

    static MessageSchema<KeyValue<byte[], byte[]>> KV_BYTES() {
        return new MessageSchemaWrapper<>(DefaultImplementation.getDefaultImplementation().newKeyValueBytesSchema());
    }

    static <K, V> MessageSchema<KeyValue<K, V>> KeyValue(Class<K> key, Class<V> value) {
        return new MessageSchemaWrapper<>(DefaultImplementation.getDefaultImplementation().newKeyValueSchema(key, value, SchemaType.JSON));
    }

    static <K, V> MessageSchema<KeyValue<K, V>> KeyValue(Schema<K> key, Schema<V> value) {
        return new MessageSchemaWrapper<>(KeyValue(key, value, KeyValueEncodingType.INLINE));
    }

    static <K, V> MessageSchema<KeyValue<K, V>> KeyValue(Schema<K> key, Schema<V> value,
                                                  KeyValueEncodingType keyValueEncodingType) {
        return new MessageSchemaWrapper<>(DefaultImplementation.getDefaultImplementation().newKeyValueSchema(key, value, keyValueEncodingType));
    }

    @Deprecated
    static MessageSchema<GenericRecord> AUTO() {
        return AUTO_CONSUME();
    }

    static MessageSchema<GenericRecord> AUTO_CONSUME() {
        return new MessageSchemaWrapper<>(DefaultImplementation.getDefaultImplementation().newAutoConsumeSchema());
    }

    static MessageSchema<byte[]> AUTO_PRODUCE_BYTES() {
        return new MessageSchemaWrapper<>(DefaultImplementation.getDefaultImplementation().newAutoProduceSchema());
    }

    static MessageSchema<byte[]> AUTO_PRODUCE_BYTES(Schema<?> schema) {
        return new MessageSchemaWrapper<>(DefaultImplementation.getDefaultImplementation().newAutoProduceSchema(schema));
    }

    static MessageSchema<byte[]> NATIVE_AVRO(Object schema) {
        return new MessageSchemaWrapper<>(DefaultImplementation.getDefaultImplementation().newAutoProduceValidatedAvroSchema(schema));
    }

    static MessageSchema<?> getSchema(SchemaInfo schemaInfo) {
        return new MessageSchemaWrapper<>(DefaultImplementation.getDefaultImplementation().getSchema(schemaInfo));
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

}
