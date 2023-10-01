package top.powerdata.powermq.common.schema;

import lombok.SneakyThrows;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.api.schema.SchemaInfoProvider;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.BytesSchema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.client.impl.schema.StringSchema;
import org.apache.pulsar.client.internal.DefaultImplementation;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import top.powerdata.powermq.common.SchemaMessage;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static top.powerdata.powermq.common.Consts.ONLY_ONE_FIELD_NAME;
import static top.powerdata.powermq.common.Consts.__SYSTEM_PROPERTIES;
import static top.powerdata.powermq.common.Consts.__TOPIC;

public class MessageSchemaWrapper<T> extends MessageSchema<T> {
    final private Schema<T> schema;
    private GenericSchema genericSchema;
    private List<FieldInfo> fieldInfos;
    public MessageSchemaWrapper(Schema<T> schema) {
        this.schema = schema;
    }

    public final static Set<MessageSchemaWrapper> simpleSet = new HashSet<>();
    static {
        simpleSet.add(STRING);
        simpleSet.add(INT8);
        simpleSet.add(INT16);
        simpleSet.add(INT32);
        simpleSet.add(INT64);
        simpleSet.add(BOOL);
        simpleSet.add(FLOAT);
        simpleSet.add(DOUBLE);
        simpleSet.add(DATE);
        simpleSet.add(TIME);
        simpleSet.add(TIMESTAMP);
        simpleSet.add(INSTANT);
        simpleSet.add(LOCAL_DATE);
        simpleSet.add(LOCAL_TIME);
        simpleSet.add(LOCAL_DATE_TIME);
    }


    @SneakyThrows
    public static <T> Map<String, Object> message2map(SchemaMessage<T> message) {
        Map<String, Object> res = new HashMap<>();
        if (message.getMessageSchema().isSimpleSchema()) {
            res.put(ONLY_ONE_FIELD_NAME, message.getMsgWithSchema()) ;
        } else {
            Object obj = message.getMsgWithSchema();
            for (Field declaredField : obj.getClass().getDeclaredFields()) {
                declaredField.setAccessible(true);
                Object value = declaredField.get(message.getMsgWithSchema());
                res.put(declaredField.getName(), value);
            }
        }
        res.put(__SYSTEM_PROPERTIES, message.getSystemProperties());
        res.put(__TOPIC, message.getTopic());
        return res;
    }

    @SneakyThrows
    public static <T> SchemaMessage map2message(Map<String, Object> res, MessageSchema<T> schema, Class<T> clazz) {
        String topic = (String) res.get(__TOPIC);
        Map<String, String> properties = (Map) res.get(__SYSTEM_PROPERTIES);
        T instance = clazz.newInstance();
        for (Field field : clazz.getDeclaredFields()) {
            if (res.containsKey(field.getName())) {
                field.setAccessible(true);
                field.set(instance, res.get(field.getName()));
            }
        }
        return new SchemaMessage(topic, properties, instance, schema);
    }

    public boolean isSimpleSchema() {
        return simpleSet.contains(schema);
    }
    public List<FieldInfo> getSchemaFields() {
        if (fieldInfos != null) {
            return fieldInfos;
        }

        if (schema instanceof AvroSchema) {
            AvroSchema avroSchema = (AvroSchema) schema;
            fieldInfos = getFieldInfosFromSchema(avroSchema.getAvroSchema().getFields());
        } else if (schema instanceof BytesSchema) {
            fieldInfos = Arrays.asList(FieldInfo.builder()
                    .fieldName(ONLY_ONE_FIELD_NAME)
                    .type(org.apache.pulsar.shade.org.apache.avro.Schema.Type.BYTES)
                    .build());
        } else if (schema instanceof StringSchema) {
            fieldInfos = Arrays.asList(FieldInfo.builder()
                    .fieldName(ONLY_ONE_FIELD_NAME)
                    .type(org.apache.pulsar.shade.org.apache.avro.Schema.Type.STRING)
                    .build());
        } else if (schema instanceof JSONSchema) {
            JSONSchema jsonSchema = (JSONSchema) schema;
            fieldInfos = getFieldInfosFromSchema(jsonSchema.getAvroSchema().getFields());
        } else {
            throw new RuntimeException("unsupported schema");
        }
        return fieldInfos;
    }

    private List<FieldInfo> getFieldInfosFromSchema(List<org.apache.pulsar.shade.org.apache.avro.Schema.Field> fields) {
        List<FieldInfo> fieldInfoResult = new LinkedList<>();
        for (org.apache.pulsar.shade.org.apache.avro.Schema.Field field : fields) {
            FieldInfo fieldInfo = FieldInfo.builder()
                    .fieldName(field.name())
                    .type(field.schema().getType())
                    .build();
            if (field.schema().getType().equals(org.apache.pulsar.shade.org.apache.avro.Schema.Type.UNION)) {
                fieldInfo.setTypes(field.schema().getTypes());
            }
            fieldInfoResult.add(fieldInfo);
        }
        return fieldInfoResult;
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
        return schema.decode(data);
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
        return schema.getNativeSchema();
    }



}
