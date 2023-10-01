package top.powerdata.powermq.common.schema;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.Field;
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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

public class MessageSchemaWrapper<T> extends MessageSchema<T> {
    final static public String ONLY_ONE_FIELD_NAME = "__";
    final private Schema<T> schema;
    private GenericSchema genericSchema;
    private List<FieldInfo> fieldInfos;
    public MessageSchemaWrapper(Schema<T> schema) {
        this.schema = schema;
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
