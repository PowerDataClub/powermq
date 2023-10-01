package top.powerdata.powermq.broker.utils;

import com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import top.powerdata.powermq.common.SchemaMessage;
import top.powerdata.powermq.common.schema.FieldInfo;
import top.powerdata.powermq.common.schema.MessageSchema;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static top.powerdata.powermq.common.Consts.DEFAULT_FIELD_EVENT_TIME;
import static top.powerdata.powermq.common.Consts.DEFAULT_FIELD_KEY;
import static top.powerdata.powermq.common.Consts.DEFAULT_FIELD_OFFSET_ID;
import static top.powerdata.powermq.common.Consts.DEFAULT_FIELD_PARTITION_ID;
import static top.powerdata.powermq.common.Consts.DEFAULT_FIELD_TAG;
import static top.powerdata.powermq.common.Consts.DEFAULT_SYSTEM_FIELDS_KEY;
import static top.powerdata.powermq.common.Consts.ONLY_ONE_FIELD_NAME;
import static top.powerdata.powermq.common.Consts.__SYSTEM_PROPERTIES;

public class IcebergRecordUtils {

    public static Schema buildLakeSchema(MessageSchema messageSchema) {
        List<Types.NestedField> fields = new LinkedList<>();
        for (int i = 0; i < messageSchema.getSchemaFields().size(); i++) {
            FieldInfo field = (FieldInfo) messageSchema.getSchemaFields().get(i);
            switch (field.getType()) {
                case UNION:
                    for (org.apache.pulsar.shade.org.apache.avro.Schema type : field.getTypes()) {
                        if (type.getType().equals(org.apache.pulsar.shade.org.apache.avro.Schema.Type.NULL)) {
                            continue;
                        }
                        if (type.getType().equals(org.apache.pulsar.shade.org.apache.avro.Schema.Type.MAP)) {
                            // map String:long
                            fields.add(Types.NestedField.required(i, field.getFieldName(),
                                    Types.MapType.ofOptional(
                                            (Integer.MAX_VALUE - i * 2 + 1),
                                            (Integer.MAX_VALUE - i * 2 + 2),
                                            Types.StringType.get(),
                                            Types.fromPrimitiveString(type.getValueType().getType().getName())
                                    )
                            ));
                        } else if (type.getType().equals(org.apache.pulsar.shade.org.apache.avro.Schema.Type.ARRAY)) {
                            // map long:long
                            List<org.apache.pulsar.shade.org.apache.avro.Schema.Field> kvField = type.getElementType().getFields();
                            fields.add(Types.NestedField.required(i, field.getFieldName(),
                                    Types.MapType.ofOptional(
                                            (Integer.MAX_VALUE - i * 2 + 1),
                                            (Integer.MAX_VALUE - i * 2 + 2),
                                            Types.fromPrimitiveString(type.getElementType().getFields().get(0).schema().getType().getName()),
                                            Types.fromPrimitiveString(type.getElementType().getFields().get(1).schema().getType().getName())
                                    )
                            ));
                        } else {
                            fields.add(Types.NestedField.required(i, field.getFieldName(), Types.fromPrimitiveString(field.getTypes().get(1).getType().toString())));
                        }
                    }
                    break;
                default:
                    fields.add(Types.NestedField.required(i, field.getFieldName(), Types.fromPrimitiveString(field.getType().toString())));
            }
        }
        fields.add(Types.NestedField.required(fields.size(), DEFAULT_FIELD_EVENT_TIME, Types.TimestampType.withoutZone()));
        fields.add(Types.NestedField.required(fields.size(), DEFAULT_FIELD_OFFSET_ID, Types.LongType.get()));
        fields.add(Types.NestedField.required(fields.size(), DEFAULT_FIELD_PARTITION_ID, Types.IntegerType.get()));
        fields.add(Types.NestedField.required(fields.size(), DEFAULT_FIELD_TAG, Types.StringType.get()));
        fields.add(Types.NestedField.required(fields.size(), DEFAULT_FIELD_KEY, Types.StringType.get()));
        return new Schema(fields);
    }

    public static Record message2record(SchemaMessage message, Schema schema, Map<Integer, Long> commitedOffsetId, Map<Integer, Long> minOffsetId) {
        Record record = org.apache.iceberg.data.GenericRecord.create(schema.asStruct());
        for (Types.NestedField column : schema.columns()) {
            if (column.name().equals(ONLY_ONE_FIELD_NAME)) {
                record.setField(ONLY_ONE_FIELD_NAME, message.getMsgWithSchema());
                break;
            }
            if (column.name().equals(DEFAULT_FIELD_EVENT_TIME)) {
                Timestamp timestamp = new Timestamp(message.getEventTimestamp());
                record.setField(DEFAULT_FIELD_EVENT_TIME, timestamp.toLocalDateTime());
                continue;
            }
            if (column.name().equals(DEFAULT_FIELD_OFFSET_ID)) {
                if (!commitedOffsetId.containsKey(message.getPartitionId())) {
                    commitedOffsetId.put(message.getPartitionId(), 0L);
                }
                if (!minOffsetId.containsKey(message.getPartitionId())) {
                    minOffsetId.put(message.getPartitionId(), 0L);
                }
                record.setField(DEFAULT_FIELD_OFFSET_ID, commitedOffsetId.get(message.getPartitionId()));
                commitedOffsetId.computeIfPresent(message.getPartitionId(), (integer, aLong) -> aLong + 1);
                continue;
            }
            if (column.name().equals(DEFAULT_FIELD_PARTITION_ID)) {
                record.setField(DEFAULT_FIELD_PARTITION_ID, message.getPartitionId());
                continue;
            }
            if (column.name().equals(DEFAULT_FIELD_TAG)) {
                record.setField(DEFAULT_FIELD_TAG, message.getTag());
                continue;
            }
            if (column.name().equals(DEFAULT_FIELD_KEY)) {
                record.setField(DEFAULT_FIELD_KEY, message.getKey());
                continue;
            }
            record.setField(column.name(), message.getField(column.name()));
        }
        return record;
    }

    public static Map<String, Object> record2message(Record record, Schema schema) {
        Map<String, Object> result = new HashMap<>();
        Map<String, String> systemProperties = new HashMap<>();
        for (Types.NestedField column : schema.columns()) {
            if (column.name().equals(DEFAULT_FIELD_EVENT_TIME)) {
                LocalDateTime localDateTime = (LocalDateTime) record.getField(column.name());
                systemProperties.put(column.name(), String.valueOf(Timestamp.valueOf(localDateTime).getTime()));
            } else if (DEFAULT_SYSTEM_FIELDS_KEY.contains(column.name())) {
                systemProperties.put(column.name(), String.valueOf(record.getField(column.name())));
            } else {
                result.put(column.name(), record.getField(column.name()));
            }
        }
        result.put(__SYSTEM_PROPERTIES, systemProperties);
        return result;
    }


    public static Record map2record(Map<String, Object> message, Schema schema, Map<Integer, Long> commitedOffsetId, Map<Integer, Long> minOffsetId) {
        Record record = org.apache.iceberg.data.GenericRecord.create(schema.asStruct());
        for (Types.NestedField column : schema.columns()) {
            if (column.name().equals(ONLY_ONE_FIELD_NAME)) {
                record.setField(ONLY_ONE_FIELD_NAME, message.get(ONLY_ONE_FIELD_NAME));
                break;
            }
            if (column.name().equals(DEFAULT_FIELD_EVENT_TIME)) {
                Timestamp timestamp = new Timestamp(SchemaMessage.getEventTimestamp(message));
                record.setField(DEFAULT_FIELD_EVENT_TIME, timestamp.toLocalDateTime());
                continue;
            }
            if (column.name().equals(DEFAULT_FIELD_OFFSET_ID)) {
                if (!commitedOffsetId.containsKey(SchemaMessage.getPartitionId(message))) {
                    commitedOffsetId.put(SchemaMessage.getPartitionId(message), 0L);
                }
                if (!minOffsetId.containsKey(SchemaMessage.getPartitionId(message))) {
                    minOffsetId.put(SchemaMessage.getPartitionId(message), 0L);
                }
                record.setField(DEFAULT_FIELD_OFFSET_ID, commitedOffsetId.get(SchemaMessage.getPartitionId(message)));
                commitedOffsetId.computeIfPresent(SchemaMessage.getPartitionId(message), (integer, aLong) -> aLong + 1);
                continue;
            }
            if (column.name().equals(DEFAULT_FIELD_PARTITION_ID)) {
                record.setField(DEFAULT_FIELD_PARTITION_ID, SchemaMessage.getPartitionId(message));
                continue;
            }
            if (column.name().equals(DEFAULT_FIELD_TAG)) {
                record.setField(DEFAULT_FIELD_TAG, SchemaMessage.getTag(message));
                continue;
            }
            if (column.name().equals(DEFAULT_FIELD_KEY)) {
                record.setField(DEFAULT_FIELD_KEY, SchemaMessage.getKey(message));
                continue;
            }
            record.setField(column.name(), message.get(column.name()));
        }
        return record;
    }
}
