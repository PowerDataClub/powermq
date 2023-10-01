package top.powerdata.powermq.broker.store;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.types.Types;
import top.powerdata.powermq.common.SchemaMessage;
import top.powerdata.powermq.common.schema.FieldInfo;
import top.powerdata.powermq.common.schema.MessageSchema;
import top.powerdata.powermq.common.server.data.PartitionData;
import top.powerdata.powermq.common.server.data.TopicData;
import org.apache.iceberg.PartitionSpec;


import java.io.IOException;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import static top.powerdata.powermq.common.Consts.DEFAULT_FIELD_EVENT_TIME;
import static top.powerdata.powermq.common.Consts.DEFAULT_FIELD_OFFSET_ID;
import static top.powerdata.powermq.common.Consts.DEFAULT_FIELD_PARTITION_ID;
import static top.powerdata.powermq.common.schema.MessageSchemaWrapper.ONLY_ONE_FIELD_NAME;

public class IcebergMessageStore extends LakeMessageStore {
    final static public String SNAPSHOT_KEY_MAX_OFFSET_ID = "max_offset_id";
    final static public int FILE_MAX_RECORD = 10000;
    private BaseMetastoreCatalog catalog;
    private Schema schema;
    private Table table;
    private TopicData topicData;
    private Executor executor;
    private final Map<String, PartitionDataFile> datafiles = new ConcurrentHashMap<>();
    private final Map<Integer, AtomicLong> commitedOffsetId = new ConcurrentHashMap<>();
    private final Gson gson = new GsonBuilder().create();
    private final ReentrantLock lock = new ReentrantLock();
    private long lastFlushTimestamp;

    public IcebergMessageStore(PartitionData partition, MessageSchema messageSchema, BaseMetastoreCatalog catalog) {
        super(partition, messageSchema);
        this.catalog = catalog;
        this.topicData = partition;
    }
    public IcebergMessageStore(PartitionData partition, MessageSchema messageSchema, BaseMetastoreCatalog catalog, Executor executor) {
        super(partition, messageSchema);
        this.catalog = catalog;
        this.topicData = partition;
        this.executor = executor;
    }

    @Override
    public void buildLakeSchema(MessageSchema messageSchema) {
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
        this.schema = new Schema(fields);
    }

    @Override
    public void createDataLakeTable() {
        TableIdentifier name = TableIdentifier.of(
                topicData.getTenant(), topicData.getTopicName(), topicData.getTopicName()
        );
        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .hour(DEFAULT_FIELD_EVENT_TIME)
                .identity(DEFAULT_FIELD_PARTITION_ID)
                .build();
        try {
            table = catalog.loadTable(name);
        } catch (NoSuchTableException exception) {
            // load failed, try to create table
            table = catalog.createTable(name, schema, spec);
        }
    }

    @Override
    public void asyncAddMessage(SchemaMessage message) throws Exception{
        lock.lock();
        try {
            Record record = message2record(message);
            StructLike partition = getIcebergPartition(message);
            String partitionLocation = table.locationProvider().newDataLocation(
                    table.spec(), partition, getFileName(message.getPartitionId()));
            if (!datafiles.containsKey(partitionLocation)) {
                FileAppender<Record> appender = Parquet.write(table.io().newOutputFile(partitionLocation))
                        .forTable(table)
                        .createWriterFunc(GenericParquetWriter::buildWriter)
                        .build();
                datafiles.put(partitionLocation, new PartitionDataFile(partitionLocation, partition, appender));
            }
            datafiles.get(partitionLocation).append(record);
        } finally {
            lock.unlock();
        }
    }

    private String getFileName(int partitionId) {
        long offset = commitedOffsetId.get(partitionId).get();
        return String.format("%019d", offset / FILE_MAX_RECORD * FILE_MAX_RECORD) + "_" + lastFlushTimestamp;
    }

    private StructLike getIcebergPartition(SchemaMessage message) {
        int size = table.spec().fields().size();

        Object[] partitionTuple = new Object[size];
        for (int i = 0; i < size; i++) {
            PartitionField field = table.spec().fields().get(i);
            Object val = null;
            if (field.name().contains(DEFAULT_FIELD_EVENT_TIME)) {
                // hour use Micros
                val = message.getEventTimestamp() * 1000;
            } else if (field.name().contains(DEFAULT_FIELD_PARTITION_ID)) {
                val = message.getPartitionId();
            }

            Transform transform = field.transform();
            partitionTuple[i] = transform.apply(val);
        }
        return new IcebergPartition(partitionTuple);
    }

    private Record message2record(SchemaMessage message) {
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
                    commitedOffsetId.put(message.getPartitionId(), new AtomicLong(0L));
                }
                record.setField(DEFAULT_FIELD_OFFSET_ID, commitedOffsetId.get(message.getPartitionId()).getAndIncrement());
                continue;
            }
            if (column.name().equals(DEFAULT_FIELD_PARTITION_ID)) {
                record.setField(DEFAULT_FIELD_PARTITION_ID, message.getPartitionId());
                continue;
            }
            record.setField(column.name(), message.getField(column.name()));
        }
        return record;
    }

    public void flush() throws IOException {
        lock.lock();
        try {
            Set<DataFile> toAdd = new HashSet<>();
            for (PartitionDataFile dataFile : datafiles.values()) {
                dataFile.close();
                DataFile file = DataFiles.builder(table.spec())
                        .withInputFile(table.io().newInputFile(dataFile.location))
                        .withPartition(dataFile.partition)
                        .withMetrics(dataFile.fileAppender.metrics())
                        .withFormat("parquet")
                        .build();
                toAdd.add(file);
            }

            AppendFiles appends = table.newAppend();
            toAdd.forEach(appends::appendFile);
            appends.set(SNAPSHOT_KEY_MAX_OFFSET_ID, String.valueOf(gson.toJson(commitedOffsetId)));
            appends.commit();
        } finally {
            lastFlushTimestamp = System.currentTimeMillis();
            datafiles.clear();
            lock.unlock();
        }
    }

    static class PartitionDataFile {
        private final String location;
        private final StructLike partition;
        private final FileAppender<Record> fileAppender;

        PartitionDataFile(String l, StructLike p, FileAppender<Record> appender) {
            this.location = l;
            this.partition = p;
            this.fileAppender = appender;
        }

        public void append(Record r) {
            fileAppender.add(r);
        }

        public void close() throws IOException {
            fileAppender.close();
        }
    }

}
