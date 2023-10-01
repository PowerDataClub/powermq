package top.powerdata.powermq.broker.store;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
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
import top.powerdata.powermq.broker.utils.IcebergRecordUtils;
import top.powerdata.powermq.common.SchemaMessage;
import top.powerdata.powermq.common.schema.FieldInfo;
import top.powerdata.powermq.common.schema.MessageSchema;
import top.powerdata.powermq.common.server.data.PartitionData;
import top.powerdata.powermq.common.server.data.TopicData;
import org.apache.iceberg.PartitionSpec;


import java.io.IOException;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReentrantLock;

import static top.powerdata.powermq.common.Consts.DEFAULT_FIELD_EVENT_TIME;
import static top.powerdata.powermq.common.Consts.DEFAULT_FIELD_KEY;
import static top.powerdata.powermq.common.Consts.DEFAULT_FIELD_OFFSET_ID;
import static top.powerdata.powermq.common.Consts.DEFAULT_FIELD_PARTITION_ID;
import static top.powerdata.powermq.common.Consts.DEFAULT_FIELD_TAG;

public class IcebergMessageStore extends LakeMessageStore {
    final static public String SNAPSHOT_KEY_MAX_OFFSET_ID = "max_offset_id";
    final static public String SNAPSHOT_KEY_MIN_OFFSET_ID = "min_offset_id";
    final static public int FILE_MAX_RECORD = 10000;
    private BaseMetastoreCatalog catalog;
    private Schema schema;
    private Table table;
    private TopicData topicData;
    private Executor executor;
    private final Map<String, PartitionDataFile> datafiles = new ConcurrentHashMap<>();
    private final Map<Integer, Long> commitedOffsetId = new ConcurrentHashMap<>();
    private final Map<Integer, Long> minOffsetId = new ConcurrentHashMap<>();
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
    public void tryRecover() {

    }

    @Override
    public void buildLakeSchema(MessageSchema messageSchema) {
        schema = IcebergRecordUtils.buildLakeSchema(messageSchema);
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
            Record record = IcebergRecordUtils.message2record(message, schema, commitedOffsetId, minOffsetId);
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

    @Override
    public void asyncAddMessage(Map<String, Object> message) throws Exception {
        lock.lock();
        try {
            Record record = IcebergRecordUtils.map2record(message, schema, commitedOffsetId, minOffsetId);
            StructLike partition = getIcebergPartition(message);
            String partitionLocation = table.locationProvider().newDataLocation(
                    table.spec(), partition, getFileName(SchemaMessage.getPartitionId(message)));
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
        long offset = commitedOffsetId.get(partitionId);
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

    private StructLike getIcebergPartition(Map<String, Object> message) {
        int size = table.spec().fields().size();

        Object[] partitionTuple = new Object[size];
        for (int i = 0; i < size; i++) {
            PartitionField field = table.spec().fields().get(i);
            Object val = null;
            if (field.name().contains(DEFAULT_FIELD_EVENT_TIME)) {
                // hour use Micros
                val = SchemaMessage.getEventTimestamp(message) * 1000;
            } else if (field.name().contains(DEFAULT_FIELD_PARTITION_ID)) {
                val = SchemaMessage.getPartitionId(message);
            }

            Transform transform = field.transform();
            partitionTuple[i] = transform.apply(val);
        }
        return new IcebergPartition(partitionTuple);
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
            appends.set(SNAPSHOT_KEY_MIN_OFFSET_ID, String.valueOf(gson.toJson(minOffsetId)));
            appends.commit();
        } finally {
            lastFlushTimestamp = System.currentTimeMillis();
            datafiles.clear();
            minOffsetId.putAll(commitedOffsetId);
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

    @Override
    public Map<String, Object> getMessage(int partitionId, long offset) throws IOException {
        try(IcebergTableScan scan = IcebergTableScan.read(table).offset(partitionId, offset).build()) {
            if (scan.iterator().hasNext()) {
                Record record = scan.iterator().next();
                return IcebergRecordUtils.record2message(record, schema);
            }
        }
        return null;
    }

    @Override
    public Map<Integer, Long> getMinOffsets() {
        Iterator<Snapshot> iterator = table.snapshots().iterator();
        if (iterator.hasNext()) {
            String msg = iterator.next().summary().get(SNAPSHOT_KEY_MIN_OFFSET_ID);
            return gson.fromJson(msg, new TypeToken<Map<Integer, Long>>() {}.getType());
        } else {
            return Collections.emptyMap();
        }
    }

    @Override
    public long getMinOffset(int partitionId) {
        return getMinOffsets().getOrDefault(partitionId, 0L);
    }

    @Override
    public long getMaxOffset(int partitionId) {
        return getMaxOffsets().getOrDefault(partitionId, 0L);
    }
    @Override
    public Map<Integer, Long> getMaxOffsets() {
        Iterator<Snapshot> iterator = table.snapshots().iterator();
        if (iterator.hasNext()) {
            String msg = iterator.next().summary().get(SNAPSHOT_KEY_MAX_OFFSET_ID);
            return gson.fromJson(msg, new TypeToken<Map<Integer, Long>>() {}.getType());
        } else {
            return Collections.emptyMap();
        }
    }
}
