package top.powerdata.powermq.broker.store;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static top.powerdata.powermq.common.Consts.DEFAULT_FIELD_OFFSET_ID;
import static top.powerdata.powermq.common.Consts.DEFAULT_FIELD_PARTITION_ID;

public class IcebergTableScan  extends CloseableGroup implements CloseableIterable<Record> {
    private final TableOperations ops;
    private final Schema schema;
    private final CloseableIterable<FileScanTask> tasks;

    public static class ScanBuilder {

        private TableScan tableScan;

        public ScanBuilder(Table table) {
            this.tableScan = table.newScan();
        }

        public ScanBuilder offsetBatch(int partitionId, long offsetId) {
            Expression expression = Expressions.and(
                    Expressions.equal(DEFAULT_FIELD_PARTITION_ID, partitionId),
                    Expressions.greaterThanOrEqual(DEFAULT_FIELD_OFFSET_ID, offsetId)
            );
            return where(expression);
        }

        public ScanBuilder offset(int partitionId, long offsetId) {
            Expression expression = Expressions.and(
                    Expressions.equal(DEFAULT_FIELD_PARTITION_ID, partitionId),
                    Expressions.equal(DEFAULT_FIELD_OFFSET_ID, offsetId)
            );
            return where(expression);
        }

        public ScanBuilder offsetBatch(int partitionId, long offsetId, int count) {
            Expression expression = Expressions.and(
                    Expressions.equal(DEFAULT_FIELD_PARTITION_ID, partitionId),
                    Expressions.greaterThanOrEqual(DEFAULT_FIELD_OFFSET_ID, offsetId),
                    Expressions.lessThan(DEFAULT_FIELD_OFFSET_ID, offsetId + count)
            );
            return where(expression);
        }

        public ScanBuilder where(Expression rowFilter) {
            this.tableScan = tableScan.filter(rowFilter);
            return this;
        }

        public ScanBuilder select(String... selectedColumns) {
            this.tableScan = tableScan.select(ImmutableList.copyOf(selectedColumns));
            return this;
        }

        public ScanBuilder useSnapshot(long scanSnapshotId) {
            this.tableScan = tableScan.useSnapshot(scanSnapshotId);
            return this;
        }

        public ScanBuilder appendsBetween(long fromSnapshotId, long toSnapshotId) {
            this.tableScan = tableScan.appendsBetween(fromSnapshotId, toSnapshotId);
            return this;
        }

        public IcebergTableScan build() {
            return new IcebergTableScan(tableScan);
        }
    }

    public static ScanBuilder read(Table table) {
        return new ScanBuilder(table);
    }

    private IcebergTableScan(TableScan scan) {
        this.ops = ((HasTableOperations) scan.table()).operations();
        this.schema = scan.schema();
        this.tasks = scan.planFiles();
    }


    @Override
    public CloseableIterator<Record> iterator() {
        ScanIterator iter = new ScanIterator(tasks);
        addCloseable(iter);
        return iter;
    }

    private class ScanIterator implements CloseableIterator<Record>, Closeable {

        private final Iterator<FileScanTask> tasks;
        private final RecordWrapper wrap;
        private Closeable closeable = null;
        private Iterator<Record> iter = Collections.emptyIterator();

        private ScanIterator(CloseableIterable<FileScanTask> tasks) {
            this.tasks = tasks.iterator();
            this.wrap = new RecordWrapper(schema.asStruct());
        }

        @Override
        public boolean hasNext() {
            while (true) {
                if (iter.hasNext()) {
                    return true;

                } else if (tasks.hasNext()) {
                    if (closeable != null) {
                        try {
                            closeable.close();
                        } catch (IOException e) {
                            throw new RuntimeIOException(e, "Failed to close task");
                        }
                    }

                    FileScanTask task = tasks.next();
                    CloseableIterable<Record> reader = open(task);
                    closeable = reader;

                    if (task.residual() != null && task.residual() != Expressions.alwaysTrue()) {
                        Evaluator eval = new Evaluator(schema.asStruct(), task.residual(), true);
                        iter = Iterables.filter(reader, r -> eval.eval(wrap.wrap(r))).iterator();
                    } else {
                        iter = reader.iterator();
                    }

                } else {
                    return false;
                }
            }
        }

        @Override
        public Record next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            return iter.next();
        }

        @Override
        public void close() throws IOException {
            if (closeable != null) {
                closeable.close();
            }
        }
    }

    private CloseableIterable<Record> open(FileScanTask task) {
        InputFile input = ops.io().newInputFile(task.file().path().toString());
        // 所有数据文件都是parquet格式
        Parquet.ReadBuilder parquet = Parquet.read(input)
                .project(schema)
                .createReaderFunc(s -> GenericParquetReaders.buildReader(schema, s))
                .split(task.start(), task.length());

        return parquet.build();
    }

    static class RecordWrapper extends InternalRecordWrapper {
        public RecordWrapper(Types.StructType struct) {
            super(struct);
        }
    }
}
