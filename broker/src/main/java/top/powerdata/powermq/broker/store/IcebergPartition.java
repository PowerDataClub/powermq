package top.powerdata.powermq.broker.store;

import org.apache.iceberg.StructLike;

public class IcebergPartition implements StructLike {

    private final Object[] partitionTuple;

    public IcebergPartition(Object[] partitionTuple) {
        this.partitionTuple = partitionTuple;
    }

    @Override
    public int size() {
        return partitionTuple.length;
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
        return javaClass.cast(partitionTuple[pos]);
    }

    @Override
    public <T> void set(int pos, T value) {
        partitionTuple[pos] = value;
    }
}
