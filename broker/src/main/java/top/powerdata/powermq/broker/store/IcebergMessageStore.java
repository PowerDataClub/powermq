package top.powerdata.powermq.broker.store;

import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import top.powerdata.powermq.common.schema.MessageSchema;
import top.powerdata.powermq.common.server.data.PartitionData;
import top.powerdata.powermq.common.server.data.TopicData;
import org.apache.iceberg.PartitionSpec;

public class IcebergMessageStore extends LakeMessageStore {
    private BaseMetastoreCatalog catalog;
    private Schema schema;
    private Table table;

    public IcebergMessageStore(TopicData topicData, PartitionData partition, MessageSchema messageSchema, BaseMetastoreCatalog catalog) {
        super(topicData, partition, messageSchema);
        this.catalog = catalog;
    }

    @Override
    public void buildLakeSchema(MessageSchema schema) {

    }

    @Override
    public void createDataLakeTable() {
        TableIdentifier name = TableIdentifier.of("logging", "logs");
        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .hour("event_time")
                .identity("level")
                .build();

        table = catalog.createTable(name, schema, spec);
    }
}
