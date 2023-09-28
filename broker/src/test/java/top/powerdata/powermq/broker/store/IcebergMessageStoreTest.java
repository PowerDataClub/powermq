package top.powerdata.powermq.broker.store;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.junit.Test;
import top.powerdata.powermq.common.schema.MessageSchema;
import top.powerdata.powermq.common.server.data.PartitionData;
import top.powerdata.powermq.common.server.data.TopicData;

public class IcebergMessageStoreTest {

    @Test
    public void testIcebergStore() throws Exception {
        Configuration conf = new Configuration();
        String warehousePath = "file:///warehouse_path";
        HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);

        TopicData topicData = new TopicData();
        PartitionData partition = new PartitionData();
        MessageSchema schema = null;

        IcebergMessageStore icebergMessageStore = new IcebergMessageStore(topicData, partition, schema, catalog);
        icebergMessageStore.start();
    }
}
