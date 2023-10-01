package top.powerdata.powermq.broker.store;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import top.powerdata.powermq.common.SchemaMessage;
import top.powerdata.powermq.common.schema.MessageSchema;
import top.powerdata.powermq.common.schema.MessageSchemaWrapper;
import top.powerdata.powermq.common.server.data.PartitionData;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class IcebergMessageStoreTest {
    static String warehousePath = "file:///tmp/test_warehouse_path";

    @Data
    @AllArgsConstructor
    static class TestBean {
        private int id;
        private String name;
        private double point;
        private Map<String, Long> map1;
        private Map<Long, Long> map2;
        private Map<String, String> map3;
    }
    @Before
    public void setup() throws IOException {
        warehousePath = warehousePath + System.currentTimeMillis();
        removeDir();
    }

    @Test
    public void testIcebergCreate() throws Exception {
        Configuration conf = new Configuration();
        HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);

        PartitionData PartitionData = new PartitionData("test_tenant", "test_namespace", "test_topicname", 0);
        MessageSchema<TestBean> schema = MessageSchemaWrapper.JSON(TestBean.class);;

        IcebergMessageStore icebergMessageStore = new IcebergMessageStore(PartitionData, schema, catalog);
        icebergMessageStore.start();
        icebergMessageStore.close();
        // test load table again
        icebergMessageStore.start();
    }

    @Test
    public void testIcebergStore() throws Exception {
        Configuration conf = new Configuration();
        HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);

        PartitionData PartitionData = new PartitionData("test_tenant", "test_namespace", "test_topicname", 0);
        MessageSchema<TestBean> schema = MessageSchemaWrapper.JSON(TestBean.class);;

        IcebergMessageStore icebergMessageStore = new IcebergMessageStore(PartitionData, schema, catalog);
        icebergMessageStore.start();

        SchemaMessage<TestBean> message = new SchemaMessage<>("t", "t", "key",
                new TestBean(1, "Anna", 1.1, ImmutableMap.of("k", 999L), ImmutableMap.of(888L, 888L), ImmutableMap.of("k", "v")),
                schema);
        message.setEventTimestamp(System.currentTimeMillis());
        message.setPartitionId(0);
        SchemaMessage<TestBean> message2 = new SchemaMessage<>("t", "t", "key",
                new TestBean(2, "Bob", 1.3, ImmutableMap.of("k", 999L), ImmutableMap.of(888L, 888L), ImmutableMap.of("k", "v")),
                schema);
        message2.setEventTimestamp(System.currentTimeMillis());
        message2.setPartitionId(1);
        SchemaMessage<TestBean> message3 = new SchemaMessage<>("t", "t", "key",
                new TestBean(3, "Candy", 1.3, ImmutableMap.of("k", 999L), ImmutableMap.of(888L, 888L), ImmutableMap.of("k", "v")),
                schema);
        message3.setEventTimestamp(System.currentTimeMillis());
        message3.setPartitionId(2);
        for (int i = 0; i < 15000; i++) {
            icebergMessageStore.asyncAddMessage(message);
            icebergMessageStore.asyncAddMessage(message2);
            icebergMessageStore.asyncAddMessage(message3);
        }
        icebergMessageStore.flush();
        icebergMessageStore.asyncAddMessage(message);
        icebergMessageStore.asyncAddMessage(message2);
        icebergMessageStore.asyncAddMessage(message3);
        icebergMessageStore.flush();
    }

    @After
    public void clean() throws IOException {
        removeDir();
    }

    private static void removeDir() throws IOException {
        File path = new File(warehousePath.replace("file://", ""));
        if (path.exists()) {
            FileUtils.deleteDirectory(path);
        }
    }
}
