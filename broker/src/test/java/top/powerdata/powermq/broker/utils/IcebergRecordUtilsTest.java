package top.powerdata.powermq.broker.utils;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.junit.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import top.powerdata.powermq.common.SchemaMessage;
import top.powerdata.powermq.common.schema.MessageSchema;
import top.powerdata.powermq.common.schema.MessageSchemaWrapper;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static top.powerdata.powermq.common.schema.MessageSchemaWrapper.message2map;

public class IcebergRecordUtilsTest {

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TestBean {
        private int id;
        private String name;
        private double point;
        private Map<String, Long> map1;
        private Map<Long, Long> map2;
        private Map<String, String> map3;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TestBean testBean = (TestBean) o;
            return id == testBean.id && Double.compare(point, testBean.point) == 0 && Objects.equals(name, testBean.name) && Objects.equals(map1, testBean.map1) && Objects.equals(map2, testBean.map2) && Objects.equals(map3, testBean.map3);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, name, point, map1, map2, map3);
        }
    }

    @Test
    public void testMap2record() {
        MessageSchema<TestBean> messageSchema = MessageSchemaWrapper.JSON(TestBean.class);
        SchemaMessage<TestBean> message = new SchemaMessage<>("t", "t", "key",
                new TestBean(2, "Bob", 1.3, ImmutableMap.of("k", 999L), ImmutableMap.of(888L, 888L), ImmutableMap.of("k", "v")),
                messageSchema);
        Map<String, Object> mapMessage = message2map(message);
        SchemaMessage<TestBean> message2 = MessageSchemaWrapper.map2message(mapMessage, messageSchema, TestBean.class);
        assertEquals(message, message2);
    }

    @Test
    public void testRecord2Message() {
        MessageSchema<TestBean> messageSchema = MessageSchemaWrapper.JSON(TestBean.class);
        SchemaMessage<TestBean> message = new SchemaMessage<>("t", "t", "key",
                new TestBean(2, "Bob", 1.3, ImmutableMap.of("k", 999L), ImmutableMap.of(888L, 888L), ImmutableMap.of("k", "v")),
                messageSchema);
        Schema icbergSchema = IcebergRecordUtils.buildLakeSchema(messageSchema);
        Record record = IcebergRecordUtils.message2record(message, icbergSchema, new HashMap<>(), new HashMap<>());
        Map instance = IcebergRecordUtils.record2message(record, icbergSchema);
        assertEquals(message2map(message).get("map1"), instance.get("map1"));
        assertEquals(message2map(message).get("map2"), instance.get("map2"));
        assertEquals(message2map(message).get("map3"), instance.get("map3"));
        assertEquals(message2map(message).get("id"), instance.get("id"));
        assertEquals(message2map(message).get("name"), instance.get("name"));
    }
}
