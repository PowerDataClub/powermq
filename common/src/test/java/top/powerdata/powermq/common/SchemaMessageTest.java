package top.powerdata.powermq.common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.Test;
import top.powerdata.powermq.common.schema.MessageSchema;
import top.powerdata.powermq.common.schema.MessageSchemaWrapper;

import static org.junit.Assert.assertEquals;

public class SchemaMessageTest {

    @NoArgsConstructor
    @AllArgsConstructor
    @Data
    static class Foo {
        int id;
        String name;
    }
    @Test
    public void testSchemaMessageAvro() throws Exception {
        MessageSchema<Foo> encodeSchema = MessageSchemaWrapper.AVRO(Foo.class);
        Foo input = new Foo(1, "Bob");
        SchemaMessage<Foo> schemaMessage = new SchemaMessage<>("test", "tag", "key", input, encodeSchema);
        assertEquals(1, schemaMessage.getField("id"));
        assertEquals("Bob", schemaMessage.getField("name"));
        byte[] rawBodys = schemaMessage.getBody();
        SchemaMessage<Foo> schemaMessage2 = new SchemaMessage<>("test", "tag", "key", rawBodys, encodeSchema);
        assertEquals(1, schemaMessage2.getField("id"));
        assertEquals("Bob", schemaMessage2.getField("name"));
    }

    @Test
    public void testSchemaMessageString() throws Exception {
        SchemaMessage<String> schemaMessage = new SchemaMessage<>("test", "tag", "key", "input", MessageSchema.STRING);
        byte[] rawBodys = schemaMessage.getBody();
        SchemaMessage<String> schemaMessage2 = new SchemaMessage<>("test", "tag", "key", rawBodys, MessageSchema.STRING);
        assertEquals("input", schemaMessage2.getMsgWithSchema());
    }
}
