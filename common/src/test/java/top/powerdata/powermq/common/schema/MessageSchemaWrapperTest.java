package top.powerdata.powermq.common.schema;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.pulsar.client.api.Schema;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MessageSchemaWrapperTest {

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    static class Foo {
        int id;
        String name;
    }

    @Test
    public void testSchemaByte() {
        MessageSchemaWrapper<byte[]> schemaWrapper = new MessageSchemaWrapper<>();
        byte[] input = "test default".getBytes();
        byte[] res1 = schemaWrapper.encode(input);
        byte[] res2 = schemaWrapper.decode(res1);
        assertEquals(input, res2);
        assertEquals(1, schemaWrapper.getSchemaFields().size());
    }

    @Test
    public void testSchemaString() {
        MessageSchemaWrapper<String> schemaWrapper = new MessageSchemaWrapper<>(Schema.STRING);
        String input = "test default";
        byte[] res1 = schemaWrapper.encode(input);
        String res2 = schemaWrapper.decode(res1);
        assertEquals(input, res2);

        MessageSchemaWrapper<String> schemaString = MessageSchemaWrapper.STRING;
        byte[] res3 = schemaString.encode(input);
        String res4 = schemaString.decode(res3);
        assertEquals(input, res4);

        assertEquals(1, schemaWrapper.getSchemaFields().size());
    }

    @Test
    public void testSchemaObjectJson() {
        MessageSchema<Foo> encodeSchema = MessageSchemaWrapper.JSON(Foo.class);
        Foo input = new Foo(1, "Bod");
        byte[] res1 = encodeSchema.encode(input);
        assertEquals("{\"id\":1,\"name\":\"Bod\"}", new String(res1));
        Foo res2 = encodeSchema.decode(res1);
        assertEquals(input, res2);

        assertEquals(2, encodeSchema.getSchemaFields().size());
        assertEquals("id", encodeSchema.getSchemaFields().get(0).getFieldName());
    }

    @Test
    public void testSchemaObjectAvro() {
        MessageSchema<Foo> encodeSchema = MessageSchemaWrapper.AVRO(Foo.class);
        Foo input = new Foo(1, "Bod");
        byte[] res1 = encodeSchema.encode(input);
        Foo res2 = encodeSchema.decode(res1);
        assertEquals(input, res2);

        assertEquals(2, encodeSchema.getSchemaFields().size());
    }

}
