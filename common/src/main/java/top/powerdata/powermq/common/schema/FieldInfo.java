package top.powerdata.powermq.common.schema;

import lombok.Builder;
import lombok.Data;
import org.apache.pulsar.shade.org.apache.avro.Schema;

import java.util.List;

@Data
@Builder
public class FieldInfo {
    private String fieldName;
    private Schema.Type type;
    private List<Schema> types;
}
