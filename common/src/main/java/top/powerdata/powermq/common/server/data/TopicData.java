package top.powerdata.powermq.common.server.data;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TopicData {
    private String tenant;
    private String namespace;
    private String topicName;
}
