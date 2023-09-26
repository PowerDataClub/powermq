package top.powerdata.powermq.common.server.data;

import lombok.Data;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@Data
public abstract class ComponentCommonData {
    private long startTimestamp;
    private String ip;
    private int servicePort;
}
