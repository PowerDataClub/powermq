package top.powerdata.powermq.common.data;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class BrokerData {
    private long startTimestamp;
    private String ip;
    private int servicePort;

}
