package top.powerdata.powermq.broker;

import lombok.Data;
import lombok.EqualsAndHashCode;
import top.powerdata.powermq.common.server.data.AbstractServerConfig;

@EqualsAndHashCode(callSuper = true)
@Data
public class BrokerConfig extends AbstractServerConfig {
    private int serverPort = 9010;
}
