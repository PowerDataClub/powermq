package top.powerdata.powermq.proxy;

import lombok.Data;
import top.powerdata.powermq.common.server.data.AbstractServerConfig;

@Data
public class ProxyConfig extends AbstractServerConfig {
    private int serverPort = 9011;
}
