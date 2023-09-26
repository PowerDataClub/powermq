package top.powerdata.powermq.proxy;

import lombok.Data;

@Data
public class ProxyConfig {
    private int proxyPort = 9010;
    private int nettyRequestThreadCoreNums = 16;
    private int nettyRequestThreadMaxNums = 16;
    private int nettyRequestQueueSizes = 16;
    private int bossLoopThreadNums = 1;
    private int workerLoopThreadNums = 16;

    private String brokerAddresses;
    private int brokerPort = 9011;
}
