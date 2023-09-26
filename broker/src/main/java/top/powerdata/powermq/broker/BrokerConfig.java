package top.powerdata.powermq.broker;

import lombok.Data;

@Data
public class BrokerConfig {
    private String metadataUrl;
    private int brokerPort = 9010;
    private int nettyRequestThreadCoreNums = 16;
    private int nettyRequestThreadMaxNums = 16;
    private int nettyRequestQueueSizes = 16;
    private int bossLoopThreadNums = 1;
    private int workerLoopThreadNums = 16;
}
