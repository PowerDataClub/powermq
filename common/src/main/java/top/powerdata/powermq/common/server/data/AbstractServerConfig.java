package top.powerdata.powermq.common.server.data;


import lombok.Data;

@Data
public abstract class AbstractServerConfig {
    private String metadataUrl;
    // netty config
    private int nettyRequestThreadCoreNums = 16;
    private int nettyRequestThreadMaxNums = 16;
    private int nettyRequestQueueSizes = 16;
    private int bossLoopThreadNums = 1;
    private int workerLoopThreadNums = 16;
    private int serverPort;
}
