package top.powerdata.powermq.broker.service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.retry.RetryOneTime;
import org.apache.zookeeper.CreateMode;
import top.powerdata.powermq.broker.BrokerConfig;
import top.powerdata.powermq.broker.Consts;
import top.powerdata.powermq.common.PowerMqService;
import top.powerdata.powermq.common.data.BrokerData;
import top.powerdata.powermq.common.utils.SystemUtils;

public class MetadataService implements PowerMqService {
    private final Gson gson = new GsonBuilder().create();
    private BrokerConfig brokerConfig;
    private BrokerData currentBrokerData;
    private CuratorFramework curatorFramework;
    private PersistentNode brokerRegisterNode;
    private String currentIp;

    public MetadataService(BrokerConfig brokerConfig) {
        this.brokerConfig = brokerConfig;
    }

    @Override
    public void start() throws Exception {
        currentIp = SystemUtils.getIP();
        currentBrokerData = BrokerData.builder()
                .ip(currentIp)
                .startTimestamp(System.currentTimeMillis())
                .servicePort(brokerConfig.getBrokerPort())
                .build();
        curatorFramework = CuratorFrameworkFactory.builder()
                .connectString(brokerConfig.getMetadataUrl())
                .retryPolicy(new RetryOneTime(1000))
                .build();
        curatorFramework.start();
        registerNode();
    }

    private void registerNode() throws Exception {
        curatorFramework.create()
                .creatingParentsIfNeeded()
                .forPath(Consts.BROKER_REGISTER_PATH);
        String brokerPath =  Consts.BROKER_REGISTER_PATH + "/" + currentIp + "-" + brokerConfig.getBrokerPort();
        brokerRegisterNode = new PersistentNode(curatorFramework, CreateMode.EPHEMERAL, true, brokerPath, gson.toJson(currentBrokerData).getBytes());
        brokerRegisterNode.start();
    }

    @Override
    public void close() throws Exception {
        if (curatorFramework != null) {
            curatorFramework.close();
        }
    }
}
