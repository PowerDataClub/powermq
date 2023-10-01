package top.powerdata.powermq.broker.service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Assert;
import org.junit.Test;
import top.powerdata.powermq.broker.BrokerConfig;
import top.powerdata.powermq.common.Consts;
import top.powerdata.powermq.common.server.data.BrokerData;


@Slf4j
public class BrokerMetadataServiceTest extends AbstractZookeeperServerTest{
    @Test
    public void testBrokerRegister () throws Exception {
        if (!isDockerRunning()) {
            return;
        }
        CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
                .connectString(getZkAddress())
                .retryPolicy(new RetryOneTime(1000))
                .build();
        curatorFramework.start();

        BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setMetadataUrl(getZkAddress());
        BrokerMetadataService brokerMetadataService = new BrokerMetadataService(brokerConfig);
        brokerMetadataService.start();
        ZooKeeper zooKeeperClient = curatorFramework.getZookeeperClient().getZooKeeper();
        Assert.assertEquals(1, zooKeeperClient.getChildren(Consts.BROKER_REGISTER_PATH, false).size());
        String node = zooKeeperClient.getChildren(Consts.BROKER_REGISTER_PATH, false).get(0);
        String nodeData = new String(curatorFramework.getData().forPath(Consts.BROKER_REGISTER_PATH + "/" + node));
        Gson gson = new GsonBuilder().create();
        BrokerData brokerData = gson.fromJson(nodeData, BrokerData.class);
        Assert.assertEquals(9010, brokerData.getServicePort());
        Assert.assertTrue(node.contains(brokerData.getIp()));
        curatorFramework.getZookeeperClient().getZooKeeper().getChildren(Consts.BROKER_REGISTER_PATH, false);

        brokerMetadataService.close();
        Assert.assertTrue(zooKeeperClient.getChildren(Consts.BROKER_REGISTER_PATH, false).isEmpty());

    }
}
