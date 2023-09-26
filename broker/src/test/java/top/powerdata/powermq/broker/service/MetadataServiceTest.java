package top.powerdata.powermq.broker.service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;
import top.powerdata.powermq.broker.BrokerConfig;
import top.powerdata.powermq.broker.Consts;
import top.powerdata.powermq.common.data.BrokerData;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Slf4j
public class MetadataServiceTest extends AbstractZookeeperServerTest{
    @Test
    public void testBrokerRegister () throws Exception {
        CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
                .connectString(getZkAddress())
                .retryPolicy(new RetryOneTime(1000))
                .build();
        curatorFramework.start();

        BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setMetadataUrl(getZkAddress());
        MetadataService metadataService = new MetadataService(brokerConfig);
        metadataService.start();
        ZooKeeper zooKeeperClient = curatorFramework.getZookeeperClient().getZooKeeper();
        assertEquals(1, zooKeeperClient.getChildren(Consts.BROKER_REGISTER_PATH, false).size());
        String node = zooKeeperClient.getChildren(Consts.BROKER_REGISTER_PATH, false).get(0);
        String nodeData = new String(curatorFramework.getData().forPath(Consts.BROKER_REGISTER_PATH + "/" + node));
        Gson gson = new GsonBuilder().create();
        BrokerData brokerData = gson.fromJson(nodeData, BrokerData.class);
        assertEquals(9010, brokerData.getServicePort());
        assertTrue(node.contains(brokerData.getIp()));
        curatorFramework.getZookeeperClient().getZooKeeper().getChildren(Consts.BROKER_REGISTER_PATH, false);

        metadataService.close();
        assertTrue(zooKeeperClient.getChildren(Consts.BROKER_REGISTER_PATH, false).isEmpty());

    }
}
