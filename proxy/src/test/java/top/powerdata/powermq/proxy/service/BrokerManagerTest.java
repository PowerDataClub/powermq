package top.powerdata.powermq.proxy.service;

import org.junit.Test;
import top.powerdata.powermq.broker.BrokerConfig;
import top.powerdata.powermq.broker.service.BrokerMetadataService;
import top.powerdata.powermq.common.server.data.BrokerData;
import top.powerdata.powermq.proxy.ProxyConfig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BrokerManagerTest extends AbstractZookeeperServerTest{

    @Test
    public void testBrokerManagerLoad() throws Exception {
        BrokerManager brokerManager = new BrokerManager();
        ProxyConfig config = new ProxyConfig();
        config.setMetadataUrl(getZkAddress());
        ProxyMetadataService proxyMetadataService = new ProxyMetadataService(config, brokerManager);
        proxyMetadataService.start();
        assertEquals(0, brokerManager.getAliveBrokers().size());

        BrokerConfig brokerConfig1 = new BrokerConfig();
        brokerConfig1.setMetadataUrl(getZkAddress());
        brokerConfig1.setServerPort(10001);
        BrokerMetadataService brokerMetadataService1 = new BrokerMetadataService(brokerConfig1);
        brokerMetadataService1.start();
        assertTrue(waitToSuccess(30, 100, 1, brokerManager));
        for (BrokerData value : brokerManager.getAliveBrokers().values()) {
            assertEquals(10001, value.getServicePort());
        }

        BrokerConfig brokerConfig2 = new BrokerConfig();
        brokerConfig2.setMetadataUrl(getZkAddress());
        brokerConfig2.setServerPort(10002);
        BrokerMetadataService brokerMetadataService2 = new BrokerMetadataService(brokerConfig2);
        brokerMetadataService2.start();
        assertTrue(waitToSuccess(30, 100, 2, brokerManager));

        brokerMetadataService1.close();
        assertTrue(waitToSuccess(30, 100, 1, brokerManager));

        brokerMetadataService2.close();
        assertTrue(waitToSuccess(30, 100, 0, brokerManager));
    }

    private static boolean waitToSuccess(int maxRetryTime, long sleepTimeMs, int expectedResult, BrokerManager brokerManager) throws InterruptedException {
        for (int i = 0; i < maxRetryTime; i++) {
            Thread.sleep(sleepTimeMs);
            if (expectedResult == brokerManager.getAliveBrokers().size()) {
                return true;
            }
        }
        return false;
    }
}
