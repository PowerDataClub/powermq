package top.powerdata.powermq.proxy.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import top.powerdata.powermq.common.Consts;
import top.powerdata.powermq.common.server.SelfRegisterService;
import top.powerdata.powermq.common.server.data.BrokerData;
import top.powerdata.powermq.common.server.data.ComponentCommonData;
import top.powerdata.powermq.common.server.data.ProxyData;
import top.powerdata.powermq.proxy.ProxyConfig;

@Slf4j
public class ProxyMetadataService extends SelfRegisterService {

    private BrokerManager brokerManager;
    public ProxyMetadataService(ProxyConfig config, BrokerManager brokerManager) {
        super(config);
        this.brokerManager = brokerManager;
    }

    @Override
    public void start() throws Exception {
        super.start();
        watchBrokerNode();
    }

    private void watchBrokerNode() throws Exception {
        ZooKeeper zooKeeperClient = getCuratorFramework().getZookeeperClient().getZooKeeper();
        zooKeeperClient.addWatch(Consts.BROKER_REGISTER_PATH, (Watcher) event -> {
            if (Consts.BROKER_REGISTER_PATH.equals(event.getPath()) && event.getType() == Watcher.Event.EventType.NodeCreated) {
                return;
            }
            String nodePath = event.getPath();
            BrokerData nodeData = null;
            try {
                switch (event.getType()) {
                    case NodeCreated:
                        nodeData = getGson().fromJson(new String(zooKeeperClient.getData(nodePath, false, null)), BrokerData.class);
                        log.info("add broker node " + nodeData);
                        brokerManager.addBrokerNode(nodePath, nodeData);
                        break;
                    case NodeDeleted:
                        nodeData = brokerManager.removeBrokerNode(nodePath);
                        log.info("delete broker node " + nodeData);
                        break;
                    case NodeDataChanged:
                        nodeData = getGson().fromJson(new String(zooKeeperClient.getData(nodePath, false, null)), BrokerData.class);
                        brokerManager.updateBrokerNode(nodePath, nodeData);
                        break;
                    case DataWatchRemoved:
                    case ChildWatchRemoved:
                    case NodeChildrenChanged:
                    case PersistentWatchRemoved:
                    case None:
                        log.warn("no support zk watcher event, skip " + event);
                        break;
                }
            } catch (Exception e) {
                log.error("zk watcher failed", e);
                throw new RuntimeException(e);
            }
        }, AddWatchMode.PERSISTENT_RECURSIVE);
    }

        @Override
    public ComponentCommonData getComponentData() {
        return ProxyData.builder()
                .ip(getCurrentIp())
                .startTimestamp(System.currentTimeMillis())
                .servicePort(getProxyConfig().getServerPort())
                .build();
    }

    private ProxyConfig getProxyConfig() {
        return (ProxyConfig) getConfig();
    }

    @Override
    public String getRegisterPath() {
        return Consts.PROXY_REGISTER_PATH;
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
