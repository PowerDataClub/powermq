package top.powerdata.powermq.common.server;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.Getter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.retry.RetryOneTime;
import org.apache.zookeeper.CreateMode;
import top.powerdata.powermq.common.PowerMqService;
import top.powerdata.powermq.common.server.data.ComponentCommonData;
import top.powerdata.powermq.common.server.data.AbstractServerConfig;
import top.powerdata.powermq.common.utils.SystemUtils;

public abstract class SelfRegisterService implements PowerMqService {
    @Getter
    private final Gson gson = new GsonBuilder().create();
    @Getter
    private final AbstractServerConfig config;
    @Getter
    private ComponentCommonData data;
    @Getter
    private CuratorFramework curatorFramework;
    private PersistentNode registerNode;
    @Getter
    private String currentIp;

    public SelfRegisterService(AbstractServerConfig config) {
        this.config = config;
    }

    @Override
    public void start() throws Exception {
        currentIp = SystemUtils.getIP();
        data = getComponentData();
        curatorFramework = CuratorFrameworkFactory.builder()
                .connectString(config.getMetadataUrl())
                .connectionTimeoutMs(5000)
                .sessionTimeoutMs(5000)
                .retryPolicy(new RetryOneTime(1000))
                .build();
        curatorFramework.start();
        registerNode();
    }

    public abstract ComponentCommonData getComponentData();

    private void registerNode() throws Exception {
        if (curatorFramework.checkExists().forPath(getRegisterPath()) == null) {
            curatorFramework.create()
                    .creatingParentsIfNeeded()
                    .forPath(getRegisterPath());
        }
        String brokerPath =  getRegisterPath() + "/" + currentIp + "@" + config.getServerPort() + "@";
        registerNode = new PersistentNode(curatorFramework, CreateMode.EPHEMERAL_SEQUENTIAL, true, brokerPath, gson.toJson(data).getBytes());
        registerNode.start();
    }
    
    public abstract String getRegisterPath();

    @Override
    public void close() throws Exception {
        if (registerNode != null) {
            registerNode.close();
        }
        if (curatorFramework != null) {
            curatorFramework.close();
        }
    }
}
