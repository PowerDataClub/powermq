package top.powerdata.powermq.proxy.service;

import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
@Slf4j
public abstract class AbstractZookeeperServerTest {
    private final static int ZOOKEEPER_PORT = 2181;
    private String zkAddress;
    private boolean dockerRunning;
    @Container
    private GenericContainer<?> zookeeperServer;


    @Before
    public void setup() {
        zookeeperServer = new GenericContainer<>("zookeeper:3.9.0")
                .withExposedPorts(ZOOKEEPER_PORT);
        dockerRunning = DockerClientFactory.instance().isDockerAvailable();
        if (!dockerRunning) {
            log.warn("docker is not available, skip unit test that depend on docker");
            return;
        }
        zookeeperServer.start();
        zkAddress = String.format("%s:%d", zookeeperServer.getHost(), zookeeperServer.getMappedPort(ZOOKEEPER_PORT));
    }

    @After
    public void close() {
        if (!dockerRunning) {
            return;
        }
        zookeeperServer.close();
    }

    public String getZkAddress() {
        return zkAddress;
    }

    public boolean isDockerRunning() {
        return dockerRunning;
    }
}
