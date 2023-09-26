package top.powerdata.powermq.proxy.service;

import lombok.Getter;
import top.powerdata.powermq.common.PowerMqService;
import top.powerdata.powermq.common.server.data.BrokerData;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class BrokerManager implements PowerMqService {

    @Getter
    private final Map<String, BrokerData> aliveBrokers;
    public BrokerManager() {
        aliveBrokers = new ConcurrentHashMap<>();
    }
    @Override
    public void start() throws Exception {

    }

    @Override
    public void close() throws Exception {

    }

    public void addBrokerNode(String nodePath, BrokerData brokerData) {
        aliveBrokers.put(nodePath, brokerData);
    }

    public BrokerData removeBrokerNode(String nodePath) {
        return aliveBrokers.remove(nodePath);
    }

    public void updateBrokerNode(String nodePath, BrokerData nodeData) {
        aliveBrokers.put(nodePath, nodeData);
    }
}
