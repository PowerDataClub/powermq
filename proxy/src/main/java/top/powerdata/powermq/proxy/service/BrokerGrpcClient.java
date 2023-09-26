package top.powerdata.powermq.proxy.service;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import top.powerdata.powermq.common.PowerMqService;
import top.powerdata.powermq.proxy.ProxyConfig;

public class BrokerGrpcClient implements PowerMqService {
    private ProxyConfig proxyConfig;

    public BrokerGrpcClient(ProxyConfig proxyConfig) {
        this.proxyConfig = proxyConfig;
    }

    @Override
    public void start() throws Exception {
        ManagedChannel brokerChannel = ManagedChannelBuilder
                .forAddress(proxyConfig.getBrokerAddresses(), proxyConfig.getBrokerPort())
                .usePlaintext()
                .maxInboundMessageSize(128 << 20)
                .build();
    }

    @Override
    public void close() throws Exception {

    }
}
