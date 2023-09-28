package top.powerdata.powermq.broker.service;

import top.powerdata.powermq.broker.BrokerConfig;
import top.powerdata.powermq.common.Consts;
import top.powerdata.powermq.common.server.SelfRegisterService;
import top.powerdata.powermq.common.server.data.BrokerData;
import top.powerdata.powermq.common.server.data.ComponentCommonData;

public class BrokerMetadataService extends SelfRegisterService {
    public BrokerMetadataService(BrokerConfig config) {
        super(config);
    }

    public BrokerConfig getBrokerConfig() {
        return (BrokerConfig) getConfig();
    }

    @Override
    public ComponentCommonData getComponentData() {
        return BrokerData.builder()
                .ip(getCurrentIp())
                .startTimestamp(System.currentTimeMillis())
                .servicePort(getBrokerConfig().getServerPort())
                .build();
    }

    @Override
    public String getRegisterPath() {
        return Consts.BROKER_REGISTER_PATH;
    }
}
