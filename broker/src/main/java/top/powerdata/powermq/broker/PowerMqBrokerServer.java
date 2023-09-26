package top.powerdata.powermq.broker;

import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import lombok.extern.slf4j.Slf4j;
import top.powerdata.powermq.broker.service.BrokerGrpcService;
import top.powerdata.powermq.broker.service.MetadataService;
import top.powerdata.powermq.common.PowerMqService;
import top.powerdata.powermq.common.utils.DefaultThreadFactory;
import top.powerdata.powermq.common.utils.SystemUtils;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
public class PowerMqBrokerServer implements PowerMqService {
    private Server server;
    private NettyServerBuilder nettyServerBuilder;
    private EventLoopGroup nettyBoss;
    private EventLoopGroup nettyWorker;
    private BrokerConfig brokerConfig;
    private ThreadPoolExecutor nettyRequestExecutor;
    private MetadataService metadataService;

    public PowerMqBrokerServer(BrokerConfig proxyConfig) {
        this.brokerConfig = proxyConfig;
    }
    @Override
    public void start() throws Exception {
        boolean epoll = SystemUtils.useEpoll();
        final Class<? extends ServerChannel> channelType = epoll ? EpollServerSocketChannel.class
                : NioServerSocketChannel.class;
        if (epoll) {
            nettyBoss = new EpollEventLoopGroup(brokerConfig.getBossLoopThreadNums(), new DefaultThreadFactory("NettyEpollBoss_"));
            nettyWorker = new EpollEventLoopGroup(brokerConfig.getWorkerLoopThreadNums(), new DefaultThreadFactory("NettyEpollWorker_"));
        } else {
            nettyBoss = new NioEventLoopGroup(brokerConfig.getBossLoopThreadNums(), new DefaultThreadFactory("NettyNIOBoss_"));
            nettyWorker = new NioEventLoopGroup(brokerConfig.getWorkerLoopThreadNums(), new DefaultThreadFactory("NettyNIOWorker_"));
        }
        nettyRequestExecutor = new ThreadPoolExecutor(
                brokerConfig.getNettyRequestThreadCoreNums(),
                brokerConfig.getNettyRequestThreadMaxNums(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(brokerConfig.getNettyRequestQueueSizes()),
                new DefaultThreadFactory("NettyRequest_"));

        nettyServerBuilder = NettyServerBuilder
                .forPort(brokerConfig.getBrokerPort())
                .bossEventLoopGroup(nettyBoss)
                .workerEventLoopGroup(nettyWorker)
                .channelType(channelType)
                .executor(nettyRequestExecutor);

        BrokerGrpcService brokerServiceGrpc = new BrokerGrpcService();
        server = nettyServerBuilder
                .maxInboundMessageSize(128 << 20)
                .withOption(ChannelOption.SO_BACKLOG, 1024)
                .addService(brokerServiceGrpc)
                .build();
        server.start();
        metadataService = new MetadataService(brokerConfig);
        metadataService.start();

    }
    @Override
    public void close() throws Exception {

    }

}
