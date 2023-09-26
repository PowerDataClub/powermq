package top.powerdata.powermq.proxy;

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
import top.powerdata.powermq.common.PowerMqService;
import top.powerdata.powermq.common.utils.DefaultThreadFactory;
import top.powerdata.powermq.common.utils.SystemUtils;
import top.powerdata.powermq.proxy.service.BrokerManager;
import top.powerdata.powermq.proxy.service.ProxyGrpcService;
import top.powerdata.powermq.proxy.service.ProxyMetadataService;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
public class PowerMqProxyServer implements PowerMqService {
    private Server server;
    private NettyServerBuilder nettyServerBuilder;
    private EventLoopGroup nettyBoss;
    private EventLoopGroup nettyWorker;
    private ProxyConfig proxyConfig;
    private ThreadPoolExecutor nettyRequestExecutor;
    private ProxyMetadataService proxyMetadataService;
    private BrokerManager brokerManager;

    public PowerMqProxyServer(ProxyConfig proxyConfig) {
        this.proxyConfig = proxyConfig;
    }
    @Override
    public void start() throws Exception {
        boolean epoll = SystemUtils.useEpoll();
        final Class<? extends ServerChannel> channelType = epoll ? EpollServerSocketChannel.class
                : NioServerSocketChannel.class;
        if (epoll) {
            nettyBoss = new EpollEventLoopGroup(proxyConfig.getBossLoopThreadNums(), new DefaultThreadFactory("NettyEpollBoss_"));
            nettyWorker = new EpollEventLoopGroup(proxyConfig.getWorkerLoopThreadNums(), new DefaultThreadFactory("NettyEpollWorker_"));
        } else {
            nettyBoss = new NioEventLoopGroup(proxyConfig.getBossLoopThreadNums(), new DefaultThreadFactory("NettyNIOBoss_"));
            nettyWorker = new NioEventLoopGroup(proxyConfig.getWorkerLoopThreadNums(), new DefaultThreadFactory("NettyNIOWorker_"));
        }
        nettyRequestExecutor = new ThreadPoolExecutor(
                proxyConfig.getNettyRequestThreadCoreNums(),
                proxyConfig.getNettyRequestThreadMaxNums(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(proxyConfig.getNettyRequestQueueSizes()),
                new DefaultThreadFactory("NettyRequest_"));

        nettyServerBuilder = NettyServerBuilder
                .forPort(proxyConfig.getServerPort())
                .bossEventLoopGroup(nettyBoss)
                .workerEventLoopGroup(nettyWorker)
                .channelType(channelType)
                .executor(nettyRequestExecutor);

        ProxyGrpcService proxyGrpcService = new ProxyGrpcService();
        server = nettyServerBuilder
                .maxInboundMessageSize(128 << 20)
                .withOption(ChannelOption.SO_BACKLOG, 1024)
                .addService(proxyGrpcService)
                .build();
        server.start();

        brokerManager = new BrokerManager();
        proxyMetadataService = new ProxyMetadataService(proxyConfig, brokerManager);
        proxyMetadataService.start();
    }
    @Override
    public void close() throws Exception {

    }

}
