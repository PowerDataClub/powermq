package top.powerdata.powermq.proxy;

public class ProxyServerStarter {

    public static void main(String[] args) throws Exception {
        ProxyConfig proxyConfig = new ProxyConfig();
        PowerMqProxyServer proxyServer = new PowerMqProxyServer(proxyConfig);
        proxyServer.start();
    }
}
