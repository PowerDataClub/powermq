package top.powerdata.powermq.proxy;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;

import static top.powerdata.powermq.common.utils.SystemUtils.updateConfig;

@Slf4j
public class ProxyServerStarter {


    @VisibleForTesting
    @Parameters(commandDescription = "Options")
    private static class StarterArguments {
        @Parameter(names = {"-c", "--proxy-conf"}, description = "Configuration file for Proxy")
        private String proxyConfigFile = "conf/proxy.conf";

        @Parameter(names = {"-h", "--help"}, description = "Show this help message")
        private boolean help = false;

    }
    public static void main(String[] args) throws Exception {
        log.info("Proxy server start");
        StarterArguments starterArguments = new StarterArguments();
        JCommander jcommander = new JCommander(starterArguments);
        jcommander.setProgramName("PulsarProxyStarter");

        // parse args by JCommander
        jcommander.parse(args);
        if (starterArguments.help) {
            jcommander.usage();
            System.exit(0);
        }

        ProxyConfig proxyConfig;
        // init Proxy config
        if (StringUtils.isBlank(starterArguments.proxyConfigFile)) {
            jcommander.usage();
            throw new IllegalArgumentException("Need to specify a configuration file for proxy");
        } else {
            final String filepath = Paths.get(starterArguments.proxyConfigFile)
                    .toAbsolutePath().normalize().toString();
            proxyConfig = loadConfig(filepath);
        }

        PowerMqProxyServer proxyServer = new PowerMqProxyServer(proxyConfig);
        proxyServer.start();
    }

    private static ProxyConfig loadConfig(String filepath) throws Exception {
        try (InputStream inputStream = new FileInputStream(filepath)) {
            Properties properties = new Properties();
            properties.load(inputStream);
            ProxyConfig proxyConfig = new ProxyConfig();
            updateConfig((Map) properties, proxyConfig);
            return proxyConfig;
        }
    }
}
