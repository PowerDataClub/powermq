package top.powerdata.powermq.broker;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import static java.lang.String.format;
import static top.powerdata.powermq.common.utils.SystemUtils.updateConfig;

public class BrokerServerStarter {

    @VisibleForTesting
    @Parameters(commandDescription = "Options")
    private static class StarterArguments {
        @Parameter(names = {"-c", "--broker-conf"}, description = "Configuration file for Broker")
        private String brokerConfigFile = "conf/broker.conf";

        @Parameter(names = {"-h", "--help"}, description = "Show this help message")
        private boolean help = false;

    }
    public static void main(String[] args) throws Exception {
        StarterArguments starterArguments = new StarterArguments();
        JCommander jcommander = new JCommander(starterArguments);
        jcommander.setProgramName("PulsarBrokerStarter");

        // parse args by JCommander
        jcommander.parse(args);
        if (starterArguments.help) {
            jcommander.usage();
            System.exit(0);
        }

        BrokerConfig brokerConfig;
        // init broker config
        if (StringUtils.isBlank(starterArguments.brokerConfigFile)) {
            jcommander.usage();
            throw new IllegalArgumentException("Need to specify a configuration file for broker");
        } else {
            final String filepath = Paths.get(starterArguments.brokerConfigFile)
                    .toAbsolutePath().normalize().toString();
            brokerConfig = loadConfig(filepath);
        }

        PowerMqBrokerServer brokerServer = new PowerMqBrokerServer(brokerConfig);
        brokerServer.start();
    }

    private static BrokerConfig loadConfig(String filepath) throws Exception {
        try (InputStream inputStream = new FileInputStream(filepath)) {
            Properties properties = new Properties();
            properties.load(inputStream);
            BrokerConfig brokerConfig = new BrokerConfig();
            updateConfig((Map) properties, brokerConfig);
            return brokerConfig;
        }
    }
}
