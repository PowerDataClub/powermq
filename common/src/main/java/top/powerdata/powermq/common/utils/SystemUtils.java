package top.powerdata.powermq.common.utils;

import io.netty.channel.epoll.Epoll;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.validator.routines.InetAddressValidator;
import org.apache.pulsar.common.util.FieldParser;
import top.powerdata.powermq.common.server.data.AbstractServerConfig;

import java.lang.reflect.Field;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.lang.String.format;

public class SystemUtils {
    public static boolean useEpoll() {
        String osName = System.getProperty("os.name");
        boolean isLinuxPlatform = false;
        if (osName != null && osName.toLowerCase().contains("linux")) {
            isLinuxPlatform = true;
        }
        return isLinuxPlatform && Epoll.isAvailable();
    }

    public static <T extends AbstractServerConfig> void updateConfig(Map<String, String> properties, T obj) throws IllegalArgumentException {
        List<Field> fields = new LinkedList<>();
        fields.addAll(Arrays.asList(obj.getClass().getDeclaredFields()));
        fields.addAll(Arrays.asList(obj.getClass().getSuperclass().getDeclaredFields()));
        fields.stream().forEach(f -> {
            if (properties.containsKey(f.getName())) {
                try {
                    f.setAccessible(true);
                    String v = properties.get(f.getName());
                    if (!StringUtils.isBlank(v)) {
                        f.set(obj, FieldParser.value(trim(v), f));
                    } else {
                        FieldParser.setEmptyValue(v, f, obj);
                    }
                } catch (Exception e) {
                    throw new IllegalArgumentException(format("failed to initialize %s field while setting value %s",
                            f.getName(), properties.get(f.getName())), e);
                }
            }
        });
    }

    private static String trim(String val) {
        Objects.requireNonNull(val);
        return val.trim();
    }


    public static String getIP() {
        try {
            Enumeration allNetInterfaces = NetworkInterface.getNetworkInterfaces();
            InetAddress ip = null;
            InetAddress internalIP = null;
            while (allNetInterfaces.hasMoreElements()) {
                NetworkInterface netInterface = (NetworkInterface) allNetInterfaces.nextElement();
                Enumeration addresses = netInterface.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    ip = (InetAddress) addresses.nextElement();
                    if (ip instanceof Inet4Address) {
                        byte[] ipByte = ip.getAddress();
                        if (ipByte.length == 4) {
                            if (!isInternalIP(ipByte)) {
                                return ip.getHostAddress();
                            } else if (internalIP == null || internalIP.getAddress()[0] == (byte) 127) {
                                internalIP = ip;
                            }
                        }
                    }
                }
            }
            if (internalIP != null) {
                return internalIP.getHostAddress();
            } else {
                throw new RuntimeException("Can not get local ip");
            }
        } catch (Exception e) {
            throw new RuntimeException("Can not get local ip", e);
        }
    }

    public static boolean isInternalIP(byte[] ip) {
        if (ip.length != 4) {
            throw new RuntimeException("illegal ipv4 bytes");
        }

        //10.0.0.0~10.255.255.255
        //172.16.0.0~172.31.255.255
        //192.168.0.0~192.168.255.255
        //127.0.0.0~127.255.255.255
        if (ip[0] == (byte) 10) {
            return true;
        } else if (ip[0] == (byte) 127) {
            return true;
        } else if (ip[0] == (byte) 172) {
            if (ip[1] >= (byte) 16 && ip[1] <= (byte) 31) {
                return true;
            }
        } else if (ip[0] == (byte) 192) {
            if (ip[1] == (byte) 168) {
                return true;
            }
        }
        return false;
    }

    private static boolean ipV6Check(byte[] ip) {
        if (ip.length != 16) {
            throw new RuntimeException("illegal ipv6 bytes");
        }

        InetAddressValidator validator = InetAddressValidator.getInstance();
        return validator.isValidInet6Address(ipToIPv6Str(ip));
    }

    public static String ipToIPv6Str(byte[] ip) {
        if (ip.length != 16) {
            return null;
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < ip.length; i++) {
            String hex = Integer.toHexString(ip[i] & 0xFF);
            if (hex.length() < 2) {
                sb.append(0);
            }
            sb.append(hex);
            if (i % 2 == 1 && i < ip.length - 1) {
                sb.append(":");
            }
        }
        return sb.toString();
    }

    public static boolean isInternalV6IP(InetAddress inetAddr) {
        if (inetAddr.isAnyLocalAddress() // Wild card ipv6
                || inetAddr.isLinkLocalAddress() // Single broadcast ipv6 address: fe80:xx:xx...
                || inetAddr.isLoopbackAddress() //Loopback ipv6 address
                || inetAddr.isSiteLocalAddress()) { // Site local ipv6 address: fec0:xx:xx...
            return true;
        }
        return false;
    }
}
