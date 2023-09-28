package top.powerdata.powermq.common;

import java.io.Serializable;
import java.util.Map;

public class Message implements Serializable {
    private String topic;
    private int partition;
    private long flag;
    private Map<String, String> properties;
    private byte[] body;
}
