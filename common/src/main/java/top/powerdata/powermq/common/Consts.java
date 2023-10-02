package top.powerdata.powermq.common;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

public class Consts {

    final public static String BROKER_REGISTER_PATH = "/powermq/broker/nodes";
    final public static String PROXY_REGISTER_PATH = "/powermq/proxy/nodes";
    final public static String DEFAULT_FIELD_OFFSET_ID = "__offset_id";
    final public static String DEFAULT_FIELD_EVENT_TIME = "__event_time";
    final public static String DEFAULT_FIELD_PARTITION_ID = "__partition_id";
    final public static String DEFAULT_FIELD_TAG = "__tag";
    final public static String DEFAULT_FIELD_KEY = "__key";

    final static public String ONLY_ONE_FIELD_NAME = "__";
    final static public String __SYSTEM_PROPERTIES = "__SYSTEM_PROPERTIES";
    final static public String __TOPIC = "__TOPIC";
    final static public Set<String> DEFAULT_SYSTEM_FIELDS_KEY = ImmutableSet.of(
            DEFAULT_FIELD_OFFSET_ID, DEFAULT_FIELD_EVENT_TIME, DEFAULT_FIELD_PARTITION_ID, DEFAULT_FIELD_TAG, DEFAULT_FIELD_KEY, __TOPIC
    );
}
