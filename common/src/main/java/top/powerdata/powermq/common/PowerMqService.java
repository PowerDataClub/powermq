package top.powerdata.powermq.common;

import java.io.IOException;

public interface PowerMqService extends AutoCloseable{
    void start() throws Exception;
    default void shutDownNow() throws Exception {
        close();
    };
}
