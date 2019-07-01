package org.graylog2.gelfclient.encoder;

import java.util.concurrent.atomic.AtomicLong;

public class SerializationTimeTracker {
    public static AtomicLong logsCount = new AtomicLong(0);
    public static AtomicLong timeSpent = new AtomicLong(0);
}
