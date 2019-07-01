package org.graylog2.gelfclient.util;

import org.graylog2.gelfclient.GelfMessage;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * based on non-blocking ConcurrentLinkedQueue
 */
public class LoggingQueue extends ConcurrentLinkedQueue<GelfMessage> {

    private QueueMessageEventListener listener;

    public LoggingQueue() {
        super();
    }

    @Override
    public boolean add(GelfMessage gelfMessage) {
        boolean res =  super.add(gelfMessage);
        if (listener != null) { listener.messageReceived(); }
        return res;
    }

    @Override
    public boolean offer(GelfMessage gelfMessage) {
        boolean res = super.offer(gelfMessage);
        if (listener != null) { listener.messageReceived(); }
        return res;
    }

    public void setListener(QueueMessageEventListener listener) {
        this.listener = listener;
    }

    public interface QueueMessageEventListener {

        void messageReceived();
    }
}
