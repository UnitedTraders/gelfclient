package org.graylog2.gelfclient.util;

import org.graylog2.gelfclient.GelfMessage;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Based on non-blocking ConcurrentLinkedQueue
 */
public class LogMessageQueue extends ConcurrentLinkedQueue<GelfMessage> {

    private QueueMessageEventListener listener;
    private AtomicLong queueSize = new AtomicLong(0L);

    public LogMessageQueue() {
        super();
    }

    @Override
    public boolean add(GelfMessage gelfMessage) {
        boolean added =  super.add(gelfMessage);
        if (added) {
            queueSize.getAndIncrement();
        }
        if (listener != null) { listener.messageReceived(); }
        return added;
    }

    @Override
    public boolean offer(GelfMessage gelfMessage) {
        boolean added = super.offer(gelfMessage);
        if (added) {
            queueSize.getAndIncrement();
        }
        if (listener != null) { listener.messageReceived(); }
        return added;
    }

    @Override
    public GelfMessage poll() {
        GelfMessage msg = super.poll();
        queueSize.getAndDecrement();
        return msg;
    }

    @Override
    public int size() {
        // parent's size() method computes size with linear complexity, so we will use atomic counter instead
        return (int) queueSize.get();
    }

    @Override
    public boolean isEmpty() { return this.size() == 0; }

    public void setListener(QueueMessageEventListener listener) {
        this.listener = listener;
    }

    public interface QueueMessageEventListener {

        void messageReceived();
    }
}
