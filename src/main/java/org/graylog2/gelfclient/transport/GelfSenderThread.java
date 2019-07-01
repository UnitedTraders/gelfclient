/*
 * Copyright 2014 TORCH GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.graylog2.gelfclient.transport;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.graylog2.gelfclient.GelfMessage;
import org.graylog2.gelfclient.util.LoggingQueue;
import org.graylog2.gelfclient.util.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * The main event thread used by the {@link org.graylog2.gelfclient.transport.GelfTransport}s.
 */
public class GelfSenderThread {
    private static final Logger LOG = LoggerFactory.getLogger(GelfSenderThread.class);
    private final ReentrantLock lock;
    private final Condition connectedCond;
    private final AtomicBoolean keepRunning = new AtomicBoolean(true);
    private final Thread senderThread;
    private Channel channel;
    private final int maxInflightSends;

    /**
     * Creates a new sender thread with the given {@link java.util.concurrent.ConcurrentLinkedQueue} as source of messages.
     *
     * @param queue            the {@link java.util.concurrent.ConcurrentLinkedQueue} used as source of {@link GelfMessage}s
     * @param maxInflightSends the maximum number of outstanding network writes/flushes before the sender spins
     */
    public GelfSenderThread(final LoggingQueue queue, int maxInflightSends) {
        this.maxInflightSends = maxInflightSends;
        this.lock = new ReentrantLock();
        this.connectedCond = lock.newCondition();

        if (maxInflightSends <= 0) {
            throw new IllegalArgumentException("maxInflightSends must be larger than 0");
        }

        this.senderThread = new Thread(new Runnable() {
            @Override
            public void run() {
                GelfMessage gelfMessage = null;
                final AtomicInteger inflightSends = new AtomicInteger(0);
                final Semaphore semaphore = new Semaphore(0);
                final ChannelFutureListener inflightListener = new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        if (future.isSuccess()) {
                            //LOG.debug("message sent ");
                            inflightSends.decrementAndGet();
                        } else {
                            Throwable th = future.cause();
                            if (th != null) {
                                LOG.error("error sending log to graylog", th);
                                throw (Exception) th;
                            }
                        }
                    }


                };

                final LoggingQueue.QueueMessageEventListener messageListener = new LoggingQueue.QueueMessageEventListener() {
                    @Override
                    public void messageReceived() {
                        semaphore.drainPermits();
                        semaphore.release();
                    }
                };
                queue.setListener(messageListener);

                while (keepRunning.get()) {
                    // wait until we are connected to the graylog2 server before polling log events from the queue
                    lock.lock();
                    try {
                        while (channel == null || !channel.isActive()) {
                            try {
                                connectedCond.await();
                            } catch (InterruptedException e) {
                                if (!keepRunning.get()) {
                                    // bail out if we are awoken because the application is stopping
                                    break;
                                }
                            }
                        }
                        try {
                            // we are connected, let's start sending logs
                            // if we have a lingering event already, try to send that instead of polling a new one.
                            if (gelfMessage == null) {
                                if (queue.isEmpty() && semaphore.tryAcquire(10, SECONDS)) {
                                    // sleep until new GelfMessage come to queue
                                    gelfMessage = queue.poll();
                                } else {
                                    gelfMessage = queue.poll();
                                }

                                // if we are still connected, convert LoggingEvent to GELF and send it
                                // but if we aren't connected anymore, we'll have already pulled an event from the queue,
                                // which we keep hanging around in this thread and in the next loop iteration will block until we are connected again.
                                if (gelfMessage != null && channel != null && channel.isActive()) {
                                    // Do not allow more than "maxInflightSends" concurrent writes in netty, to avoid having netty buffer
                                    // excessively when faced with slower consumers
                                    while (inflightSends.get() > GelfSenderThread.this.maxInflightSends) {
                                        Uninterruptibles.sleepUninterruptibly(1, MICROSECONDS);
                                    }
                                    inflightSends.incrementAndGet();

                                    // Write the GELF message to the pipeline. The protocol specific channel handler
                                    // will take care of encoding.
                                    channel.writeAndFlush(gelfMessage).addListener(inflightListener);
                                    gelfMessage = null;
                                }
                            }
                        } catch (InterruptedException ex) {
                            // ignore, when stopping keepRunning will be set to false outside
                        }
                    } finally {
                        lock.unlock();
                    }
                }

                LOG.debug("GelfSenderThread exiting!");

            }
        });

        this.senderThread.setDaemon(true);
        this.senderThread.setName("GelfSenderThread-" + senderThread.getId());

    }

    public void start(Channel channel) {
        lock.lock();
        try {
            this.channel = channel;
            this.connectedCond.signalAll();
        } finally {
            lock.unlock();
        }
        senderThread.start();
    }

    public void stop() {
        keepRunning.set(false);
        senderThread.interrupt();
    }
}
