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
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.graylog2.gelfclient.GelfConfiguration;
import org.graylog2.gelfclient.GelfMessage;
import org.graylog2.gelfclient.util.LogMessageQueue;
import org.graylog2.gelfclient.util.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

/**
 * An abstract {@link GelfTransport} implementation serving as parent for the concrete implementations.
 * <p>This class is thread-safe.</p>
 */
public abstract class AbstractGelfTransport implements GelfTransport {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractGelfTransport.class);

    protected final GelfConfiguration config;
    private final LogMessageQueue queue;
    private final EventLoopGroup workerGroup;

    protected volatile Channel channel = null;
    private final ScheduledExecutorService queueProcessor;
    private final AtomicInteger inflightSends = new AtomicInteger(0);
    private final ChannelFutureListener inflightListener;

    /**
     * Creates a new GELF transport with the given configuration and queue.
     *
     * @param config the client configuration
     */
    public AbstractGelfTransport(final GelfConfiguration config) {
        this.config = config;
        this.workerGroup = new NioEventLoopGroup(config.getThreads(), new DefaultThreadFactory(getClass(), true));
        this.queue = new LogMessageQueue();
        this.queueProcessor = Executors.newSingleThreadScheduledExecutor();
        this.queueProcessor.scheduleAtFixedRate(this::processQueue, 5, config.getQueueProcessRateInSec(), TimeUnit.SECONDS);
        this.inflightListener = this::handleChannelCallback;
        createBootstrap(workerGroup);
    }

    private void handleChannelCallback(ChannelFuture future) throws Exception {
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

    private void processQueue() {
        if (channel == null || !channel.isActive() || queue.isEmpty()) {
            return;
        }

        while (inflightSends.get() > config.getMaxInflightSends()) {
            Uninterruptibles.sleepUninterruptibly(1, MICROSECONDS);
        }

        while(!queue.isEmpty() && inflightSends.get() < config.getMaxInflightSends()) {
            inflightSends.incrementAndGet();
            channel.writeAndFlush(queue.poll()).addListener(inflightListener);
        }
    }

    protected abstract void createBootstrap(final EventLoopGroup workerGroup);

    void scheduleReconnect(final EventLoopGroup workerGroup) {
        workerGroup.schedule(() -> {
            LOG.debug("Starting reconnect!");
            createBootstrap(workerGroup);
        }, config.getReconnectDelay(), TimeUnit.MILLISECONDS);
    }

    /**
     * {@inheritDoc}
     * <p>This implementation is backed by a {@link java.util.concurrent.ConcurrentLinkedQueue}. When this method returns the
     * message has been added to the {@link java.util.concurrent.ConcurrentLinkedQueue} but has not been sent to the remote
     * host yet.</p>
     *
     * @param message message to send to the remote host
     */
    @Override
    public void send(GelfMessage message) {
        queue.add(message);
    }

    /**
     * {@inheritDoc}
     * <p>This implementation is backed by a {@link java.util.concurrent.ConcurrentLinkedQueue}. When this method returns the
     * message has been added to the {@link java.util.concurrent.ConcurrentLinkedQueue} but has not been sent to the remote
     * host yet.</p>
     *
     * @param message message to send to the remote host
     * @return true if the message could be dispatched, false otherwise (or queue is full)
     */
    @Override
    public boolean trySend(final GelfMessage message) {
        if (queue.size() < config.getQueueSize()) {
            return queue.add(message);
        } else {
            return false;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop() {
        workerGroup.shutdownGracefully().syncUninterruptibly();
    }
}
