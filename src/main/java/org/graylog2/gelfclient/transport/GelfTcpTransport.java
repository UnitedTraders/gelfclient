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

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import org.graylog2.gelfclient.GelfConfiguration;
import org.graylog2.gelfclient.encoder.GelfMessageDslJsonEncoder;
import org.graylog2.gelfclient.encoder.GelfTcpFrameDelimiterEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link GelfTransport} implementation that uses TCP to send GELF messages.
 * <p>This class is thread-safe.</p>
 */
public class GelfTcpTransport extends AbstractGelfTransport {
    private static final Logger LOG = LoggerFactory.getLogger(GelfTcpTransport.class);

    /**
     * Creates a new TCP GELF transport.
     *
     * @param config the GELF client configuration
     */
    public GelfTcpTransport(GelfConfiguration config) {
        super(config);
    }

    @Override
    protected void createBootstrap(final EventLoopGroup workerGroup) {
        final Bootstrap bootstrap = new Bootstrap();

        bootstrap.group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.getConnectTimeout())
                .option(ChannelOption.TCP_NODELAY, config.isTcpNoDelay())
                .option(ChannelOption.SO_KEEPALIVE, config.isTcpKeepAlive())
                .remoteAddress(config.getRemoteAddress())
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        if (config.isTlsEnabled()) {
                            LOG.debug("TLS enabled.");
                            final SslContext sslContext;

                            if (!config.isTlsCertVerificationEnabled()) {
                                // If the cert should not be verified just use an insecure trust manager.
                                LOG.debug("TLS certificate verification disabled!");
                                sslContext = SslContextBuilder.forClient()
                                        .trustManager(InsecureTrustManagerFactory.INSTANCE)
                                        .build();
                            } else if (config.getTlsTrustCertChainFile() != null) {
                                // If a cert chain file is set, use it.
                                LOG.debug("TLS certificate chain file: {}", config.getTlsTrustCertChainFile());
                                sslContext = SslContextBuilder.forClient()
                                        .trustManager(config.getTlsTrustCertChainFile())
                                        .build();
                            } else {
                                // Otherwise use the JVM default cert chain.
                                sslContext = SslContextBuilder.forClient().build();
                            }

                            ch.pipeline().addLast(sslContext.newHandler(ch.alloc()));
                        }

                        // The graylog2-server uses '\0'-bytes as delimiter for TCP frames.
                        ch.pipeline().addLast(new GelfTcpFrameDelimiterEncoder());

                        // We cannot use GZIP encoding for TCP because the headers contain '\0'-bytes then.
                       // ch.pipeline().addLast(new GelfMessageJsonEncoder(config.isTrackSerializationTime()));
                        ch.pipeline().addLast(new GelfMessageDslJsonEncoder(config.isTrackSerializationTime())); // use a bit efficient dsl-json encoder
                        ch.pipeline().addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
                                // We do not receive data.
                            }

                            @Override
                            public void channelActive(ChannelHandlerContext ctx) throws Exception {
                                channel = ctx.channel();
                            }

                            @Override
                            public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                                LOG.info("Channel disconnected!");
                                channel = ctx.channel();
                                scheduleReconnect(ctx.channel().eventLoop());
                            }

                            @Override
                            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                                LOG.error("Exception caught", cause);
                            }


                        });
                    }
                });

        if (config.getSendBufferSize() != -1) {
            bootstrap.option(ChannelOption.SO_SNDBUF, config.getSendBufferSize());
        }

        bootstrap.connect().addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                LOG.debug("Connected!");
            } else {
                LOG.error("Connection failed: {}", future.cause().getMessage());
                scheduleReconnect(future.channel().eventLoop());
            }
        });
    }
}
