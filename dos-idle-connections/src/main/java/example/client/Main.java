package example.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        InetSocketAddress address = new InetSocketAddress("localhost", 7000);
        Bootstrap bootstrap = new Bootstrap()
                .group(new NioEventLoopGroup())
                .channel(NioSocketChannel.class)
                .handler(
                        new ChannelInitializer<SocketChannel>() {
                            @Override
                            protected void initChannel(SocketChannel ch) {
                                ConnectionExceptionHandler exceptionHandler = new ConnectionExceptionHandler();
                                ch.pipeline()
                                        .addLast(exceptionHandler);
                            }
                        });
        AtomicInteger opened = new AtomicInteger();
        AtomicInteger closed = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();

        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

        executorService.schedule(new Runnable() {
            int count;

            @Override
            public void run() {
                for (int i = 0; i < 1000; i++) {
                    ChannelFuture connectFuture = bootstrap.connect(address);
                    connectFuture.addListener(future -> {
                        Throwable error = future.cause();
                        if (error != null) {
                            rejected.incrementAndGet();
                        } else {
                            opened.incrementAndGet();
                            Channel channel = connectFuture.channel();
                            channel.closeFuture()
                                    .addListener(f -> closed.incrementAndGet());
                        }
                    });
                }
                if (++count < 10) {
                    executorService.schedule(this, 1, TimeUnit.SECONDS);
                }
            }
        }, 1, TimeUnit.SECONDS);

        executorService.scheduleAtFixedRate(() -> {
            logger.info("connections, opened: {}, closed: {}, rejected: {}", opened.get(), closed.get(), rejected.get());
        }, 5, 5, TimeUnit.SECONDS);

        Thread.currentThread().join();
    }

    private static class ConnectionExceptionHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            if (cause instanceof IOException) {
                return;
            }
            logger.error("connection error", cause);
            ctx.close();
        }
    }
}
