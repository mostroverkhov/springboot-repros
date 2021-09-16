package example.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.databind.CBORMapper;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.rsocket.metadata.TaggingMetadataCodec;
import io.rsocket.metadata.WellKnownMimeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        InetSocketAddress address = new InetSocketAddress("localhost", 7000);
        Bootstrap bootstrap = new Bootstrap()
                .group(new NioEventLoopGroup())
                .channel(NioSocketChannel.class)
                .handler(
                        new ChannelInitializer<SocketChannel>() {
                            @Override
                            protected void initChannel(SocketChannel ch) {
                                ch.pipeline().addLast(new RSocketResponseOverflowHandler());
                            }
                        });

        ChannelFuture connect = bootstrap.connect(address);
        connect.awaitUninterruptibly();
        logger.info("connected to {}", address);
        connect.channel().closeFuture().awaitUninterruptibly();
    }

    private static final class RSocketResponseOverflowHandler extends ChannelDuplexHandler {
        private static final byte[] METADATA_TYPE = "message/x.rsocket.composite-metadata.v0".getBytes(StandardCharsets.UTF_8);
        private static final byte[] DATA_TYPE = "application/cbor".getBytes(StandardCharsets.UTF_8);

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            super.channelActive(ctx);

            /*stop reading socket*/
            ctx.channel().config().setAutoRead(false);

            ByteBufAllocator allocator = ctx.alloc();

            /*setup frame*/

            ByteBuf setupFrame = allocator.buffer();
            setupFrame/*streamId*/
                    .writeInt(0)
                    /*flags*/
                    .writeShort(/*FrameType.SETUP*/0x01 << 10)
                    /*version*/
                    .writeInt(1 << 16)
                    /*keep-alive interval*/
                    .writeInt(100_000)
                    /*keep-alive timeout*/
                    .writeInt(1_000_000)
                    /*metadata type*/
                    .writeByte(METADATA_TYPE.length).writeBytes(METADATA_TYPE)
                    /*data type*/
                    .writeByte(DATA_TYPE.length).writeBytes(DATA_TYPE);

            ByteBuf setupLengthPrefix = encodeLength(allocator, setupFrame.readableBytes());

            ctx.write(setupLengthPrefix);
            ctx.writeAndFlush(setupFrame);

            /*request data*/
            StringBuilder content = new StringBuilder();
            for (int i = 0; i < 1000; i++) {
                content.append("response-overflow");
            }
            Request request = new Request();
            request.setMessage(content.toString());
            ObjectMapper mapper = new CBORMapper();

            /*send requests periodically*/
            ctx.channel().eventLoop().scheduleAtFixedRate(new Runnable() {
                int streamId = 1;

                @Override
                public void run() {
                    for (int i = 0; i < 300; i++) {
                        /*request-response frame*/

                        /*spring's request metadata dance*/
                        CompositeByteBuf metadata = allocator.compositeBuffer();
                        ByteBuf routeMetadata = TaggingMetadataCodec.createRoutingMetadata(
                                allocator, Collections.singletonList("response")).getContent();

                        io.rsocket.metadata.CompositeMetadataCodec.encodeAndAddMetadata(metadata, allocator,
                                WellKnownMimeType.MESSAGE_RSOCKET_ROUTING, routeMetadata);

                        ByteBuf metadataLengthPrefix = encodeLength(allocator, metadata.readableBytes());
                        int id = streamId;
                        int nextId = id + 2;
                        if (nextId < 0) {
                            nextId = 1;
                        }
                        streamId = nextId;
                        ByteBuf requestFrame = allocator.buffer();
                        requestFrame/*streamId*/
                                .writeInt(id)
                                /*flags*/
                                .writeShort(/*FrameType.REQUEST_RESPONSE*/0x04 << 10 | /*metadata*/ 1 << 8);
                        ByteBuf data;
                        try {
                            data = Unpooled.wrappedBuffer(mapper.writeValueAsBytes(request));
                        } catch (JsonProcessingException e) {
                            ctx.close();
                            logger.error("cbor mapper error", e);
                            metadataLengthPrefix.release();
                            metadata.release();
                            return;
                        }

                        ByteBuf requestLengthPrefix = encodeLength(allocator,
                                requestFrame.readableBytes() + metadataLengthPrefix.readableBytes() +
                                        metadata.readableBytes() + data.readableBytes());

                        ctx.write(requestLengthPrefix);
                        ctx.write(requestFrame);
                        ctx.write(metadataLengthPrefix);
                        ctx.write(metadata);
                        ctx.writeAndFlush(data);

                        if (!ctx.channel().isWritable()) {
                            ctx.flush();
                        }
                    }

                }
            }, 0, 100, TimeUnit.MILLISECONDS);
        }

        private ByteBuf encodeLength(ByteBufAllocator allocator, int length) {
            ByteBuf lengthPrefix = allocator.buffer(3);
            lengthPrefix.writeByte(length >> 16);
            lengthPrefix.writeByte(length >> 8);
            lengthPrefix.writeByte(length);
            return lengthPrefix;
        }
    }
}
