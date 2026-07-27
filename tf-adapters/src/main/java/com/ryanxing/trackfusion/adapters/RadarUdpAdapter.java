package com.ryanxing.trackfusion.adapters;

import com.ryanxing.trackfusion.common.Detection;
import com.ryanxing.trackfusion.common.RadarPacket;
import com.ryanxing.trackfusion.common.RadarPacketCodec;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.DatagramChannel;
import java.time.Instant;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

public final class RadarUdpAdapter implements SourceAdapter {
    private static final int MAX_UDP_PACKET_BYTES = 65_507;

    private final String sourceId;
    private final int port;

    public RadarUdpAdapter(String sourceId, int port) {
        if (sourceId == null || sourceId.isBlank()) {
            throw new IllegalArgumentException("sourceId is required");
        }
        if (port < 1 || port > 65_535) {
            throw new IllegalArgumentException("port must be in [1, 65535]");
        }
        this.sourceId = sourceId;
        this.port = port;
    }

    @Override
    public String sourceId() {
        return sourceId;
    }

    @Override
    public String sourceType() {
        return "RADAR";
    }

    @Override
    public Flux<Detection> stream() {
        return Flux.create(
                sink -> {
                    try {
                        DatagramChannel channel =
                                DatagramChannel.open()
                                        .bind(new InetSocketAddress("0.0.0.0", port));
                        sink.onDispose(() -> close(channel));
                        Thread.ofVirtual()
                                .name("radar-udp-" + port)
                                .start(() -> receive(channel, sink));
                    } catch (IOException bindFailure) {
                        sink.error(bindFailure);
                    }
                },
                FluxSink.OverflowStrategy.LATEST);
    }

    static Detection decode(String sourceId, byte[] bytes, Instant receivedAt) {
        RadarPacket packet = RadarPacketCodec.decode(bytes);
        if (!sourceId.equals(packet.sourceId())) {
            throw new IllegalArgumentException("unexpected radar source " + packet.sourceId());
        }
        return packet.toDetection(receivedAt);
    }

    private void receive(DatagramChannel channel, FluxSink<Detection> sink) {
        ByteBuffer buffer = ByteBuffer.allocate(MAX_UDP_PACKET_BYTES);
        try {
            while (!sink.isCancelled()) {
                buffer.clear();
                channel.receive(buffer);
                buffer.flip();
                byte[] bytes = new byte[buffer.remaining()];
                buffer.get(bytes);
                try {
                    sink.next(decode(sourceId, bytes, Instant.now()));
                } catch (IllegalArgumentException malformedOrWrongSource) {
                    // A bad datagram must not stop the feed.
                }
            }
        } catch (AsynchronousCloseException stopped) {
            if (!sink.isCancelled()) {
                sink.error(stopped);
            }
        } catch (IOException receiveFailure) {
            if (!sink.isCancelled()) {
                sink.error(receiveFailure);
            }
        } finally {
            close(channel);
        }
    }

    private static void close(DatagramChannel channel) {
        try {
            channel.close();
        } catch (IOException ignored) {
            // Already closed.
        }
    }
}
