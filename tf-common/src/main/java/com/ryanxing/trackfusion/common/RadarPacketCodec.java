package com.ryanxing.trackfusion.common;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

public final class RadarPacketCodec {
    public static final int MAGIC = 0x54465231;
    public static final byte VERSION = 1;
    private static final int HEADER_BYTES = Integer.BYTES + Byte.BYTES + Integer.BYTES;
    private static final int FIXED_PAYLOAD_BYTES =
            2 * Short.BYTES + Long.BYTES + 6 * Double.BYTES;

    private RadarPacketCodec() {}

    public static byte[] encode(RadarPacket packet) {
        if (packet == null) {
            throw new IllegalArgumentException("packet is required");
        }
        byte[] sourceId = utf8(packet.sourceId());
        byte[] groundTruthId = utf8(packet.groundTruthId());
        int payloadLength = FIXED_PAYLOAD_BYTES + sourceId.length + groundTruthId.length;
        return ByteBuffer.allocate(HEADER_BYTES + payloadLength)
                .putInt(MAGIC)
                .put(VERSION)
                .putInt(payloadLength)
                .putShort((short) sourceId.length)
                .put(sourceId)
                .putShort((short) groundTruthId.length)
                .put(groundTruthId)
                .putLong(packet.observedAt().toEpochMilli())
                .putDouble(packet.latDeg())
                .putDouble(packet.lonDeg())
                .putDouble(packet.altMeters())
                .putDouble(packet.speedMps())
                .putDouble(packet.headingDeg())
                .putDouble(packet.positionSigmaMeters())
                .array();
    }

    public static RadarPacket decode(byte[] bytes) {
        if (bytes == null || bytes.length < HEADER_BYTES) {
            throw new IllegalArgumentException("truncated radar packet");
        }
        try {
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            if (buffer.getInt() != MAGIC) {
                throw new IllegalArgumentException("invalid radar packet magic");
            }
            if (buffer.get() != VERSION) {
                throw new IllegalArgumentException("unsupported radar packet version");
            }
            int payloadLength = buffer.getInt();
            if (payloadLength != buffer.remaining()) {
                throw new IllegalArgumentException("invalid radar packet length");
            }
            String sourceId = readUtf8(buffer);
            String groundTruthId = readUtf8(buffer);
            RadarPacket packet =
                    new RadarPacket(
                            sourceId,
                            groundTruthId,
                            java.time.Instant.ofEpochMilli(buffer.getLong()),
                            buffer.getDouble(),
                            buffer.getDouble(),
                            buffer.getDouble(),
                            buffer.getDouble(),
                            buffer.getDouble(),
                            buffer.getDouble());
            if (buffer.hasRemaining()) {
                throw new IllegalArgumentException("trailing radar packet data");
            }
            return packet;
        } catch (BufferUnderflowException truncated) {
            throw new IllegalArgumentException("truncated radar packet", truncated);
        }
    }

    private static byte[] utf8(String value) {
        byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        if (encoded.length > 0xffff) {
            throw new IllegalArgumentException("radar packet string is too long");
        }
        return encoded;
    }

    private static String readUtf8(ByteBuffer buffer) {
        int length = Short.toUnsignedInt(buffer.getShort());
        if (length > buffer.remaining()) {
            throw new IllegalArgumentException("invalid radar packet string length");
        }
        byte[] encoded = new byte[length];
        buffer.get(encoded);
        try {
            return StandardCharsets.UTF_8
                    .newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(encoded))
                    .toString();
        } catch (CharacterCodingException invalidUtf8) {
            throw new IllegalArgumentException("invalid UTF-8 in radar packet", invalidUtf8);
        }
    }
}
