package com.ryanxing.trackfusion.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class RadarPacketCodecTest {
    private final RadarPacket packet =
            new RadarPacket(
                    "radar-east",
                    "target-42",
                    Instant.parse("2026-01-02T03:04:05.678Z"),
                    43.6532,
                    -79.3832,
                    1_250,
                    95.4,
                    271.5,
                    18);

    @Test
    void roundTripsTheVersionedBinaryProtocol() {
        byte[] encoded = RadarPacketCodec.encode(packet);
        ByteBuffer header = ByteBuffer.wrap(encoded);

        assertThat(header.getInt()).isEqualTo(RadarPacketCodec.MAGIC);
        assertThat(header.get()).isEqualTo(RadarPacketCodec.VERSION);
        assertThat(header.getInt()).isEqualTo(encoded.length - 9);
        assertThat(RadarPacketCodec.decode(encoded)).isEqualTo(packet);
    }

    @Test
    void rejectsUnknownMagicAndVersion() {
        byte[] badMagic = RadarPacketCodec.encode(packet);
        badMagic[0] ^= 1;
        byte[] badVersion = RadarPacketCodec.encode(packet);
        badVersion[4] = 2;

        assertThatIllegalArgumentException()
                .isThrownBy(() -> RadarPacketCodec.decode(badMagic));
        assertThatIllegalArgumentException()
                .isThrownBy(() -> RadarPacketCodec.decode(badVersion));
    }

    @Test
    void rejectsTruncatedOrLengthMismatchedPackets() {
        byte[] encoded = RadarPacketCodec.encode(packet);
        byte[] truncated = Arrays.copyOf(encoded, encoded.length - 1);
        byte[] wrongLength = encoded.clone();
        ByteBuffer.wrap(wrongLength).putInt(5, encoded.length);

        assertThatIllegalArgumentException()
                .isThrownBy(() -> RadarPacketCodec.decode(truncated));
        assertThatIllegalArgumentException()
                .isThrownBy(() -> RadarPacketCodec.decode(wrongLength));
    }
}
