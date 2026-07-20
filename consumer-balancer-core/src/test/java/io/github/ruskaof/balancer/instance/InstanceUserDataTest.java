package io.github.ruskaof.balancer.instance;

import io.github.ruskaof.balancer.instance.InstanceUserData.Decoded;
import io.github.ruskaof.balancer.instance.InstanceUserData.Status;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class InstanceUserDataTest {

    @Test
    void roundTripsInstanceIds() {
        for (String id : new String[]{"pod-a", "my-app-7d9f6c5b4-x2klm", "инстанс-1", "ポッド"}) {
            Decoded decoded = InstanceUserData.decode(InstanceUserData.encode(id));
            assertEquals(Status.OK, decoded.status(), id);
            assertEquals(id, decoded.instanceId());
        }
    }

    @Test
    void encodedBufferIsReadyToSend() {
        ByteBuffer buffer = InstanceUserData.encode("pod-a");
        assertEquals(0, buffer.position(), "Kafka copies duplicate().remaining() bytes, so position must be 0");
        assertTrue(buffer.remaining() > 0);
    }

    @Test
    void decodeDoesNotMoveTheInputPosition() {
        ByteBuffer buffer = InstanceUserData.encode("pod-a");
        InstanceUserData.decode(buffer);
        assertEquals(0, buffer.position());
    }

    @Test
    void absentWhenNullOrEmpty() {
        assertEquals(Status.ABSENT, InstanceUserData.decode(null).status());
        assertEquals(Status.ABSENT, InstanceUserData.decode(ByteBuffer.allocate(0)).status());
    }

    @Test
    void corruptWhenTooShortForPrefix() {
        for (int size = 1; size < 4; size++) {
            ByteBuffer buffer = ByteBuffer.allocate(size);
            assertEquals(Status.CORRUPT, InstanceUserData.decode(buffer).status(), "size " + size);
        }
    }

    @Test
    void corruptOnNonPositiveVersion() {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putShort((short) 0).putShort((short) 1).put((byte) 'a').flip();
        assertEquals(Status.CORRUPT, InstanceUserData.decode(buffer).status());

        ByteBuffer negative = ByteBuffer.allocate(8);
        negative.putShort((short) -3).putShort((short) 1).put((byte) 'a').flip();
        assertEquals(Status.CORRUPT, InstanceUserData.decode(negative).status());
    }

    @Test
    void corruptOnBadLength() {
        ByteBuffer zeroLength = ByteBuffer.allocate(4);
        zeroLength.putShort(InstanceUserData.VERSION).putShort((short) 0).flip();
        assertEquals(Status.CORRUPT, InstanceUserData.decode(zeroLength).status());

        ByteBuffer negativeLength = ByteBuffer.allocate(4);
        negativeLength.putShort(InstanceUserData.VERSION).putShort((short) -1).flip();
        assertEquals(Status.CORRUPT, InstanceUserData.decode(negativeLength).status());

        ByteBuffer truncated = ByteBuffer.allocate(6);
        truncated.putShort(InstanceUserData.VERSION).putShort((short) 10).put((byte) 'a').put((byte) 'b').flip();
        assertEquals(Status.CORRUPT, InstanceUserData.decode(truncated).status());
    }

    @Test
    void corruptWhenDecodedIdIsBlank() {
        byte[] blank = "   ".getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(4 + blank.length);
        buffer.putShort(InstanceUserData.VERSION).putShort((short) blank.length).put(blank).flip();
        assertEquals(Status.CORRUPT, InstanceUserData.decode(buffer).status());
    }

    @Test
    void readsFutureVersionsAndIgnoresTrailingBytes() {
        byte[] id = "pod-a".getBytes(StandardCharsets.UTF_8);
        ByteBuffer future = ByteBuffer.allocate(4 + id.length + 8);
        future.putShort((short) 2).putShort((short) id.length).put(id).putLong(42L).flip();

        Decoded decoded = InstanceUserData.decode(future);

        assertEquals(Status.OK, decoded.status());
        assertEquals("pod-a", decoded.instanceId());
    }

    @Test
    void encodeRejectsBlankAndOversizedIds() {
        assertThrows(IllegalArgumentException.class, () -> InstanceUserData.encode(null));
        assertThrows(IllegalArgumentException.class, () -> InstanceUserData.encode("  "));
        assertThrows(IllegalArgumentException.class,
                () -> InstanceUserData.encode("x".repeat(InstanceUserData.MAX_INSTANCE_ID_BYTES + 1)));
    }
}
