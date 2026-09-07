package io.github.ruskaof.balancer.instance;

import io.github.ruskaof.balancer.instance.GroupInstanceUserData.Decoded;
import io.github.ruskaof.balancer.instance.GroupInstanceUserData.Status;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class GroupInstanceUserDataTest {

    private static final Map<String, String> MAPPING = Map.of(
            "consumer-g-1-3f2a", "pod-a",
            "consumer-g-2-9b1c", "pod-a",
            "consumer-g-3-77de", "pod-b");

    @Test
    void roundTripsTheMapping() {
        Decoded decoded = GroupInstanceUserData.decode(GroupInstanceUserData.encode(MAPPING));

        assertEquals(Status.OK, decoded.status());
        assertEquals(MAPPING, decoded.instanceIdByMember());
    }

    @Test
    void roundTripsNonAsciiIds() {
        Map<String, String> mapping = Map.of("участник-1", "инстанс-1", "メンバー", "ポッド");

        Decoded decoded = GroupInstanceUserData.decode(GroupInstanceUserData.encode(mapping));

        assertEquals(mapping, decoded.instanceIdByMember());
    }

    @Test
    void roundTripsAnEmptyMapping() {
        Decoded decoded = GroupInstanceUserData.decode(GroupInstanceUserData.encode(Map.of()));

        assertEquals(Status.OK, decoded.status());
        assertEquals(Map.of(), decoded.instanceIdByMember());
    }

    @Test
    void writesEachInstanceIdOnlyOnce() {
        // One pod usually runs several members, so repeating a 36-byte id per member is what
        // the instance table exists to avoid.
        Map<String, String> shared = Map.of("m1", "pod-a", "m2", "pod-a", "m3", "pod-a");
        Map<String, String> distinct = Map.of("m1", "pod-a", "m2", "pod-b", "m3", "pod-c");

        assertTrue(GroupInstanceUserData.encode(shared).remaining()
                        < GroupInstanceUserData.encode(distinct).remaining(),
                "members sharing an instance must not each carry a copy of its id");
    }

    @Test
    void encodesTheSameMappingToTheSameBytes() {
        Map<String, String> reordered = new LinkedHashMap<>();
        reordered.put("consumer-g-3-77de", "pod-b");
        reordered.put("consumer-g-1-3f2a", "pod-a");
        reordered.put("consumer-g-2-9b1c", "pod-a");

        assertEquals(GroupInstanceUserData.encode(MAPPING), GroupInstanceUserData.encode(reordered));
    }

    @Test
    void encodedBufferIsReadyToSend() {
        ByteBuffer buffer = GroupInstanceUserData.encode(MAPPING);

        assertEquals(0, buffer.position(), "Kafka copies duplicate().remaining() bytes, so position must be 0");
        assertTrue(buffer.remaining() > 0);
    }

    @Test
    void decodeDoesNotMoveTheInputPosition() {
        ByteBuffer buffer = GroupInstanceUserData.encode(MAPPING);

        GroupInstanceUserData.decode(buffer);

        assertEquals(0, buffer.position());
    }

    @Test
    void absentWhenNullOrEmpty() {
        assertEquals(Status.ABSENT, GroupInstanceUserData.decode(null).status());
        assertEquals(Status.ABSENT, GroupInstanceUserData.decode(ByteBuffer.allocate(0)).status());
    }

    @Test
    void absentAndCorruptCarryNoMapping() {
        assertEquals(Map.of(), GroupInstanceUserData.decode(null).instanceIdByMember());
        assertEquals(Map.of(),
                GroupInstanceUserData.decode(ByteBuffer.wrap(new byte[]{7, 7, 7, 7, 7, 7})).instanceIdByMember());
    }

    @Test
    void corruptWhenTooShortForTheHeader() {
        for (int size = 1; size < 6; size++) {
            assertEquals(Status.CORRUPT, GroupInstanceUserData.decode(ByteBuffer.allocate(size)).status(),
                    "size " + size);
        }
    }

    @Test
    void corruptOnNonPositiveVersion() {
        assertEquals(Status.CORRUPT, GroupInstanceUserData.decode(header((short) 0, 0)).status());
        assertEquals(Status.CORRUPT, GroupInstanceUserData.decode(header((short) -3, 0)).status());
    }

    @Test
    void corruptOnANegativeOrOversizedInstanceCount() {
        assertEquals(Status.CORRUPT,
                GroupInstanceUserData.decode(header(GroupInstanceUserData.VERSION, -1)).status());
        assertEquals(Status.CORRUPT,
                GroupInstanceUserData.decode(
                        header(GroupInstanceUserData.VERSION, GroupInstanceUserData.MAX_INSTANCES + 1)).status());
    }

    @Test
    void corruptWhenTruncatedMidway() {
        ByteBuffer encoded = GroupInstanceUserData.encode(MAPPING);
        for (int length = 6; length < encoded.remaining(); length += 3) {
            ByteBuffer truncated = encoded.duplicate();
            truncated.limit(length);
            assertEquals(Status.CORRUPT, GroupInstanceUserData.decode(truncated).status(), "length " + length);
        }
    }

    @Test
    void corruptWhenAMemberPointsOutsideTheInstanceTable() {
        byte[] instanceId = "pod-a".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] memberId = "m1".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(64);
        buffer.putShort(GroupInstanceUserData.VERSION);
        buffer.putInt(1).putShort((short) instanceId.length).put(instanceId);
        buffer.putInt(1).putShort((short) memberId.length).put(memberId).putShort((short) 5);
        buffer.flip();

        assertEquals(Status.CORRUPT, GroupInstanceUserData.decode(buffer).status());
    }

    @Test
    void readsFutureVersionsAndIgnoresTrailingBytes() {
        ByteBuffer encoded = GroupInstanceUserData.encode(MAPPING);
        ByteBuffer future = ByteBuffer.allocate(encoded.remaining() + Long.BYTES);
        future.put(encoded.duplicate()).putLong(42L).flip();
        future.putShort(0, (short) 2);

        Decoded decoded = GroupInstanceUserData.decode(future);

        assertEquals(Status.OK, decoded.status());
        assertEquals(MAPPING, decoded.instanceIdByMember());
    }

    @Test
    void encodeRejectsNullAndBlankIds() {
        assertThrows(IllegalArgumentException.class, () -> GroupInstanceUserData.encode(null));
        assertThrows(IllegalArgumentException.class, () -> GroupInstanceUserData.encode(mapOf("  ", "pod-a")));
        assertThrows(IllegalArgumentException.class, () -> GroupInstanceUserData.encode(mapOf("m1", "  ")));
    }

    @Test
    void encodeRejectsOversizedIds() {
        String oversized = "x".repeat(GroupInstanceUserData.MAX_ID_BYTES + 1);

        assertThrows(IllegalArgumentException.class, () -> GroupInstanceUserData.encode(mapOf(oversized, "pod-a")));
        assertThrows(IllegalArgumentException.class, () -> GroupInstanceUserData.encode(mapOf("m1", oversized)));
    }

    private static ByteBuffer header(short version, int instanceCount) {
        ByteBuffer buffer = ByteBuffer.allocate(Short.BYTES + Integer.BYTES);
        buffer.putShort(version).putInt(instanceCount).flip();
        return buffer;
    }

    /** {@link Map#of} rejects nulls, and these cases need the codec to do the rejecting. */
    private static Map<String, String> mapOf(String memberId, String instanceId) {
        Map<String, String> mapping = new LinkedHashMap<>();
        mapping.put(memberId, instanceId);
        return mapping;
    }
}
