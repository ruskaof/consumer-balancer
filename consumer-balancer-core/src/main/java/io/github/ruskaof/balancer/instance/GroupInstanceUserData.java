package io.github.ruskaof.balancer.instance;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Legacy assignment codec retained to recognize cached assignments during a coordinated
 * upgrade. New assignments use {@link MonitoringProtocol} and send the topology only once.
 * This legacy format sent the instance mapping back to every member.
 *
 * <p>It exists because the Kafka admin API exposes no subscription userData — so the
 * {@link io.github.ruskaof.balancer.trigger.threshold.ThresholdTrigger}, which watches the
 * group from the outside, has no other way to learn which members share a JVM. The leader
 * already knows the whole mapping while it assigns, so it hands it back to everyone; each
 * member caches it in a {@link io.github.ruskaof.balancer.MemberIdTracker} and the trigger
 * reads it from there.
 *
 * <p>Format (big-endian):
 * <pre>
 * int16  version         (currently 1)
 * int32  instanceCount   K
 *   K x  int16 length    (UTF-8 byte count, &gt;= 1)
 *        byte[] instanceId (UTF-8)
 * int32  memberCount     N
 *   N x  int16 length    (UTF-8 byte count, &gt;= 1)
 *        byte[] memberId (UTF-8)
 *        int16 instanceIndex (into the table above)
 * </pre>
 *
 * <p>Instance ids go into a table rather than next to every member because one instance
 * usually runs several members, and a UUID costs 36 bytes each time it is repeated.
 *
 * <p>Forward compatibility: every future version MUST keep this prefix and only append
 * fields after it, and readers accept any {@code version >= 1} while ignoring trailing
 * bytes — so an old member can still read the mapping from a newer leader.
 */
public final class GroupInstanceUserData {

    public static final short VERSION = 1;
    public static final int MAX_ID_BYTES = Short.MAX_VALUE;
    /** {@link #MAX_INSTANCES} fits the {@code int16} index every member entry carries. */
    public static final int MAX_INSTANCES = Short.MAX_VALUE;

    /**
     * Budget for the whole mapping across the group: the leader sends the same payload to
     * every member, so a group of {@code N} members multiplies it {@code N} times inside
     * the group metadata record Kafka persists to {@code __consumer_offsets}. Staying well
     * under the 1 MB default {@code message.max.bytes} leaves room for the subscriptions
     * and partition assignments sharing that record.
     */
    public static final int MAX_TOTAL_BYTES = 512 * 1024;

    private static final int HEADER_BYTES = Short.BYTES + Integer.BYTES;

    private GroupInstanceUserData() {
    }

    public enum Status {
        OK,
        /** No userData at all — e.g. a leader running an older library version. */
        ABSENT,
        /** userData present but not a readable instance mapping. */
        CORRUPT
    }

    /** {@code instanceIdByMember} is empty unless {@code status == OK}. */
    public record Decoded(Map<String, String> instanceIdByMember, Status status) {

        static final Decoded ABSENT = new Decoded(Map.of(), Status.ABSENT);
        static final Decoded CORRUPT = new Decoded(Map.of(), Status.CORRUPT);

        public Decoded {
            instanceIdByMember = Map.copyOf(instanceIdByMember);
        }

        public boolean ok() {
            return status == Status.OK;
        }
    }

    /**
     * Encodes the mapping into a buffer positioned at 0 (Kafka's protocol serializer copies
     * {@code duplicate().remaining()} bytes). Member ids are written in natural order, so
     * one mapping always encodes to the same bytes.
     *
     * @throws IllegalArgumentException on a blank id, an id over {@link #MAX_ID_BYTES}
     *                                  UTF-8 bytes, or more than {@link #MAX_INSTANCES}
     *                                  distinct instances
     */
    public static ByteBuffer encode(Map<String, String> instanceIdByMember) {
        if (instanceIdByMember == null) {
            throw new IllegalArgumentException("instanceIdByMember must not be null");
        }
        List<byte[]> instanceTable = new ArrayList<>();
        Map<String, Integer> instanceIndexes = new HashMap<>();
        List<byte[]> memberIds = new ArrayList<>();
        List<Integer> memberInstances = new ArrayList<>();
        int size = HEADER_BYTES + Integer.BYTES;

        // Sorted so one mapping always encodes to the same bytes.
        for (var entry : new TreeMap<>(instanceIdByMember).entrySet()) {
            byte[] memberId = utf8(entry.getKey(), "memberId");
            Integer index = instanceIndexes.get(entry.getValue());
            if (index == null) {
                byte[] instanceId = utf8(entry.getValue(), "instanceId");
                index = instanceTable.size();
                instanceTable.add(instanceId);
                instanceIndexes.put(entry.getValue(), index);
                size += Short.BYTES + instanceId.length;
            }
            memberIds.add(memberId);
            memberInstances.add(index);
            size += Short.BYTES + memberId.length + Short.BYTES;
        }
        if (instanceTable.size() > MAX_INSTANCES) {
            throw new IllegalArgumentException(
                    "The group has more than " + MAX_INSTANCES + " instances: " + instanceTable.size());
        }

        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.putShort(VERSION);
        buffer.putInt(instanceTable.size());
        for (byte[] instanceId : instanceTable) {
            putString(buffer, instanceId);
        }
        buffer.putInt(memberIds.size());
        for (int i = 0; i < memberIds.size(); i++) {
            putString(buffer, memberIds.get(i));
            buffer.putShort(memberInstances.get(i).shortValue());
        }
        buffer.flip();
        return buffer;
    }

    /**
     * Decodes a member's assignment userData; never throws and never moves the given
     * buffer's position.
     */
    public static Decoded decode(ByteBuffer userData) {
        if (userData == null) {
            return Decoded.ABSENT;
        }
        ByteBuffer buffer = userData.duplicate();
        if (buffer.remaining() == 0) {
            return Decoded.ABSENT;
        }
        if (buffer.remaining() < HEADER_BYTES) {
            return Decoded.CORRUPT;
        }
        try {
            short version = buffer.getShort();
            if (version < 1) {
                return Decoded.CORRUPT;
            }
            int instanceCount = buffer.getInt();
            if (instanceCount < 0 || instanceCount > MAX_INSTANCES) {
                return Decoded.CORRUPT;
            }
            String[] instanceIds = new String[instanceCount];
            for (int i = 0; i < instanceCount; i++) {
                instanceIds[i] = getString(buffer);
            }
            int memberCount = buffer.getInt();
            // Every member entry costs at least a length, one byte of id and an index, so a
            // count larger than the bytes left is a malformed payload rather than a big group.
            if (memberCount < 0 || memberCount > buffer.remaining() / (Short.BYTES + 1 + Short.BYTES)) {
                return Decoded.CORRUPT;
            }
            Map<String, String> instanceIdByMember = new LinkedHashMap<>();
            for (int i = 0; i < memberCount; i++) {
                String memberId = getString(buffer);
                int index = buffer.getShort();
                if (index < 0 || index >= instanceCount) {
                    return Decoded.CORRUPT;
                }
                instanceIdByMember.put(memberId, instanceIds[index]);
            }
            return new Decoded(instanceIdByMember, Status.OK);
        } catch (RuntimeException e) {
            // Truncated, over-long lengths, blank ids — all read as an unusable payload.
            return Decoded.CORRUPT;
        }
    }

    private static byte[] utf8(String value, String what) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(what + " must not be blank");
        }
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > MAX_ID_BYTES) {
            throw new IllegalArgumentException(
                    what + " exceeds " + MAX_ID_BYTES + " UTF-8 bytes: " + value);
        }
        return bytes;
    }

    private static void putString(ByteBuffer buffer, byte[] bytes) {
        buffer.putShort((short) bytes.length);
        buffer.put(bytes);
    }

    /** @throws RuntimeException on a length that is negative, blank or past the end */
    private static String getString(ByteBuffer buffer) {
        short length = buffer.getShort();
        if (length < 1 || length > buffer.remaining()) {
            throw new IllegalArgumentException("Unreadable string length " + length);
        }
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        String value = new String(bytes, StandardCharsets.UTF_8);
        if (value.isBlank()) {
            throw new IllegalArgumentException("Blank id");
        }
        return value;
    }
}
