package io.github.ruskaof.balancer.instance;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Wire codec for the subscription userData every member sends to the group leader so
 * co-located members can be grouped into one application instance.
 *
 * <p>Format (big-endian):
 * <pre>
 * int16  version           (currently 1)
 * int16  instanceIdLength  (UTF-8 byte count, &gt;= 1)
 * byte[] instanceId        (UTF-8)
 * </pre>
 *
 * <p>Forward compatibility: every future version MUST keep this prefix and only append
 * fields after it, and readers accept any {@code version >= 1} while ignoring trailing
 * bytes — so an old leader can still read instance ids from newer members.
 */
public final class InstanceUserData {

    public static final short VERSION = 1;
    public static final int MAX_INSTANCE_ID_BYTES = Short.MAX_VALUE;

    private static final int PREFIX_BYTES = Short.BYTES + Short.BYTES;

    private InstanceUserData() {
    }

    public enum Status {
        OK,
        /** No userData at all — e.g. a member running an older library version. */
        ABSENT,
        /** userData present but not a readable instance-id payload. */
        CORRUPT
    }

    /** {@code instanceId} is non-null exactly when {@code status == OK}. */
    public record Decoded(String instanceId, Status status) {

        static final Decoded ABSENT = new Decoded(null, Status.ABSENT);
        static final Decoded CORRUPT = new Decoded(null, Status.CORRUPT);

        public boolean ok() {
            return status == Status.OK;
        }
    }

    /**
     * Encodes the instance id into a buffer positioned at 0 (Kafka's protocol serializer
     * copies {@code duplicate().remaining()} bytes).
     */
    public static ByteBuffer encode(String instanceId) {
        if (instanceId == null || instanceId.isBlank()) {
            throw new IllegalArgumentException("instanceId must not be blank");
        }
        byte[] bytes = instanceId.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > MAX_INSTANCE_ID_BYTES) {
            throw new IllegalArgumentException(
                    "instanceId exceeds " + MAX_INSTANCE_ID_BYTES + " UTF-8 bytes: " + instanceId);
        }
        ByteBuffer buffer = ByteBuffer.allocate(PREFIX_BYTES + bytes.length);
        buffer.putShort(VERSION);
        buffer.putShort((short) bytes.length);
        buffer.put(bytes);
        buffer.flip();
        return buffer;
    }

    /**
     * Decodes a member's subscription userData; never throws and never moves the given
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
        if (buffer.remaining() < PREFIX_BYTES) {
            return Decoded.CORRUPT;
        }
        short version = buffer.getShort();
        if (version < 1) {
            return Decoded.CORRUPT;
        }
        short length = buffer.getShort();
        if (length < 1 || length > buffer.remaining()) {
            return Decoded.CORRUPT;
        }
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        String instanceId = new String(bytes, StandardCharsets.UTF_8);
        if (instanceId.isBlank()) {
            return Decoded.CORRUPT;
        }
        return new Decoded(instanceId, Status.OK);
    }
}
