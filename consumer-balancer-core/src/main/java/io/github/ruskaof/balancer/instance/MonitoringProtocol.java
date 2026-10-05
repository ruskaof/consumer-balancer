package io.github.ruskaof.balancer.instance;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

/**
 * Versioned monitoring metadata. Only the monitoring member receives the group topology;
 * other members receive an acknowledgement of their own instance and subscriptions.
 * Static Kafka identities are deliberately separate from application-instance identities.
 *
 * <p>The assignment wire format is a magic, version, snapshot UUID, owner identity and
 * request revision, followed by dictionaries of instances, topics and subscription sets,
 * the recipient's dictionary indexes, and member entries (identity and dictionary indexes).
 * Strings use a signed int16 UTF-8 length; counts and indexes use int32. All integers are
 * big-endian. Unknown versions are rejected, rather than partially interpreting topology.
 */
public final class MonitoringProtocol {
    /** Budget for added assignment metadata across the entire consumer group. */
    public static final int MAX_TOTAL_BYTES = 512 * 1024;

    private static final int ASSIGNMENT_MAGIC = 0xCB4D4F4E;
    private static final int SUBSCRIPTION_MAGIC = 0xCB4D5355;
    private static final short VERSION = 1;
    /** Maximum UTF-8 byte length of an instance id, consumer identity or topic. */
    public static final int MAX_STRING_BYTES = Short.MAX_VALUE;

    private MonitoringProtocol() {
    }

    public record Subscription(String instanceId, boolean monitoring, long revision) {
        public Subscription {
            requireString(instanceId);
            requireRevision(revision);
        }
    }

    public record Member(String instanceId, Set<String> topics) {
        public Member {
            requireString(instanceId);
            topics = immutableTopics(topics);
        }
    }

    public record Assignment(UUID snapshotId, String ownerIdentity, String instanceId,
                             Set<String> topics, long revision, Map<String, Member> members) {
        public Assignment {
            if (snapshotId == null || members == null || members.size() > MAX_TOTAL_BYTES / 13) {
                throw new IllegalArgumentException("Missing snapshot/members or too many members");
            }
            requireIdentity(ownerIdentity);
            requireString(instanceId);
            requireRevision(revision);
            topics = immutableTopics(topics);
            Map<String, Member> sorted = new TreeMap<>();
            members.forEach((identity, member) -> {
                requireIdentity(identity);
                if (member == null) {
                    throw new IllegalArgumentException("Missing member metadata");
                }
                sorted.put(identity, member);
            });
            if (!sorted.isEmpty() && !sorted.containsKey(ownerIdentity)) {
                throw new IllegalArgumentException("The monitoring owner must belong to the topology");
            }
            members = Collections.unmodifiableMap(sorted);
        }
    }

    /** Prefixes prevent a dynamic member ID from colliding with a static consumer ID. */
    public static String identity(String memberId, Optional<String> groupInstanceId) {
        String id = groupInstanceId.orElse(memberId);
        requireString(id);
        return (groupInstanceId.isPresent() ? "s:" : "m:") + id;
    }

    /** Encodes the subscription for the load-aware-v2 assignor protocol. */
    public static ByteBuffer subscription(String instanceId, boolean monitoring, long revision) {
        requireRevision(revision);
        Writer writer = new Writer();
        writer.integer(SUBSCRIPTION_MAGIC);
        writer.shortInteger(VERSION);
        writer.string(instanceId);
        writer.flag(monitoring);
        writer.longInteger(revision);
        return ByteBuffer.wrap(writer.bytes.toByteArray());
    }

    /** Returns null for absent/unreadable data, without moving the caller's buffer. */
    public static Subscription readSubscription(ByteBuffer userData) {
        if (userData == null || userData.remaining() > MAX_TOTAL_BYTES) {
            return null;
        }
        try {
            ByteBuffer buffer = userData.duplicate();
            if (buffer.getInt() != SUBSCRIPTION_MAGIC || buffer.getShort() != VERSION) {
                return null;
            }
            String instanceId = getString(buffer);
            if (buffer.remaining() != 1 + Long.BYTES) {
                return null;
            }
            byte monitoring = buffer.get();
            if (monitoring != 0 && monitoring != 1) {
                return null;
            }
            return new Subscription(instanceId, monitoring == 1, buffer.getLong());
        } catch (RuntimeException e) {
            return null;
        }
    }

    /**
     * Encodes a deterministic payload. Throws IllegalArgumentException if it exceeds the
     * budget; callers must additionally check the sum of all assignment payloads.
     */
    public static ByteBuffer assignment(Assignment assignment) {
        if (assignment == null) {
            throw new IllegalArgumentException("Missing assignment");
        }
        Writer writer = new Writer();
        writer.integer(ASSIGNMENT_MAGIC);
        writer.shortInteger(VERSION);
        writer.longInteger(assignment.snapshotId().getMostSignificantBits());
        writer.longInteger(assignment.snapshotId().getLeastSignificantBits());
        writer.string(assignment.ownerIdentity());
        writer.longInteger(assignment.revision());

        // Insertion order is deterministic: the recipient first, then sorted member IDs.
        Dictionary<String> instances = new Dictionary<>();
        Dictionary<String> topics = new Dictionary<>();
        Dictionary<Set<String>> subscriptions = new Dictionary<>();
        addDictionaries(instances, topics, subscriptions, assignment.instanceId(), assignment.topics());
        for (Member member : assignment.members().values()) {
            addDictionaries(instances, topics, subscriptions, member.instanceId(), member.topics());
        }
        writer.integer(instances.values.size());
        instances.values.forEach(writer::string);
        writer.integer(topics.values.size());
        topics.values.forEach(writer::string);
        writer.integer(subscriptions.values.size());
        for (Set<String> subscription : subscriptions.values) {
            writer.integer(subscription.size());
            subscription.forEach(topic -> writer.integer(topics.indexes.get(topic)));
        }
        writer.integer(instances.indexes.get(assignment.instanceId()));
        writer.integer(subscriptions.indexes.get(assignment.topics()));
        writer.integer(assignment.members().size());
        assignment.members().forEach((identity, member) -> {
            writer.string(identity);
            writer.integer(instances.indexes.get(member.instanceId()));
            writer.integer(subscriptions.indexes.get(member.topics()));
        });
        return ByteBuffer.wrap(writer.bytes.toByteArray());
    }

    /** Returns null for absent, oversized or malformed data; input position is preserved. */
    public static Assignment readAssignment(ByteBuffer userData) {
        if (userData == null || userData.remaining() > MAX_TOTAL_BYTES) {
            return null;
        }
        try {
            ByteBuffer buffer = userData.duplicate();
            if (buffer.getInt() != ASSIGNMENT_MAGIC || buffer.getShort() != VERSION) {
                return null;
            }
            UUID snapshotId = new UUID(buffer.getLong(), buffer.getLong());
            String owner = getString(buffer);
            long revision = buffer.getLong();
            List<String> instances = readStrings(buffer);
            List<String> topics = readStrings(buffer);
            int subscriptionCount = getCount(buffer, Integer.BYTES);
            List<Set<String>> subscriptions = new ArrayList<>(subscriptionCount);
            Set<Set<String>> uniqueSubscriptions = new HashSet<>();
            for (int i = 0; i < subscriptionCount; i++) {
                int topicCount = getCount(buffer, Integer.BYTES);
                Set<String> subscription = new TreeSet<>();
                for (int j = 0; j < topicCount; j++) {
                    if (!subscription.add(topics.get(buffer.getInt()))) {
                        return null;
                    }
                }
                if (!uniqueSubscriptions.add(subscription)) {
                    return null;
                }
                subscriptions.add(immutableTopics(subscription));
            }
            String instanceId = instances.get(buffer.getInt());
            Set<String> recipientTopics = subscriptions.get(buffer.getInt());
            int memberCount = getCount(buffer, Short.BYTES + 3 + 2 * Integer.BYTES);
            Map<String, Member> members = new TreeMap<>();
            for (int i = 0; i < memberCount; i++) {
                String identity = getString(buffer);
                Member member = new Member(instances.get(buffer.getInt()), subscriptions.get(buffer.getInt()));
                if (members.put(identity, member) != null) {
                    return null;
                }
            }
            if (buffer.hasRemaining()) {
                return null;
            }
            return new Assignment(snapshotId, owner, instanceId, recipientTopics, revision, members);
        } catch (RuntimeException e) {
            return null;
        }
    }

    private static void addDictionaries(Dictionary<String> instances, Dictionary<String> topics,
                                        Dictionary<Set<String>> subscriptions, String instance,
                                        Set<String> subscription) {
        instances.add(instance);
        subscription.forEach(topics::add);
        subscriptions.add(subscription);
    }

    private static List<String> readStrings(ByteBuffer buffer) {
        int count = getCount(buffer, Short.BYTES + 1);
        List<String> values = new ArrayList<>(count);
        Set<String> unique = new HashSet<>();
        for (int i = 0; i < count; i++) {
            String value = getString(buffer);
            if (!unique.add(value)) {
                throw new IllegalArgumentException("Duplicate dictionary entry");
            }
            values.add(value);
        }
        return values;
    }

    private static int getCount(ByteBuffer buffer, int minimumEntryBytes) {
        int count = buffer.getInt();
        if (count < 0 || count > buffer.remaining() / minimumEntryBytes) {
            throw new IllegalArgumentException("Invalid entry count");
        }
        return count;
    }

    private static String getString(ByteBuffer buffer) {
        int length = buffer.getShort();
        if (length < 1 || length > buffer.remaining()) {
            throw new IllegalArgumentException("Invalid string length");
        }
        ByteBuffer bytes = buffer.slice();
        bytes.limit(length);
        try {
            String value = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(bytes).toString();
            requireString(value);
            buffer.position(buffer.position() + length);
            return value;
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException("Invalid UTF-8", e);
        }
    }

    private static Set<String> immutableTopics(Set<String> topics) {
        if (topics instanceof Topics) {
            return topics;
        }
        if (topics == null || topics.size() > MAX_TOTAL_BYTES / (Short.BYTES + 1)) {
            throw new IllegalArgumentException("Missing or oversized subscriptions");
        }
        topics.forEach(MonitoringProtocol::requireString);
        return new Topics(topics);
    }

    private static void requireString(String value) {
        if (value == null || value.isBlank() || value.length() > MAX_STRING_BYTES) {
            throw new IllegalArgumentException("Missing or oversized string");
        }
    }

    private static void requireIdentity(String value) {
        requireString(value);
        if ((!value.startsWith("s:") && !value.startsWith("m:")) || value.substring(2).isBlank()) {
            throw new IllegalArgumentException("Invalid typed consumer identity");
        }
    }

    private static void requireRevision(long revision) {
        if (revision < 0) {
            throw new IllegalArgumentException("Negative request revision");
        }
    }

    /** Shared dictionary sets stay shared after decoding, avoiding members x topics copies. */
    private static final class Topics extends AbstractSet<String> {
        private final Set<String> values;
        private final int hashCode;

        Topics(Set<String> values) {
            this.values = Collections.unmodifiableSet(new TreeSet<>(values));
            this.hashCode = values.hashCode();
        }

        @Override
        public Iterator<String> iterator() {
            return values.iterator();
        }

        @Override
        public int size() {
            return values.size();
        }

        @Override
        public boolean contains(Object value) {
            return values.contains(value);
        }

        @Override
        public int hashCode() {
            return hashCode;
        }
    }

    private static final class Dictionary<T> {
        private final List<T> values = new ArrayList<>();
        private final Map<T, Integer> indexes = new HashMap<>();

        void add(T value) {
            if (!indexes.containsKey(value)) {
                if (values.size() >= MAX_TOTAL_BYTES / (Short.BYTES + 1)) {
                    throw new IllegalArgumentException("Oversized dictionary");
                }
                indexes.put(value, values.size());
                values.add(value);
            }
        }
    }

    private static final class Writer {
        private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        void reserve(int count) {
            if (count > MAX_TOTAL_BYTES - bytes.size()) {
                throw new IllegalArgumentException("Monitoring metadata exceeds " + MAX_TOTAL_BYTES + " bytes");
            }
        }

        void shortInteger(int value) {
            reserve(Short.BYTES);
            bytes.write(value >>> 8);
            bytes.write(value);
        }

        void integer(int value) {
            reserve(Integer.BYTES);
            bytes.write(value >>> 24);
            bytes.write(value >>> 16);
            bytes.write(value >>> 8);
            bytes.write(value);
        }

        void flag(boolean value) {
            reserve(1);
            bytes.write(value ? 1 : 0);
        }

        void longInteger(long value) {
            integer((int) (value >>> 32));
            integer((int) value);
        }

        void string(String value) {
            requireString(value);
            byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
            if (encoded.length > MAX_STRING_BYTES) {
                throw new IllegalArgumentException("String exceeds " + MAX_STRING_BYTES + " UTF-8 bytes");
            }
            reserve(Short.BYTES + encoded.length);
            shortInteger(encoded.length);
            bytes.writeBytes(encoded);
        }
    }
}
