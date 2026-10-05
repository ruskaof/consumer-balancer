package io.github.ruskaof.balancer.instance;

import io.github.ruskaof.balancer.instance.MonitoringProtocol.Assignment;
import io.github.ruskaof.balancer.instance.MonitoringProtocol.Member;
import io.github.ruskaof.balancer.instance.MonitoringProtocol.Subscription;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class MonitoringProtocolTest {
    private static final UUID SNAPSHOT = UUID.fromString("a2112751-100d-4e08-9958-de943f216489");
    private static final Set<String> TOPICS = Set.of("orders", "payments");
    private static final String OWNER = "s:consumer-00000";

    @Test
    void roundTripsOwnerAndFollowerAssignments() {
        Assignment owner = owner(Map.of(OWNER, new Member("pod-a", TOPICS),
                "m:member-b", new Member("pod-b", Set.of("shipments"))));
        Assignment follower = new Assignment(SNAPSHOT, OWNER, "pod-b", Set.of("shipments"), 17, Map.of());

        assertEquals(owner, MonitoringProtocol.readAssignment(MonitoringProtocol.assignment(owner)));
        assertEquals(follower, MonitoringProtocol.readAssignment(MonitoringProtocol.assignment(follower)));
        assertTrue(MonitoringProtocol.assignment(follower).remaining()
                < MonitoringProtocol.assignment(owner).remaining());
    }

    @Test
    void subscriptionPrefixRemainsReadableAndLegacySubscriptionsAreNotMonitorCandidates() {
        ByteBuffer encoded = MonitoringProtocol.subscription("pod-a", true, 19);

        assertEquals("pod-a", InstanceUserData.decode(encoded).instanceId());
        assertEquals(new Subscription("pod-a", true, 19), MonitoringProtocol.readSubscription(encoded));
        assertEquals(new Subscription("pod-a", false, 0),
                MonitoringProtocol.readSubscription(InstanceUserData.encode("pod-a")));
        assertEquals(new Subscription("pod-a", false, 20),
                MonitoringProtocol.readSubscription(MonitoringProtocol.subscription("pod-a", false, 20)));
    }

    @Test
    void staticAndDynamicIdentitiesCannotCollide() {
        assertEquals("s:stable", MonitoringProtocol.identity("current", Optional.of("stable")));
        assertEquals("m:stable", MonitoringProtocol.identity("stable", Optional.empty()));
        assertNotEquals(MonitoringProtocol.identity("x", Optional.of("same")),
                MonitoringProtocol.identity("same", Optional.empty()));
        assertEquals(MonitoringProtocol.identity("before-restart", Optional.of("stable")),
                MonitoringProtocol.identity("after-restart", Optional.of("stable")));
    }

    @Test
    void assignmentIsDistinctFromTheLegacyMappingProtocol() {
        ByteBuffer legacy = GroupInstanceUserData.encode(Map.of("member-a", "pod-a"));
        ByteBuffer current = MonitoringProtocol.assignment(owner(Map.of(OWNER, new Member("pod-a", TOPICS))));

        assertNull(MonitoringProtocol.readAssignment(legacy));
        assertFalse(GroupInstanceUserData.decode(current).ok());
    }

    @Test
    void recordsTakeDefensiveImmutableCopiesAndEncodingIsDeterministic() {
        Set<String> topics = new HashSet<>(TOPICS);
        Map<String, Member> members = new LinkedHashMap<>();
        members.put("m:member-b", new Member("pod-b", Set.of("shipments")));
        members.put(OWNER, new Member("pod-a", topics));
        Assignment assignment = new Assignment(SNAPSHOT, OWNER, "pod-a", topics, 3, members);
        topics.clear();
        members.clear();

        assertEquals(TOPICS, assignment.topics());
        assertEquals(TOPICS, assignment.members().get(OWNER).topics());
        assertEquals(2, assignment.members().size());
        assertThrows(UnsupportedOperationException.class, () -> assignment.topics().add("extra"));
        assertThrows(UnsupportedOperationException.class, () -> assignment.members().clear());
        assertThrows(UnsupportedOperationException.class,
                () -> assignment.members().get(OWNER).topics().clear());
        assertEquals(MonitoringProtocol.assignment(assignment), MonitoringProtocol.assignment(owner(Map.of(
                OWNER, new Member("pod-a", Set.of("payments", "orders")),
                "m:member-b", new Member("pod-b", Set.of("shipments"))))));
    }

    @Test
    void codecHandlesNonAsciiStringsAndEmptySubscriptions() {
        Assignment assignment = new Assignment(SNAPSHOT, "s:участник", "インスタンス",
                Set.of("события"), 0, Map.of("s:участник", new Member("インスタンス", Set.of())));

        assertEquals(assignment, MonitoringProtocol.readAssignment(MonitoringProtocol.assignment(assignment)));
        assertEquals(new Subscription("инстанс", true, 1),
                MonitoringProtocol.readSubscription(MonitoringProtocol.subscription("инстанс", true, 1)));
    }

    @Test
    void decodingPreservesPositionAndWorksWithReadOnlyLittleEndianInputs() {
        Assignment assignment = owner(Map.of(OWNER, new Member("pod-a", TOPICS)));
        ByteBuffer encoded = MonitoringProtocol.assignment(assignment);
        ByteBuffer framed = ByteBuffer.allocate(encoded.remaining() + 8).putLong(0).put(encoded);
        framed.flip().position(8);
        ByteBuffer input = framed.asReadOnlyBuffer().order(ByteOrder.LITTLE_ENDIAN);

        assertEquals(assignment, MonitoringProtocol.readAssignment(input));
        assertEquals(8, input.position());
        ByteBuffer subscription = MonitoringProtocol.subscription("pod-a", true, 1).asReadOnlyBuffer();
        assertNotNull(MonitoringProtocol.readSubscription(subscription));
        assertEquals(0, subscription.position());
    }

    @Test
    void absentUnknownVersionAndTrailingBytesAreRejected() {
        assertNull(MonitoringProtocol.readAssignment(null));
        assertNull(MonitoringProtocol.readAssignment(ByteBuffer.allocate(0)));
        assertNull(MonitoringProtocol.readSubscription(null));
        assertNull(MonitoringProtocol.readSubscription(ByteBuffer.allocate(0)));

        ByteBuffer encoded = MonitoringProtocol.assignment(owner(Map.of(OWNER, new Member("pod-a", TOPICS))));
        encoded.putShort(Integer.BYTES, (short) 2);
        assertNull(MonitoringProtocol.readAssignment(encoded));
        encoded.putShort(Integer.BYTES, (short) 1);
        ByteBuffer trailing = ByteBuffer.allocate(encoded.remaining() + 1).put(encoded).put((byte) 0).flip();
        assertNull(MonitoringProtocol.readAssignment(trailing));

        ByteBuffer subscription = MonitoringProtocol.subscription("pod-a", true, 3);
        subscription.putShort(InstanceUserData.encode("pod-a").remaining() + Integer.BYTES, (short) 2);
        assertNull(MonitoringProtocol.readSubscription(subscription));
    }

    @Test
    void everyTruncatedAssignmentAndSubscriptionExtensionIsRejected() {
        ByteBuffer assignment = MonitoringProtocol.assignment(owner(Map.of(OWNER, new Member("pod-a", TOPICS))));
        for (int length = 0; length < assignment.remaining(); length++) {
            assertNull(MonitoringProtocol.readAssignment(assignment.duplicate().limit(length)), "length " + length);
        }

        ByteBuffer subscription = MonitoringProtocol.subscription("pod-a", true, 4);
        int legacyLength = InstanceUserData.encode("pod-a").remaining();
        for (int length = 0; length < subscription.remaining(); length++) {
            if (length != legacyLength) {
                assertNull(MonitoringProtocol.readSubscription(subscription.duplicate().limit(length)),
                        "length " + length);
            }
        }
    }

    @Test
    void rejectsMalformedFlagsRevisionsCountsIndexesAndUtf8() {
        ByteBuffer subscription = MonitoringProtocol.subscription("pod-a", true, 4);
        subscription.put(subscription.limit() - Long.BYTES - 1, (byte) 2);
        assertNull(MonitoringProtocol.readSubscription(subscription));
        subscription.put(subscription.limit() - Long.BYTES - 1, (byte) 1);
        subscription.putLong(subscription.limit() - Long.BYTES, -1);
        assertNull(MonitoringProtocol.readSubscription(subscription));

        ByteBuffer assignment = MonitoringProtocol.assignment(owner(Map.of(OWNER, new Member("pod-a", TOPICS))));
        int firstCount = Integer.BYTES + Short.BYTES + 2 * Long.BYTES
                + Short.BYTES + OWNER.length() + Long.BYTES;
        assignment.putInt(firstCount, Integer.MAX_VALUE);
        assertNull(MonitoringProtocol.readAssignment(assignment));
        assignment.putInt(firstCount, -1);
        assertNull(MonitoringProtocol.readAssignment(assignment));

        assignment = MonitoringProtocol.assignment(owner(Map.of(OWNER, new Member("pod-a", TOPICS))));
        assignment.putInt(assignment.limit() - Integer.BYTES, Integer.MAX_VALUE);
        assertNull(MonitoringProtocol.readAssignment(assignment));

        assignment = MonitoringProtocol.assignment(owner(Map.of(OWNER, new Member("pod-a", TOPICS))));
        assignment.put(Integer.BYTES + Short.BYTES + 2 * Long.BYTES + Short.BYTES, (byte) 0xFF);
        assertNull(MonitoringProtocol.readAssignment(assignment));
    }

    @Test
    void randomUntrustedDataDoesNotThrow() {
        Random random = new Random(579);
        for (int i = 0; i < 1000; i++) {
            byte[] bytes = new byte[random.nextInt(256)];
            random.nextBytes(bytes);
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            assertDoesNotThrow(() -> MonitoringProtocol.readAssignment(buffer));
            assertDoesNotThrow(() -> MonitoringProtocol.readSubscription(buffer));
            assertEquals(0, buffer.position());
        }
    }

    @Test
    void encodingAndDecodingEnforceTheSizeBudget() {
        ByteBuffer oversized = ByteBuffer.allocate(MonitoringProtocol.MAX_TOTAL_BYTES + 1);
        assertNull(MonitoringProtocol.readAssignment(oversized));
        assertNull(MonitoringProtocol.readSubscription(oversized));

        Map<String, Member> members = new HashMap<>();
        members.put(OWNER, new Member("pod-a", TOPICS));
        for (int i = 0; i < 32; i++) {
            members.put("m:" + i + "x".repeat(20_000), new Member("pod-a", TOPICS));
        }
        assertThrows(IllegalArgumentException.class, () -> MonitoringProtocol.assignment(owner(members)));
        assertThrows(IllegalArgumentException.class, () -> MonitoringProtocol.assignment(
                new Assignment(SNAPSHOT, OWNER, "я".repeat(20_000), TOPICS, 0, Map.of())));
    }

    @Test
    void rejectsInvalidRecordsAndOwnerlessTopology() {
        assertThrows(IllegalArgumentException.class, () -> MonitoringProtocol.subscription("pod-a", true, -1));
        assertThrows(IllegalArgumentException.class, () -> new Member(" ", TOPICS));
        assertThrows(IllegalArgumentException.class, () -> new Member("pod-a", Set.of(" ")));
        assertThrows(IllegalArgumentException.class, () -> owner(Map.of("member", new Member("pod-a", TOPICS))));
        assertThrows(IllegalArgumentException.class, () -> owner(Map.of("s:other", new Member("pod-a", TOPICS))));
        assertThrows(IllegalArgumentException.class,
                () -> new Assignment(SNAPSHOT, "", "pod-a", TOPICS, 0, Map.of()));
    }

    @Test
    void fiveHundredMembersAcrossOneHundredInstancesFitAndGrowLinearly() {
        Map<String, Member> members = group(500);
        String ownerIdentity = members.keySet().stream().sorted().findFirst().orElseThrow();
        Member owner = members.get(ownerIdentity);
        Assignment assignment = new Assignment(SNAPSHOT, ownerIdentity, owner.instanceId(), TOPICS, 0, members);
        assertEquals(assignment, MonitoringProtocol.readAssignment(MonitoringProtocol.assignment(assignment)));

        int smaller = totalGroupBytes(group(250));
        int larger = totalGroupBytes(members);
        assertTrue(larger < MonitoringProtocol.MAX_TOTAL_BYTES, "500-member metadata bytes: " + larger);
        assertTrue(larger > smaller * 1.9 && larger < smaller * 2.1,
                "Doubling members must approximately double bytes: " + smaller + " -> " + larger);
    }

    @Test
    void decodedMembersShareTheirSubscriptionDictionaryEntry() {
        Assignment assignment = owner(Map.of(OWNER, new Member("pod-a", TOPICS),
                "s:follower", new Member("pod-b", TOPICS)));
        Assignment decoded = MonitoringProtocol.readAssignment(MonitoringProtocol.assignment(assignment));

        assertNotNull(decoded);
        assertSame(decoded.topics(), decoded.members().get(OWNER).topics());
        assertSame(decoded.topics(), decoded.members().get("s:follower").topics());
    }

    private static Assignment owner(Map<String, Member> members) {
        return new Assignment(SNAPSHOT, OWNER, "pod-a", TOPICS, 3, members);
    }

    private static Map<String, Member> group(int count) {
        Map<String, Member> members = new HashMap<>();
        for (int i = 0; i < count; i++) {
            String identity = "s:order-processing-consumer-" + String.format("%05d", i)
                    + "-93268138-ddcf-4f4d-93d3-c4f2c82e73e6";
            String instance = new UUID(0x9afb5a52ef884c8aL, i / 5).toString();
            members.put(identity, new Member(instance, TOPICS));
        }
        return members;
    }

    private static int totalGroupBytes(Map<String, Member> members) {
        String owner = members.keySet().stream().sorted().findFirst().orElseThrow();
        int total = 0;
        for (var entry : members.entrySet()) {
            Member member = entry.getValue();
            total += MonitoringProtocol.assignment(new Assignment(SNAPSHOT, owner, member.instanceId(),
                    member.topics(), 0, entry.getKey().equals(owner) ? members : Map.of())).remaining();
        }
        return total;
    }
}
