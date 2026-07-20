package com.apache.ranger.pulsar;

import com.apache.ranger.pulsar.plugin.RangerPulsarPlugin;
import com.apache.ranger.pulsar.resource.RangerPulsarResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static com.apache.ranger.pulsar.RangerPulsarConstants.*;
import static org.junit.jupiter.api.Assertions.*;

public class RangerPulsarIntegrationTest {

    private RangerPulsarPlugin plugin;

    @BeforeEach
    public void setUp() {
        plugin = RangerPulsarPlugin.getInstance();
        plugin.init();
    }

    @AfterEach
    public void tearDown() {
        plugin.cleanup();
    }

    @Test
    public void testFullPolicyHierarchy() {
        RangerPulsarResource resource = new RangerPulsarResource("standalone", "public/default", "test-topic");
        assertTrue(plugin.isAccessAllowed("admin", Set.of(), ACCESS_TYPE_ADMIN, resource));
        assertTrue(plugin.isAccessAllowed("admin", Set.of(), ACCESS_TYPE_PRODUCE, resource));
        assertTrue(plugin.isAccessAllowed("admin", Set.of(), ACCESS_TYPE_CONSUME, resource));
    }

    @Test
    public void testProducerConsumerSeparation() {
        RangerPulsarResource resource = new RangerPulsarResource("standalone", "public/default", "test-topic");
        assertTrue(plugin.isAccessAllowed("producer1", Set.of(), ACCESS_TYPE_PRODUCE, resource));
        assertFalse(plugin.isAccessAllowed("producer1", Set.of(), ACCESS_TYPE_CONSUME, resource));
        assertTrue(plugin.isAccessAllowed("consumer1", Set.of(), ACCESS_TYPE_CONSUME, resource));
        assertFalse(plugin.isAccessAllowed("consumer1", Set.of(), ACCESS_TYPE_PRODUCE, resource));
    }

    @Test
    public void testLookupAccessForAllUsers() {
        RangerPulsarResource resource = new RangerPulsarResource("standalone", "public/default", "test-topic");
        assertTrue(plugin.isAccessAllowed("any-user", Set.of(), ACCESS_TYPE_LOOKUP, resource));
        assertTrue(plugin.isAccessAllowed("admin", Set.of(), ACCESS_TYPE_LOOKUP, resource));
        assertTrue(plugin.isAccessAllowed("producer1", Set.of(), ACCESS_TYPE_LOOKUP, resource));
    }

    @Test
    public void testUnknownUserDenied() {
        RangerPulsarResource resource = new RangerPulsarResource("standalone", "public/default", "test-topic");
        assertFalse(plugin.isAccessAllowed("unknown", Set.of(), ACCESS_TYPE_PRODUCE, resource));
        assertFalse(plugin.isAccessAllowed("unknown", Set.of(), ACCESS_TYPE_CONSUME, resource));
        assertFalse(plugin.isAccessAllowed("unknown", Set.of(), ACCESS_TYPE_ADMIN, resource));
    }

    @Test
    public void testWildcardTopicMatching() {
        RangerPulsarResource r1 = new RangerPulsarResource("standalone", "public/default", "topic1");
        RangerPulsarResource r2 = new RangerPulsarResource("standalone", "public/default", "topic2");
        RangerPulsarResource r3 = new RangerPulsarResource("standalone", "public/default", "another-topic");
        assertTrue(plugin.isAccessAllowed("producer1", Set.of(), ACCESS_TYPE_PRODUCE, r1));
        assertTrue(plugin.isAccessAllowed("producer1", Set.of(), ACCESS_TYPE_PRODUCE, r2));
        assertTrue(plugin.isAccessAllowed("producer1", Set.of(), ACCESS_TYPE_PRODUCE, r3));
    }

    @Test
    public void testDifferentClusterDenied() {
        RangerPulsarResource resource = new RangerPulsarResource("other-cluster", "public/default", "test-topic");
        assertFalse(plugin.isAccessAllowed("admin", Set.of(), ACCESS_TYPE_ADMIN, resource));
        assertFalse(plugin.isAccessAllowed("producer1", Set.of(), ACCESS_TYPE_PRODUCE, resource));
    }

    @Test
    public void testDifferentNamespaceDenied() {
        RangerPulsarResource resource = new RangerPulsarResource("standalone", "public/other", "test-topic");
        assertFalse(plugin.isAccessAllowed("producer1", Set.of(), ACCESS_TYPE_PRODUCE, resource));
        assertFalse(plugin.isAccessAllowed("consumer1", Set.of(), ACCESS_TYPE_CONSUME, resource));
    }

    @Test
    public void testCustomPolicyAddition() {
        RangerPulsarResource resource = new RangerPulsarResource("standalone", "public/custom", "custom-topic");
        assertFalse(plugin.isAccessAllowed("custom-user", Set.of(), ACCESS_TYPE_PRODUCE, resource));
        plugin.addPolicy(ACCESS_TYPE_PRODUCE, "standalone", "public/custom", "custom-topic", "*", Set.of("custom-user"));
        assertTrue(plugin.isAccessAllowed("custom-user", Set.of(), ACCESS_TYPE_PRODUCE, resource));
    }

    @Test
    public void testEmptyParameters() {
        RangerPulsarResource resource = new RangerPulsarResource("standalone", "public/default", "test-topic");
        assertFalse(plugin.isAccessAllowed(null, Set.of(), ACCESS_TYPE_PRODUCE, resource));
        assertFalse(plugin.isAccessAllowed("admin", Set.of(), null, resource));
        assertFalse(plugin.isAccessAllowed("admin", Set.of(), ACCESS_TYPE_PRODUCE, null));
    }

    @Test
    public void testResourceWithAllLevels() {
        RangerPulsarResource resource = new RangerPulsarResource("standalone", "public/default", "test-topic", "test-subscription");
        assertTrue(plugin.isAccessAllowed("consumer1", Set.of(), ACCESS_TYPE_CONSUME, resource));
    }
}
