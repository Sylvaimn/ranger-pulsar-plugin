package com.apache.ranger.pulsar;

import com.apache.ranger.pulsar.plugin.RangerPulsarPlugin;
import com.apache.ranger.pulsar.resource.RangerPulsarResource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class RangerPulsarPluginTest {

    private RangerPulsarPlugin plugin;

    @BeforeEach
    void setUp() {
        plugin = RangerPulsarPlugin.getInstance();
    }

    @Test
    void testSingletonInstance() {
        RangerPulsarPlugin anotherInstance = RangerPulsarPlugin.getInstance();
        assertSame(plugin, anotherInstance);
    }

    @Test
    void testIsAccessAllowedWithAdminUser() {
        RangerPulsarResource resource = new RangerPulsarResource("standalone", "public/default", "test-topic");
        boolean allowed = plugin.isAccessAllowed("admin", Collections.emptySet(), "admin", resource);
        assertTrue(allowed);
    }

    @Test
    void testIsAccessAllowedWithProducer() {
        RangerPulsarResource resource = new RangerPulsarResource("standalone", "public/default", "test-topic");
        boolean allowed = plugin.isAccessAllowed("producer1", Collections.emptySet(), "produce", resource);
        assertTrue(allowed);
    }

    @Test
    void testIsAccessAllowedWithConsumer() {
        RangerPulsarResource resource = new RangerPulsarResource("standalone", "public/default", "test-topic");
        boolean allowed = plugin.isAccessAllowed("consumer1", Collections.emptySet(), "consume", resource);
        assertTrue(allowed);
    }

    @Test
    void testIsAccessAllowedWithUnknownUser() {
        RangerPulsarResource resource = new RangerPulsarResource("standalone", "public/default", "test-topic");
        boolean allowed = plugin.isAccessAllowed("unknown-user", Collections.emptySet(), "produce", resource);
        assertFalse(allowed);
    }

    @Test
    void testIsAccessAllowedWithLookup() {
        RangerPulsarResource resource = new RangerPulsarResource("standalone", "public/default", "test-topic");
        boolean allowed = plugin.isAccessAllowed("any-user", Collections.emptySet(), "lookup", resource);
        assertTrue(allowed);
    }

    @Test
    void testResourceHierarchy() {
        RangerPulsarResource clusterResource = new RangerPulsarResource("standalone");
        assertNotNull(clusterResource.getValue(RangerPulsarConstants.RESOURCE_TYPE_CLUSTER));
        assertEquals("standalone", clusterResource.getValue(RangerPulsarConstants.RESOURCE_TYPE_CLUSTER));

        RangerPulsarResource nsResource = new RangerPulsarResource("standalone", "public/default");
        assertEquals("public/default", nsResource.getValue(RangerPulsarConstants.RESOURCE_TYPE_NAMESPACE));

        RangerPulsarResource topicResource = new RangerPulsarResource("standalone", "public/default", "my-topic");
        assertEquals("my-topic", topicResource.getValue(RangerPulsarConstants.RESOURCE_TYPE_TOPIC));
    }
}
