package com.apache.ranger.pulsar;

/**
 * Constants for Ranger Pulsar plugin
 */
public final class RangerPulsarConstants {

    private RangerPulsarConstants() {
    }

    public static final String PLUGIN_TYPE = "pulsar";
    public static final String SERVICE_TYPE = "pulsar";
    public static final String PLUGIN_APP_TYPE = "pulsar";

    public static final String RESOURCE_TYPE_CLUSTER = "cluster";
    public static final String RESOURCE_TYPE_NAMESPACE = "namespace";
    public static final String RESOURCE_TYPE_TOPIC = "topic";
    public static final String RESOURCE_TYPE_SUBSCRIPTION = "subscription";
    public static final String RESOURCE_TYPE_FUNCTION = "function";
    public static final String RESOURCE_TYPE_SOURCE = "source";
    public static final String RESOURCE_TYPE_SINK = "sink";

    public static final String ACCESS_TYPE_ADMIN = "admin";
    public static final String ACCESS_TYPE_PRODUCE = "produce";
    public static final String ACCESS_TYPE_CONSUME = "consume";
    public static final String ACCESS_TYPE_LOOKUP = "lookup";
    public static final String ACCESS_TYPE_FUNCTION = "function";

    public static final String WILDCARD = "*";
    public static final String WILDCARD_ASTERISK = "*";
}
