package com.apache.ranger.pulsar.resource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static com.apache.ranger.pulsar.RangerPulsarConstants.*;

/**
 * Pulsar 资源层级模型，支持 Cluster -> Namespace -> Topic -> Subscription 层级化资源定义
 */
public class RangerPulsarResource {

    private final Map<String, String> values;

    public RangerPulsarResource() {
        Map<String, String> m = new HashMap<>();
        m.put(RESOURCE_TYPE_CLUSTER, WILDCARD);
        m.put(RESOURCE_TYPE_NAMESPACE, WILDCARD);
        m.put(RESOURCE_TYPE_TOPIC, WILDCARD);
        m.put(RESOURCE_TYPE_SUBSCRIPTION, WILDCARD);
        this.values = m;
    }

    public RangerPulsarResource(String cluster) {
        Map<String, String> m = new HashMap<>();
        m.put(RESOURCE_TYPE_CLUSTER, cluster);
        m.put(RESOURCE_TYPE_NAMESPACE, WILDCARD);
        m.put(RESOURCE_TYPE_TOPIC, WILDCARD);
        m.put(RESOURCE_TYPE_SUBSCRIPTION, WILDCARD);
        this.values = m;
    }

    public RangerPulsarResource(String cluster, String namespace) {
        Map<String, String> m = new HashMap<>();
        m.put(RESOURCE_TYPE_CLUSTER, cluster);
        m.put(RESOURCE_TYPE_NAMESPACE, namespace);
        m.put(RESOURCE_TYPE_TOPIC, WILDCARD);
        m.put(RESOURCE_TYPE_SUBSCRIPTION, WILDCARD);
        this.values = m;
    }

    public RangerPulsarResource(String cluster, String namespace, String topic) {
        Map<String, String> m = new HashMap<>();
        m.put(RESOURCE_TYPE_CLUSTER, cluster);
        m.put(RESOURCE_TYPE_NAMESPACE, namespace);
        m.put(RESOURCE_TYPE_TOPIC, topic);
        m.put(RESOURCE_TYPE_SUBSCRIPTION, WILDCARD);
        this.values = m;
    }

    public RangerPulsarResource(String cluster, String namespace, String topic, String subscription) {
        Map<String, String> m = new HashMap<>();
        m.put(RESOURCE_TYPE_CLUSTER, cluster);
        m.put(RESOURCE_TYPE_NAMESPACE, namespace);
        m.put(RESOURCE_TYPE_TOPIC, topic);
        m.put(RESOURCE_TYPE_SUBSCRIPTION, subscription);
        this.values = m;
    }

    public void setValue(String key, String value) {
        values.put(key, value);
    }

    public String getValue(String key) {
        return values.get(key);
    }

    public boolean exists(String name) {
        return values.containsKey(name) && values.get(name) != null;
    }

    public boolean isValidLeaf(String name) {
        return exists(name);
    }

    /**
     * 获取资源的层级深度
     */
    public int getDepth() {
        int depth = 0;
        if (!WILDCARD.equals(values.get(RESOURCE_TYPE_CLUSTER))) depth++;
        if (!WILDCARD.equals(values.get(RESOURCE_TYPE_NAMESPACE))) depth++;
        if (!WILDCARD.equals(values.get(RESOURCE_TYPE_TOPIC))) depth++;
        if (!WILDCARD.equals(values.get(RESOURCE_TYPE_SUBSCRIPTION))) depth++;
        return depth;
    }

    public String getAsString() {
        return values.toString();
    }

    /**
     * 获取可读的资源路径字符串
     */
    public String getResourcePath() {
        StringBuilder sb = new StringBuilder();
        sb.append(values.getOrDefault(RESOURCE_TYPE_CLUSTER, WILDCARD));
        sb.append("/").append(values.getOrDefault(RESOURCE_TYPE_NAMESPACE, WILDCARD));
        sb.append("/").append(values.getOrDefault(RESOURCE_TYPE_TOPIC, WILDCARD));
        if (!WILDCARD.equals(values.get(RESOURCE_TYPE_SUBSCRIPTION))) {
            sb.append("/").append(values.get(RESOURCE_TYPE_SUBSCRIPTION));
        }
        return sb.toString();
    }

    public Map<String, String> getValues() {
        return Collections.unmodifiableMap(values);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RangerPulsarResource that = (RangerPulsarResource) o;
        return Objects.equals(values, that.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(values);
    }

    @Override
    public String toString() {
        return "RangerPulsarResource{" + getResourcePath() + "}";
    }
}
