package com.apache.ranger.pulsar.model;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * 结构化策略模型，替代原始的 String key 方式
 */
public class PolicyModel {

    private final String id;
    private final String accessType;
    private final String cluster;
    private final String namespace;
    private final String topic;
    private final String subscription;
    private final Set<String> allowedUsers;
    private final Set<String> allowedGroups;
    private final boolean delegateAdmin;
    private final int priority;

    public PolicyModel(String id, String accessType, String cluster, String namespace,
                       String topic, String subscription, Set<String> allowedUsers,
                       Set<String> allowedGroups, boolean delegateAdmin, int priority) {
        this.id = id;
        this.accessType = accessType;
        this.cluster = cluster;
        this.namespace = namespace;
        this.topic = topic;
        this.subscription = subscription;
        this.allowedUsers = allowedUsers == null ? Collections.emptySet() : Collections.unmodifiableSet(new HashSet<>(allowedUsers));
        this.allowedGroups = allowedGroups == null ? Collections.emptySet() : Collections.unmodifiableSet(new HashSet<>(allowedGroups));
        this.delegateAdmin = delegateAdmin;
        this.priority = priority;
    }

    public boolean matchesUser(String user) {
        return allowedUsers.contains(user) || allowedUsers.contains("*");
    }

    public boolean matchesGroup(Set<String> groups) {
        if (groups == null || groups.isEmpty()) {
            return false;
        }
        for (String group : groups) {
            if (allowedGroups.contains(group) || allowedGroups.contains("*")) {
                return true;
            }
        }
        return false;
    }

    public boolean matchesResource(String accessType, String cluster, String namespace, String topic, String subscription) {
        if (!wildcardMatch(this.accessType, accessType)) return false;
        if (!wildcardMatch(this.cluster, cluster)) return false;
        if (!wildcardMatch(this.namespace, namespace)) return false;
        if (!wildcardMatch(this.topic, topic)) return false;
        if (!wildcardMatch(this.subscription, subscription)) return false;
        return true;
    }

    /**
     * 支持通配符和正则前缀匹配
     * "*" 匹配任意值
     * "regex:xxx" 使用正则匹配
     * 其他值精确匹配
     */
    private boolean wildcardMatch(String pattern, String value) {
        if (pattern == null || "*".equals(pattern)) {
            return true;
        }
        if (value == null) {
            return false;
        }
        if (pattern.startsWith("regex:")) {
            String regex = pattern.substring(6);
            return value.matches(regex);
        }
        return pattern.equals(value);
    }

    // Getters
    public String getId() { return id; }
    public String getAccessType() { return accessType; }
    public String getCluster() { return cluster; }
    public String getNamespace() { return namespace; }
    public String getTopic() { return topic; }
    public String getSubscription() { return subscription; }
    public Set<String> getAllowedUsers() { return allowedUsers; }
    public Set<String> getAllowedGroups() { return allowedGroups; }
    public boolean isDelegateAdmin() { return delegateAdmin; }
    public int getPriority() { return priority; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PolicyModel that = (PolicyModel) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return "PolicyModel{" +
                "id='" + id + '\'' +
                ", accessType='" + accessType + '\'' +
                ", cluster='" + cluster + '\'' +
                ", namespace='" + namespace + '\'' +
                ", topic='" + topic + '\'' +
                ", users=" + allowedUsers +
                ", groups=" + allowedGroups +
                ", priority=" + priority +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String id;
        private String accessType;
        private String cluster = "*";
        private String namespace = "*";
        private String topic = "*";
        private String subscription = "*";
        private Set<String> allowedUsers = new HashSet<>();
        private Set<String> allowedGroups = new HashSet<>();
        private boolean delegateAdmin = false;
        private int priority = 0;

        public Builder id(String id) { this.id = id; return this; }
        public Builder accessType(String accessType) { this.accessType = accessType; return this; }
        public Builder cluster(String cluster) { this.cluster = cluster; return this; }
        public Builder namespace(String namespace) { this.namespace = namespace; return this; }
        public Builder topic(String topic) { this.topic = topic; return this; }
        public Builder subscription(String subscription) { this.subscription = subscription; return this; }
        public Builder users(Set<String> users) { this.allowedUsers = users; return this; }
        public Builder groups(Set<String> groups) { this.allowedGroups = groups; return this; }
        public Builder delegateAdmin(boolean delegateAdmin) { this.delegateAdmin = delegateAdmin; return this; }
        public Builder priority(int priority) { this.priority = priority; return this; }

        public PolicyModel build() {
            return new PolicyModel(id, accessType, cluster, namespace, topic, subscription,
                    allowedUsers, allowedGroups, delegateAdmin, priority);
        }
    }
}
