package com.apache.ranger.pulsar.plugin;

import com.apache.ranger.pulsar.RangerPulsarConstants;
import com.apache.ranger.pulsar.audit.RangerPulsarAuditHandler;
import com.apache.ranger.pulsar.cache.AccessDecisionCache;
import com.apache.ranger.pulsar.config.RangerPulsarConfig;
import com.apache.ranger.pulsar.model.PolicyModel;
import com.apache.ranger.pulsar.resource.RangerPulsarResource;
import com.apache.ranger.pulsar.sync.PolicyRefresher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

public class RangerPulsarPlugin {

    private static final Logger LOG = LoggerFactory.getLogger(RangerPulsarPlugin.class);

    private static volatile RangerPulsarPlugin instance = null;

    private final List<PolicyModel> policies = new CopyOnWriteArrayList<>();
    private final AccessDecisionCache decisionCache;
    private final RangerPulsarAuditHandler auditHandler;
    private final AtomicLong accessCheckCount = new AtomicLong(0);
    private final AtomicLong deniedCount = new AtomicLong(0);
    private RangerPulsarConfig config;
    private PolicyRefresher refresher;

    private RangerPulsarPlugin() {
        this(RangerPulsarConfig.load());
    }

    public RangerPulsarPlugin(RangerPulsarConfig config) {
        this.config = config;
        this.decisionCache = new AccessDecisionCache(config.getCacheSize());
        this.auditHandler = new RangerPulsarAuditHandler(config.getAuditMaxQueue());
        loadDefaultPolicies();
    }

    public static RangerPulsarPlugin getInstance() {
        if (instance == null) {
            synchronized (RangerPulsarPlugin.class) {
                if (instance == null) {
                    instance = new RangerPulsarPlugin();
                }
            }
        }
        return instance;
    }

    /**
     * 用于测试时重置单例
     */
    public static void resetForTesting() {
        synchronized (RangerPulsarPlugin.class) {
            if (instance != null && instance.refresher != null) {
                instance.refresher.stop();
            }
            instance = null;
        }
    }

    private void loadDefaultPolicies() {
        addPolicy(PolicyModel.builder()
                .id("default-admin")
                .accessType(RangerPulsarConstants.ACCESS_TYPE_ADMIN)
                .cluster("standalone").namespace("*").topic("*").subscription("*")
                .users(Set.of("admin"))
                .groups(Set.of("admin-group"))
                .delegateAdmin(true)
                .priority(100)
                .build());

        addPolicy(PolicyModel.builder()
                .id("default-produce")
                .accessType(RangerPulsarConstants.ACCESS_TYPE_PRODUCE)
                .cluster("standalone").namespace("public/default").topic("*").subscription("*")
                .users(Set.of("producer1", "admin"))
                .groups(Set.of("producer-group"))
                .priority(50)
                .build());

        addPolicy(PolicyModel.builder()
                .id("default-consume")
                .accessType(RangerPulsarConstants.ACCESS_TYPE_CONSUME)
                .cluster("standalone").namespace("public/default").topic("*").subscription("*")
                .users(Set.of("consumer1", "admin"))
                .groups(Set.of("consumer-group"))
                .priority(50)
                .build());

        addPolicy(PolicyModel.builder()
                .id("default-lookup")
                .accessType(RangerPulsarConstants.ACCESS_TYPE_LOOKUP)
                .cluster("standalone").namespace("public/default").topic("*").subscription("*")
                .users(Set.of("*"))
                .priority(10)
                .build());

        LOG.info("Loaded {} default policies", policies.size());
    }

    public void init() {
        LOG.info("RangerPulsarPlugin initialized - config: {}, policies: {}", config, policies.size());

        // 启动策略同步器
        if (config.isSyncEnabled()) {
            this.refresher = new PolicyRefresher(config, this);
            this.refresher.start();
        }
    }

    public void shutdown() {
        if (refresher != null) {
            refresher.stop();
        }
        LOG.info("RangerPulsarPlugin shutdown");
    }

    /**
     * 权限校验核心方法，集成缓存和审计
     */
    public boolean isAccessAllowed(String user, Set<String> groups, String accessType,
                                   RangerPulsarResource resource) {
        accessCheckCount.incrementAndGet();

        if (user == null || accessType == null || resource == null) {
            LOG.warn("Invalid parameters - user: {}, accessType: {}, resource: {}", user, accessType, resource);
            return false;
        }

        String cluster = resource.getValue(RangerPulsarConstants.RESOURCE_TYPE_CLUSTER);
        String namespace = resource.getValue(RangerPulsarConstants.RESOURCE_TYPE_NAMESPACE);
        String topic = resource.getValue(RangerPulsarConstants.RESOURCE_TYPE_TOPIC);
        String subscription = resource.getValue(RangerPulsarConstants.RESOURCE_TYPE_SUBSCRIPTION);

        if (cluster == null) cluster = "*";
        if (namespace == null) namespace = "*";
        if (topic == null) topic = "*";
        if (subscription == null) subscription = "*";

        final String fCluster = cluster;
        final String fNamespace = namespace;
        final String fTopic = topic;
        final String fSubscription = subscription;

        // 1. 查缓存
        String cacheKey = AccessDecisionCache.buildKey(user, accessType, fCluster, fNamespace, fTopic, fSubscription);
        Boolean cached = decisionCache.get(cacheKey);
        if (cached != null) {
            LOG.debug("Cache hit for key: {}", cacheKey);
            auditHandler.logAccess(user, fNamespace + "/" + fTopic, accessType, cached);
            return cached;
        }

        // 2. 遍历策略（按优先级排序）
        List<PolicyModel> sortedPolicies = new ArrayList<>(policies);
        sortedPolicies.sort(Comparator.comparingInt(PolicyModel::getPriority).reversed());

        boolean allowed = false;
        for (PolicyModel policy : sortedPolicies) {
            if (!policy.matchesResource(accessType, fCluster, fNamespace, fTopic, fSubscription)) {
                continue;
            }
            if (policy.matchesUser(user) || policy.matchesGroup(groups)) {
                allowed = true;
                break;
            }
        }

        // 3. 写缓存
        decisionCache.put(cacheKey, allowed);

        // 4. 审计日志
        String resourceStr = fCluster + "/" + fNamespace + "/" + fTopic;
        auditHandler.logAccess(user, resourceStr, accessType, allowed);

        if (!allowed) {
            deniedCount.incrementAndGet();
            LOG.warn("Access denied - user: {}, groups: {}, accessType: {}, resource: {}/{}/{}",
                    user, groups, accessType, fCluster, fNamespace, fTopic);
        }

        return allowed;
    }

    public void addPolicy(PolicyModel policy) {
        if (policy != null) {
            policies.add(policy);
            // 策略变更后清除缓存
            decisionCache.clear();
            LOG.info("Added policy: {}", policy);
        }
    }

    public void addPolicy(String accessType, String cluster, String namespace,
                          String topic, String subscription, Set<String> users) {
        addPolicy(PolicyModel.builder()
                .id("custom-" + System.nanoTime())
                .accessType(accessType)
                .cluster(cluster).namespace(namespace).topic(topic).subscription(subscription)
                .users(users)
                .priority(50)
                .build());
    }

    public boolean removePolicy(String policyId) {
        boolean removed = policies.removeIf(p -> p.getId().equals(policyId));
        if (removed) {
            decisionCache.clear();
            LOG.info("Removed policy: {}", policyId);
        }
        return removed;
    }

    public List<PolicyModel> getPolicies() {
        return Collections.unmodifiableList(policies);
    }

    public AccessDecisionCache.CacheStats getCacheStats() {
        return decisionCache.getStats();
    }

    public long getAccessCheckCount() {
        return accessCheckCount.get();
    }

    public long getDeniedCount() {
        return deniedCount.get();
    }

    public RangerPulsarConfig getConfig() {
        return config;
    }

    public PolicyRefresher.SyncStats getSyncStats() {
        return refresher != null ? refresher.getStats() : new PolicyRefresher.SyncStats(false, 0, 0, "disabled", 0, 0, "", 0);
    }

    public void cleanup() {
        if (refresher != null) {
            refresher.stop();
            refresher = null;
        }
        policies.clear();
        decisionCache.clear();
        accessCheckCount.set(0);
        deniedCount.set(0);
        loadDefaultPolicies();
        LOG.info("RangerPulsarPlugin cleaned up, default policies reloaded");
    }
}
