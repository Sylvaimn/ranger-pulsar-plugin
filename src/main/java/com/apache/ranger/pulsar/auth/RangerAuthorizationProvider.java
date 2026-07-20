package com.apache.ranger.pulsar.auth;

import com.apache.ranger.pulsar.RangerPulsarConstants;
import com.apache.ranger.pulsar.plugin.RangerPulsarPlugin;
import com.apache.ranger.pulsar.resource.RangerPulsarResource;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authorization.AuthorizationProvider;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.NamespaceOperation;
import org.apache.pulsar.common.policies.data.PolicyName;
import org.apache.pulsar.common.policies.data.PolicyOperation;
import org.apache.pulsar.common.policies.data.TenantOperation;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Pulsar AuthorizationProvider 实现 - 委托给 Ranger 策略引擎进行权限判断
 */
public class RangerAuthorizationProvider implements AuthorizationProvider {

    private static final Logger LOG = LoggerFactory.getLogger(RangerAuthorizationProvider.class);

    private RangerPulsarPlugin rangerPlugin;
    private String clusterName = "standalone";

    @Override
    public void initialize(ServiceConfiguration conf, org.apache.pulsar.broker.resources.PulsarResources pulsarResources) throws IOException {
        this.clusterName = conf.getClusterName() != null ? conf.getClusterName() : "standalone";
        this.rangerPlugin = RangerPulsarPlugin.getInstance();
        LOG.info("RangerAuthorizationProvider initialized - cluster: {}", clusterName);
    }

    @Override
    public void close() {
        if (rangerPlugin != null) {
            rangerPlugin.cleanup();
        }
        LOG.info("RangerAuthorizationProvider closed");
    }

    // ==================== 核心权限检查 ====================

    @Override
    public CompletableFuture<Boolean> canProduceAsync(TopicName topicName, String role, AuthenticationDataSource authData) {
        return checkAccess(role, RangerPulsarConstants.ACCESS_TYPE_PRODUCE,
                topicName.getNamespace(), topicName.getLocalName());
    }

    @Override
    public CompletableFuture<Boolean> canConsumeAsync(TopicName topicName, String role, AuthenticationDataSource authData, String subscription) {
        return checkAccess(role, RangerPulsarConstants.ACCESS_TYPE_CONSUME,
                topicName.getNamespace(), topicName.getLocalName(), subscription);
    }

    @Override
    public CompletableFuture<Boolean> canLookupAsync(TopicName topicName, String role, AuthenticationDataSource authData) {
        String ns = topicName.getNamespace();
        String topic = topicName.getLocalName();
        // LOOKUP 权限：admin 或有 LOOKUP/PRODUCE/CONSUME 任一权限的用户
        return checkAccess(role, RangerPulsarConstants.ACCESS_TYPE_ADMIN, "*")
                .thenCompose(isAdmin -> {
                    if (isAdmin) return CompletableFuture.completedFuture(true);
                    return checkAccess(role, RangerPulsarConstants.ACCESS_TYPE_LOOKUP, ns, topic);
                })
                .thenCompose(allowed -> {
                    if (allowed) return CompletableFuture.completedFuture(true);
                    return checkAccess(role, RangerPulsarConstants.ACCESS_TYPE_PRODUCE, ns, topic);
                })
                .thenCompose(allowed -> {
                    if (allowed) return CompletableFuture.completedFuture(true);
                    return checkAccess(role, RangerPulsarConstants.ACCESS_TYPE_CONSUME, ns, topic);
                });
    }

    @Override
    public CompletableFuture<Boolean> allowFunctionOpsAsync(NamespaceName namespaceName, String role, AuthenticationDataSource authData) {
        return checkAccess(role, RangerPulsarConstants.ACCESS_TYPE_FUNCTION, namespaceName.getLocalName());
    }

    @Override
    public CompletableFuture<Boolean> allowSourceOpsAsync(NamespaceName namespaceName, String role, AuthenticationDataSource authData) {
        return checkAccess(role, RangerPulsarConstants.ACCESS_TYPE_FUNCTION, namespaceName.getLocalName());
    }

    @Override
    public CompletableFuture<Boolean> allowSinkOpsAsync(NamespaceName namespaceName, String role, AuthenticationDataSource authData) {
        return checkAccess(role, RangerPulsarConstants.ACCESS_TYPE_FUNCTION, namespaceName.getLocalName());
    }

    // ==================== Admin 权限检查 ====================

    @Override
    public CompletableFuture<Boolean> isSuperUser(String role, AuthenticationDataSource authData, ServiceConfiguration conf) {
        return checkAccess(role, RangerPulsarConstants.ACCESS_TYPE_ADMIN, "*");
    }

    @Override
    public CompletableFuture<Boolean> isSuperUser(String role, ServiceConfiguration conf) {
        return checkAccess(role, RangerPulsarConstants.ACCESS_TYPE_ADMIN, "*");
    }

    @Override
    public CompletableFuture<Boolean> allowTenantOperationAsync(String tenantName, String role, TenantOperation operation, AuthenticationDataSource authData) {
        return checkAccess(role, RangerPulsarConstants.ACCESS_TYPE_ADMIN, tenantName);
    }

    @Override
    public CompletableFuture<Boolean> allowNamespaceOperationAsync(NamespaceName namespaceName, String role, NamespaceOperation operation, AuthenticationDataSource authData) {
        return checkAccess(role, RangerPulsarConstants.ACCESS_TYPE_ADMIN, namespaceName.getLocalName());
    }

    @Override
    public CompletableFuture<Boolean> allowTopicOperationAsync(TopicName topicName, String role, TopicOperation operation, AuthenticationDataSource authData) {
        String ns = topicName.getNamespace();
        String topic = topicName.getLocalName();
        String opName = operation != null ? operation.name() : "null";
        LOG.debug("allowTopicOperationAsync - role={}, operation={}, topic={}/{}", role, opName, ns, topic);

        // 先检查是否是 admin
        return checkAccess(role, RangerPulsarConstants.ACCESS_TYPE_ADMIN, "*")
                .thenCompose(isAdmin -> {
                    if (isAdmin) {
                        LOG.debug("allowTopicOperationAsync - admin granted for role={}, op={}", role, opName);
                        return CompletableFuture.completedFuture(true);
                    }

                    // 根据操作类型检查对应权限
                    if ("PRODUCE".equals(opName)) {
                        return checkAccess(role, RangerPulsarConstants.ACCESS_TYPE_PRODUCE, ns, topic);
                    }
                    if ("CONSUME".equals(opName) || "SUBSCRIBE".equals(opName)) {
                        return checkAccess(role, RangerPulsarConstants.ACCESS_TYPE_CONSUME, ns, topic);
                    }
                    if ("LOOKUP".equals(opName) || "GET_METADATA".equals(opName) || "GET_STATS".equals(opName)
                            || opName.startsWith("GET_")) {
                        // LOOKUP / 读取类操作：允许有 LOOKUP/PRODUCE/CONSUME 任一权限的用户
                        return checkAccess(role, RangerPulsarConstants.ACCESS_TYPE_LOOKUP, ns, topic)
                                .thenCompose(allowed -> {
                                    if (allowed) return CompletableFuture.completedFuture(true);
                                    return checkAccess(role, RangerPulsarConstants.ACCESS_TYPE_PRODUCE, ns, topic);
                                })
                                .thenCompose(allowed -> {
                                    if (allowed) return CompletableFuture.completedFuture(true);
                                    return checkAccess(role, RangerPulsarConstants.ACCESS_TYPE_CONSUME, ns, topic);
                                });
                    }

                    // 其他操作（管理类）：需要 ADMIN 权限
                    LOG.debug("allowTopicOperationAsync - fallback to admin check for role={}, op={}", role, opName);
                    return checkAccess(role, RangerPulsarConstants.ACCESS_TYPE_ADMIN, ns, topic);
                });
    }

    // ==================== 权限管理（Ranger 外部管理，返回不支持）====================

    @Override
    public CompletableFuture<Void> grantPermissionAsync(NamespaceName namespaceName, Set<AuthAction> actions, String role, String authDataJson) {
        LOG.warn("grantPermissionAsync not supported - Ranger manages permissions externally");
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> grantSubscriptionPermissionAsync(NamespaceName namespaceName, String subscription, Set<String> roles, String authDataJson) {
        LOG.warn("grantSubscriptionPermissionAsync not supported - Ranger manages permissions externally");
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> revokeSubscriptionPermissionAsync(NamespaceName namespaceName, String subscription, String role, String authDataJson) {
        LOG.warn("revokeSubscriptionPermissionAsync not supported - Ranger manages permissions externally");
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> grantPermissionAsync(TopicName topicName, Set<AuthAction> actions, String role, String authDataJson) {
        LOG.warn("grantPermissionAsync(topic) not supported - Ranger manages permissions externally");
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> revokePermissionAsync(NamespaceName namespaceName, String role) {
        LOG.warn("revokePermissionAsync not supported - Ranger manages permissions externally");
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> revokePermissionAsync(TopicName topicName, String role) {
        LOG.warn("revokePermissionAsync(topic) not supported - Ranger manages permissions externally");
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> removePermissionsAsync(TopicName topicName) {
        LOG.warn("removePermissionsAsync not supported - Ranger manages permissions externally");
        return CompletableFuture.completedFuture(null);
    }

    // ==================== 权限查询 ====================

    @Override
    public CompletableFuture<Map<String, Set<AuthAction>>> getPermissionsAsync(TopicName topicName) {
        LOG.warn("getPermissionsAsync not fully implemented");
        return CompletableFuture.completedFuture(Collections.emptyMap());
    }

    @Override
    public CompletableFuture<Map<String, Set<String>>> getSubscriptionPermissionsAsync(NamespaceName namespaceName) {
        LOG.warn("getSubscriptionPermissionsAsync not fully implemented");
        return CompletableFuture.completedFuture(Collections.emptyMap());
    }

    @Override
    public CompletableFuture<Map<String, Set<AuthAction>>> getPermissionsAsync(NamespaceName namespaceName) {
        LOG.warn("getPermissionsAsync(namespace) not fully implemented");
        return CompletableFuture.completedFuture(Collections.emptyMap());
    }

    // ==================== 默认实现覆盖（简化逻辑）====================

    @Override
    public Boolean allowTenantOperation(String tenantName, String role, TenantOperation operation, AuthenticationDataSource authData) {
        return checkAccess(role, RangerPulsarConstants.ACCESS_TYPE_ADMIN, tenantName).join();
    }

    @Override
    public Boolean allowNamespaceOperation(NamespaceName namespaceName, String role, NamespaceOperation operation, AuthenticationDataSource authData) {
        return checkAccess(role, RangerPulsarConstants.ACCESS_TYPE_ADMIN, namespaceName.getLocalName()).join();
    }

    @Override
    public Boolean allowNamespacePolicyOperation(NamespaceName namespaceName, PolicyName policy, PolicyOperation operation, String role, AuthenticationDataSource authData) {
        return checkAccess(role, RangerPulsarConstants.ACCESS_TYPE_ADMIN, namespaceName.getLocalName()).join();
    }

    @Override
    public Boolean allowTopicOperation(TopicName topicName, String role, TopicOperation operation, AuthenticationDataSource authData) {
        return allowTopicOperationAsync(topicName, role, operation, authData).join();
    }

    @Override
    public Boolean allowTopicPolicyOperation(TopicName topicName, String role, PolicyName policy, PolicyOperation operation, AuthenticationDataSource authData) {
        return allowTopicOperationAsync(topicName, role, TopicOperation.LOOKUP, authData).join();
    }

    // ==================== 内部辅助方法 ====================

    private CompletableFuture<Boolean> checkAccess(String role, String accessType, String namespace) {
        if (rangerPlugin == null) {
            LOG.error("Ranger plugin is not initialized");
            return CompletableFuture.completedFuture(false);
        }
        RangerPulsarResource resource = new RangerPulsarResource(clusterName, namespace);
        boolean allowed = rangerPlugin.isAccessAllowed(role, Collections.emptySet(), accessType, resource);
        return CompletableFuture.completedFuture(allowed);
    }

    private CompletableFuture<Boolean> checkAccess(String role, String accessType, String namespace, String topic) {
        if (rangerPlugin == null) {
            LOG.error("Ranger plugin is not initialized");
            return CompletableFuture.completedFuture(false);
        }
        RangerPulsarResource resource = new RangerPulsarResource(clusterName, namespace, topic);
        boolean allowed = rangerPlugin.isAccessAllowed(role, Collections.emptySet(), accessType, resource);
        return CompletableFuture.completedFuture(allowed);
    }

    private CompletableFuture<Boolean> checkAccess(String role, String accessType, String namespace, String topic, String subscription) {
        if (rangerPlugin == null) {
            LOG.error("Ranger plugin is not initialized");
            return CompletableFuture.completedFuture(false);
        }
        RangerPulsarResource resource = new RangerPulsarResource(clusterName, namespace, topic, subscription);
        boolean allowed = rangerPlugin.isAccessAllowed(role, Collections.emptySet(), accessType, resource);
        return CompletableFuture.completedFuture(allowed);
    }
}
