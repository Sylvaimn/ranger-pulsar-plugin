package com.apache.ranger.pulsar.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 插件外部化配置
 * 支持三层配置覆盖：默认值 < 配置文件 < 环境变量/系统属性
 */
public class RangerPulsarConfig {

    private final Properties props;

    public RangerPulsarConfig() {
        this.props = new Properties();
        loadDefaults();
        overrideFromEnv();
    }

    public RangerPulsarConfig(String configPath) throws IOException {
        this.props = new Properties();
        loadDefaults();
        try (InputStream is = new FileInputStream(configPath)) {
            props.load(is);
        }
        overrideFromEnv();
    }

    public static RangerPulsarConfig load() {
        String configPath = System.getenv("RANGER_PULSAR_CONFIG");
        if (configPath == null) {
            configPath = System.getProperty("ranger.pulsar.config", "");
        }
        if (!configPath.isEmpty()) {
            try {
                return new RangerPulsarConfig(configPath);
            } catch (IOException e) {
                throw new RuntimeException("Failed to load config from: " + configPath, e);
            }
        }
        return new RangerPulsarConfig();
    }

    private void loadDefaults() {
        props.setProperty("ranger.pulsar.service.name", "pulsar");
        props.setProperty("ranger.pulsar.cluster.name", "standalone");
        props.setProperty("ranger.pulsar.cache.size", "10000");
        props.setProperty("ranger.pulsar.cache.ttl.seconds", "300");
        props.setProperty("ranger.pulsar.sync.enabled", "true");
        props.setProperty("ranger.pulsar.sync.interval.seconds", "30");
        props.setProperty("ranger.pulsar.sync.retry.max", "3");
        props.setProperty("ranger.pulsar.sync.retry.backoff.ms", "1000");
        props.setProperty("ranger.pulsar.admin.url", "http://localhost:6080");
        props.setProperty("ranger.pulsar.http.connect.timeout.ms", "5000");
        props.setProperty("ranger.pulsar.http.read.timeout.ms", "10000");
        props.setProperty("ranger.pulsar.auth.enabled", "true");
        props.setProperty("ranger.pulsar.audit.enabled", "true");
        props.setProperty("ranger.pulsar.audit.max.queue", "5000");
        props.setProperty("ranger.pulsar.ssl.enabled", "false");
        props.setProperty("ranger.pulsar.admin.auth.user", "");
        props.setProperty("ranger.pulsar.admin.auth.password", "");
    }

    private void overrideFromEnv() {
        overrideFromEnv("RANGER_SERVICE_NAME", "ranger.pulsar.service.name");
        overrideFromEnv("RANGER_CLUSTER_NAME", "ranger.pulsar.cluster.name");
        overrideFromEnv("RANGER_CACHE_SIZE", "ranger.pulsar.cache.size");
        overrideFromEnv("RANGER_CACHE_TTL", "ranger.pulsar.cache.ttl.seconds");
        overrideFromEnv("RANGER_SYNC_ENABLED", "ranger.pulsar.sync.enabled");
        overrideFromEnv("RANGER_SYNC_INTERVAL", "ranger.pulsar.sync.interval.seconds");
        overrideFromEnv("RANGER_SYNC_RETRY_MAX", "ranger.pulsar.sync.retry.max");
        overrideFromEnv("RANGER_SYNC_RETRY_BACKOFF", "ranger.pulsar.sync.retry.backoff.ms");
        overrideFromEnv("RANGER_ADMIN_URL", "ranger.pulsar.admin.url");
        overrideFromEnv("RANGER_HTTP_CONNECT_TIMEOUT", "ranger.pulsar.http.connect.timeout.ms");
        overrideFromEnv("RANGER_HTTP_READ_TIMEOUT", "ranger.pulsar.http.read.timeout.ms");
        overrideFromEnv("RANGER_AUTH_ENABLED", "ranger.pulsar.auth.enabled");
        overrideFromEnv("RANGER_AUDIT_ENABLED", "ranger.pulsar.audit.enabled");
        overrideFromEnv("RANGER_AUDIT_MAX_QUEUE", "ranger.pulsar.audit.max.queue");
        overrideFromEnv("RANGER_SSL_ENABLED", "ranger.pulsar.ssl.enabled");
        overrideFromEnv("RANGER_ADMIN_AUTH_USER", "ranger.pulsar.admin.auth.user");
        overrideFromEnv("RANGER_ADMIN_AUTH_PASSWORD", "ranger.pulsar.admin.auth.password");

        // 系统属性覆盖（优先级最高）
        overrideFromSystemProperty("ranger.pulsar.service.name");
        overrideFromSystemProperty("ranger.pulsar.cluster.name");
        overrideFromSystemProperty("ranger.pulsar.cache.size");
        overrideFromSystemProperty("ranger.pulsar.sync.enabled");
        overrideFromSystemProperty("ranger.pulsar.sync.interval.seconds");
        overrideFromSystemProperty("ranger.pulsar.admin.url");
        overrideFromSystemProperty("ranger.pulsar.auth.enabled");
        overrideFromSystemProperty("ranger.pulsar.audit.enabled");
    }

    private void overrideFromEnv(String envKey, String propKey) {
        String val = System.getenv(envKey);
        if (val != null && !val.isEmpty()) {
            props.setProperty(propKey, val);
        }
    }

    private void overrideFromSystemProperty(String propKey) {
        String val = System.getProperty(propKey);
        if (val != null && !val.isEmpty()) {
            props.setProperty(propKey, val);
        }
    }

    // === 基础配置 ===
    public String getServiceName() { return props.getProperty("ranger.pulsar.service.name"); }
    public String getClusterName() { return props.getProperty("ranger.pulsar.cluster.name"); }
    public boolean isAuthEnabled() { return Boolean.parseBoolean(props.getProperty("ranger.pulsar.auth.enabled")); }
    public boolean isAuditEnabled() { return Boolean.parseBoolean(props.getProperty("ranger.pulsar.audit.enabled")); }

    // === 缓存配置 ===
    public int getCacheSize() { return Integer.parseInt(props.getProperty("ranger.pulsar.cache.size")); }
    public long getCacheTtlSeconds() { return Long.parseLong(props.getProperty("ranger.pulsar.cache.ttl.seconds")); }

    // === 同步配置 ===
    public boolean isSyncEnabled() { return Boolean.parseBoolean(props.getProperty("ranger.pulsar.sync.enabled")); }
    public long getSyncIntervalSeconds() { return Long.parseLong(props.getProperty("ranger.pulsar.sync.interval.seconds")); }
    public int getSyncRetryMax() { return Integer.parseInt(props.getProperty("ranger.pulsar.sync.retry.max")); }
    public long getSyncRetryBackoffMs() { return Long.parseLong(props.getProperty("ranger.pulsar.sync.retry.backoff.ms")); }

    // === HTTP 配置 ===
    public String getRangerAdminUrl() { return props.getProperty("ranger.pulsar.admin.url"); }
    public int getHttpConnectTimeoutMs() { return Integer.parseInt(props.getProperty("ranger.pulsar.http.connect.timeout.ms")); }
    public int getHttpReadTimeoutMs() { return Integer.parseInt(props.getProperty("ranger.pulsar.http.read.timeout.ms")); }

    // === SSL 配置 ===
    public boolean isSslEnabled() { return Boolean.parseBoolean(props.getProperty("ranger.pulsar.ssl.enabled")); }

    // === 认证配置 ===
    public String getRangerAuthUser() { return props.getProperty("ranger.pulsar.admin.auth.user"); }
    public String getRangerAuthPassword() { return props.getProperty("ranger.pulsar.admin.auth.password"); }

    // === 审计配置 ===
    public int getAuditMaxQueue() { return Integer.parseInt(props.getProperty("ranger.pulsar.audit.max.queue")); }

    // === 通用方法 ===
    public String getProperty(String key, String defaultValue) {
        return props.getProperty(key, defaultValue);
    }

    /**
     * 配置验证，返回错误信息列表，空列表表示配置合法
     */
    public java.util.List<String> validate() {
        java.util.List<String> errors = new java.util.ArrayList<>();
        if (getServiceName() == null || getServiceName().isEmpty()) {
            errors.add("service.name cannot be empty");
        }
        if (getClusterName() == null || getClusterName().isEmpty()) {
            errors.add("cluster.name cannot be empty");
        }
        if (getCacheSize() <= 0) {
            errors.add("cache.size must be > 0");
        }
        if (getSyncIntervalSeconds() <= 0) {
            errors.add("sync.interval.seconds must be > 0");
        }
        if (getSyncRetryMax() < 0) {
            errors.add("sync.retry.max must be >= 0");
        }
        if (getRangerAdminUrl() == null || getRangerAdminUrl().isEmpty()) {
            errors.add("admin.url cannot be empty");
        }
        if (getHttpConnectTimeoutMs() <= 0) {
            errors.add("http.connect.timeout.ms must be > 0");
        }
        if (getHttpReadTimeoutMs() <= 0) {
            errors.add("http.read.timeout.ms must be > 0");
        }
        if (isSslEnabled() && !getRangerAdminUrl().startsWith("https")) {
            errors.add("admin.url must use HTTPS when ssl.enabled=true");
        }
        return errors;
    }

    /**
     * 判断配置是否合法
     */
    public boolean isValid() {
        return validate().isEmpty();
    }

    @Override
    public String toString() {
        return "RangerPulsarConfig{" +
                "service=" + getServiceName() +
                ", cluster=" + getClusterName() +
                ", cacheSize=" + getCacheSize() +
                ", cacheTtl=" + getCacheTtlSeconds() + "s" +
                ", syncEnabled=" + isSyncEnabled() +
                ", syncInterval=" + getSyncIntervalSeconds() + "s" +
                ", retryMax=" + getSyncRetryMax() +
                ", retryBackoff=" + getSyncRetryBackoffMs() + "ms" +
                ", adminUrl=" + getRangerAdminUrl() +
                ", httpConnectTimeout=" + getHttpConnectTimeoutMs() + "ms" +
                ", httpReadTimeout=" + getHttpReadTimeoutMs() + "ms" +
                ", sslEnabled=" + isSslEnabled() +
                ", authEnabled=" + isAuthEnabled() +
                ", auditEnabled=" + isAuditEnabled() +
                ", auditMaxQueue=" + getAuditMaxQueue() +
                '}';
    }
}
