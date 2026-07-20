package com.apache.ranger.pulsar.sync;

import com.apache.ranger.pulsar.config.RangerPulsarConfig;
import com.apache.ranger.pulsar.model.PolicyModel;
import com.apache.ranger.pulsar.plugin.RangerPulsarPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Ranger 策略同步器 - 从 Ranger Admin REST API 定期拉取策略并同步到本地插件
 * 支持：增量同步、失败重试、同步锁保护、HTTP 超时配置
 */
public class PolicyRefresher {

    private static final Logger LOG = LoggerFactory.getLogger(PolicyRefresher.class);

    private final RangerPulsarConfig config;
    private final RangerPulsarPlugin plugin;
    private ScheduledExecutorService scheduler;
    private final AtomicLong lastSyncTime = new AtomicLong(0);
    private final AtomicLong syncCount = new AtomicLong(0);
    private final AtomicLong failCount = new AtomicLong(0);
    private final AtomicLong lastPolicyCount = new AtomicLong(0);
    private final AtomicLong totalSyncedPolicies = new AtomicLong(0);
    private volatile boolean running = false;

    /** 同步锁，防止并发同步 */
    private final ReentrantLock syncLock = new ReentrantLock();

    /** 上次策略内容的 hash，用于增量同步判断 */
    private volatile String lastPolicyHash = "";

    /** 上次成功同步的策略数量 */
    private volatile int lastSyncedCount = 0;

    public PolicyRefresher(RangerPulsarConfig config, RangerPulsarPlugin plugin) {
        this.config = config;
        this.plugin = plugin;
    }

    public void start() {
        if (!config.isSyncEnabled()) {
            LOG.info("Policy sync is disabled");
            return;
        }
        if (running) {
            LOG.warn("PolicyRefresher is already running");
            return;
        }

        running = true;
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ranger-policy-refresher");
            t.setDaemon(true);
            return t;
        });

        long interval = config.getSyncIntervalSeconds();
        scheduler.scheduleAtFixedRate(this::syncWithRetry, interval, interval, TimeUnit.SECONDS);
        LOG.info("PolicyRefresher started - interval: {}s, url: {}, retryMax: {}, retryBackoff: {}ms",
                interval, config.getRangerAdminUrl(), config.getSyncRetryMax(), config.getSyncRetryBackoffMs());

        // 启动时立即同步一次
        scheduler.submit(this::syncWithRetry);
    }

    public void stop() {
        running = false;
        if (scheduler != null) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        LOG.info("PolicyRefresher stopped - totalSyncs={}, totalFails={}, totalPolicies={}",
                syncCount.get(), failCount.get(), totalSyncedPolicies.get());
    }

    /**
     * 带重试的策略同步
     */
    public void syncWithRetry() {
        int maxRetries = config.getSyncRetryMax();
        long backoffMs = config.getSyncRetryBackoffMs();

        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                if (syncPolicies()) {
                    return; // 同步成功
                }
            } catch (Exception e) {
                LOG.warn("Sync attempt {}/{} failed: {}", attempt + 1, maxRetries + 1, e.getMessage());
            }

            if (attempt < maxRetries) {
                try {
                    long sleepTime = backoffMs * (1L << attempt); // 指数退避
                    LOG.info("Retrying in {}ms (attempt {}/{})", sleepTime, attempt + 2, maxRetries + 1);
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }

        failCount.incrementAndGet();
        LOG.error("Policy sync failed after {} retries", maxRetries + 1);
    }

    /**
     * 执行一次策略同步，返回是否成功
     */
    public boolean syncPolicies() {
        if (!syncLock.tryLock()) {
            LOG.debug("Sync already in progress, skipping");
            return true;
        }

        try {
            String url = config.getRangerAdminUrl() + "/service/public/policies";
            String response = httpGet(url);
            if (response == null) {
                return false;
            }

            // 增量同步：计算 hash 判断是否有变更
            String currentHash = Integer.toHexString(response.hashCode());
            if (currentHash.equals(lastPolicyHash)) {
                LOG.debug("Policy content unchanged (hash={}), skipping apply", currentHash);
                lastSyncTime.set(System.currentTimeMillis());
                syncCount.incrementAndGet();
                return true;
            }

            List<PolicyModel> policies = parsePolicies(response);
            applyPolicies(policies);

            lastPolicyHash = currentHash;
            lastSyncedCount = policies.size();
            lastPolicyCount.set(policies.size());
            totalSyncedPolicies.addAndGet(policies.size());
            lastSyncTime.set(System.currentTimeMillis());
            syncCount.incrementAndGet();

            LOG.info("Policy sync completed - fetched {} policies (hash={})", policies.size(), currentHash);
            return true;
        } catch (Exception e) {
            LOG.error("Policy sync failed: {}", e.getMessage());
            return false;
        } finally {
            syncLock.unlock();
        }
    }

    private String httpGet(String urlStr) throws Exception {
        HttpURLConnection conn = null;
        try {
            URL url = new URL(urlStr);
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Accept", "application/json");
            conn.setConnectTimeout(config.getHttpConnectTimeoutMs());
            conn.setReadTimeout(config.getHttpReadTimeoutMs());

            // 认证头
            if (config.getRangerAuthUser() != null && !config.getRangerAuthUser().isEmpty()) {
                String auth = config.getRangerAuthUser() + ":" + config.getRangerAuthPassword();
                String encoded = Base64.getEncoder().encodeToString(auth.getBytes());
                conn.setRequestProperty("Authorization", "Basic " + encoded);
            }

            int code = conn.getResponseCode();
            if (code == 200) {
                try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                    StringBuilder sb = new StringBuilder();
                    String line;
                    while ((line = br.readLine()) != null) {
                        sb.append(line);
                    }
                    return sb.toString();
                }
            } else {
                LOG.warn("Ranger Admin returned status: {} for URL: {}", code, urlStr);
                return null;
            }
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    private List<PolicyModel> parsePolicies(String json) {
        List<PolicyModel> result = new ArrayList<>();
        try {
            int policiesIdx = json.indexOf("\"policies\"");
            if (policiesIdx < 0) return result;

            String[] policyBlocks = json.split("\"id\"");
            for (int i = 1; i < policyBlocks.length; i++) {
                try {
                    PolicyModel policy = parseSinglePolicy(policyBlocks[i]);
                    if (policy != null) {
                        result.add(policy);
                    }
                } catch (Exception e) {
                    LOG.debug("Failed to parse policy block: {}", e.getMessage());
                }
            }
        } catch (Exception e) {
            LOG.error("Failed to parse policies JSON: {}", e.getMessage());
        }
        return result;
    }

    private PolicyModel parseSinglePolicy(String block) {
        String id = extractStringValue(block, "\"guid\"");
        if (id == null) id = extractStringValue(block, "\"name\"");
        String name = extractStringValue(block, "\"name\"");
        String accessType = null;
        Set<String> users = new HashSet<>();
        Set<String> groups = new HashSet<>();
        String cluster = "*", namespace = "*", topic = "*", subscription = "*";

        String[] accessParts = block.split("\"type\"");
        for (int i = 1; i < accessParts.length; i++) {
            String t = extractStringValue(accessParts[i], "");
            if (t != null && !t.isEmpty()) {
                accessType = t;
                break;
            }
        }

        String usersSection = extractArrayValue(block, "\"users\"");
        if (usersSection != null) {
            String[] userArr = usersSection.split(",");
            for (String u : userArr) {
                String cleaned = u.replaceAll("[\"\\[\\] ]", "").trim();
                if (!cleaned.isEmpty()) users.add(cleaned);
            }
        }

        // 提取 groups
        String groupsSection = extractArrayValue(block, "\"groups\"");
        if (groupsSection != null) {
            String[] groupArr = groupsSection.split(",");
            for (String g : groupArr) {
                String cleaned = g.replaceAll("[\"\\[\\] ]", "").trim();
                if (!cleaned.isEmpty()) groups.add(cleaned);
            }
        }

        String resourcesSection = extractSection(block, "\"resources\"");
        if (resourcesSection != null) {
            cluster = extractResourceValue(resourcesSection, "\"cluster\"");
            namespace = extractResourceValue(resourcesSection, "\"namespace\"");
            topic = extractResourceValue(resourcesSection, "\"topic\"");
            subscription = extractResourceValue(resourcesSection, "\"subscription\"");
        }

        if (accessType == null || id == null) return null;

        return PolicyModel.builder()
                .id("ranger-" + id)
                .accessType(accessType)
                .cluster(cluster != null ? cluster : "*")
                .namespace(namespace != null ? namespace : "*")
                .topic(topic != null ? topic : "*")
                .subscription(subscription != null ? subscription : "*")
                .users(users)
                .groups(groups)
                .priority(50)
                .build();
    }

    private String extractStringValue(String text, String key) {
        int idx = text.indexOf(key);
        if (idx < 0) return null;
        int colonIdx = text.indexOf(":", idx + key.length());
        if (colonIdx < 0) return null;
        int startQuote = text.indexOf("\"", colonIdx);
        if (startQuote < 0) return null;
        int endQuote = text.indexOf("\"", startQuote + 1);
        if (endQuote < 0) return null;
        return text.substring(startQuote + 1, endQuote);
    }

    private String extractArrayValue(String text, String key) {
        int idx = text.indexOf(key);
        if (idx < 0) return null;
        int startBracket = text.indexOf("[", idx);
        int endBracket = text.indexOf("]", startBracket);
        if (startBracket < 0 || endBracket < 0) return null;
        return text.substring(startBracket + 1, endBracket);
    }

    private String extractSection(String text, String key) {
        int idx = text.indexOf(key);
        if (idx < 0) return null;
        int startBrace = text.indexOf("{", idx);
        if (startBrace < 0) return null;
        int depth = 1;
        int pos = startBrace + 1;
        while (pos < text.length() && depth > 0) {
            char c = text.charAt(pos);
            if (c == '{') depth++;
            else if (c == '}') depth--;
            pos++;
        }
        return text.substring(startBrace, pos);
    }

    private String extractResourceValue(String resourcesSection, String resourceKey) {
        int idx = resourcesSection.indexOf(resourceKey);
        if (idx < 0) return "*";
        int valuesIdx = resourcesSection.indexOf("\"values\"", idx);
        if (valuesIdx < 0) return "*";
        int startBracket = resourcesSection.indexOf("[", valuesIdx);
        int endBracket = resourcesSection.indexOf("]", startBracket);
        if (startBracket < 0 || endBracket < 0) return "*";
        String arrContent = resourcesSection.substring(startBracket + 1, endBracket);
        String cleaned = arrContent.replaceAll("[\"\\[\\] ]", "").trim();
        return cleaned.isEmpty() ? "*" : cleaned;
    }

    private void applyPolicies(List<PolicyModel> remotePolicies) {
        List<String> toRemove = new ArrayList<>();
        for (PolicyModel p : plugin.getPolicies()) {
            if (p.getId().startsWith("ranger-")) {
                toRemove.add(p.getId());
            }
        }
        for (String id : toRemove) {
            plugin.removePolicy(id);
        }
        for (PolicyModel policy : remotePolicies) {
            plugin.addPolicy(policy);
        }
    }

    /**
     * 强制全量同步（忽略增量判断）
     */
    public void forceSync() {
        lastPolicyHash = "";
        syncWithRetry();
    }

    public SyncStats getStats() {
        return new SyncStats(
                running,
                syncCount.get(),
                failCount.get(),
                lastSyncTime.get() > 0 ? new Date(lastSyncTime.get()).toString() : "never",
                lastPolicyCount.get(),
                totalSyncedPolicies.get(),
                lastPolicyHash,
                lastSyncedCount
        );
    }

    public static class SyncStats {
        public final boolean running;
        public final long syncCount;
        public final long failCount;
        public final String lastSyncTime;
        public final long lastPolicyCount;
        public final long totalSyncedPolicies;
        public final String policyHash;
        public final int lastSyncedCount;

        public SyncStats(boolean running, long syncCount, long failCount, String lastSyncTime,
                         long lastPolicyCount, long totalSyncedPolicies, String policyHash, int lastSyncedCount) {
            this.running = running;
            this.syncCount = syncCount;
            this.failCount = failCount;
            this.lastSyncTime = lastSyncTime;
            this.lastPolicyCount = lastPolicyCount;
            this.totalSyncedPolicies = totalSyncedPolicies;
            this.policyHash = policyHash;
            this.lastSyncedCount = lastSyncedCount;
        }

        @Override
        public String toString() {
            return String.format("SyncStats{running=%s, syncs=%d, fails=%d, lastSync=%s, policies=%d, totalPolicies=%d, hash=%s}",
                    running, syncCount, failCount, lastSyncTime, lastPolicyCount, totalSyncedPolicies, policyHash);
        }
    }
}
