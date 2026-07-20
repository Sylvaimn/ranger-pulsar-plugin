package com.apache.ranger.pulsar;

import com.apache.ranger.pulsar.audit.RangerPulsarAuditHandler;
import com.apache.ranger.pulsar.auth.RangerAuthorizationProvider;
import com.apache.ranger.pulsar.cache.AccessDecisionCache;
import com.apache.ranger.pulsar.config.RangerPulsarConfig;
import com.apache.ranger.pulsar.model.PolicyModel;
import com.apache.ranger.pulsar.plugin.RangerPulsarPlugin;
import com.apache.ranger.pulsar.resource.RangerPulsarResource;
import com.apache.ranger.pulsar.sync.PolicyRefresher;
import org.junit.jupiter.api.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.apache.ranger.pulsar.RangerPulsarConstants.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * 系统测试 - 覆盖缓存、组权限、正则匹配、并发、性能等场景
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class RangerPulsarSystemTest {

    private static RangerPulsarPlugin plugin;

    @BeforeAll
    static void setUpClass() {
        plugin = RangerPulsarPlugin.getInstance();
        plugin.init();
    }

    @AfterAll
    static void tearDownClass() {
        plugin.cleanup();
    }

    @BeforeEach
    void setUp() {
        plugin.cleanup();
    }

    // ==================== 缓存测试 ====================

    @Test
    @Order(1)
    @DisplayName("缓存命中测试 - 重复访问应命中缓存")
    void testCacheHit() {
        RangerPulsarResource resource = new RangerPulsarResource("standalone", "public/default", "cache-test-topic");

        // 第一次访问 - miss
        plugin.isAccessAllowed("producer1", Set.of(), ACCESS_TYPE_PRODUCE, resource);
        AccessDecisionCache.CacheStats stats1 = plugin.getCacheStats();
        assertEquals(1, stats1.misses, "首次访问应为 miss");

        // 第二次访问 - hit
        plugin.isAccessAllowed("producer1", Set.of(), ACCESS_TYPE_PRODUCE, resource);
        AccessDecisionCache.CacheStats stats2 = plugin.getCacheStats();
        assertEquals(1, stats2.hits, "第二次访问应为 hit");
        assertTrue(stats2.hitRate > 0, "命中率应大于 0");
    }

    @Test
    @Order(2)
    @DisplayName("缓存失效测试 - 策略变更后缓存应清空")
    void testCacheInvalidationOnPolicyChange() {
        RangerPulsarResource resource = new RangerPulsarResource("standalone", "public/default", "invalidation-topic");

        plugin.isAccessAllowed("producer1", Set.of(), ACCESS_TYPE_PRODUCE, resource);
        assertEquals(1, plugin.getCacheStats().size);

        // 添加策略应清空缓存
        plugin.addPolicy(ACCESS_TYPE_PRODUCE, "standalone", "public/default", "new-topic", "*", Set.of("new-user"));
        assertEquals(0, plugin.getCacheStats().size, "策略变更后缓存应清空");
    }

    @Test
    @Order(3)
    @DisplayName("缓存 LRU 淘汰测试")
    void testCacheLRUEviction() {
        AccessDecisionCache cache = new AccessDecisionCache(3);

        cache.put("k1", true);
        cache.put("k2", true);
        cache.put("k3", true);
        assertEquals(3, cache.getStats().size);

        // 插入第4个，应淘汰最旧的
        cache.put("k4", true);
        assertEquals(3, cache.getStats().size);
        assertNull(cache.get("k1"), "k1 应被 LRU 淘汰");
    }

    // ==================== 组权限测试 ====================

    @Test
    @Order(10)
    @DisplayName("组权限测试 - 用户通过组获得访问权限")
    void testGroupBasedAccess() {
        // admin-group 有 admin 权限
        RangerPulsarResource resource = new RangerPulsarResource("standalone", "public/default", "group-test");

        assertTrue(plugin.isAccessAllowed("user-in-admin-group", Set.of("admin-group"), ACCESS_TYPE_ADMIN, resource),
                "admin-group 成员应有 admin 权限");
        assertTrue(plugin.isAccessAllowed("user-in-producer-group", Set.of("producer-group"), ACCESS_TYPE_PRODUCE, resource),
                "producer-group 成员应有 produce 权限");
        assertTrue(plugin.isAccessAllowed("user-in-consumer-group", Set.of("consumer-group"), ACCESS_TYPE_CONSUME, resource),
                "consumer-group 成员应有 consume 权限");
    }

    @Test
    @Order(11)
    @DisplayName("组权限隔离测试 - 不同组不应交叉访问")
    void testGroupIsolation() {
        RangerPulsarResource resource = new RangerPulsarResource("standalone", "public/default", "isolation-test");

        assertFalse(plugin.isAccessAllowed("producer-group-user", Set.of("producer-group"), ACCESS_TYPE_CONSUME, resource),
                "producer-group 成员不应有 consume 权限");
        assertFalse(plugin.isAccessAllowed("consumer-group-user", Set.of("consumer-group"), ACCESS_TYPE_PRODUCE, resource),
                "consumer-group 成员不应有 produce 权限");
    }

    @Test
    @Order(12)
    @DisplayName("组+用户混合权限测试")
    void testMixedUserAndGroupAccess() {
        RangerPulsarResource resource = new RangerPulsarResource("standalone", "public/default", "mixed-test");

        // admin 用户同时有 produce 权限（通过用户和组）
        assertTrue(plugin.isAccessAllowed("admin", Set.of("admin-group"), ACCESS_TYPE_PRODUCE, resource));
        assertTrue(plugin.isAccessAllowed("admin", Set.of("producer-group"), ACCESS_TYPE_PRODUCE, resource));

        // 非授权用户+非授权组
        assertFalse(plugin.isAccessAllowed("unknown", Set.of("unknown-group"), ACCESS_TYPE_PRODUCE, resource));
    }

    // ==================== 正则匹配测试 ====================

    @Test
    @Order(20)
    @DisplayName("正则匹配测试 - topic 名称正则策略")
    void testRegexTopicMatching() {
        plugin.addPolicy(PolicyModel.builder()
                .id("regex-test-policy")
                .accessType(ACCESS_TYPE_PRODUCE)
                .cluster("standalone")
                .namespace("public/default")
                .topic("regex:order-.*")
                .subscription("*")
                .users(Set.of("order-producer"))
                .priority(60)
                .build());

        RangerPulsarResource matchResource = new RangerPulsarResource("standalone", "public/default", "order-created");
        RangerPulsarResource noMatchResource = new RangerPulsarResource("standalone", "public/default", "payment-topic");

        assertTrue(plugin.isAccessAllowed("order-producer", Set.of(), ACCESS_TYPE_PRODUCE, matchResource),
                "order-producer 应能访问 order-created topic");
        assertFalse(plugin.isAccessAllowed("order-producer", Set.of(), ACCESS_TYPE_PRODUCE, noMatchResource),
                "order-producer 不应能访问 payment-topic");
    }

    @Test
    @Order(21)
    @DisplayName("正则命名空间匹配测试")
    void testRegexNamespaceMatching() {
        plugin.addPolicy(PolicyModel.builder()
                .id("regex-ns-policy")
                .accessType(ACCESS_TYPE_PRODUCE)
                .cluster("standalone")
                .namespace("regex:tenant-\\w+/default")
                .topic("*")
                .subscription("*")
                .users(Set.of("tenant-producer"))
                .priority(60)
                .build());

        RangerPulsarResource matchResource = new RangerPulsarResource("standalone", "tenant-a/default", "test-topic");
        assertTrue(plugin.isAccessAllowed("tenant-producer", Set.of(), ACCESS_TYPE_PRODUCE, matchResource),
                "tenant-producer 应能匹配 regex:tenant-\\w+/default 命名空间");
    }

    // ==================== 策略优先级测试 ====================

    @Test
    @Order(30)
    @DisplayName("策略优先级测试 - 高优先级策略先匹配")
    void testPolicyPriority() {
        // 添加低优先级拒绝策略
        plugin.addPolicy(PolicyModel.builder()
                .id("low-priority-deny")
                .accessType(ACCESS_TYPE_PRODUCE)
                .cluster("standalone")
                .namespace("public/default")
                .topic("priority-topic")
                .subscription("*")
                .users(Collections.emptySet())  // 没有任何用户
                .priority(1)
                .build());

        // 添加高优先级允许策略
        plugin.addPolicy(PolicyModel.builder()
                .id("high-priority-allow")
                .accessType(ACCESS_TYPE_PRODUCE)
                .cluster("standalone")
                .namespace("public/default")
                .topic("priority-topic")
                .subscription("*")
                .users(Set.of("priority-user"))
                .priority(200)
                .build());

        RangerPulsarResource resource = new RangerPulsarResource("standalone", "public/default", "priority-topic");
        assertTrue(plugin.isAccessAllowed("priority-user", Set.of(), ACCESS_TYPE_PRODUCE, resource),
                "高优先级策略应优先匹配");
    }

    // ==================== 策略管理测试 ====================

    @Test
    @Order(40)
    @DisplayName("策略删除测试")
    void testPolicyRemoval() {
        plugin.addPolicy(PolicyModel.builder()
                .id("removable-policy")
                .accessType(ACCESS_TYPE_PRODUCE)
                .cluster("standalone")
                .namespace("public/removable")
                .topic("*")
                .subscription("*")
                .users(Set.of("removable-user"))
                .priority(50)
                .build());

        RangerPulsarResource resource = new RangerPulsarResource("standalone", "public/removable", "test");
        assertTrue(plugin.isAccessAllowed("removable-user", Set.of(), ACCESS_TYPE_PRODUCE, resource));

        assertTrue(plugin.removePolicy("removable-policy"), "策略应被删除");
        assertFalse(plugin.isAccessAllowed("removable-user", Set.of(), ACCESS_TYPE_PRODUCE, resource),
                "策略删除后应拒绝访问");
    }

    @Test
    @Order(41)
    @DisplayName("策略列表查询测试")
    void testGetPolicies() {
        List<PolicyModel> policies = plugin.getPolicies();
        assertNotNull(policies);
        assertTrue(policies.size() >= 4, "应至少有 4 个默认策略");
    }

    // ==================== 审计测试 ====================

    @Test
    @Order(50)
    @DisplayName("审计日志统计测试")
    void testAuditStats() {
        RangerPulsarResource resource = new RangerPulsarResource("standalone", "public/default", "audit-test");

        plugin.isAccessAllowed("admin", Set.of(), ACCESS_TYPE_PRODUCE, resource);     // granted
        plugin.isAccessAllowed("unknown", Set.of(), ACCESS_TYPE_PRODUCE, resource);   // denied

        assertEquals(2, plugin.getAccessCheckCount(), "应有 2 次访问检查");
        assertEquals(1, plugin.getDeniedCount(), "应有 1 次拒绝");
    }

    @Test
    @Order(51)
    @DisplayName("审计事件收集测试")
    void testAuditEventCollection() {
        RangerPulsarAuditHandler handler = new RangerPulsarAuditHandler();

        handler.logAccess("user1", "res1", "produce", true);
        handler.logAccess("user2", "res2", "consume", false);

        RangerPulsarAuditHandler.AuditStats stats = handler.getStats();
        assertEquals(2, stats.totalEvents);
        assertEquals(1, stats.grantedCount);
        assertEquals(1, stats.deniedCount);
        assertEquals(0.5, stats.getDenyRate(), 0.001);
        assertEquals(2, stats.queueSize);

        // 验证事件队列
        Queue<RangerPulsarAuditHandler.AuditEvent> events = handler.getAuditEvents();
        assertEquals(2, events.size());
    }

    // ==================== 资源模型测试 ====================

    @Test
    @Order(60)
    @DisplayName("资源 equals/hashCode 测试")
    void testResourceEquals() {
        RangerPulsarResource r1 = new RangerPulsarResource("standalone", "public/default", "topic1");
        RangerPulsarResource r2 = new RangerPulsarResource("standalone", "public/default", "topic1");
        RangerPulsarResource r3 = new RangerPulsarResource("standalone", "public/default", "topic2");

        assertEquals(r1, r2);
        assertEquals(r1.hashCode(), r2.hashCode());
        assertNotEquals(r1, r3);
    }

    @Test
    @Order(61)
    @DisplayName("资源层级深度测试")
    void testResourceDepth() {
        assertEquals(0, new RangerPulsarResource().getDepth());
        assertEquals(1, new RangerPulsarResource("standalone").getDepth());
        assertEquals(2, new RangerPulsarResource("standalone", "public/default").getDepth());
        assertEquals(3, new RangerPulsarResource("standalone", "public/default", "topic1").getDepth());
        assertEquals(4, new RangerPulsarResource("standalone", "public/default", "topic1", "sub1").getDepth());
    }

    @Test
    @Order(62)
    @DisplayName("资源路径字符串测试")
    void testResourcePath() {
        RangerPulsarResource r = new RangerPulsarResource("standalone", "public/default", "topic1", "sub1");
        assertEquals("standalone/public/default/topic1/sub1", r.getResourcePath());

        RangerPulsarResource r2 = new RangerPulsarResource("standalone", "public/default", "topic1");
        assertEquals("standalone/public/default/topic1", r2.getResourcePath());
    }

    // ==================== AuthorizationProvider 测试 ====================

    @Test
    @Order(70)
    @DisplayName("AuthorizationProvider 异步权限校验测试")
    void testProviderAsyncAccess() throws Exception {
        RangerAuthorizationProvider provider = new RangerAuthorizationProvider();
        provider.initialize("standalone");

        CompletableFuture<Boolean> future = provider.canProduce("producer1", Set.of(), "public/default", "async-topic");
        assertTrue(future.get(5, TimeUnit.SECONDS), "producer1 应有 produce 权限");

        CompletableFuture<Boolean> denyFuture = provider.canProduce("unknown", Set.of(), "public/default", "async-topic");
        assertFalse(denyFuture.get(5, TimeUnit.SECONDS), "unknown 应无 produce 权限");

        provider.close();
    }

    @Test
    @Order(71)
    @DisplayName("AuthorizationProvider 角色组注册测试")
    void testProviderRoleGroupRegistration() throws Exception {
        RangerAuthorizationProvider provider = new RangerAuthorizationProvider();
        provider.initialize("standalone");
        provider.registerRoleGroups("dev-user", Set.of("producer-group"));

        // dev-user 通过 producer-group 获得 produce 权限
        CompletableFuture<Boolean> future = provider.canProduce("dev-user", Set.of(), "public/default", "role-topic");
        assertTrue(future.get(5, TimeUnit.SECONDS), "dev-user 通过 producer-group 应有 produce 权限");

        provider.close();
    }

    @Test
    @Order(72)
    @DisplayName("AuthorizationProvider 禁用授权测试")
    void testProviderAuthorizationDisabled() throws Exception {
        RangerAuthorizationProvider provider = new RangerAuthorizationProvider();
        provider.initialize("standalone");
        provider.setAuthorizationEnabled(false);

        CompletableFuture<Boolean> future = provider.canProduce("anyone", Set.of(), "public/default", "any-topic");
        assertTrue(future.get(5, TimeUnit.SECONDS), "禁用授权后应允许所有访问");

        provider.close();
    }

    @Test
    @Order(73)
    @DisplayName("AuthorizationProvider 权限查询测试")
    void testProviderPermissionQuery() {
        RangerAuthorizationProvider provider = new RangerAuthorizationProvider();
        provider.initialize("standalone");

        Map<String, String> topicPerms = provider.getTopicPermissions("public/default");
        assertNotNull(topicPerms);
        assertFalse(topicPerms.isEmpty(), "应有 topic 权限信息");

        Map<String, Set<Object>> nsPerms = provider.getPermissions("public/default");
        assertNotNull(nsPerms);
        assertFalse(nsPerms.isEmpty(), "应有 namespace 权限信息");

        provider.close();
    }

    // ==================== 并发测试 ====================

    @Test
    @Order(80)
    @DisplayName("并发权限校验测试 - 多线程同时访问")
    void testConcurrentAccess() throws Exception {
        int threadCount = 20;
        int opsPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            final int idx = i;
            executor.submit(() -> {
                try {
                    latch.countDown();
                    latch.await();
                    for (int j = 0; j < opsPerThread; j++) {
                        RangerPulsarResource resource = new RangerPulsarResource("standalone", "public/default", "concurrent-topic-" + (j % 5));
                        boolean result = plugin.isAccessAllowed("producer1", Set.of(), ACCESS_TYPE_PRODUCE, resource);
                        if (result) successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                }
            });
        }

        executor.shutdown();
        assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
        assertEquals(0, errorCount.get(), "不应有异常");
        assertEquals(threadCount * opsPerThread, successCount.get(), "所有 produce 操作应成功");
    }

    @Test
    @Order(81)
    @DisplayName("并发策略添加测试")
    void testConcurrentPolicyAddition() throws Exception {
        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger addedCount = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            final int idx = i;
            executor.submit(() -> {
                try {
                    latch.countDown();
                    latch.await();
                    plugin.addPolicy(PolicyModel.builder()
                            .id("concurrent-policy-" + idx)
                            .accessType(ACCESS_TYPE_PRODUCE)
                            .cluster("standalone")
                            .namespace("public/concurrent")
                            .topic("topic-" + idx)
                            .subscription("*")
                            .users(Set.of("concurrent-user-" + idx))
                            .priority(50)
                            .build());
                    addedCount.incrementAndGet();
                } catch (Exception e) {
                    // ignore
                }
            });
        }

        executor.shutdown();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        assertEquals(threadCount, addedCount.get(), "应成功添加 10 个策略");
        assertTrue(plugin.getPolicies().size() >= threadCount + 4, "策略总数应包含默认和并发添加的");
    }

    // ==================== 性能测试 ====================

    @Test
    @Order(90)
    @DisplayName("性能基准测试 - 10000 次权限校验")
    void testPerformanceBenchmark() {
        RangerPulsarResource resource = new RangerPulsarResource("standalone", "public/default", "perf-topic");
        int iterations = 10000;

        long start = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            plugin.isAccessAllowed("producer1", Set.of(), ACCESS_TYPE_PRODUCE, resource);
        }
        long elapsed = System.nanoTime() - start;
        double avgMs = elapsed / 1_000_000.0 / iterations;

        // 第一次 miss，后续全部 cache hit，应该非常快
        AccessDecisionCache.CacheStats stats = plugin.getCacheStats();
        assertTrue(stats.hitRate > 0.99, "缓存命中率应 >99%: " + stats.hitRate);
        assertTrue(avgMs < 1.0, "平均每次校验应 <1ms, 实际: " + avgMs + "ms");

        System.out.printf("性能测试结果: %d 次校验, 总耗时 %.2fms, 平均 %.4fms/次, 缓存命中率 %.2f%%%n",
                iterations, elapsed / 1_000_000.0, avgMs, stats.hitRate * 100);
    }

    @Test
    @Order(91)
    @DisplayName("大量策略性能测试")
    void testLargePolicySetPerformance() {
        // 添加 100 个策略
        for (int i = 0; i < 100; i++) {
            plugin.addPolicy(PolicyModel.builder()
                    .id("bulk-policy-" + i)
                    .accessType(ACCESS_TYPE_PRODUCE)
                    .cluster("standalone")
                    .namespace("public/bulk")
                    .topic("bulk-topic-" + i)
                    .subscription("*")
                    .users(Set.of("bulk-user-" + i))
                    .priority(50)
                    .build());
        }

        RangerPulsarResource resource = new RangerPulsarResource("standalone", "public/bulk", "bulk-topic-50");
        long start = System.nanoTime();
        boolean allowed = plugin.isAccessAllowed("bulk-user-50", Set.of(), ACCESS_TYPE_PRODUCE, resource);
        long elapsed = System.nanoTime() - start;

        assertTrue(allowed, "bulk-user-50 应有权限");
        assertTrue(elapsed < 50_000_000, "100+ 策略下首次校验应 <50ms, 实际: " + elapsed / 1_000_000 + "ms");
    }

    // ==================== 边界测试 ====================

    @Test
    @Order(100)
    @DisplayName("空字符串参数测试")
    void testEmptyStringParameters() {
        RangerPulsarResource resource = new RangerPulsarResource("standalone", "public/default", "test");
        assertFalse(plugin.isAccessAllowed("", Set.of(), ACCESS_TYPE_PRODUCE, resource), "空用户名应被拒绝");
        assertFalse(plugin.isAccessAllowed("admin", Set.of(), "", resource), "空访问类型应被拒绝");
    }

    @Test
    @Order(101)
    @DisplayName("空组和 null 组测试")
    void testNullAndEmptyGroups() {
        RangerPulsarResource resource = new RangerPulsarResource("standalone", "public/default", "test");

        // admin 用户不需要组
        assertTrue(plugin.isAccessAllowed("admin", null, ACCESS_TYPE_ADMIN, resource), "admin 无组也应有权限");
        assertTrue(plugin.isAccessAllowed("admin", Set.of(), ACCESS_TYPE_ADMIN, resource), "admin 空组也应有权限");

        // 非admin用户通过组获取权限
        assertTrue(plugin.isAccessAllowed("any-user", Set.of("admin-group"), ACCESS_TYPE_ADMIN, resource),
                "admin-group 组成员应有 admin 权限");
    }

    @Test
    @Order(102)
    @DisplayName("PolicyModel Builder 测试")
    void testPolicyModelBuilder() {
        PolicyModel policy = PolicyModel.builder()
                .id("builder-test")
                .accessType(ACCESS_TYPE_PRODUCE)
                .cluster("standalone")
                .namespace("public/default")
                .topic("*")
                .subscription("*")
                .users(Set.of("user1", "user2"))
                .groups(Set.of("group1"))
                .delegateAdmin(true)
                .priority(75)
                .build();

        assertEquals("builder-test", policy.getId());
        assertEquals(ACCESS_TYPE_PRODUCE, policy.getAccessType());
        assertEquals(2, policy.getAllowedUsers().size());
        assertEquals(1, policy.getAllowedGroups().size());
        assertTrue(policy.isDelegateAdmin());
        assertEquals(75, policy.getPriority());
    }

    // ==================== 配置测试 ====================

    @Test
    @Order(110)
    @DisplayName("默认配置加载测试")
    void testDefaultConfigLoading() {
        RangerPulsarConfig config = new RangerPulsarConfig();
        assertEquals("pulsar", config.getServiceName());
        assertEquals("standalone", config.getClusterName());
        assertEquals(10000, config.getCacheSize());
        assertEquals(300, config.getCacheTtlSeconds());
        assertTrue(config.isSyncEnabled());
        assertEquals(30, config.getSyncIntervalSeconds());
        assertEquals("http://localhost:6080", config.getRangerAdminUrl());
        assertTrue(config.isAuthEnabled());
        assertTrue(config.isAuditEnabled());
        assertEquals(5000, config.getAuditMaxQueue());
    }

    @Test
    @Order(111)
    @DisplayName("配置 toString 测试")
    void testConfigToString() {
        RangerPulsarConfig config = new RangerPulsarConfig();
        String str = config.toString();
        assertTrue(str.contains("pulsar"));
        assertTrue(str.contains("standalone"));
        assertTrue(str.contains("10000"));
    }

    @Test
    @Order(112)
    @DisplayName("插件配置集成测试")
    void testPluginConfigIntegration() {
        RangerPulsarConfig config = plugin.getConfig();
        assertNotNull(config, "插件应有配置对象");
        assertEquals("standalone", config.getClusterName());
    }

    // ==================== 策略同步器测试 ====================

    @Test
    @Order(120)
    @DisplayName("策略同步器 - 禁用同步测试")
    void testSyncDisabled() {
        RangerPulsarConfig config = new RangerPulsarConfig();
        // 创建一个禁用同步的配置
        System.setProperty("ranger.pulsar.sync.enabled", "false");
        try {
            RangerPulsarPlugin testPlugin = new RangerPulsarPlugin(RangerPulsarConfig.load());
            PolicyRefresher.SyncStats stats = testPlugin.getSyncStats();
            assertFalse(stats.running, "同步应禁用");
            assertEquals("disabled", stats.lastSyncTime);
            testPlugin.cleanup();
        } finally {
            System.clearProperty("ranger.pulsar.sync.enabled");
        }
    }

    @Test
    @Order(121)
    @DisplayName("策略同步器 - 手动同步测试（连接 Ranger Mock）")
    void testManualSyncWithRangerMock() {
        // 确认 Ranger Mock 在运行
        try {
            java.net.HttpURLConnection conn = (java.net.HttpURLConnection)
                    new java.net.URL("http://localhost:6080/").openConnection();
            conn.setConnectTimeout(2000);
            int code = conn.getResponseCode();
            conn.disconnect();
            if (code != 200) {
                System.out.println("Ranger Mock not available, skipping sync test");
                return;
            }
        } catch (Exception e) {
            System.out.println("Ranger Mock not available, skipping sync test: " + e.getMessage());
            return;
        }

        RangerPulsarConfig config = new RangerPulsarConfig();
        PolicyRefresher refresher = new PolicyRefresher(config, plugin);
        refresher.syncPolicies();

        PolicyRefresher.SyncStats stats = refresher.getStats();
        assertEquals(1, stats.syncCount, "应有 1 次同步");
        assertEquals(0, stats.failCount, "不应有失败");
        assertNotNull(stats.lastSyncTime);
        assertNotEquals("never", stats.lastSyncTime);

        // 验证同步后远程策略已加载
        long remotePolicies = plugin.getPolicies().stream()
                .filter(p -> p.getId().startsWith("ranger-"))
                .count();
        assertTrue(remotePolicies > 0, "应有远程策略已加载: " + remotePolicies);

        System.out.println("策略同步测试通过 - 同步了 " + remotePolicies + " 个远程策略");
    }

    @Test
    @Order(122)
    @DisplayName("策略同步器 - 连接失败测试")
    void testSyncFailureHandling() {
        // 配置一个不可达的 URL，retryMax=0 且超时短以加速测试
        RangerPulsarConfig config = new RangerPulsarConfig() {
            @Override
            public String getRangerAdminUrl() {
                return "http://localhost:19999";
            }
            @Override
            public int getSyncRetryMax() { return 0; }
            @Override
            public int getHttpConnectTimeoutMs() { return 500; }
            @Override
            public int getHttpReadTimeoutMs() { return 500; }
        };
        PolicyRefresher refresher = new PolicyRefresher(config, plugin);
        // 使用 syncWithRetry：retryMax=0 时只尝试一次，失败后会增加 failCount
        refresher.syncWithRetry();

        PolicyRefresher.SyncStats stats = refresher.getStats();
        assertEquals(0, stats.syncCount, "同步不应成功");
        assertTrue(stats.failCount > 0, "应有失败记录");
    }

    @Test
    @Order(123)
    @DisplayName("策略同步器 - 同步状态统计测试")
    void testSyncStats() {
        // 适配新的 8 参数 SyncStats 构造器
        PolicyRefresher.SyncStats stats = new PolicyRefresher.SyncStats(
                true, 5, 1, "2026-07-16", 3, 15, "abc12345", 3);
        assertTrue(stats.running);
        assertEquals(5, stats.syncCount);
        assertEquals(1, stats.failCount);
        assertEquals("2026-07-16", stats.lastSyncTime);
        assertEquals(3, stats.lastPolicyCount, "lastPolicyCount 应为 3");
        assertEquals(15, stats.totalSyncedPolicies, "totalSyncedPolicies 应为 15");
        assertEquals("abc12345", stats.policyHash, "policyHash 应为 abc12345");
        assertEquals(3, stats.lastSyncedCount, "lastSyncedCount 应为 3");
        assertTrue(stats.toString().contains("syncs=5"), "toString 应包含 syncs=5");
        assertTrue(stats.toString().contains("hash=abc12345"), "toString 应包含 hash 信息");
    }

    // ==================== 端到端：Ranger策略同步 -> Pulsar访问控制 ====================

    @Test
    @Order(130)
    @DisplayName("端到端: Ranger策略同步后权限生效")
    void testE2EPolicySyncAndAccess() {
        // 先确认 Ranger Mock 可用
        try {
            java.net.HttpURLConnection conn = (java.net.HttpURLConnection)
                    new java.net.URL("http://localhost:6080/").openConnection();
            conn.setConnectTimeout(2000);
            int code = conn.getResponseCode();
            conn.disconnect();
            if (code != 200) return;
        } catch (Exception e) {
            return;
        }

        // 清理所有策略，只剩默认的
        plugin.cleanup();

        // 同步 Ranger 策略
        RangerPulsarConfig config = new RangerPulsarConfig();
        PolicyRefresher refresher = new PolicyRefresher(config, plugin);
        refresher.syncPolicies();

        // 同步后验证 Ranger 策略中的用户能访问
        // Ranger Mock 中 producer1 在 pulsar-producer 策略里
        RangerPulsarResource resource = new RangerPulsarResource("standalone", "public/default", "synced-topic");
        assertTrue(plugin.isAccessAllowed("producer1", Set.of(), ACCESS_TYPE_PRODUCE, resource),
                "Ranger 同步后 producer1 应有 produce 权限");
        assertTrue(plugin.isAccessAllowed("consumer1", Set.of(), ACCESS_TYPE_CONSUME, resource),
                "Ranger 同步后 consumer1 应有 consume 权限");
        assertFalse(plugin.isAccessAllowed("unknown-user", Set.of(), ACCESS_TYPE_PRODUCE, resource),
                "未知用户不应有权限");

        System.out.println("端到端策略同步测试通过!");
    }

    // ==================== 配置验证测试 (优化点2) ====================

    @Test
    @Order(140)
    @DisplayName("配置验证 - 默认配置应合法")
    void testConfigValidation_DefaultValid() {
        RangerPulsarConfig config = new RangerPulsarConfig();
        List<String> errors = config.validate();
        assertTrue(errors.isEmpty(), "默认配置应合法, 错误: " + errors);
        assertTrue(config.isValid(), "默认配置 isValid 应为 true");
    }

    @Test
    @Order(141)
    @DisplayName("配置验证 - SSL启用但URL非HTTPS应报错")
    void testConfigValidation_SslWithoutHttps() {
        RangerPulsarConfig config = new RangerPulsarConfig() {
            @Override
            public boolean isSslEnabled() { return true; }
            @Override
            public String getRangerAdminUrl() { return "http://localhost:6080"; }
        };
        List<String> errors = config.validate();
        assertFalse(errors.isEmpty(), "SSL启用但URL非HTTPS应有错误");
        assertTrue(errors.stream().anyMatch(e -> e.contains("HTTPS")), "应包含 HTTPS 错误信息");
        assertFalse(config.isValid(), "配置应不合法");
    }

    @Test
    @Order(142)
    @DisplayName("配置验证 - 各项非法值检测")
    void testConfigValidation_InvalidValues() {
        RangerPulsarConfig config = new RangerPulsarConfig() {
            @Override
            public int getCacheSize() { return 0; }
            @Override
            public long getSyncIntervalSeconds() { return 0; }
            @Override
            public int getSyncRetryMax() { return -1; }
            @Override
            public int getHttpConnectTimeoutMs() { return 0; }
            @Override
            public int getHttpReadTimeoutMs() { return -1; }
            @Override
            public String getServiceName() { return ""; }
            @Override
            public String getClusterName() { return null; }
        };
        List<String> errors = config.validate();
        assertFalse(errors.isEmpty(), "多项非法配置应有错误");
        assertTrue(errors.size() >= 7, "应有至少7个错误, 实际: " + errors.size() + " -> " + errors);
        assertFalse(config.isValid(), "配置应不合法");
    }

    // ==================== SSL 配置测试 (优化点2) ====================

    @Test
    @Order(150)
    @DisplayName("SSL配置 - 默认应关闭")
    void testSslConfig_DefaultDisabled() {
        RangerPulsarConfig config = new RangerPulsarConfig();
        assertFalse(config.isSslEnabled(), "默认 SSL 应关闭");
    }

    @Test
    @Order(151)
    @DisplayName("SSL配置 - SSL启用且HTTPS时配置合法")
    void testSslConfig_ValidWithHttps() {
        RangerPulsarConfig config = new RangerPulsarConfig() {
            @Override
            public boolean isSslEnabled() { return true; }
            @Override
            public String getRangerAdminUrl() { return "https://ranger.secure:6180"; }
        };
        List<String> errors = config.validate();
        assertTrue(errors.stream().noneMatch(e -> e.contains("HTTPS")),
                "SSL+HTTPS 时不应有 HTTPS 错误: " + errors);
    }

    // ==================== HTTP 超时配置测试 (优化点1+2) ====================

    @Test
    @Order(160)
    @DisplayName("HTTP超时配置 - 默认值测试")
    void testHttpTimeout_Defaults() {
        RangerPulsarConfig config = new RangerPulsarConfig();
        assertEquals(5000, config.getHttpConnectTimeoutMs(), "默认连接超时应为 5000ms");
        assertEquals(10000, config.getHttpReadTimeoutMs(), "默认读取超时应为 10000ms");
    }

    @Test
    @Order(161)
    @DisplayName("HTTP超时配置 - 验证为正数且读取>=连接")
    void testHttpTimeout_PositiveValues() {
        RangerPulsarConfig config = new RangerPulsarConfig();
        assertTrue(config.getHttpConnectTimeoutMs() > 0, "连接超时必须为正数");
        assertTrue(config.getHttpReadTimeoutMs() > 0, "读取超时必须为正数");
        assertTrue(config.getHttpReadTimeoutMs() >= config.getHttpConnectTimeoutMs(),
                "读取超时一般应 >= 连接超时");
    }

    // ==================== 重试配置测试 (优化点1+2) ====================

    @Test
    @Order(170)
    @DisplayName("重试配置 - 默认值测试")
    void testRetryConfig_Defaults() {
        RangerPulsarConfig config = new RangerPulsarConfig();
        assertEquals(3, config.getSyncRetryMax(), "默认最大重试次数应为 3");
        assertEquals(1000, config.getSyncRetryBackoffMs(), "默认退避时间应为 1000ms");
        assertTrue(config.getSyncRetryMax() >= 0, "重试次数应 >= 0");
        assertTrue(config.getSyncRetryBackoffMs() > 0, "退避时间应 > 0");
    }

    @Test
    @Order(171)
    @DisplayName("重试机制 - 不可达URL触发重试后失败计数增加")
    void testSyncRetryMechanism_Failure() {
        RangerPulsarConfig config = new RangerPulsarConfig() {
            @Override
            public String getRangerAdminUrl() { return "http://localhost:19999"; }
            @Override
            public int getSyncRetryMax() { return 2; }
            @Override
            public long getSyncRetryBackoffMs() { return 50; }  // 加速测试
            @Override
            public int getHttpConnectTimeoutMs() { return 300; }
            @Override
            public int getHttpReadTimeoutMs() { return 300; }
        };
        PolicyRefresher refresher = new PolicyRefresher(config, plugin);
        long start = System.currentTimeMillis();
        refresher.syncWithRetry();  // 带重试的同步
        long elapsed = System.currentTimeMillis() - start;

        PolicyRefresher.SyncStats stats = refresher.getStats();
        assertTrue(stats.failCount > 0, "应有失败记录");
        assertEquals(0, stats.syncCount, "不应有成功同步");
        // 2次重试间有指数退避：50ms + 100ms = 150ms
        assertTrue(elapsed >= 100, "应至少经历指数退避等待, 实际: " + elapsed + "ms");
    }

    // ==================== 系统属性覆盖测试 (优化点2) ====================

    @Test
    @Order(180)
    @DisplayName("系统属性覆盖 - 系统属性优先级最高")
    void testSystemPropertyOverride() {
        System.setProperty("ranger.pulsar.service.name", "override-service");
        System.setProperty("ranger.pulsar.cluster.name", "override-cluster");
        System.setProperty("ranger.pulsar.cache.size", "2048");
        try {
            RangerPulsarConfig config = new RangerPulsarConfig();
            assertEquals("override-service", config.getServiceName(), "系统属性应覆盖默认值");
            assertEquals("override-cluster", config.getClusterName());
            assertEquals(2048, config.getCacheSize());
        } finally {
            System.clearProperty("ranger.pulsar.service.name");
            System.clearProperty("ranger.pulsar.cluster.name");
            System.clearProperty("ranger.pulsar.cache.size");
        }
    }

    @Test
    @Order(181)
    @DisplayName("系统属性覆盖 - 可通过系统属性禁用同步")
    void testSystemPropertyDisableSync() {
        System.setProperty("ranger.pulsar.sync.enabled", "false");
        try {
            RangerPulsarConfig config = new RangerPulsarConfig();
            assertFalse(config.isSyncEnabled(), "系统属性应禁用同步");
        } finally {
            System.clearProperty("ranger.pulsar.sync.enabled");
        }
    }

    // ==================== 配置文件加载测试 (优化点2) ====================

    @Test
    @Order(190)
    @DisplayName("配置文件加载 - 从外部文件加载配置")
    void testConfigFileLoading() throws Exception {
        Path tmpFile = Files.createTempFile("ranger-pulsar-test-", ".properties");
        try {
            String content = "ranger.pulsar.service.name=file-service\n" +
                    "ranger.pulsar.cluster.name=file-cluster\n" +
                    "ranger.pulsar.cache.size=512\n" +
                    "ranger.pulsar.sync.retry.max=5\n" +
                    "ranger.pulsar.http.connect.timeout.ms=8000\n";
            Files.write(tmpFile, content.getBytes());

            RangerPulsarConfig config = new RangerPulsarConfig(tmpFile.toString());
            assertEquals("file-service", config.getServiceName(), "应从文件加载 service.name");
            assertEquals("file-cluster", config.getClusterName());
            assertEquals(512, config.getCacheSize());
            assertEquals(5, config.getSyncRetryMax());
            assertEquals(8000, config.getHttpConnectTimeoutMs());
            assertTrue(config.isValid(), "文件加载的配置应合法");
        } finally {
            Files.deleteIfExists(tmpFile);
        }
    }

    @Test
    @Order(191)
    @DisplayName("配置文件加载 - load() 静态方法读取指定路径")
    void testConfigLoad_StaticMethod() throws Exception {
        Path tmpFile = Files.createTempFile("ranger-pulsar-load-", ".properties");
        try {
            String content = "ranger.pulsar.service.name=loaded-service\n" +
                    "ranger.pulsar.cluster.name=loaded-cluster\n";
            Files.write(tmpFile, content.getBytes());

            System.setProperty("ranger.pulsar.config", tmpFile.toString());
            try {
                RangerPulsarConfig config = RangerPulsarConfig.load();
                assertEquals("loaded-service", config.getServiceName());
                assertEquals("loaded-cluster", config.getClusterName());
            } finally {
                System.clearProperty("ranger.pulsar.config");
            }
        } finally {
            Files.deleteIfExists(tmpFile);
        }
    }

    // ==================== 增量同步测试 (优化点1) ====================

    @Test
    @Order(200)
    @DisplayName("增量同步 - 相同内容第二次同步应跳过apply")
    void testIncrementalSync_SkipUnchanged() {
        try {
            java.net.HttpURLConnection conn = (java.net.HttpURLConnection)
                    new java.net.URL("http://localhost:6080/").openConnection();
            conn.setConnectTimeout(2000);
            int code = conn.getResponseCode();
            conn.disconnect();
            if (code != 200) {
                System.out.println("Ranger Mock not available, skipping incremental sync test");
                return;
            }
        } catch (Exception e) {
            System.out.println("Ranger Mock not available, skipping incremental sync test: " + e.getMessage());
            return;
        }

        RangerPulsarConfig config = new RangerPulsarConfig();
        PolicyRefresher refresher = new PolicyRefresher(config, plugin);

        // 第一次同步
        assertTrue(refresher.syncPolicies(), "第一次同步应成功");
        PolicyRefresher.SyncStats stats1 = refresher.getStats();
        String hash1 = stats1.policyHash;
        assertTrue(stats1.syncCount >= 1, "应有至少1次同步");
        assertFalse(hash1.isEmpty(), "应有非空 hash");

        // 第二次同步 - 内容相同应跳过 apply（增量同步）
        assertTrue(refresher.syncPolicies(), "第二次同步应成功");
        PolicyRefresher.SyncStats stats2 = refresher.getStats();
        assertEquals(hash1, stats2.policyHash, "hash 应保持不变（增量同步）");
        assertEquals(2, stats2.syncCount, "syncCount 应为 2");
    }

    @Test
    @Order(201)
    @DisplayName("forceSync - 强制全量同步忽略增量判断")
    void testForceSync() {
        try {
            java.net.HttpURLConnection conn = (java.net.HttpURLConnection)
                    new java.net.URL("http://localhost:6080/").openConnection();
            conn.setConnectTimeout(2000);
            int code = conn.getResponseCode();
            conn.disconnect();
            if (code != 200) {
                System.out.println("Ranger Mock not available, skipping forceSync test");
                return;
            }
        } catch (Exception e) {
            System.out.println("Ranger Mock not available, skipping forceSync test: " + e.getMessage());
            return;
        }

        RangerPulsarConfig config = new RangerPulsarConfig();
        PolicyRefresher refresher = new PolicyRefresher(config, plugin);

        // 第一次同步
        refresher.syncPolicies();
        PolicyRefresher.SyncStats stats1 = refresher.getStats();
        long totalSynced1 = stats1.totalSyncedPolicies;
        assertFalse(stats1.policyHash.isEmpty(), "应有 hash");

        // forceSync - 即使内容相同也会清空 hash 并重新 apply
        refresher.forceSync();
        PolicyRefresher.SyncStats stats2 = refresher.getStats();
        assertTrue(stats2.totalSyncedPolicies >= totalSynced1,
                "forceSync 后 totalSyncedPolicies 不应减少");
        assertFalse(stats2.policyHash.isEmpty(), "forceSync 后应有 hash");
    }

    // ==================== 同步锁测试 (优化点1) ====================

    @Test
    @Order(210)
    @DisplayName("同步锁 - 并发同步时 tryLock 保证不阻塞")
    void testConcurrentSyncLock() throws Exception {
        RangerPulsarConfig config = new RangerPulsarConfig() {
            @Override
            public String getRangerAdminUrl() { return "http://localhost:19999"; }  // 不可达
            @Override
            public int getHttpConnectTimeoutMs() { return 500; }
            @Override
            public int getHttpReadTimeoutMs() { return 500; }
        };
        PolicyRefresher refresher = new PolicyRefresher(config, plugin);

        int threadCount = 5;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger completedCount = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    latch.countDown();
                    latch.await();
                    refresher.syncPolicies();  // 并发调用
                    completedCount.incrementAndGet();
                } catch (Exception e) {
                    // ignore
                }
            });
        }

        executor.shutdown();
        assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS), "所有线程应完成");
        assertEquals(threadCount, completedCount.get(), "所有线程应正常返回（tryLock 不阻塞）");
    }

    // ==================== 审计队列满测试 (优化点2) ====================

    @Test
    @Order(220)
    @DisplayName("审计队列满 - 超出容量后不再入队但计数继续")
    void testAuditQueueFull() {
        int maxQueue = 5;
        RangerPulsarAuditHandler handler = new RangerPulsarAuditHandler(maxQueue);

        // 写入 10 个事件，超过队列容量 5
        for (int i = 0; i < 10; i++) {
            handler.logAccess("user" + i, "res" + i, "produce", i % 2 == 0);
        }

        RangerPulsarAuditHandler.AuditStats stats = handler.getStats();
        assertEquals(10, stats.totalEvents, "totalEvents 应计数所有 10 次");
        assertEquals(5, stats.grantedCount, "grantedCount 应为 5 (偶数索引)");
        assertEquals(5, stats.deniedCount, "deniedCount 应为 5 (奇数索引)");
        assertEquals(maxQueue, stats.queueSize, "队列大小应被限制为 " + maxQueue);
        assertEquals(maxQueue, handler.getAuditEvents().size(), "事件队列不应超过 " + maxQueue);
    }

    // ==================== 缓存 TTL 配置测试 (优化点2) ====================

    @Test
    @Order(230)
    @DisplayName("缓存TTL配置 - 默认值与有效性测试")
    void testCacheTtlConfig() {
        RangerPulsarConfig config = new RangerPulsarConfig();
        assertEquals(300, config.getCacheTtlSeconds(), "默认 TTL 应为 300 秒");
        assertTrue(config.getCacheTtlSeconds() > 0, "TTL 必须为正数");
    }

    // ==================== 配置 toString 完整性测试 (优化点2) ====================

    @Test
    @Order(240)
    @DisplayName("配置toString完整性 - 应包含所有新增配置项")
    void testConfigToString_Completeness() {
        RangerPulsarConfig config = new RangerPulsarConfig();
        String str = config.toString();
        assertTrue(str.contains("retryMax"), "应包含 retryMax");
        assertTrue(str.contains("retryBackoff"), "应包含 retryBackoff");
        assertTrue(str.contains("httpConnectTimeout"), "应包含 httpConnectTimeout");
        assertTrue(str.contains("httpReadTimeout"), "应包含 httpReadTimeout");
        assertTrue(str.contains("sslEnabled"), "应包含 sslEnabled");
        assertTrue(str.contains("auditMaxQueue"), "应包含 auditMaxQueue");
    }

    // ==================== PolicyRefresher 生命周期测试 (优化点1) ====================

    @Test
    @Order(250)
    @DisplayName("PolicyRefresher生命周期 - start/stop 测试")
    void testRefresherLifecycle() throws Exception {
        RangerPulsarConfig config = new RangerPulsarConfig() {
            @Override
            public boolean isSyncEnabled() { return true; }
            @Override
            public long getSyncIntervalSeconds() { return 1; }  // 1秒间隔
            @Override
            public String getRangerAdminUrl() { return "http://localhost:19999"; }
            @Override
            public int getSyncRetryMax() { return 0; }  // 不重试，加速测试
            @Override
            public int getHttpConnectTimeoutMs() { return 300; }
            @Override
            public int getHttpReadTimeoutMs() { return 300; }
        };

        PolicyRefresher refresher = new PolicyRefresher(config, plugin);
        refresher.start();

        // 等待一小段时间让调度器运行
        Thread.sleep(1500);

        PolicyRefresher.SyncStats stats = refresher.getStats();
        assertTrue(stats.running, "refresher 应处于运行状态");

        refresher.stop();
        PolicyRefresher.SyncStats statsAfterStop = refresher.getStats();
        assertFalse(statsAfterStop.running, "stop 后应不再运行");
    }
}
