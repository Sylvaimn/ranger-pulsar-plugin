package com.apache.ranger.pulsar.audit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 审计日志处理器，支持结构化审计记录和异步收集
 */
public class RangerPulsarAuditHandler {

    private static final Logger LOG = LoggerFactory.getLogger(RangerPulsarAuditHandler.class);
    private static final Logger AUDIT_LOG = LoggerFactory.getLogger("RANGER_AUDIT");

    private final Queue<AuditEvent> auditEvents = new ConcurrentLinkedQueue<>();
    private final AtomicLong totalEvents = new AtomicLong(0);
    private final AtomicLong grantedCount = new AtomicLong(0);
    private final AtomicLong deniedCount = new AtomicLong(0);
    private final int maxQueueSize;

    public RangerPulsarAuditHandler() {
        this(5000);
    }

    public RangerPulsarAuditHandler(int maxQueueSize) {
        this.maxQueueSize = maxQueueSize;
    }

    public void logAccess(String user, String resource, String accessType, boolean allowed) {
        totalEvents.incrementAndGet();
        if (allowed) {
            grantedCount.incrementAndGet();
            AUDIT_LOG.info("{\"timestamp\":\"{}\",\"event\":\"ACCESS\",\"result\":\"GRANTED\",\"user\":\"{}\",\"accessType\":\"{}\",\"resource\":\"{}\"}",
                    Instant.now(), user, accessType, resource);
        } else {
            deniedCount.incrementAndGet();
            AUDIT_LOG.warn("{\"timestamp\":\"{}\",\"event\":\"ACCESS\",\"result\":\"DENIED\",\"user\":\"{}\",\"accessType\":\"{}\",\"resource\":\"{}\"}",
                    Instant.now(), user, accessType, resource);
        }

        // 入队审计事件（用于后续分析）
        if (auditEvents.size() < maxQueueSize) {
            auditEvents.offer(new AuditEvent(Instant.now(), user, resource, accessType, allowed));
        }
    }

    public void logAdminOperation(String user, String operation, String resource) {
        totalEvents.incrementAndGet();
        AUDIT_LOG.info("{\"timestamp\":\"{}\",\"event\":\"ADMIN\",\"user\":\"{}\",\"operation\":\"{}\",\"resource\":\"{}\"}",
                Instant.now(), user, operation, resource);
        if (auditEvents.size() < maxQueueSize) {
            auditEvents.offer(new AuditEvent(Instant.now(), user, resource, operation, true));
        }
    }

    public AuditStats getStats() {
        return new AuditStats(totalEvents.get(), grantedCount.get(), deniedCount.get(), auditEvents.size());
    }

    public Queue<AuditEvent> getAuditEvents() {
        return auditEvents;
    }

    public void clearEvents() {
        auditEvents.clear();
    }

    public static class AuditEvent {
        public final Instant timestamp;
        public final String user;
        public final String resource;
        public final String accessType;
        public final boolean allowed;

        public AuditEvent(Instant timestamp, String user, String resource, String accessType, boolean allowed) {
            this.timestamp = timestamp;
            this.user = user;
            this.resource = resource;
            this.accessType = accessType;
            this.allowed = allowed;
        }

        @Override
        public String toString() {
            return String.format("AuditEvent{ts=%s, user='%s', access='%s', resource='%s', allowed=%s}",
                    timestamp, user, accessType, resource, allowed);
        }
    }

    public static class AuditStats {
        public final long totalEvents;
        public final long grantedCount;
        public final long deniedCount;
        public final int queueSize;

        public AuditStats(long totalEvents, long grantedCount, long deniedCount, int queueSize) {
            this.totalEvents = totalEvents;
            this.grantedCount = grantedCount;
            this.deniedCount = deniedCount;
            this.queueSize = queueSize;
        }

        public double getDenyRate() {
            return totalEvents == 0 ? 0.0 : (double) deniedCount / totalEvents;
        }

        @Override
        public String toString() {
            return String.format("AuditStats{total=%d, granted=%d, denied=%d, denyRate=%.2f%%, queueSize=%d}",
                    totalEvents, grantedCount, deniedCount, getDenyRate() * 100, queueSize);
        }
    }
}
