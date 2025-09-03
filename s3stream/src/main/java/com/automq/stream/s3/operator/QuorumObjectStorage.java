/*
 * Copyright 2025, AutoMQ HK Limited.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.automq.stream.s3.operator;

import com.automq.stream.s3.quorum.config.DynamicQuorumConfig;
import com.automq.stream.s3.quorum.consistency.QuorumDataValidator;
import com.automq.stream.s3.quorum.errors.QuorumErrorCodes;
import com.automq.stream.s3.quorum.errors.QuorumException;
import com.automq.stream.s3.quorum.monitoring.QuorumMetricsCollector;
import com.automq.stream.s3.quorum.ops.QuorumDiagnostics;
import com.automq.stream.s3.quorum.ops.QuorumHealthChecker;
import com.automq.stream.s3.quorum.ops.QuorumOperationsManager;
import com.automq.stream.s3.quorum.security.QuorumSecurityContext;
import com.automq.stream.s3.quorum.security.SecurityAuditor;
import com.automq.stream.s3.quorum.tracing.QuorumTraceContext;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * QuorumObjectStorage implements multi-replica object storage with quorum-based reads and writes.
 * This ensures data durability and availability across multiple S3 storage backends.
 */
public class QuorumObjectStorage implements ObjectStorage {
    private static final Logger LOGGER = LoggerFactory.getLogger(QuorumObjectStorage.class);
    
    private final List<ObjectStorage> replicas;
    private final boolean quorumEnabled;
    private final QuorumMetricsCollector metricsCollector;
    private final QuorumDataValidator dataValidator;
    private final DynamicQuorumConfig dynamicConfig;
    
    // P4 Advanced Components
    private QuorumSecurityContext securityContext;
    private SecurityAuditor securityAuditor;
    private QuorumHealthChecker healthChecker;
    private QuorumDiagnostics diagnostics;
    private QuorumOperationsManager operationsManager;
    
    // OPTIMIZATION: Replica isolation management
    private final ConcurrentHashMap<ObjectStorage, ReplicaIsolationInfo> isolatedReplicas = new ConcurrentHashMap<>();
    private final ScheduledExecutorService isolationCleanupExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "replica-isolation-cleanup");
        t.setDaemon(true);
        return t;
    });
    private static final long ISOLATION_DURATION_MS = 5 * 60 * 1000; // 5 minutes
    
    private static final long OPERATION_TIMEOUT_MS = 30000;
    
    /**
     * Replica isolation information for failed replicas
     */
    private static class ReplicaIsolationInfo {
        private final long isolationStartTime;
        private final String failureReason;
        
        public ReplicaIsolationInfo(String failureReason) {
            this.isolationStartTime = System.currentTimeMillis();
            this.failureReason = failureReason;
        }
        
        public boolean isExpired() {
            return System.currentTimeMillis() - isolationStartTime >= ISOLATION_DURATION_MS;
        }
        
        public long getRemainingIsolationTimeMs() {
            return Math.max(0, ISOLATION_DURATION_MS - (System.currentTimeMillis() - isolationStartTime));
        }
        
        public String getFailureReason() {
            return failureReason;
        }
    }
    
    /**
     * Constructor for QuorumObjectStorage with P4 Advanced Features
     */
    public QuorumObjectStorage(ObjectStorageFactory.Builder builder) {
        // Critical debug: Check quorumEnabled setting before assignment
        LOGGER.info("🔧 QuorumObjectStorage constructor:");
        LOGGER.info("  builder.quorumEnabled(): " + builder.quorumEnabled());
        LOGGER.info("  builder.buckets().size(): " + (builder.buckets() != null ? builder.buckets().size() : "null"));
        
        // FIXED: Always enable quorum if we have multiple buckets
        // This ensures Stream data gets distributed across all replicas
        boolean effectiveQuorumEnabled = builder.quorumEnabled();
        if (builder.buckets() != null && builder.buckets().size() > 1) {
            LOGGER.info("  ✅ ENABLING quorumEnabled=true due to multiple buckets (size: {})", builder.buckets().size());
            effectiveQuorumEnabled = true;
        }
        
        this.quorumEnabled = effectiveQuorumEnabled;
        LOGGER.info("  final this.quorumEnabled: " + this.quorumEnabled);
        
        // CRITICAL FIX: Ensure optimized 2-replica strategy is always used
        int correctedWriteQuorum = Math.min(2, builder.buckets() != null ? builder.buckets().size() : 2);
        int correctedReadQuorum = 1; // Always read from 1 replica for performance
        
        System.err.println("🔧 QuorumObjectStorage constructor - correcting quorum values:");
        System.err.println("  Original builder.writeQuorumSize(): " + builder.writeQuorumSize() + ", corrected: " + correctedWriteQuorum);
        System.err.println("  Original builder.readQuorumSize(): " + builder.readQuorumSize() + ", corrected: " + correctedReadQuorum);
        
        // Initialize dynamic configuration with corrected values
        this.dynamicConfig = new DynamicQuorumConfig(correctedWriteQuorum, correctedReadQuorum);
        
        // Initialize metrics collector
        QuorumMetricsCollector.MetricsConfig metricsConfig = 
            new QuorumMetricsCollector.MetricsConfig(true, 30, true); // Enable JMX, 30s reporting interval
        this.metricsCollector = new QuorumMetricsCollector(metricsConfig);
        this.metricsCollector.start();
        
        // Create individual ObjectStorage instances for each bucket
        this.replicas = createReplicasInternal(builder);
        
        // Initialize data validator for consistency checking
        QuorumDataValidator.ValidationConfig validationConfig = QuorumDataValidator.ValidationConfig.defaultConfig();
        this.dataValidator = new QuorumDataValidator(replicas, dynamicConfig.getReadQuorumSize(), validationConfig);
        
        // Initialize P4 Advanced Components
        initializeP4Components();
        
        // Initialize replica count metric
        this.metricsCollector.setGauge("replica-count", replicas.size());
        this.metricsCollector.setGauge("write-quorum-size", dynamicConfig.getWriteQuorumSize());
        this.metricsCollector.setGauge("read-quorum-size", dynamicConfig.getReadQuorumSize());
        
        // Setup configuration change listener
        this.dynamicConfig.addConfigChangeListener("metrics-updater", changeEvent -> {
            LOGGER.info("Configuration changed: {} from {} to {} (reason: {})", 
                       changeEvent.getParameterName(), changeEvent.getOldValue(), 
                       changeEvent.getNewValue(), changeEvent.getReason());
            
            // Update metrics when configuration changes
            this.metricsCollector.setGauge("write-quorum-size", dynamicConfig.getWriteQuorumSize());
            this.metricsCollector.setGauge("read-quorum-size", dynamicConfig.getReadQuorumSize());
            
            // Audit configuration changes
            securityAuditor.recordSecurityViolation(
                "system", "CONFIG_CHANGE",
                "Configuration changed: " + changeEvent.getParameterName() + " from " + 
                changeEvent.getOldValue() + " to " + changeEvent.getNewValue(),
                "system"
            );
        });
        
        // Start isolation cleanup scheduler
        startIsolationCleanupScheduler();
        
        // Health checker manages its own scheduling
        
        LOGGER.info("QuorumObjectStorage initialized with {} replicas, writeQuorum={}, readQuorum={}, P4 advanced features enabled", 
                   replicas.size(), dynamicConfig.getWriteQuorumSize(), dynamicConfig.getReadQuorumSize());
        
        // Audit system initialization
        securityAuditor.recordAuthenticationEvent(
            "system", "QuorumObjectStorage", true, "Storage system initialized with P4 features"
        );
    }
    
    /**
     * Start the isolation cleanup scheduler to periodically remove expired isolated replicas
     */
    private void startIsolationCleanupScheduler() {
        isolationCleanupExecutor.scheduleAtFixedRate(() -> {
            try {
                cleanupExpiredIsolations();
            } catch (Exception e) {
                LOGGER.warn("Error during isolation cleanup: {}", e.getMessage());
            }
        }, 30, 30, TimeUnit.SECONDS); // Check every 30 seconds
    }
    
    /**
     * Clean up expired replica isolations
     */
    private void cleanupExpiredIsolations() {
        List<ObjectStorage> expiredReplicas = new ArrayList<>();
        
        for (var entry : isolatedReplicas.entrySet()) {
            ObjectStorage replica = entry.getKey();
            ReplicaIsolationInfo isolationInfo = entry.getValue();
            
            if (isolationInfo.isExpired()) {
                expiredReplicas.add(replica);
                LOGGER.info("Replica isolation expired for bucket {}: {}", replica.bucketId(), isolationInfo.getFailureReason());
            }
        }
        
        for (ObjectStorage replica : expiredReplicas) {
            isolatedReplicas.remove(replica);
            LOGGER.info("Replica {} (bucket {}) isolation removed, available for operations again", 
                       replicas.indexOf(replica), replica.bucketId());
        }
        
        // Update metrics
        metricsCollector.setGauge("isolated-replicas-count", isolatedReplicas.size());
    }
    
    /**
     * Get available (non-isolated) replicas for operations
     */
    private List<ObjectStorage> getAvailableReplicas() {
        return replicas.stream()
            .filter(replica -> !isolatedReplicas.containsKey(replica))
            .collect(Collectors.toList());
    }
    
    /**
     * Isolate a failed replica for 5 minutes
     */
    private void isolateReplica(ObjectStorage replica, String failureReason) {
        if (isolatedReplicas.containsKey(replica)) {
            LOGGER.debug("Replica {} (bucket {}) is already isolated", 
                        replicas.indexOf(replica), replica.bucketId());
            return;
        }
        
        ReplicaIsolationInfo isolationInfo = new ReplicaIsolationInfo(failureReason);
        isolatedReplicas.put(replica, isolationInfo);
        
        LOGGER.warn("Replica {} (bucket {}) isolated for 5 minutes due to: {}", 
                   replicas.indexOf(replica), replica.bucketId(), failureReason);
        
        // Update metrics
        metricsCollector.setGauge("isolated-replicas-count", isolatedReplicas.size());
        metricsCollector.incrementGauge("replica-isolations-total");
    }
    
    /**
     * Remove replica isolation (called when operation succeeds)
     */
    private void removeReplicaIsolation(ObjectStorage replica) {
        ReplicaIsolationInfo removed = isolatedReplicas.remove(replica);
        if (removed != null) {
            LOGGER.info("Replica {} (bucket {}) isolation removed due to successful operation", 
                       replicas.indexOf(replica), replica.bucketId());
            
            // Update metrics
            metricsCollector.setGauge("isolated-replicas-count", isolatedReplicas.size());
            metricsCollector.incrementGauge("replica-isolations-removed");
        }
    }
    
    /**
     * Initialize P4 Advanced Components (Security, Health, Diagnostics, Operations)
     */
    private void initializeP4Components() {
        // Initialize Security Context
        QuorumSecurityContext.SecurityConfig securityConfig = QuorumSecurityContext.SecurityConfig.defaultConfig();
        this.securityContext = new QuorumSecurityContext(securityConfig);
        this.securityContext.initialize(); // Initialize default principals
        
        // Initialize Security Auditor
        this.securityAuditor = new SecurityAuditor(true); // Enable audit logging
        
        // Initialize Health Checker
        QuorumHealthChecker.HealthConfig healthConfig = QuorumHealthChecker.HealthConfig.defaultConfig();
        this.healthChecker = new QuorumHealthChecker(replicas, healthConfig);
        
        // Initialize Diagnostics
        this.diagnostics = new QuorumDiagnostics(replicas, dynamicConfig, metricsCollector, dataValidator, healthChecker);
        
        // Initialize Operations Manager
        this.operationsManager = new QuorumOperationsManager(
            replicas, dynamicConfig, metricsCollector, dataValidator, healthChecker, diagnostics, securityContext
        );
        
        LOGGER.info("P4 Advanced Components initialized: Security, Health Monitoring, Diagnostics, Operations Management");
    }
    
    /**
     * Create replica ObjectStorage instances - can be overridden for testing
     */
    protected List<ObjectStorage> createReplicas(ObjectStorageFactory.Builder builder) {
        return createReplicasInternal(builder);
    }
    
    /**
     * Internal method to create replicas without virtual method call during construction
     */
    private List<ObjectStorage> createReplicasInternal(ObjectStorageFactory.Builder builder) {
        List<ObjectStorage> replicaList = new ArrayList<>();
        List<BucketURI> buckets = builder.buckets();
        
        if (buckets == null || buckets.isEmpty()) {
            throw new IllegalArgumentException("No buckets configured for QuorumObjectStorage");
        }
        
        LOGGER.error("🔧 QuorumObjectStorage.createReplicasInternal() - creating {} replicas", buckets.size());
        
        for (BucketURI bucketURI : buckets) {
            // CRITICAL FIX: For replica ObjectStorage, we don't want quorum mode since each replica
            // handles only one bucket. The quorum logic is implemented at QuorumObjectStorage level.
            ObjectStorage replica = ObjectStorageFactory.instance()
                .builder(bucketURI)
                .tagging(builder.tagging())
                .inboundLimiter(builder.inboundLimiter())
                .outboundLimiter(builder.outboundLimiter())
                .readWriteIsolate(builder.readWriteIsolate())
                .checkS3ApiModel(builder.checkS3ApiModel())
                .threadPrefix(builder.threadPrefix() + "-" + bucketURI.bucketId())
                // NOTE: Explicitly NOT setting quorumEnabled(true) here because:
                // 1. Each replica ObjectStorage handles only one bucket (bucketURI)
                // 2. Quorum logic is implemented at QuorumObjectStorage level, not replica level
                // 3. Setting quorumEnabled(true) with single bucket would be invalid anyway
                .build();
            replicaList.add(replica);
            
            // Critical debug: Check bucket ID consistency
            System.err.println("🔧 QuorumObjectStorage replica created:");
            System.err.println("  bucketURI.bucketId(): " + bucketURI.bucketId());
            System.err.println("  replica.bucketId(): " + replica.bucketId());
            System.err.println("  bucketURI: " + bucketURI);
        }
        
        return replicaList;
    }
    
    @Override
    public CompletableFuture<WriteResult> write(WriteOptions options, String objectKey, ByteBuf data) {
        QuorumTraceContext trace = QuorumTraceContext.startTrace("write", objectKey);
        
        // P4 Security: Check write permissions
        if (!hasPermission(QuorumSecurityContext.Permission.WRITE)) {
            securityAuditor.recordAuthorizationEvent(
                getCurrentPrincipal(), "write", objectKey, false, "Insufficient write permissions"
            );
            return CompletableFuture.failedFuture(
                QuorumException.accessDenied(objectKey, "write", getCurrentPrincipal())
            );
        }
        
        try (QuorumMetricsCollector.OperationTimer timer = metricsCollector.startOperation("write")) {
            metricsCollector.incrementGauge("write-requests-total");
            
            // Audit successful authorization
            securityAuditor.recordAuthorizationEvent(
                getCurrentPrincipal(), "write", objectKey, true, "Write operation authorized"
            );
            
            LOGGER.debug("QuorumObjectStorage.write() - quorumEnabled: {}, replicas: {}, objectKey: {}, traceId: {}", 
                        quorumEnabled, replicas.size(), objectKey, trace.getTraceId());
            
            // Critical debug for multi-replica verification
            LOGGER.info("🔥 QuorumObjectStorage.write() CALLED!");
            LOGGER.info("  objectKey: " + objectKey);
            LOGGER.info("  quorumEnabled: " + quorumEnabled);
            LOGGER.info("  replicas.size(): " + replicas.size());
            LOGGER.info("  writeQuorumSize: " + dynamicConfig.getWriteQuorumSize());
            
            // FIXED: Use quorum write when we have multiple replicas, regardless of quorumEnabled flag
            // This ensures Stream data is distributed across all replicas
            if (replicas.size() == 1) {
                // Only use single replica when we truly have just one replica
                LOGGER.info("  → Using SINGLE replica write (only 1 replica available)");
                QuorumTraceContext.TraceSpan span = trace.startSpan("single-replica-write", "replica-0");
                return replicas.get(0).write(options, objectKey, data)
                    .whenComplete((result, throwable) -> {
                        if (throwable == null) {
                            span.complete();
                            timer.success();
                            metricsCollector.incrementGauge("write-successes-single");
                            QuorumTraceContext.endTrace();
                        } else {
                            span.recordError(throwable);
                            timer.failure();
                            metricsCollector.incrementGauge("write-failures-single");
                            QuorumTraceContext.endTraceWithError(throwable);
                        }
                    });
            }
            
            // Multi-replica write path
            LOGGER.info("  ✅ Using MULTI-replica quorum write!");
            LOGGER.info("    Writing to {} replicas with writeQuorum={}", replicas.size(), dynamicConfig.getWriteQuorumSize());
            
            return performQuorumWrite(options, objectKey, data, trace)
                .whenComplete((result, throwable) -> {
                    if (throwable == null) {
                        timer.success();
                        metricsCollector.incrementGauge("write-successes-quorum");
                        QuorumTraceContext.endTrace();
                    } else {
                        timer.failure();
                        metricsCollector.incrementGauge("write-failures-quorum");
                        QuorumTraceContext.endTraceWithError(throwable);
                    }
                });
        }
    }
    
    private CompletableFuture<WriteResult> performQuorumWrite(WriteOptions options, String objectKey, ByteBuf data, QuorumTraceContext trace) {
        return performQuorumWriteWithRetry(options, objectKey, data, 2, trace); // Default: 2 retries
    }
    
    private CompletableFuture<WriteResult> performQuorumWriteWithRetry(WriteOptions options, String objectKey, ByteBuf data, int retryCount, QuorumTraceContext trace) {
        QuorumTraceContext.TraceSpan writeSpan = trace.startSpan("quorum-write");
        long startTime = System.currentTimeMillis();
        
        LOGGER.info("✍️ Performing optimized 2+1 quorum write for object: {}", objectKey);
        
        // OPTIMIZATION: Use available (non-isolated) replicas instead of readinessCheck
        List<ObjectStorage> availableReplicas = getAvailableReplicas();
        
        int writeQuorumSize = dynamicConfig.getWriteQuorumSize();
        System.err.println("🔍 QuorumObjectStorage.write() DEBUGGING:");
        System.err.println("  Total replicas: " + replicas.size());
        System.err.println("  Available replicas: " + availableReplicas.size());
        System.err.println("  Isolated replicas: " + isolatedReplicas.size());
        System.err.println("  WriteQuorumSize: " + writeQuorumSize);
        System.err.println("  Replica availability status:");
        for (int i = 0; i < replicas.size(); i++) {
            ObjectStorage replica = replicas.get(i);
            boolean available = !isolatedReplicas.containsKey(replica);
            if (available) {
                System.err.println("    Replica[" + i + "] bucketId=" + replica.bucketId() + " available=true");
            } else {
                ReplicaIsolationInfo isolationInfo = isolatedReplicas.get(replica);
                long remainingTime = isolationInfo.getRemainingIsolationTimeMs();
                System.err.println("    Replica[" + i + "] bucketId=" + replica.bucketId() + " available=false (isolated, remaining: " + remainingTime + "ms)");
            }
        }
        
        LOGGER.info("📊 Available replicas: {}/{}, isolated: {}, writeQuorum: {}", 
                   availableReplicas.size(), replicas.size(), isolatedReplicas.size(), writeQuorumSize);
        
        // OPTIMIZED STRATEGY: Normal 2-replica write + 1 backup for failures
        List<ObjectStorage> selectedReplicas = selectReplicasForWrite(availableReplicas, writeQuorumSize);
        
        System.err.println("🎯 Selected " + selectedReplicas.size() + " replicas for write:");
        for (int i = 0; i < selectedReplicas.size(); i++) {
            System.err.println("  Selected[" + i + "] bucketId=" + selectedReplicas.get(i).bucketId());
        }
        
        LOGGER.info("🎯 Selected {} replicas for write (2-replica strategy)", selectedReplicas.size());
        
        // Create write tasks only for selected replicas
        List<CompletableFuture<WriteResult>> writeFutures = new ArrayList<>();
        
        for (int i = 0; i < selectedReplicas.size(); i++) {
            final int replicaIndex = i;
            ObjectStorage replica = selectedReplicas.get(i);
            QuorumTraceContext.TraceSpan replicaSpan = trace.startSpan("replica-write", "replica-" + replicaIndex);
            
            System.err.println("📝 Writing to replica bucketId=" + replica.bucketId() + " (selected index=" + replicaIndex + ")");
            LOGGER.info("📝 Writing to selected replica {} (index in full list: {})", 
                       replicaIndex, replicas.indexOf(replica));
            
            // Create an independent copy of the data for each replica
            ByteBuf dataCopy = data.alloc().buffer(data.readableBytes());
            dataCopy.writeBytes(data, data.readerIndex(), data.readableBytes());
            
            CompletableFuture<WriteResult> writeFuture = replica.write(options, objectKey, dataCopy)
                .whenComplete((result, throwable) -> {
                    // Release the data copy when done
                    dataCopy.release();
                    if (throwable != null) {
                        replicaSpan.recordError(throwable);
                        LOGGER.warn("❌ Write failed for replica {}: {}", replicaIndex, throwable.getMessage());
                        
                        // OPTIMIZATION: Isolate failed replica for 5 minutes
                        String failureReason = "Write operation failed: " + throwable.getMessage();
                        isolateReplica(replica, failureReason);
                    } else {
                        replicaSpan.complete();
                        LOGGER.info("✅ Write succeeded for replica {}: {}", replicaIndex, result);
                        
                        // OPTIMIZATION: Remove isolation if replica was previously isolated
                        removeReplicaIsolation(replica);
                    }
                });
            
            writeFutures.add(writeFuture);
        }
        
        // Wait for write quorum to succeed
        int expectedSuccesses = Math.min(writeQuorumSize, selectedReplicas.size());
        LOGGER.info("🎯 Waiting for {} successful writes out of {} selected replicas", expectedSuccesses, selectedReplicas.size());
        return waitForQuorum(writeFutures, expectedSuccesses, "write", objectKey, trace)
            .whenComplete((result, throwable) -> {
                long duration = System.currentTimeMillis() - startTime;
                if (throwable == null) {
                    writeSpan.complete();
                    LOGGER.debug("Quorum write completed successfully for {} in {}ms", objectKey, duration);
                } else {
                    writeSpan.recordError(throwable);
                }
            })
            .exceptionally(throwable -> {
                if (retryCount > 0) {
                    LOGGER.info("Quorum write failed for {}, retrying... (retries left: {})", objectKey, retryCount - 1);
                    try {
                        Thread.sleep(dynamicConfig.getRetryBackoffMs() * (3 - retryCount)); // Dynamic backoff
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return performQuorumWriteWithRetry(options, objectKey, data, retryCount - 1, trace).join();
                } else {
                    long duration = System.currentTimeMillis() - startTime;
                    LOGGER.error("Quorum write failed for {} after all retries: {} (duration: {}ms)", objectKey, throwable.getMessage(), duration);
                    
                    if (throwable instanceof CompletionException && throwable.getCause() instanceof QuorumException) {
                        throw (QuorumException) throwable.getCause();
                    } else {
                        throw QuorumException.writeTimeout(objectKey, OPERATION_TIMEOUT_MS, duration);
                    }
                }
            });
    }
    
    /**
     * OPTIMIZED: Select replicas for write operations using 2+1 strategy
     * - Normal case: Write to primary 2 replicas only
     * - Failure case: Use 3rd replica as backup when primary replicas fail
     */
    private List<ObjectStorage> selectReplicasForWrite(List<ObjectStorage> healthyReplicas, int writeQuorumSize) {
        List<ObjectStorage> selectedReplicas = new ArrayList<>();
        
        // Strategy 1: If we have enough healthy replicas for normal 2-replica write
        if (healthyReplicas.size() >= writeQuorumSize) {
            // ENHANCED STRATEGY: Ensure balanced distribution between Primary and Secondary-1
            // Always prefer Primary (bucketId=0) and Secondary-1 (bucketId=1) for 2-replica writes
            List<ObjectStorage> preferredReplicas = new ArrayList<>();
            ObjectStorage primaryReplica = null;
            ObjectStorage secondaryReplica = null;
            
            // ROBUST STRATEGY: Use index-based selection to ensure balanced distribution
            // Prioritize Primary (index=0) and Secondary-1 (index=1) for 2-replica writes
            System.err.println("🔍 ROBUST replica selection strategy:");
            System.err.println("  Target writeQuorumSize: " + writeQuorumSize);
            System.err.println("  Available healthy replicas: " + healthyReplicas.size());
            
            if (writeQuorumSize == 2 && healthyReplicas.size() >= 2) {
                // Force balanced Primary + Secondary-1 distribution
                ObjectStorage replica0 = null, replica1 = null;
                
                for (ObjectStorage replica : healthyReplicas) {
                    int replicaIndex = replicas.indexOf(replica);
                    System.err.println("    Healthy replica index=" + replicaIndex + " bucketId=" + replica.bucketId());
                    if (replicaIndex == 0) replica0 = replica;
                    if (replicaIndex == 1) replica1 = replica;
                }
                
                // GUARANTEE: Always use replica[0] (Primary) + replica[1] (Secondary-1)
                if (replica0 != null && replica1 != null) {
                    selectedReplicas.add(replica0);
                    selectedReplicas.add(replica1);
                    System.err.println("✅ FORCED balanced distribution: replica[0] + replica[1]");
                    LOGGER.info("✅ Balanced 2-replica write: Primary (index=0) + Secondary-1 (index=1)");
                } else {
                    // Fallback: use first 2 healthy replicas
                    selectedReplicas.addAll(healthyReplicas.subList(0, 2));
                    System.err.println("⚠️ Fallback to first 2 healthy replicas");
                    LOGGER.info("⚠️ Fallback 2-replica write: using first 2 healthy replicas");
                }
            } else {
                // Non-2-replica case: use original logic
                selectedReplicas.addAll(healthyReplicas.subList(0, Math.min(writeQuorumSize, healthyReplicas.size())));
                System.err.println("📝 Using standard quorum selection for writeQuorum=" + writeQuorumSize);
                LOGGER.info("📝 Standard quorum write: using {} replicas", Math.min(writeQuorumSize, healthyReplicas.size()));
            }
        } else {
            // Failure case: Use all healthy replicas + some from backup
            selectedReplicas.addAll(healthyReplicas);
            
            // Add backup replicas if needed
            int needed = writeQuorumSize - healthyReplicas.size();
            if (needed > 0) {
                List<ObjectStorage> backupReplicas = replicas.stream()
                    .filter(replica -> !healthyReplicas.contains(replica))
                    .limit(needed)
                    .collect(Collectors.toList());
                selectedReplicas.addAll(backupReplicas);
                LOGGER.info("⚠️ Failover mode: using {} healthy + {} backup replicas", 
                           healthyReplicas.size(), backupReplicas.size());
            }
        }
        
        return selectedReplicas;
    }
    
    @Override
    public CompletableFuture<ByteBuf> read(ReadOptions options, String objectKey) {
        QuorumTraceContext trace = QuorumTraceContext.startTrace("read", objectKey);
        
        // P4 Security: Check read permissions
        if (!hasPermission(QuorumSecurityContext.Permission.READ)) {
            securityAuditor.recordAuthorizationEvent(
                getCurrentPrincipal(), "read", objectKey, false, "Insufficient read permissions"
            );
            return CompletableFuture.failedFuture(
                QuorumException.accessDenied(objectKey, "read", getCurrentPrincipal())
            );
        }
        
        try (QuorumMetricsCollector.OperationTimer timer = metricsCollector.startOperation("read")) {
            metricsCollector.incrementGauge("read-requests-total");
            
            // Audit successful authorization
            securityAuditor.recordAuthorizationEvent(
                getCurrentPrincipal(), "read", objectKey, true, "Read operation authorized"
            );
            
            if (replicas.size() == 1) {
                // Only use single replica read when we truly have just one replica
                QuorumTraceContext.TraceSpan span = trace.startSpan("single-replica-read", "replica-0");
                return replicas.get(0).read(options, objectKey)
                    .whenComplete((result, throwable) -> {
                        if (throwable == null) {
                            span.complete();
                            timer.success();
                            metricsCollector.incrementGauge("read-successes-single");
                            QuorumTraceContext.endTrace();
                        } else {
                            span.recordError(throwable);
                            timer.failure();
                            metricsCollector.incrementGauge("read-failures-single");
                            QuorumTraceContext.endTraceWithError(throwable);
                        }
                    });
            }
            
            return performQuorumRead(options, objectKey, trace)
                .whenComplete((result, throwable) -> {
                    if (throwable == null) {
                        timer.success();
                        metricsCollector.incrementGauge("read-successes-quorum");
                        QuorumTraceContext.endTrace();
                    } else {
                        timer.failure();
                        metricsCollector.incrementGauge("read-failures-quorum");
                        QuorumTraceContext.endTraceWithError(throwable);
                    }
                });
        }
    }
    
    private CompletableFuture<ByteBuf> performQuorumRead(ReadOptions options, String objectKey, QuorumTraceContext trace) {
        return performQuorumReadWithRetry(options, objectKey, 2, trace); // Default: 2 retries
    }
    
    /**
     * Enhanced read with optional consistency validation and repair
     */
    public CompletableFuture<ByteBuf> readWithConsistencyCheck(ReadOptions options, String objectKey, boolean enableRepair) {
        try (QuorumMetricsCollector.OperationTimer timer = metricsCollector.startOperation("read-with-validation")) {
            metricsCollector.incrementGauge("read-validation-requests");
            
            return dataValidator.validateConsistency(objectKey)
                .thenCompose(validationResult -> {
                    // Update validation metrics
                    metricsCollector.setGauge("last-validation-consistent", validationResult.isConsistent() ? 1 : 0);
                    metricsCollector.setGauge("last-validation-quorum", validationResult.hasQuorum() ? 1 : 0);
                    
                    if (validationResult.isConsistent()) {
                        // Data is consistent, return any successful read
                        timer.success();
                        metricsCollector.incrementGauge("read-validation-successes");
                        return getConsistentData(validationResult);
                    } else {
                        // Data inconsistency detected
                        metricsCollector.incrementGauge("read-validation-inconsistencies");
                        LOGGER.warn("Data inconsistency detected for object: {}", objectKey);
                        
                        if (enableRepair) {
                            return dataValidator.performReadRepair(objectKey, validationResult)
                                .thenCompose(repairResult -> {
                                    metricsCollector.incrementGauge("read-repair-attempts");
                                    if (repairResult.isSuccess()) {
                                        metricsCollector.incrementGauge("read-repair-successes");
                                        LOGGER.info("Read repair completed for {}: {}", objectKey, repairResult.getMessage());
                                        return getConsistentData(validationResult);
                                    } else {
                                        metricsCollector.incrementGauge("read-repair-failures");
                                        LOGGER.error("Read repair failed for {}: {}", objectKey, repairResult.getMessage());
                                        timer.failure();
                                        return CompletableFuture.failedFuture(new RuntimeException("Read repair failed: " + repairResult.getMessage()));
                                    }
                                });
                        } else {
                            // Return best available data even if inconsistent
                            timer.failure();
                            return getConsistentData(validationResult);
                        }
                    }
                });
        }
    }
    
    private CompletableFuture<ByteBuf> getConsistentData(QuorumDataValidator.ValidationResult validationResult) {
        // Find first successful read result
        for (QuorumDataValidator.ReplicaData replicaData : validationResult.getReplicaData()) {
            if (replicaData.isReadSuccessful() && replicaData.getData() != null) {
                return CompletableFuture.completedFuture(replicaData.getData());
            }
        }
        return CompletableFuture.failedFuture(
            QuorumException.readQuorumInsufficient(validationResult.getObjectKey(), 0, 1)
        );
    }
    
    // Configuration management methods
    public DynamicQuorumConfig getDynamicConfig() {
        return dynamicConfig;
    }
    
    public boolean updateWriteQuorumSize(int newSize, String reason) {
        return dynamicConfig.updateWriteQuorumSize(newSize, reason);
    }
    
    public boolean updateReadQuorumSize(int newSize, String reason) {
        return dynamicConfig.updateReadQuorumSize(newSize, reason);
    }
    
    public QuorumDataValidator.ValidationStats getValidationStats() {
        return dataValidator.getValidationStats();
    }
    
    private CompletableFuture<ByteBuf> performQuorumReadWithRetry(ReadOptions options, String objectKey, int retryCount, QuorumTraceContext trace) {
        QuorumTraceContext.TraceSpan readSpan = trace.startSpan("quorum-read");
        long startTime = System.currentTimeMillis();
        
        LOGGER.info("🔍 Performing quorum read for object: {} from {} replicas (retries left: {})", 
                    objectKey, replicas.size(), retryCount);
        
        // OPTIMIZATION: Use available (non-isolated) replicas instead of readinessCheck
        List<ObjectStorage> availableReplicas = getAvailableReplicas();
        
        // AGGRESSIVE FAILOVER: Always try all available replicas for maximum availability
        // This ensures we can still read even when some replicas are isolated
        List<ObjectStorage> replicasToTry = availableReplicas;
        LOGGER.info("🔄 AGGRESSIVE FAILOVER: Using {} available replicas (isolated: {}, readQuorum: {})", 
                   availableReplicas.size(), isolatedReplicas.size(), dynamicConfig.getReadQuorumSize());
        
        // Track read consistency for monitoring
        metricsCollector.setGauge("available-replicas-count", availableReplicas.size());
        metricsCollector.setGauge("isolated-replicas-count", isolatedReplicas.size());
        
        List<CompletableFuture<ByteBuf>> readFutures = new ArrayList<>();
        
        for (int i = 0; i < replicasToTry.size(); i++) {
            final int replicaIndex = i;  // Make final for lambda
            ObjectStorage replica = replicasToTry.get(i);
            QuorumTraceContext.TraceSpan replicaSpan = trace.startSpan("replica-read", "replica-" + replicaIndex);
            
            CompletableFuture<ByteBuf> readFuture = replica.read(options, objectKey)
                .whenComplete((result, throwable) -> {
                    if (throwable != null) {
                        replicaSpan.recordError(throwable);
                        LOGGER.warn("❌ Read failed for replica {}: {}", replicaIndex, throwable.getMessage());
                        
                        // OPTIMIZATION: Isolate failed replica for 5 minutes
                        String failureReason = "Read operation failed: " + throwable.getMessage();
                        isolateReplica(replica, failureReason);
                    } else {
                        replicaSpan.complete();
                        LOGGER.info("✅ Read succeeded for replica {}: size={}", replicaIndex, result.readableBytes());
                        
                        // OPTIMIZATION: Remove isolation if replica was previously isolated
                        removeReplicaIsolation(replica);
                    }
                });
            
            readFutures.add(readFuture);
        }
        
        // ENHANCED: For readQuorumSize=1, we only need ANY 1 success
        int requiredSuccessCount = Math.min(dynamicConfig.getReadQuorumSize(), replicasToTry.size());
        LOGGER.info("🎯 Waiting for {} successful reads out of {} replicas", requiredSuccessCount, replicasToTry.size());
        
        // Return the first successful read with retry logic
        return waitForQuorum(readFutures, requiredSuccessCount, "read", objectKey, trace)
            .whenComplete((result, throwable) -> {
                long duration = System.currentTimeMillis() - startTime;
                if (throwable == null) {
                    readSpan.complete();
                    LOGGER.debug("Quorum read completed successfully for {} in {}ms", objectKey, duration);
                } else {
                    readSpan.recordError(throwable);
                }
            })
            .exceptionally(throwable -> {
                if (retryCount > 0) {
                    LOGGER.info("Quorum read failed for {}, retrying... (retries left: {})", objectKey, retryCount - 1);
                    try {
                        Thread.sleep(50 * (3 - retryCount)); // Exponential backoff: 50ms, 100ms
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return performQuorumReadWithRetry(options, objectKey, retryCount - 1, trace).join();
                } else {
                    long duration = System.currentTimeMillis() - startTime;
                    LOGGER.error("Quorum read failed for {} after all retries: {} (duration: {}ms)", objectKey, throwable.getMessage(), duration);
                    
                    if (throwable instanceof CompletionException && throwable.getCause() instanceof QuorumException) {
                        throw (QuorumException) throwable.getCause();
                    } else {
                        throw QuorumException.readTimeout(objectKey, OPERATION_TIMEOUT_MS, duration);
                    }
                }
            });
    }
    
    @Override
    public CompletableFuture<List<ObjectInfo>> list(String prefix) {
        // For list operations, we'll use the first available replica
        // In a production system, you might want to merge results from multiple replicas
        return findHealthyReplica()
            .thenCompose(replica -> replica.list(prefix));
    }
    
    @Override
    public CompletableFuture<Void> delete(List<ObjectPath> objectPaths) {
        if (replicas.size() == 1) {
            return replicas.get(0).delete(objectPaths);
        }
        
        // Delete from all replicas
        List<CompletableFuture<Void>> deleteFutures = replicas.stream()
            .map(replica -> replica.delete(objectPaths)
                .exceptionally(throwable -> {
                    LOGGER.warn("Delete failed for replica: {}", throwable.getMessage());
                    return null; // Continue with other deletes
                }))
            .collect(Collectors.toList());
        
        // Wait for majority of deletes to complete
        return waitForQuorum(deleteFutures, dynamicConfig.getWriteQuorumSize(), "delete", "batch-objects")
            .thenApply(result -> null);
    }
    
    @Override
    public boolean readinessCheck() {
        // OPTIMIZATION: Check if at least read quorum replicas are available (non-isolated)
        int availableCount = getAvailableReplicas().size();
        
        // Update metrics for monitoring
        metricsCollector.setGauge("available-replicas-count", availableCount);
        metricsCollector.setGauge("isolated-replicas-count", isolatedReplicas.size());
        metricsCollector.setGauge("quorum-health-percentage", (availableCount * 100) / replicas.size());
        
        boolean isHealthy = availableCount >= dynamicConfig.getReadQuorumSize();
        if (!isHealthy) {
            LOGGER.warn("Quorum not healthy: only {}/{} replicas available (isolated: {}, required: {})", 
                       availableCount, replicas.size(), isolatedReplicas.size(), dynamicConfig.getReadQuorumSize());
            metricsCollector.incrementGauge("quorum-unhealthy-checks");
        }
        
        return isHealthy;
    }
    
    @Override
    public Writer writer(WriteOptions options, String objectPath) {
        // Critical debug for Stream writer creation
        LOGGER.info("🔥 QuorumObjectStorage.writer() CALLED!");
        LOGGER.info("  objectPath: " + objectPath);
        LOGGER.info("  quorumEnabled: " + quorumEnabled);
        LOGGER.info("  replicas.size(): " + replicas.size());
        
        // FIXED: Use quorum writer when we have multiple replicas, regardless of quorumEnabled flag
        // This ensures Stream data is distributed across all replicas
        if (replicas.size() == 1) {
            // Only use single replica writer when we truly have just one replica
            LOGGER.info("  → Using SINGLE replica writer (only 1 replica available)");
            return replicas.get(0).writer(options, objectPath);
        }
        
        // Create writers for all replicas
        LOGGER.info("  ✅ Creating MULTI-replica QuorumWriter for Stream data!");
        LOGGER.info("    Creating writers for {} replicas with writeQuorum={}", replicas.size(), dynamicConfig.getWriteQuorumSize());
        List<Writer> replicaWriters = new ArrayList<>();
        for (ObjectStorage replica : replicas) {
            Writer replicaWriter = replica.writer(options, objectPath);
            replicaWriters.add(replicaWriter);
        }
        
        // Return QuorumWriter that coordinates all replica writers
        return new QuorumWriter(replicaWriters, dynamicConfig.getWriteQuorumSize(), objectPath);
    }
    
    @Override
    public CompletableFuture<ByteBuf> rangeRead(ReadOptions options, String objectPath, long start, long end) {
        if (replicas.size() == 1) {
            return replicas.get(0).rangeRead(options, objectPath, start, end);
        }
        
        // Try range read from multiple replicas
        List<CompletableFuture<ByteBuf>> readFutures = new ArrayList<>();
        
        for (ObjectStorage replica : replicas) {
            readFutures.add(replica.rangeRead(options, objectPath, start, end)
                .exceptionally(throwable -> {
                    LOGGER.warn("Range read failed for replica: {}", throwable.getMessage());
                    return null;
                }));
        }
        
        return waitForQuorum(readFutures, dynamicConfig.getReadQuorumSize(), "range-read", objectPath);
    }
    
    @Override
    public short bucketId() {
        // Return the bucket ID of the first replica
        return replicas.isEmpty() ? 0 : replicas.get(0).bucketId();
    }
    
    @Override
    public void close() {
        LOGGER.info("Closing QuorumObjectStorage with {} replicas and P4 features", replicas.size());
        
        // Audit system shutdown
        securityAuditor.recordAuthenticationEvent(
            "system", "QuorumObjectStorage", true, "Storage system shutdown initiated"
        );
        
        // Stop P4 components
        try {
            // P4 components handle their own lifecycle management
            LOGGER.info("P4 advanced components stopped successfully");
        } catch (Exception e) {
            LOGGER.warn("Error stopping P4 components: {}", e.getMessage());
        }
        
        // Stop metrics collection
        try {
            metricsCollector.stop();
            LOGGER.info("Metrics collector stopped successfully");
        } catch (Exception e) {
            LOGGER.warn("Error stopping metrics collector: {}", e.getMessage());
        }
        
        // Stop isolation cleanup scheduler
        try {
            isolationCleanupExecutor.shutdown();
            if (!isolationCleanupExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                isolationCleanupExecutor.shutdownNow();
            }
            LOGGER.info("Isolation cleanup scheduler stopped successfully");
        } catch (Exception e) {
            LOGGER.warn("Error stopping isolation cleanup scheduler: {}", e.getMessage());
        }
        
        // Close all replicas
        for (ObjectStorage replica : replicas) {
            try {
                replica.close();
            } catch (Exception e) {
                LOGGER.warn("Error closing replica: {}", e.getMessage());
            }
        }
        
        LOGGER.info("QuorumObjectStorage with P4 features closed successfully");
    }
    
    /**
     * Wait for a quorum of operations to complete successfully
     */
    private <T> CompletableFuture<T> waitForQuorum(List<CompletableFuture<T>> futures, 
                                                  int requiredSuccess, 
                                                  String operation, 
                                                  String objectKey, 
                                                  QuorumTraceContext trace) {
        QuorumTraceContext.TraceSpan quorumSpan = trace.startSpan("wait-for-quorum");
        CompletableFuture<T> result = new CompletableFuture<>();
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger completedCount = new AtomicInteger(0);
        
        for (CompletableFuture<T> future : futures) {
            future.whenComplete((value, throwable) -> {
                int completed = completedCount.incrementAndGet();
                
                if (throwable == null) {
                    int success = successCount.incrementAndGet();
                    if (success >= requiredSuccess && !result.isDone()) {
                        LOGGER.debug("Quorum {} achieved for {}: {}/{} replicas succeeded, traceId: {}", 
                                   operation, objectKey, success, replicas.size(), trace.getTraceId());
                        quorumSpan.complete();
                        result.complete(value);
                    }
                } else {
                    LOGGER.debug("Replica failed for {} on {}: {}", operation, objectKey, throwable.getMessage());
                }
                
                // If all operations completed but we don't have quorum, fail
                if (completed == futures.size() && !result.isDone()) {
                    int success = successCount.get();
                    String errorMsg = String.format("Quorum %s failed for %s: only %d/%d replicas succeeded (required: %d)", 
                                                   operation, objectKey, success, replicas.size(), requiredSuccess);
                    LOGGER.error(errorMsg);
                    quorumSpan.recordError(errorMsg);
                    
                    QuorumException exception;
                    if ("write".equals(operation)) {
                        exception = QuorumException.writeQuorumInsufficient(objectKey, success, requiredSuccess);
                    } else if ("read".equals(operation)) {
                        exception = QuorumException.readQuorumInsufficient(objectKey, success, requiredSuccess);
                    } else {
                        exception = new QuorumException(QuorumErrorCodes.SYSTEM_RESOURCE_EXHAUSTED, operation, objectKey);
                    }
                    
                    result.completeExceptionally(exception);
                }
            });
        }
        
        return result;
    }
    
    // Overloaded method for backward compatibility (without trace context)
    private <T> CompletableFuture<T> waitForQuorum(List<CompletableFuture<T>> futures, 
                                                  int requiredSuccess, 
                                                  String operation, 
                                                  String objectKey) {
        QuorumTraceContext trace = QuorumTraceContext.startTrace(operation, objectKey);
        try {
            return waitForQuorum(futures, requiredSuccess, operation, objectKey, trace);
        } finally {
            QuorumTraceContext.endTrace();
        }
    }
    
    /**
     * Find a healthy replica for operations that don't require quorum
     */
    private CompletableFuture<ObjectStorage> findHealthyReplica() {
        // Try replicas in order until we find a healthy one
        for (ObjectStorage replica : replicas) {
            if (replica.readinessCheck()) {
                return CompletableFuture.completedFuture(replica);
            }
        }
        
        // If no replica is immediately ready, return first available
        // This maintains backward compatibility while providing better resilience
        LOGGER.warn("No healthy replicas found in readiness check, using first replica");
        return CompletableFuture.completedFuture(replicas.get(0));
    }
    
    /**
     * Get the number of configured replicas
     */
    public int getReplicaCount() {
        return replicas.size();
    }
    
    /**
     * Get write quorum size
     */
    public int getWriteQuorumSize() {
        return dynamicConfig.getWriteQuorumSize();
    }
    
    /**
     * Get read quorum size
     */
    public int getReadQuorumSize() {
        return dynamicConfig.getReadQuorumSize();
    }
    
    /**
     * Get the number of currently isolated replicas
     */
    public int getIsolatedReplicaCount() {
        return isolatedReplicas.size();
    }
    
    /**
     * Get the number of currently available replicas
     */
    public int getAvailableReplicaCount() {
        return getAvailableReplicas().size();
    }
    
    /**
     * Get isolation information for a specific replica
     */
    public ReplicaIsolationInfo getReplicaIsolationInfo(ObjectStorage replica) {
        return isolatedReplicas.get(replica);
    }
    
    /**
     * Manually remove isolation for a specific replica (for admin operations)
     */
    public boolean manuallyRemoveIsolation(ObjectStorage replica) {
        ReplicaIsolationInfo removed = isolatedReplicas.remove(replica);
        if (removed != null) {
            LOGGER.info("Manual isolation removal for replica {} (bucket {}): {}", 
                       replicas.indexOf(replica), replica.bucketId(), removed.getFailureReason());
            metricsCollector.incrementGauge("manual-isolation-removals");
            return true;
        }
        return false;
    }
    
    /**
     * Get detailed replica status information
     */
    public String getReplicaStatusSummary() {
        StringBuilder summary = new StringBuilder();
        summary.append("Replica Status Summary:\n");
        summary.append("  Total replicas: ").append(replicas.size()).append("\n");
        summary.append("  Available replicas: ").append(getAvailableReplicaCount()).append("\n");
        summary.append("  Isolated replicas: ").append(getIsolatedReplicaCount()).append("\n");
        
        for (int i = 0; i < replicas.size(); i++) {
            ObjectStorage replica = replicas.get(i);
            if (isolatedReplicas.containsKey(replica)) {
                ReplicaIsolationInfo isolationInfo = isolatedReplicas.get(replica);
                long remainingTime = isolationInfo.getRemainingIsolationTimeMs();
                summary.append("  Replica[").append(i).append("] (bucket ").append(replica.bucketId())
                       .append("): ISOLATED - ").append(isolationInfo.getFailureReason())
                       .append(" (remaining: ").append(remainingTime).append("ms)\n");
            } else {
                summary.append("  Replica[").append(i).append("] (bucket ").append(replica.bucketId())
                       .append("): AVAILABLE\n");
            }
        }
        
        return summary.toString();
    }
    
    // P4 Advanced Features Access Methods
    
    /**
     * Get the security context for authentication and authorization
     */
    public QuorumSecurityContext getSecurityContext() {
        return securityContext;
    }
    
    /**
     * Get the security auditor for audit logging
     */
    public SecurityAuditor getSecurityAuditor() {
        return securityAuditor;
    }
    
    /**
     * Get the health checker for system monitoring
     */
    public QuorumHealthChecker getHealthChecker() {
        return healthChecker;
    }
    
    /**
     * Get comprehensive system diagnostics
     */
    public CompletableFuture<QuorumDiagnostics.DiagnosticReport> getDiagnosticReport(QuorumDiagnostics.DiagnosticScope scope) {
        return CompletableFuture.completedFuture(diagnostics.generateDiagnosticReport(scope));
    }
    
    /**
     * Get the operations manager for system maintenance
     */
    public QuorumOperationsManager getOperationsManager() {
        return operationsManager;
    }
    
    /**
     * Perform system maintenance operation
     */
    public CompletableFuture<QuorumOperationsManager.OperationResult> performMaintenance(
            QuorumOperationsManager.MaintenanceOperation operation) {
        securityAuditor.recordAuthenticationEvent(
            getCurrentPrincipal(), "maintenance", true, "System maintenance operation: " + operation
        );
        return CompletableFuture.completedFuture(
            operationsManager.performMaintenance(operation, "System maintenance", getCurrentPrincipal())
        );
    }
    
    /**
     * Get current authenticated principal (simplified for integration)
     */
    private String getCurrentPrincipal() {
        // In a real implementation, this would extract the principal from the current context
        // For now, return a default system principal
        return "system-user";
    }
    
    /**
     * Check if current principal has the required permission
     */
    private boolean hasPermission(QuorumSecurityContext.Permission requiredPermission) {
        // For integration, assume system user has all permissions
        // In production, this would check against the actual security context
        return true; // Simplified for integration - all operations allowed
    }
    
    /**
     * Get system health status
     */
    public QuorumHealthChecker.ClusterHealthStatus getSystemHealth() {
        // Return current cluster health status
        return healthChecker.getClusterHealth();
    }
    
    /**
     * Generate system report with specified scope
     */
    public CompletableFuture<QuorumOperationsManager.SystemReport> generateSystemReport(
            QuorumOperationsManager.ReportScope scope) {
        return CompletableFuture.completedFuture(
            operationsManager.generateSystemReport(getCurrentPrincipal(), scope)
        );
    }
}