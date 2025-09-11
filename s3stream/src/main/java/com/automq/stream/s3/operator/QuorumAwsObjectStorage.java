package com.automq.stream.s3.operator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.buffer.ByteBuf;

public class QuorumAwsObjectStorage implements ObjectStorage {

    private static final Logger LOGGER = LoggerFactory.getLogger(QuorumAwsObjectStorage.class);
    private final List<AwsObjectStorage> storages;
    private final Map<String, AwsObjectStorage> storageMap = new ConcurrentHashMap<>();
    private final com.automq.stream.s3.Config config;
    private final int walQuorumSize;
    private final int walQuorumWriteSize;
    private final int walQuorumReadSize;
    private final int retryCount;
    private final long isolationDurationMs;

    // 隔离管理：存储节点bucket -> 隔离结束时间
    private final Map<String, Long> isolatedNodes = new ConcurrentHashMap<>();

    // 隔离节点清理线程池（单线程）
    private final ScheduledExecutorService isolationCleanupScheduler;

    // 写入重试线程池（4个核心线程，队列深度1000，阻塞等待策略）
    private final ScheduledExecutorService retryScheduler;
    private final ThreadPoolExecutor retryExecutor;

    {
        isolationCleanupScheduler = Executors.newSingleThreadScheduledExecutor(
            r -> new Thread(r, "quorum-isolation-cleanup"));

        // 创建普通线程池用于执行任务（支持队列深度限制）
        retryExecutor = new ThreadPoolExecutor(
            4, // 核心线程数
            4, // 最大线程数
            60L, TimeUnit.SECONDS, // 线程空闲时间
            new LinkedBlockingQueue<>(1000), // 队列深度1000
            r -> new Thread(r, "quorum-write-retry"), // 线程命名
            new ThreadPoolExecutor.CallerRunsPolicy() // 队列满时阻塞等待策略
        );

        // 创建调度线程池用于延迟调度（单线程足够）
        retryScheduler = Executors.newSingleThreadScheduledExecutor(
            r -> new Thread(r, "quorum-write-retry-scheduler"));
    }

    static class RetryContext {
        int attempt;
        List<String> failedNodes = new CopyOnWriteArrayList<>();
        List<String> successfulNodes = new CopyOnWriteArrayList<>();

        RetryContext() {
            this.attempt = 0;
        }

        synchronized void recordSuccess(String bucket) {
            if (!successfulNodes.contains(bucket)) {
                successfulNodes.add(bucket);
            }
        }

        synchronized boolean isSuccessful(String bucket) {
            return successfulNodes.contains(bucket);
        }

        synchronized List<String> getSuccessfulNodes() {
            return new CopyOnWriteArrayList<>(successfulNodes);
        }
    }


    public QuorumAwsObjectStorage(List<AwsObjectStorage> storages, com.automq.stream.s3.Config config) {
        if (storages == null || storages.isEmpty()) {
            throw new IllegalArgumentException("storages must not be empty");
        }
        this.storages = storages;
        this.config = config;

        // 初始化bucket到storage的映射
        for (AwsObjectStorage storage : storages) {
            String bucket = storage.bucketURI.bucket();
            storageMap.put(bucket, storage);
        }

        if (config != null) {
            this.walQuorumSize = config.walQuorumSize();
            this.walQuorumWriteSize = config.walQuorumWriteSize();
            this.walQuorumReadSize = config.walQuorumReadSize();
            this.retryCount = config.walQuorumRetryCount();
            this.isolationDurationMs = config.walQuorumIsolationDurationMs();
        } else {
            //TODO metrics和WAL日志传递config配置待沟通合适实现方式
            this.walQuorumSize = 3;
            this.walQuorumWriteSize = 2;
            this.walQuorumReadSize = 1;
            this.retryCount = 3;
            this.isolationDurationMs = 300000L; // 5 minutes
        }

        // 启动隔离清理任务
        isolationCleanupScheduler.scheduleAtFixedRate(this::cleanupIsolatedNodes,
            isolationDurationMs, isolationDurationMs, TimeUnit.MILLISECONDS);

        if (walQuorumSize <= 0 || walQuorumSize < walQuorumWriteSize) {
            throw new IllegalArgumentException("walQuorumSize must large than zero and walQuorumSize must large than walQuorumWriteSize");
        }
    }

    @Override
    public boolean readinessCheck() {
        AtomicInteger success = new AtomicInteger(0);
        List<String> availableBuckets = getAvailableBuckets();
        if (availableBuckets.isEmpty()) {
            availableBuckets = getAllBuckets();
        }

        for (String bucket : availableBuckets) {
            AwsObjectStorage storage = getStorageByBucket(bucket);
            if (storage != null && storage.readinessCheck()) {
                if (success.incrementAndGet() >= walQuorumWriteSize) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public void close() {
        // 关闭隔离清理线程池
        isolationCleanupScheduler.shutdown();
        try {
            if (!isolationCleanupScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                isolationCleanupScheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            isolationCleanupScheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // 关闭重试调度线程池
        retryScheduler.shutdown();
        try {
            if (!retryScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                retryScheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            retryScheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // 关闭重试执行线程池
        retryExecutor.shutdown();
        try {
            if (!retryExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                retryExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            retryExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        for (AwsObjectStorage storage : storages) {
            storage.close();
        }
    }

    /**
     * 检查节点是否被隔离
     */
    private boolean isIsolated(String bucket) {
        Long isolationEndTime = isolatedNodes.get(bucket);
        if (isolationEndTime == null) {
            return false;
        }
        if (System.currentTimeMillis() > isolationEndTime) {
            isolatedNodes.remove(bucket);
            return false;
        }
        return true;
    }

    /**
     * 隔离失败的节点
     */
    private void isolateNode(String bucket) {
        long isolationEndTime = System.currentTimeMillis() + isolationDurationMs;
        isolatedNodes.put(bucket, isolationEndTime);
        LOGGER.info("[QUORUM-ISOLATION] Isolating bucket {} until {}",
            bucket, isolationEndTime);
    }

    /**
     * 清理过期的隔离节点
     */
    private void cleanupIsolatedNodes() {
        long currentTime = System.currentTimeMillis();
        isolatedNodes.entrySet().removeIf(entry -> {
            boolean expired = currentTime > entry.getValue();
            if (expired) {
                LOGGER.info("[QUORUM-ISOLATION] Bucket {} isolation expired", entry.getKey());
            }
            return expired;
        });
    }

    /**
     * 获取可用的（未隔离的）存储节点bucket列表
     */
    private List<String> getAvailableBuckets() {
        List<String> available = new ArrayList<>();
        for (AwsObjectStorage storage : storages) {
            String bucket = storage.bucketURI.bucket();
            if (!isIsolated(bucket)) {
                available.add(bucket);
            }
        }
        return available;
    }


    /**
     * 获取节点的bucket字符串 - 保留用于兼容性
     */
    private String getNodeBucket(int nodeIndex) {
        if (nodeIndex >= 0 && nodeIndex < storages.size()) {
            return storages.get(nodeIndex).bucketURI.bucket();
        }
        return null;
    }

    /**
     * 通过bucket名称获取AwsObjectStorage
     */
    private AwsObjectStorage getStorageByBucket(String bucket) {
        return storageMap.get(bucket);
    }

    /**
     * 获取所有可用的bucket列表
     */
    private List<String> getAllBuckets() {
        List<String> buckets = new ArrayList<>();
        for (AwsObjectStorage storage : storages) {
            buckets.add(storage.bucketURI.bucket());
        }
        return buckets;
    }

    /**
     * 获取排除已成功写入节点的可用bucket列表
     */
    private List<String> getAvailableBucketsExcludeSuccessful(RetryContext retryContext) {
        List<String> available = new ArrayList<>();
        for (AwsObjectStorage storage : storages) {
            String bucket = storage.bucketURI.bucket();
            if (!isIsolated(bucket) && !retryContext.isSuccessful(bucket)) {
                available.add(bucket);
            }
        }
        return available;
    }

    /**
     * 获取所有排除已成功写入节点的bucket列表（包括被隔离的）
     */
    private List<String> getAllBucketsExcludeSuccessful(RetryContext retryContext) {
        List<String> buckets = new ArrayList<>();
        for (AwsObjectStorage storage : storages) {
            String bucket = storage.bucketURI.bucket();
            if (!retryContext.isSuccessful(bucket)) {
                buckets.add(bucket);
            }
        }
        return buckets;
    }

    /**
     * 计算重试延迟时间（指数退避）
     */
    private long calculateDelay(int attempt) {
        return Math.min(1000L * (1L << (attempt - 1)), 10000L); // 最大延迟10秒
    }

    // 1. doWrite: 2+1优化写入策略 - 优先写前2个副本，成功则不写第3个，支持重试和节点隔离
    public CompletableFuture<Void> doWrite(WriteOptions options, String path, ByteBuf data) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        RetryContext retryContext = new RetryContext();

        data.retain(); // 保持引用计数，用于重试
        doWriteWithRetry(options, path, data, result, retryContext);

        return result;
    }

    private void doWriteWithRetry(WriteOptions options, String path, ByteBuf data,
                                  CompletableFuture<Void> result, RetryContext retryContext) {
        if (retryContext.attempt >= retryCount) {
            // 重试次数耗尽，释放数据并返回失败
            data.release();
            result.completeExceptionally(new RuntimeException(
                "Write failed after " + retryCount + " retries"));
            return;
        }

        retryContext.attempt++;

        // 检查是否已经达到写入quorum
        if (retryContext.getSuccessfulNodes().size() >= walQuorumWriteSize) {
            LOGGER.info("[QUORUM-RETRY] Attempt {}: Already achieved write quorum with {} successful nodes",
                retryContext.attempt, retryContext.getSuccessfulNodes().size());
            data.release();
            result.complete(null);
            return;
        }

        // 获取排除已成功节点的可用bucket列表
        List<String> availableBuckets = getAvailableBucketsExcludeSuccessful(retryContext);

        if (availableBuckets.size() < (walQuorumWriteSize - retryContext.getSuccessfulNodes().size())) {
            // 可用节点不足，尝试包含隔离节点但仍排除已成功的节点
            availableBuckets = getAllBucketsExcludeSuccessful(retryContext);
            LOGGER.info("[QUORUM-RETRY] Attempt {}: Using isolated nodes due to insufficient available nodes, excluding {} successful nodes",
                retryContext.attempt, retryContext.getSuccessfulNodes().size());
        }

        int remainingQuorum = walQuorumWriteSize - retryContext.getSuccessfulNodes().size();
        if (availableBuckets.size() < remainingQuorum) {
            // 回退到所有副本并行写入
            doWriteAllReplicasWithRetry(options, path, data, result, retryContext, availableBuckets);
        } else {
            // 使用2+1优化策略，优先使用可用节点
            writeToPrimaryReplicasWithRetry(options, path, data, result, retryContext, availableBuckets);
        }
    }

    private void writeToPrimaryReplicasWithRetry(WriteOptions options, String path, ByteBuf data,
                                                CompletableFuture<Void> result, RetryContext retryContext,
                                                List<String> availableBuckets) {
        AtomicInteger primaryCompleted = new AtomicInteger(0);

        int remainingQuorum = walQuorumWriteSize - retryContext.getSuccessfulNodes().size();
        int primaryCount = Math.min(remainingQuorum, availableBuckets.size());
        // 向前N个可用副本写入
        for (int i = 0; i < primaryCount; i++) {
            ByteBuf copy = data.retainedDuplicate();
            String bucket = availableBuckets.get(i);
            AwsObjectStorage storage = getStorageByBucket(bucket);

            if (storage == null) {
                LOGGER.warn("[QUORUM-RETRY] Storage not found for bucket: {}", bucket);
                copy.release();
                continue;
            }

            storage.doWrite(options, path, copy).whenComplete((v, ex) -> {
                try {
                    if (ex == null) {
                        // 记录成功写入的节点
                        retryContext.recordSuccess(bucket);
                        int totalSuccessCount = retryContext.getSuccessfulNodes().size();

                        LOGGER.debug("[QUORUM-RETRY] Attempt {}: Bucket {} write succeeded, total successful: {}",
                            retryContext.attempt, bucket, totalSuccessCount);

                        if (totalSuccessCount >= walQuorumWriteSize) {
                            // 足够的副本写入成功，释放原始data并返回成功
                            if (data.refCnt() > 0) {
                                data.release();
                            }
                            result.complete(null);
                            return;
                        }
                    } else {
                        // 记录失败的节点用于隔离
                        retryContext.failedNodes.add(bucket);
                        LOGGER.info("[QUORUM-RETRY] Attempt {}: Bucket {} failed: {}",
                            retryContext.attempt, bucket, ex.getMessage());
                    }

                    // 检查所有主副本是否都完成了
                    if (primaryCompleted.incrementAndGet() >= primaryCount) {
                        if (!result.isDone() && availableBuckets.size() > primaryCount) {
                            // 尝试备用副本
                            writeToBackupReplicaWithRetry(options, path, data, retryContext.getSuccessfulNodes().size(),
                                result, retryContext, availableBuckets, primaryCount);
                        } else if (!result.isDone()) {
                            // 当前尝试失败，隔离失败节点并重试
                            for (String failedBucket : retryContext.failedNodes) {
                                isolateNode(failedBucket);
                            }
                            retryContext.failedNodes.clear();

                            // 延迟重试：调度器延迟后提交到执行器
                            retryScheduler.schedule(() -> retryExecutor.execute(() -> {
                                try {
                                    doWriteWithRetry(options, path, data, result, retryContext);
                                } catch (Exception retryEx) {
                                    // 异常时确保释放ByteBuf
                                    if (data != null && data.refCnt() > 0) {
                                        data.release();
                                    }
                                    result.completeExceptionally(retryEx);
                                }
                            }), 100, TimeUnit.MILLISECONDS);
                        }
                    }
                } finally {
                    // 释放ByteBuf副本
                    if (copy != null && copy.refCnt() > 0) {
                        copy.release();
                    }
                }
            });
        }
    }

    private void writeToBackupReplicaWithRetry(WriteOptions options, String path, ByteBuf data,
                                              int totalSuccessCount, CompletableFuture<Void> result,
                                              RetryContext retryContext, List<String> availableBuckets,
                                              int primaryCount) {
        // 如果结果已经完成，直接返回，避免无意义的重试
        if (result.isDone()) {
            return;
        }

        // 如果没有更多备用副本，重试整个操作
        if (availableBuckets.size() <= primaryCount) {
            doWriteRetryAll(options, path, data,
                totalSuccessCount, result,
                retryContext, availableBuckets,
                primaryCount);
            return;
        }

        // 尝试下一个可用节点
        String backupBucket = availableBuckets.get(primaryCount);
        AwsObjectStorage backupStorage = getStorageByBucket(backupBucket);
        if (backupStorage == null) {
            LOGGER.warn("[QUORUM-RETRY] Backup storage not found for bucket: {}", backupBucket);
            doWriteRetryAll(options, path, data,
                totalSuccessCount, result,
                retryContext, availableBuckets,
                primaryCount);
            return;
        }

        ByteBuf backupCopy = data.retainedDuplicate();
        backupStorage.doWrite(options, path, backupCopy).whenComplete((v, ex) -> {
            try {
                if (ex == null) {
                    // 记录备用副本成功
                    retryContext.recordSuccess(backupBucket);
                    int newTotalSuccessCount = retryContext.getSuccessfulNodes().size();

                    LOGGER.debug("[QUORUM-RETRY] Attempt {}: Backup bucket {} write succeeded, total successful: {}",
                        retryContext.attempt, backupBucket, newTotalSuccessCount);

                    // 备用副本成功，检查总成功数是否达到quorum
                    if (newTotalSuccessCount >= walQuorumWriteSize) {
                        if (data.refCnt() > 0) {
                            data.release();
                        }
                        result.complete(null);
                        return;
                    }
                } else {
                    // 备用副本也失败，记录失败节点
                    retryContext.failedNodes.add(backupBucket);
                    LOGGER.info("[QUORUM-RETRY] Attempt {}: Backup bucket {} failed: {}",
                        retryContext.attempt, backupBucket, ex.getMessage());
                }

                // 当前尝试失败，隔离失败节点并重试
                if (retryContext.getSuccessfulNodes().size() < walQuorumWriteSize) {
                    for (String failedBucket : retryContext.failedNodes) {
                        isolateNode(failedBucket);
                    }
                    retryContext.failedNodes.clear();

                    retryScheduler.schedule(() -> retryExecutor.execute(() ->
                        doWriteWithRetry(options, path, data, result, retryContext)),
                        100, TimeUnit.MILLISECONDS);
                }
            } finally {
                // 释放ByteBuf副本
                if (backupCopy != null && backupCopy.refCnt() > 0) {
                    backupCopy.release();
                }
            }
        });
    }

    private void doWriteRetryAll(WriteOptions options, String path, ByteBuf data,
                                 int totalSuccessCount, CompletableFuture<Void> result,
                                 RetryContext retryContext, List<String> availableBuckets,
                                 int primaryCount){
        // 重试整个操作
        for (String failedBucket : retryContext.failedNodes) {
            isolateNode(failedBucket);
        }
        retryContext.failedNodes.clear();
        retryScheduler.schedule(() -> retryExecutor.execute(() -> {
            try {
                doWriteWithRetry(options, path, data, result, retryContext);
            } catch (Exception retryEx) {
                // 异常时确保释放ByteBuf
                if (data != null && data.refCnt() > 0) {
                    data.release();
                }
                result.completeExceptionally(retryEx);
            }
        }), 100, TimeUnit.MILLISECONDS);
    }

    // 回退方法：所有副本并行写入逻辑（支持重试）
    private void doWriteAllReplicasWithRetry(WriteOptions options, String path, ByteBuf data,
                                            CompletableFuture<Void> result, RetryContext retryContext,
                                            List<String> availableBuckets) {
        AtomicInteger completed = new AtomicInteger(0);

        for (String bucket : availableBuckets) {
            AwsObjectStorage storage = getStorageByBucket(bucket);
            if (storage == null) {
                LOGGER.warn("[QUORUM-RETRY] Storage not found for bucket: {}", bucket);
                if (completed.incrementAndGet() == availableBuckets.size()) {
                    handleAllReplicasCompleted(options, path, data, result, retryContext);
                }
                continue;
            }

            ByteBuf copy = data.retainedDuplicate();
            storage.doWrite(options, path, copy).whenComplete((v, ex) -> {
                try {
                    if (ex == null) {
                        // 记录成功写入的节点
                        retryContext.recordSuccess(bucket);
                        int totalSuccessCount = retryContext.getSuccessfulNodes().size();

                        if (totalSuccessCount >= walQuorumWriteSize) {
                            if (data.refCnt() > 0) {
                                data.release();
                            }
                            result.complete(null);
                        }
                    } else {
                        // 记录失败节点
                        synchronized (retryContext.failedNodes) {
                            if (!retryContext.failedNodes.contains(bucket)) {
                                retryContext.failedNodes.add(bucket);
                            }
                        }
                    }
                } finally {
                    if (copy != null && copy.refCnt() > 0) {
                        copy.release();
                    }

                    if (completed.incrementAndGet() == availableBuckets.size()) {
                        if (!result.isDone()) {
                            handleAllReplicasCompleted(options, path, data, result, retryContext);
                        }
                    }
                }
            });
        }
    }

    private void handleAllReplicasCompleted(WriteOptions options, String path, ByteBuf data,
                                          CompletableFuture<Void> result, RetryContext retryContext) {
        if (!result.isDone()) {
            // 如果没有达到quorum且还有重试次数，则重试
            if (retryContext.attempt < retryCount) {
                // 隔离失败的节点
                for (String failedBucket : retryContext.failedNodes) {
                    isolateNode(failedBucket);
                }

                // 准备重试
                retryContext.failedNodes.clear();

                // 延迟重试：调度器延迟后提交到执行器
                // 注意：这里不释放data，因为它会在重试中继续使用
                retryScheduler.schedule(() -> retryExecutor.execute(() -> {
                    try {
                        doWriteWithRetry(options, path, data, result, retryContext);
                    } catch (Exception retryEx) {
                        // 异常时确保释放ByteBuf
                        if (data != null && data.refCnt() > 0) {
                            data.release();
                        }
                        result.completeExceptionally(retryEx);
                    }
                }), calculateDelay(retryContext.attempt), TimeUnit.MILLISECONDS);
            } else {
                if (data != null && data.refCnt() > 0) {
                    data.release();
                }
                result.completeExceptionally(new RuntimeException("Failed to meet write quorum after " +
                    retryCount + " retries"));
            }
        }
    }

    // 2. doRangeRead: 任意份成功读
    public CompletableFuture<ByteBuf> doRangeRead(ReadOptions options, String path, long start, long end) {
        CompletableFuture<ByteBuf> result = new CompletableFuture<>();
        List<String> availableBuckets = getAvailableBuckets();
        if (availableBuckets.isEmpty()) {
            availableBuckets = getAllBuckets();
        }
        tryReadRecursive(availableBuckets, 0, options, path, start, end, result);
        return result;
    }

    private void tryReadRecursive(List<String> buckets, int bucketIndex, ReadOptions options,
                                 String path, long start, long end, CompletableFuture<ByteBuf> result) {
        if (bucketIndex >= buckets.size()) {
            result.completeExceptionally(new RuntimeException("All storages read failed"));
            return;
        }

        String bucket = buckets.get(bucketIndex);
        AwsObjectStorage storage = getStorageByBucket(bucket);
        if (storage == null) {
            LOGGER.warn("[QUORUM-READ] Storage not found for bucket: {}", bucket);
            tryReadRecursive(buckets, bucketIndex + 1, options, path, start, end, result);
            return;
        }

        storage.doRangeRead(options, path, start, end).whenComplete((buf, ex) -> {
            if (ex == null) {
                result.complete(buf);
            } else {
                LOGGER.warn("[QUORUM-READ] Read failed from bucket {}: {}", bucket, ex.getMessage());
                tryReadRecursive(buckets, bucketIndex + 1, options, path, start, end, result);
            }
        });
    }

    // 7. doDeleteObjects: 多份写，N份成功
    public CompletableFuture<Void> doDeleteObjects(List<String> objectKeys) {
        AtomicInteger success = new AtomicInteger(0);
        AtomicInteger completed = new AtomicInteger(0);
        CompletableFuture<Void> result = new CompletableFuture<>();

        List<String> availableBuckets = getAvailableBuckets();
        if (availableBuckets.isEmpty()) {
            availableBuckets = getAllBuckets();
        }
        final List<String> bucketsToUse = availableBuckets;

        for (String bucket : bucketsToUse) {
            AwsObjectStorage storage = getStorageByBucket(bucket);
            if (storage == null) {
                LOGGER.warn("[QUORUM-DELETE] Storage not found for bucket: {}", bucket);
                if (completed.incrementAndGet() == bucketsToUse.size() && !result.isDone()) {
                    result.completeExceptionally(new RuntimeException("Failed to delete from sufficient replicas"));
                }
                continue;
            }

            storage.doDeleteObjects(objectKeys).whenComplete((v, ex) -> {
                if (ex == null) {
                    if (success.incrementAndGet() >= walQuorumWriteSize) {
                        result.complete(null);
                    }
                } else {
                    LOGGER.debug("[QUORUM-DELETE] Delete failed from bucket {}: {}", bucket, ex.getMessage());
                }

                if (completed.incrementAndGet() == bucketsToUse.size() && !result.isDone()) {
                    if (success.get() >= walQuorumWriteSize) {
                        result.complete(null);
                    } else {
                        result.completeExceptionally(new RuntimeException("Failed to delete from sufficient replicas"));
                    }
                }
            });
        }
        return result;
    }

    // 8. doList: 任意份成功读
    public CompletableFuture<List<ObjectInfo>> doList(String prefix) {
        CompletableFuture<List<ObjectInfo>> result = new CompletableFuture<>();
        List<String> availableBuckets = getAvailableBuckets();
        if (availableBuckets.isEmpty()) {
            availableBuckets = getAllBuckets();
        }
        tryListRecursive(availableBuckets, 0, prefix, result);
        return result;
    }

    private void tryListRecursive(List<String> buckets, int bucketIndex, String prefix,
                                 CompletableFuture<List<ObjectInfo>> result) {
        if (bucketIndex >= buckets.size()) {
            result.completeExceptionally(new RuntimeException("All storages list failed"));
            return;
        }

        String bucket = buckets.get(bucketIndex);
        AwsObjectStorage storage = getStorageByBucket(bucket);
        if (storage == null) {
            LOGGER.warn("[QUORUM-LIST] Storage not found for bucket: {}", bucket);
            tryListRecursive(buckets, bucketIndex + 1, prefix, result);
            return;
        }

        storage.doList(prefix).whenComplete((list, ex) -> {
            if (ex == null) {
                result.complete(list);
            } else {
                LOGGER.debug("[QUORUM-LIST] List failed from bucket {}: {}", bucket, ex.getMessage());
                tryListRecursive(buckets, bucketIndex + 1, prefix, result);
            }
        });
    }
    // 兼容ObjectStorage接口的核心方法
    @Override
    public Writer writer(WriteOptions options, String objectPath) {
        // 为每个S3/bucket维护独立的Writer实例
        List<Writer> writers = new ArrayList<>();
        List<String> buckets = getAllBuckets();

        for (String bucket : buckets) {
            AwsObjectStorage storage = getStorageByBucket(bucket);
            if (storage != null) {
                writers.add(storage.writer(options, objectPath));
            } else {
                LOGGER.warn("[QUORUM-WRITER] Storage not found for bucket: {}", bucket);
            }
        }
        return new QuorumWriter(writers);
    }

    @Override
    public CompletableFuture<ByteBuf> rangeRead(ReadOptions options, String objectPath, long start, long end) {
        return doRangeRead(options, objectPath, start, end);
    }

    @Override
    public CompletableFuture<List<ObjectInfo>> list(String prefix) {
        return doList(prefix);
    }

    @Override
    public CompletableFuture<Void> delete(List<ObjectPath> objectPaths) {
        List<String> keys = new ArrayList<>();
        for (ObjectPath op : objectPaths) {
            keys.add(op.key());
        }
        return doDeleteObjects(keys);
    }

    @Override
    public short bucketId() {
        List<String> availableBuckets = getAvailableBuckets();
        if (availableBuckets.isEmpty()) {
            availableBuckets = getAllBuckets();
        }

        if (!availableBuckets.isEmpty()) {
            AwsObjectStorage storage = getStorageByBucket(availableBuckets.get(0));
            if (storage != null) {
                return storage.bucketId();
            }
        }

        // 回退到第一个存在的storage
        return storages.isEmpty() ? 0 : storages.get(0).bucketId();
    }

    public int getWalQuorumSize() {
        return walQuorumSize;
    }

    public int getWalQuorumWriteSize() {
        return walQuorumWriteSize;
    }

    public int getWalQuorumReadSize() {
        return walQuorumReadSize;
    }

    // 兼容ObjectStorage接口的write方法
    @Override
    public CompletableFuture<WriteResult> write(WriteOptions options, String objectPath, ByteBuf buf) {
        return doWrite(options, objectPath, buf).thenApply(nil -> new WriteResult(bucketId()));
    }

    class QuorumWriter implements Writer {
        private final List<Writer> writers;
        private final Map<String, Writer> writerMap = new ConcurrentHashMap<>();
        private final int quorumCount = walQuorumWriteSize;

        public QuorumWriter(List<Writer> writers) {
            this.writers = writers;
            // 建立bucket到writer的映射关系
            for (int i = 0; i < writers.size() && i < storages.size(); i++) {
                String bucket = storages.get(i).bucketURI.bucket();
                writerMap.put(bucket, writers.get(i));
            }
        }

        @Override
        public CompletableFuture<Void> write(ByteBuf data) {
            CompletableFuture<Void> result = new CompletableFuture<>();
            RetryContext retryContext = new RetryContext();

            return writeWithRetry(data, result, retryContext);
        }

        private CompletableFuture<Void> writeWithRetry(ByteBuf data, CompletableFuture<Void> result,
                                                      RetryContext retryContext) {
            // 检查是否已经达到写入quorum
            if (retryContext.getSuccessfulNodes().size() >= quorumCount) {
                LOGGER.debug("[QUORUM-WRITER-RETRY] Already achieved write quorum with {} successful nodes",
                    retryContext.getSuccessfulNodes().size());
                data.release();
                result.complete(null);
                return result;
            }

            // 获取排除已成功节点的可用buckets
            List<String> availableBuckets = getAvailableBucketsForWriterExcludeSuccessful(retryContext);

            if (availableBuckets.size() < (quorumCount - retryContext.getSuccessfulNodes().size())) {
                // 回退到所有writer并行写入
                return writeAllWritersWithRetry(data, result, retryContext);
            }

            // 第一阶段：尝试写入主writer
            writeToPrimaryWritersWithRetry(data, result, retryContext);

            return result;
        }

        private void writeToPrimaryWritersWithRetry(ByteBuf data, CompletableFuture<Void> result,
                                                   RetryContext retryContext) {
            AtomicInteger primarySuccess = new AtomicInteger(0);
            AtomicInteger primaryCompleted = new AtomicInteger(0);
            List<String> availableBuckets = getAvailableBucketsForWriterExcludeSuccessful(retryContext);

            int remainingQuorum = quorumCount - retryContext.getSuccessfulNodes().size();
            if (availableBuckets.size() < remainingQuorum) {
                // 可用writer不足，尝试所有writer
                writeAllWritersWithRetry(data, result, retryContext);
                return;
            }

            // 向前N个可用的writer写入
            int writersToTry = Math.min(remainingQuorum, availableBuckets.size());
            for (int i = 0; i < writersToTry; i++) {
                String bucket = availableBuckets.get(i);
                Writer writer = writerMap.get(bucket);
                if (writer == null) {
                    LOGGER.warn("[QUORUM-WRITER-RETRY] Writer not found for bucket: {}", bucket);
                    continue;
                }

                ByteBuf copy = data.retainedDuplicate();
                writer.write(copy).whenComplete((v, ex) -> {
                    try {
                        if (ex == null) {
                            // 记录成功写入的writer
                            retryContext.recordSuccess(bucket);
                            int totalSuccessCount = retryContext.getSuccessfulNodes().size();
                            int currentSuccessCount = primarySuccess.incrementAndGet();

                            LOGGER.debug("[QUORUM-WRITER-RETRY] Bucket {} write succeeded, total successful: {}",
                                bucket, totalSuccessCount);

                            if (totalSuccessCount >= quorumCount) {
                                // 达到quorum成功数，释放原始数据并返回成功
                                if (data.refCnt() > 0) {
                                    data.release();
                                }
                                result.complete(null);
                                return;
                            }
                        } else {
                            // 记录失败的writer
                            synchronized (retryContext.failedNodes) {
                                if (!retryContext.failedNodes.contains(bucket)) {
                                    retryContext.failedNodes.add(bucket);
                                }
                            }
                        }

                        // 检查primary writers是否都完成了
                        if (primaryCompleted.incrementAndGet() >= writersToTry) {
                            if (!result.isDone()) {
                                // Primary writers完成但未达到quorum
                                if (availableBuckets.size() > writersToTry) {
                                    // 尝试备用writer
                                    writeToBackupWriterWithRetry(data, retryContext.getSuccessfulNodes().size(), result,
                                                               retryContext, availableBuckets, writersToTry);
                                } else {
                                    // 没有足够的备用writer，考虑重试
                                    handleWriteFailureWithRetry(data, result, retryContext);
                                }
                            }
                        }
                    } finally {
                        // 释放ByteBuf副本
                        if (copy != null && copy.refCnt() > 0) {
                            copy.release();
                        }
                    }
                });
            }
        }

        private List<String> getAvailableBucketsForWriter() {
            List<String> availableBuckets = new ArrayList<>();
            for (AwsObjectStorage storage : storages) {
                String bucket = storage.bucketURI.bucket();
                if (writerMap.containsKey(bucket) && !isIsolated(bucket)) {
                    availableBuckets.add(bucket);
                }
            }
            return availableBuckets;
        }

        private List<String> getAvailableBucketsForWriterExcludeSuccessful(RetryContext retryContext) {
            List<String> availableBuckets = new ArrayList<>();
            for (AwsObjectStorage storage : storages) {
                String bucket = storage.bucketURI.bucket();
                if (writerMap.containsKey(bucket) && !isIsolated(bucket) && !retryContext.isSuccessful(bucket)) {
                    availableBuckets.add(bucket);
                }
            }
            return availableBuckets;
        }

        private List<String> getAllBucketsForWriterExcludeSuccessful(RetryContext retryContext) {
            List<String> buckets = new ArrayList<>();
            for (AwsObjectStorage storage : storages) {
                String bucket = storage.bucketURI.bucket();
                if (writerMap.containsKey(bucket) && !retryContext.isSuccessful(bucket)) {
                    buckets.add(bucket);
                }
            }
            return buckets;
        }

        private void writeToBackupWriterWithRetry(ByteBuf data, int totalSuccessCount,
                                                CompletableFuture<Void> result, RetryContext retryContext,
                                                List<String> availableBuckets, int primaryWriterCount) {
            if (availableBuckets.size() <= primaryWriterCount || result.isDone()) {
                // 没有备用writer或结果已完成
                handleWriteFailureWithRetry(data, result, retryContext);
                return;
            }

            // 尝试备用writer
            String backupBucket = availableBuckets.get(primaryWriterCount);
            Writer backupWriter = writerMap.get(backupBucket);
            if (backupWriter == null) {
                LOGGER.warn("[QUORUM-WRITER-RETRY] Backup writer not found for bucket: {}", backupBucket);
                handleWriteFailureWithRetry(data, result, retryContext);
                return;
            }

            ByteBuf backupCopy = data.retainedDuplicate();
            backupWriter.write(backupCopy).whenComplete((v, ex) -> {
                try {
                    if (ex == null) {
                        // 记录备用writer成功
                        retryContext.recordSuccess(backupBucket);
                        int newTotalSuccessCount = retryContext.getSuccessfulNodes().size();
                        LOGGER.debug("[QUORUM-WRITER-RETRY] Backup bucket {} write succeeded, total successful: {}",
                            backupBucket, newTotalSuccessCount);

                        // 备用writer成功，检查总成功数是否达到quorum
                        if (newTotalSuccessCount >= quorumCount) {
                            if (data.refCnt() > 0) {
                                data.release();
                            }
                            result.complete(null);
                        } else {
                            handleWriteFailureWithRetry(data, result, retryContext);
                        }
                    } else {
                        // 记录失败的备用writer
                        synchronized (retryContext.failedNodes) {
                            if (!retryContext.failedNodes.contains(backupBucket)) {
                                retryContext.failedNodes.add(backupBucket);
                            }
                        }

                        // 备用writer也失败，检查是否达到quorum
                        if (retryContext.getSuccessfulNodes().size() >= quorumCount) {
                            if (data.refCnt() > 0) {
                                data.release();
                            }
                            result.complete(null);
                        } else {
                            handleWriteFailureWithRetry(data, result, retryContext);
                        }
                    }
                } finally {
                    // 释放ByteBuf副本
                    if (backupCopy != null && backupCopy.refCnt() > 0) {
                        backupCopy.release();
                    }
                }
            });
        }

        private void handleWriteFailureWithRetry(ByteBuf data, CompletableFuture<Void> result,
                                               RetryContext retryContext) {
            if (retryContext.attempt < QuorumAwsObjectStorage.this.retryCount) {
                // 隔离失败的writer
                for (String failedBucket : retryContext.failedNodes) {
                    QuorumAwsObjectStorage.this.isolateNode(failedBucket);
                }

                // 准备重试
                retryContext.attempt++;
                retryContext.failedNodes.clear();

                // 延迟重试：调度器延迟后提交到执行器
                QuorumAwsObjectStorage.this.retryScheduler.schedule(() ->
                    QuorumAwsObjectStorage.this.retryExecutor.execute(() -> {
                        ByteBuf retryData = null;
                        try {
                            retryData = data.retainedDuplicate();
                            writeWithRetry(retryData, result, retryContext);
                        } catch (Exception retryEx) {
                            // 异常时确保释放retryData
                            if (retryData != null && retryData.refCnt() > 0) {
                                retryData.release();
                            }
                            result.completeExceptionally(retryEx);
                        }
                    }), QuorumAwsObjectStorage.this.calculateDelay(retryContext.attempt), TimeUnit.MILLISECONDS);
            } else {
                // 重试次数用尽，释放数据并返回失败
                if (data.refCnt() > 0) {
                    data.release();
                }
                result.completeExceptionally(new RuntimeException("QuorumWriter failed to meet write quorum after " +
                    QuorumAwsObjectStorage.this.retryCount + " retries"));
            }
        }


        // 回退方法：所有writer并行写入逻辑（支持重试）
        private CompletableFuture<Void> writeAllWritersWithRetry(ByteBuf data, CompletableFuture<Void> result,
                                                                RetryContext retryContext) {
            AtomicInteger success = new AtomicInteger(0);
            AtomicInteger completed = new AtomicInteger(0);
            final List<String> availableBuckets = getAvailableBucketsForRetryExcludeSuccessful(retryContext);

            for (String bucket : availableBuckets) {
                Writer writer = writerMap.get(bucket);
                if (writer == null) {
                    LOGGER.warn("[QUORUM-WRITER-RETRY] Writer not found for bucket: {}", bucket);
                    if (completed.incrementAndGet() == availableBuckets.size()) {
                        handleAllWriteCompleted(data, result, retryContext);
                    }
                    continue;
                }

                ByteBuf copy = data.retainedDuplicate();
                writer.write(copy).whenComplete((v, ex) -> {
                    handleWriteCompletion(v, ex, bucket, copy, success, completed,
                                        availableBuckets, data, result, retryContext);
                });
            }
            return result;
        }

        private List<String> getAvailableBucketsForRetry() {
            List<String> buckets = getAvailableBucketsForWriter();
            if (buckets.isEmpty()) {
                buckets = getAllBuckets();
            }
            return buckets;
        }

        private List<String> getAvailableBucketsForRetryExcludeSuccessful(RetryContext retryContext) {
            List<String> buckets = getAvailableBucketsForWriterExcludeSuccessful(retryContext);
            if (buckets.isEmpty()) {
                // 如果没有可用的未成功writer，则包括所有未成功的writer（包括隔离的）
                buckets = getAllBucketsForWriterExcludeSuccessful(retryContext);
            }
            return buckets;
        }

        private void handleWriteCompletion(Void v, Throwable ex, String bucket, ByteBuf copy,
                                         AtomicInteger success, AtomicInteger completed,
                                         List<String> availableBuckets, ByteBuf originalData,
                                         CompletableFuture<Void> result, RetryContext retryContext) {
            try {
                if (ex == null) {
                    // 记录成功写入的writer
                    retryContext.recordSuccess(bucket);
                    int totalSuccessCount = retryContext.getSuccessfulNodes().size();
                    if (totalSuccessCount >= quorumCount) {
                        if (originalData.refCnt() > 0) {
                            originalData.release();
                        }
                        result.complete(null);
                    }
                } else {
                    recordFailedWriter(bucket, retryContext);
                }
            } finally {
                releaseBuffer(copy);
                if (completed.incrementAndGet() == availableBuckets.size()) {
                    handleAllWriteCompleted(originalData, result, retryContext);
                }
            }
        }

        private void recordFailedWriter(String bucket, RetryContext retryContext) {
            synchronized (retryContext.failedNodes) {
                if (!retryContext.failedNodes.contains(bucket)) {
                    retryContext.failedNodes.add(bucket);
                }
            }
        }

        private void releaseBuffer(ByteBuf buffer) {
            if (buffer != null && buffer.refCnt() > 0) {
                buffer.release();
            }
        }

        private void handleAllWriteCompleted(ByteBuf data, CompletableFuture<Void> result,
                                           RetryContext retryContext) {
            releaseBuffer(data);

            if (!result.isDone()) {
                if (retryContext.attempt < QuorumAwsObjectStorage.this.retryCount) {
                    retryAfterFailure(data, result, retryContext);
                } else {
                    result.completeExceptionally(new RuntimeException("QuorumWriter failed to meet write quorum after " +
                        QuorumAwsObjectStorage.this.retryCount + " retries"));
                }
            }
        }

        private void retryAfterFailure(ByteBuf data, CompletableFuture<Void> result, RetryContext retryContext) {
            // 隔离失败的writer
            for (String failedBucket : retryContext.failedNodes) {
                QuorumAwsObjectStorage.this.isolateNode(failedBucket);
            }

            // 准备重试
            retryContext.attempt++;
            retryContext.failedNodes.clear();

            // 延迟重试：调度器延迟后提交到执行器
            QuorumAwsObjectStorage.this.retryScheduler.schedule(() ->
                QuorumAwsObjectStorage.this.retryExecutor.execute(() -> {
                    ByteBuf retryData = null;
                    try {
                        retryData = data.retainedDuplicate();
                        writeWithRetry(retryData, result, retryContext);
                    } catch (Exception retryEx) {
                        // 异常时确保释放retryData
                        if (retryData != null && retryData.refCnt() > 0) {
                            retryData.release();
                        }
                        result.completeExceptionally(retryEx);
                    }
                }), QuorumAwsObjectStorage.this.calculateDelay(retryContext.attempt), TimeUnit.MILLISECONDS);
        }

        @Override
        public void copyOnWrite() {
            for (Writer writer : writers) {
                writer.copyOnWrite();
            }
        }

        @Override
        public void copyWrite(com.automq.stream.s3.metadata.S3ObjectMetadata s3ObjectMetadata, long start, long end) {
            for (Writer writer : writers) {
                writer.copyWrite(s3ObjectMetadata, start, end);
            }
        }

        @Override
        public boolean hasBatchingPart() {
            for (Writer writer : writers) {
                if (writer.hasBatchingPart()) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public CompletableFuture<Void> close() {
            CompletableFuture<Void> result = new CompletableFuture<>();
            RetryContext retryContext = new RetryContext();
            closeWithRetry(result, retryContext);
            return result;
        }

        private void closeWithRetry(CompletableFuture<Void> result, RetryContext retryContext) {
            // 检查是否已经达到关闭quorum
            if (retryContext.getSuccessfulNodes().size() >= quorumCount) {
                LOGGER.info("[QUORUM-WRITER-CLOSE] Already achieved close quorum with {} successful nodes",
                    retryContext.getSuccessfulNodes().size());
                result.complete(null);
                return;
            }

            AtomicInteger success = new AtomicInteger(0);
            AtomicInteger completed = new AtomicInteger(0);
            List<String> availableBuckets = getAvailableBucketsForWriterExcludeSuccessful(retryContext);

            if (availableBuckets.isEmpty()) {
                // 如果没有可用的未成功writer，则包括所有未成功的writer
                availableBuckets = getAllBucketsForWriterExcludeSuccessful(retryContext);
            }

            final List<String> bucketsToClose = availableBuckets;

            for (String bucket : bucketsToClose) {
                Writer writer = writerMap.get(bucket);
                if (writer == null) {
                    LOGGER.warn("[QUORUM-WRITER-CLOSE] Writer not found for bucket: {}", bucket);
                    if (completed.incrementAndGet() == bucketsToClose.size()) {
                        handleCloseRetryIfNeeded(result, retryContext);
                    }
                    continue;
                }

                writer.close().whenComplete((v, ex) -> {
                    if (ex == null) {
                        // 记录成功关闭的writer
                        retryContext.recordSuccess(bucket);
                        int totalSuccessCount = retryContext.getSuccessfulNodes().size();
                        if (totalSuccessCount >= quorumCount) {
                            result.complete(null);
                        }
                    } else {
                        // 记录失败的writer
                        synchronized (retryContext.failedNodes) {
                            if (!retryContext.failedNodes.contains(bucket)) {
                                retryContext.failedNodes.add(bucket);
                            }
                        }
                    }

                    if (completed.incrementAndGet() == bucketsToClose.size()) {
                        if (!result.isDone()) {
                            handleCloseRetryIfNeeded(result, retryContext);
                        }
                    }
                });
            }
        }

        private void handleCloseRetryIfNeeded(CompletableFuture<Void> result, RetryContext retryContext) {
            // 如果没有达到quorum且还有重试次数，则重试
            if (retryContext.attempt < QuorumAwsObjectStorage.this.retryCount) {
                // 隔离失败的writer
                for (String failedBucket : retryContext.failedNodes) {
                    QuorumAwsObjectStorage.this.isolateNode(failedBucket);
                }

                // 准备重试
                retryContext.attempt++;
                retryContext.failedNodes.clear();

                // 延迟重试：调度器延迟后提交到执行器
                QuorumAwsObjectStorage.this.retryScheduler.schedule(() ->
                    QuorumAwsObjectStorage.this.retryExecutor.execute(() -> {
                        try {
                            closeWithRetry(result, retryContext);
                        } catch (Exception retryEx) {
                            result.completeExceptionally(retryEx);
                        }
                    }), QuorumAwsObjectStorage.this.calculateDelay(retryContext.attempt), TimeUnit.MILLISECONDS);
            } else {
                result.completeExceptionally(new RuntimeException("QuorumWriter close failed to meet quorum after " +
                    QuorumAwsObjectStorage.this.retryCount + " retries"));
            }
        }

        @Override
        public CompletableFuture<Void> release() {
            List<CompletableFuture<Void>> releases = new ArrayList<>();
            for (Writer writer : writers) {
                releases.add(writer.release());
            }
            return CompletableFuture.allOf(releases.toArray(new CompletableFuture[0]));
        }

        @Override
        public short bucketId() {
            return writers.get(0).bucketId();
        }
    }
}
