package com.automq.stream.s3.operator;

import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public class QuorumAwsObjectStorage implements ObjectStorage {
    private final List<AwsObjectStorage> storages;
    private final int quorum;

    public QuorumAwsObjectStorage(List<AwsObjectStorage> storages, int quorum) {
        if (storages == null || storages.isEmpty()) {
            throw new IllegalArgumentException("storages must not be empty");
        }
        if (quorum < 1 || quorum > storages.size()) {
            throw new IllegalArgumentException("quorum must be between 1 and storages.size()");
        }
        this.storages = storages;
        this.quorum = quorum;
    }

    @Override
    public boolean readinessCheck() {
        AtomicInteger success = new AtomicInteger(0);
        for (AwsObjectStorage storage : storages) {
            if (storage.readinessCheck()) {
                if (success.incrementAndGet() >= quorum) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public void close() {
        for (AwsObjectStorage storage : storages) {
            storage.close();
        }
    }

    // 1. doWrite: 2+1优化写入策略 - 优先写前2个副本，成功则不写第3个
    public CompletableFuture<Void> doWrite(WriteOptions options, String path, ByteBuf data) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        
        if (storages.size() < 2) {
            // 如果副本数小于2，回退到原始逻辑
            return doWriteAllReplicas(options, path, data);
        }
        
        // 第一阶段：尝试写入前2个副本
        writeToPrimaryReplicas(options, path, data, result);
        
        return result;
    }
    
    private void writeToPrimaryReplicas(WriteOptions options, String path, ByteBuf data, CompletableFuture<Void> result) {
        AtomicInteger primarySuccess = new AtomicInteger(0);
        AtomicInteger primaryCompleted = new AtomicInteger(0);
        
        // 向前2个副本写入
        for (int i = 0; i < Math.min(2, storages.size()); i++) {
            ByteBuf copy = data.retainedDuplicate();
            
            storages.get(i).doWrite(options, path, copy).whenComplete((v, ex) -> {
                try {
                    if (ex == null) {
                        int successCount = primarySuccess.incrementAndGet();
                        if (successCount >= 2) {
                            // 前2个副本都成功，释放原始data并返回成功
                            if (data.refCnt() > 0) {
                                data.release();
                            }
                            result.complete(null);
                            return;
                        }
                    }
                    
                    // 检查前2个副本是否都完成了
                    if (primaryCompleted.incrementAndGet() >= 2) {
                        // 前2个副本完成，但没有2个都成功，尝试第3个副本
                        if (!result.isDone() && storages.size() > 2) {
                            writeToBackupReplica(options, path, data, primarySuccess.get(), result);
                        } else if (!result.isDone()) {
                            // 没有第3个副本，且前2个没有都成功，释放原始data
                            if (data.refCnt() > 0) {
                                data.release();
                            }
                            result.completeExceptionally(new RuntimeException("Primary replicas write failed and no backup available"));
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
    
    private void writeToBackupReplica(WriteOptions options, String path, ByteBuf data, int primarySuccessCount, CompletableFuture<Void> result) {
        if (storages.size() <= 2 || result.isDone()) {
            // 如果没有备用副本或结果已完成，释放原始数据
            if (data.refCnt() > 0) {
                data.release();
            }
            return;
        }
        
        ByteBuf backupCopy = data.retainedDuplicate();
        storages.get(2).doWrite(options, path, backupCopy).whenComplete((v, ex) -> {
            try {
                if (ex == null) {
                    // 第3个副本成功，检查总成功数是否达到quorum
                    if (primarySuccessCount + 1 >= quorum) {
                        result.complete(null);
                    } else {
                        result.completeExceptionally(new RuntimeException("Insufficient successful replicas"));
                    }
                } else {
                    // 第3个副本也失败
                    if (primarySuccessCount >= quorum) {
                        result.complete(null);
                    } else {
                        result.completeExceptionally(new RuntimeException("All replica writes failed to meet quorum"));
                    }
                }
            } finally {
                // 释放ByteBuf副本
                if (backupCopy != null && backupCopy.refCnt() > 0) {
                    backupCopy.release();
                }
                // 释放原始数据 - 这里是最后的释放点
                if (data != null && data.refCnt() > 0) {
                    data.release();
                }
            }
        });
    }
    
    // 回退方法：原始的所有副本并行写入逻辑
    private CompletableFuture<Void> doWriteAllReplicas(WriteOptions options, String path, ByteBuf data) {
        AtomicInteger success = new AtomicInteger(0);
        AtomicInteger completed = new AtomicInteger(0);
        CompletableFuture<Void> result = new CompletableFuture<>();
        
        for (AwsObjectStorage storage : storages) {
            ByteBuf copy = data.retainedDuplicate();
            storage.doWrite(options, path, copy).whenComplete((v, ex) -> {
                try {
                    if (ex == null && success.incrementAndGet() >= quorum) {
                        result.complete(null);
                    }
                } finally {
                    if (copy != null && copy.refCnt() > 0) {
                        copy.release();
                    }
                    // 只有最后一个完成的操作释放原始data
                    if (completed.incrementAndGet() == storages.size()) {
                        if (data != null && data.refCnt() > 0) {
                            data.release();
                        }
                        // 如果还没有完成结果，说明没有达到quorum
                        if (!result.isDone()) {
                            result.completeExceptionally(new RuntimeException("Failed to meet write quorum"));
                        }
                    }
                }
            });
        }
        return result;
    }

    // 2. doRangeRead: 任意份成功读
    public CompletableFuture<ByteBuf> doRangeRead(ReadOptions options, String path, long start, long end) {
        CompletableFuture<ByteBuf> result = new CompletableFuture<>();
        tryReadRecursive(0, options, path, start, end, result);
        return result;
    }

    private void tryReadRecursive(int idx, ReadOptions options, String path, long start, long end, CompletableFuture<ByteBuf> result) {
        if (idx >= storages.size()) {
            result.completeExceptionally(new RuntimeException("All storages read failed"));
            return;
        }
        storages.get(idx).doRangeRead(options, path, start, end).whenComplete((buf, ex) -> {
            if (ex == null) {
                result.complete(buf);
            } else {
                tryReadRecursive(idx + 1, options, path, start, end, result);
            }
        });
    }

    // 7. doDeleteObjects: 多份写，N份成功
    public CompletableFuture<Void> doDeleteObjects(List<String> objectKeys) {
        AtomicInteger success = new AtomicInteger(0);
        CompletableFuture<Void> result = new CompletableFuture<>();
        for (AwsObjectStorage storage : storages) {
            storage.doDeleteObjects(objectKeys).whenComplete((v, ex) -> {
                if (ex == null && success.incrementAndGet() >= quorum) {
                    result.complete(null);
                }
            });
        }
        return result;
    }

    // 8. doList: 任意份成功读
    public CompletableFuture<List<ObjectInfo>> doList(String prefix) {
        CompletableFuture<List<ObjectInfo>> result = new CompletableFuture<>();
        tryListRecursive(0, prefix, result);
        return result;
    }
    private void tryListRecursive(int idx, String prefix, CompletableFuture<List<ObjectInfo>> result) {
        if (idx >= storages.size()) {
            result.completeExceptionally(new RuntimeException("All storages list failed"));
            return;
        }
        storages.get(idx).doList(prefix).whenComplete((list, ex) -> {
            if (ex == null) {
                result.complete(list);
            } else {
                tryListRecursive(idx + 1, prefix, result);
            }
        });
    }
    // 兼容ObjectStorage接口的核心方法
    @Override
    public Writer writer(WriteOptions options, String objectPath) {
        // 为每个S3/bucket维护独立的Writer实例
        List<Writer> writers = new ArrayList<>();
        for (AwsObjectStorage storage : storages) {
            writers.add(storage.writer(options, objectPath));
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
        return storages.get(0).bucketId();
    }

    // 兼容ObjectStorage接口的write方法
    @Override
    public CompletableFuture<WriteResult> write(WriteOptions options, String objectPath, ByteBuf buf) {
        return doWrite(options, objectPath, buf).thenApply(nil -> new WriteResult(bucketId()));
    }

    class QuorumWriter implements Writer {
        private final List<Writer> writers;
        private final int quorumCount = quorum;

        public QuorumWriter(List<Writer> writers) {
            this.writers = writers;
        }

        @Override
        public CompletableFuture<Void> write(ByteBuf data) {
            CompletableFuture<Void> result = new CompletableFuture<>();
            
            if (writers.size() < 2) {
                // 如果writer数小于2，回退到原始逻辑
                return writeAllWriters(data);
            }
            
            // 第一阶段：尝试写入前2个writer
            writeToPrimaryWriters(data, result);
            
            return result;
        }
        
        private void writeToPrimaryWriters(ByteBuf data, CompletableFuture<Void> result) {
            AtomicInteger primarySuccess = new AtomicInteger(0);
            AtomicInteger primaryCompleted = new AtomicInteger(0);
            
            // 向前2个writer写入
            for (int i = 0; i < Math.min(2, writers.size()); i++) {
                ByteBuf copy = data.retainedDuplicate();
                
                writers.get(i).write(copy).whenComplete((v, ex) -> {
                    try {
                        if (ex == null) {
                            int successCount = primarySuccess.incrementAndGet();
                            if (successCount >= 2) {
                                // 前2个writer都成功，释放原始数据并返回成功
                                if (data.refCnt() > 0) {
                                    data.release();
                                }
                                result.complete(null);
                                return;
                            }
                        }
                        
                        // 检查前2个writer是否都完成了
                        if (primaryCompleted.incrementAndGet() >= 2) {
                            // 前2个writer完成，但没有2个都成功，尝试第3个writer
                            if (!result.isDone() && writers.size() > 2) {
                                writeToBackupWriter(data, primarySuccess.get(), result);
                            } else if (!result.isDone()) {
                                // 没有第3个writer，且前2个没有都成功，释放原始数据
                                if (data.refCnt() > 0) {
                                    data.release();
                                }
                                result.completeExceptionally(new RuntimeException("Primary writers write failed and no backup available"));
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
        
        private void writeToBackupWriter(ByteBuf data, int primarySuccessCount, CompletableFuture<Void> result) {
            if (writers.size() <= 2 || result.isDone()) {
                // 如果没有备用writer或结果已完成，释放原始数据
                if (data.refCnt() > 0) {
                    data.release();
                }
                return;
            }
            
            ByteBuf backupCopy = data.retainedDuplicate();
            writers.get(2).write(backupCopy).whenComplete((v, ex) -> {
                try {
                    if (ex == null) {
                        // 第3个writer成功，检查总成功数是否达到quorum
                        if (primarySuccessCount + 1 >= quorumCount) {
                            result.complete(null);
                        } else {
                            result.completeExceptionally(new RuntimeException("Insufficient successful writers"));
                        }
                    } else {
                        // 第3个writer也失败
                        if (primarySuccessCount >= quorumCount) {
                            result.complete(null);
                        } else {
                            result.completeExceptionally(new RuntimeException("All writer writes failed to meet quorum"));
                        }
                    }
                } finally {
                    // 释放ByteBuf副本
                    if (backupCopy != null && backupCopy.refCnt() > 0) {
                        backupCopy.release();
                    }
                    // 释放原始数据
                    if (data != null && data.refCnt() > 0) {
                        data.release();
                    }
                }
            });
        }
        
        // 回退方法：原始的所有writer并行写入逻辑
        private CompletableFuture<Void> writeAllWriters(ByteBuf data) {
            AtomicInteger success = new AtomicInteger(0);
            AtomicInteger completed = new AtomicInteger(0);
            CompletableFuture<Void> result = new CompletableFuture<>();
            
            for (Writer writer : writers) {
                ByteBuf copy = data.retainedDuplicate();
                writer.write(copy).whenComplete((v, ex) -> {
                    try {
                        if (ex == null && success.incrementAndGet() >= quorumCount) {
                            result.complete(null);
                        }
                    } finally {
                        if (copy != null && copy.refCnt() > 0) {
                            copy.release();
                        }
                        if (completed.incrementAndGet() == writers.size()) {
                            if (data != null && data.refCnt() > 0) {
                                data.release();
                            }
                        }
                    }
                });
            }
            return result;
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
            AtomicInteger success = new AtomicInteger(0);
            CompletableFuture<Void> result = new CompletableFuture<>();
            for (Writer writer : writers) {
                writer.close().whenComplete((v, ex) -> {
                    if (ex == null && success.incrementAndGet() >= quorumCount) {
                        result.complete(null);
                    }
                });
            }
            return result;
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
