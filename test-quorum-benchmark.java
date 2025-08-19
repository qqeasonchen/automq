import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Performance benchmark for S3QuorumStorage
 */
public class TestQuorumBenchmark {
    
    public static void main(String[] args) {
        System.out.println("🚀 S3QuorumStorage Performance Benchmark");
        System.out.println("=========================================");
        System.out.println();
        
        try {
            runThroughputBenchmark();
            runLatencyBenchmark();
            runScalabilityBenchmark();
            runSustainedLoadBenchmark();
            
            System.out.println();
            System.out.println("🎉 All benchmarks completed successfully!");
            
        } catch (Exception e) {
            System.err.println("❌ Benchmark failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    static void runThroughputBenchmark() throws Exception {
        System.out.println("📈 Throughput Benchmark");
        System.out.println("=======================");
        
        MockQuorumStorage storage = new MockQuorumStorage(3, 2, 1);
        storage.setAllReplicasHealthy();
        
        int[] operationCounts = {100, 500, 1000, 2000};
        
        for (int opCount : operationCounts) {
            System.out.printf("Testing %d operations...", opCount);
            
            long startTime = System.nanoTime();
            
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (int i = 0; i < opCount; i++) {
                futures.add(storage.write("benchmark-data-" + i));
            }
            
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
            
            long endTime = System.nanoTime();
            double durationSeconds = (endTime - startTime) / 1_000_000_000.0;
            double throughput = opCount / durationSeconds;
            
            System.out.printf(" %.2f ops/sec (%.3f seconds)%n", throughput, durationSeconds);
        }
        
        System.out.println();
    }
    
    static void runLatencyBenchmark() throws Exception {
        System.out.println("⏱️  Latency Benchmark");
        System.out.println("====================");
        
        MockQuorumStorage storage = new MockQuorumStorage(3, 2, 1);
        storage.setAllReplicasHealthy();
        
        int operationCount = 1000;
        List<Long> latencies = new ArrayList<>();
        
        // Warmup
        for (int i = 0; i < 100; i++) {
            storage.write("warmup-" + i).get();
        }
        
        System.out.printf("Measuring latency for %d operations...", operationCount);
        
        for (int i = 0; i < operationCount; i++) {
            long startTime = System.nanoTime();
            storage.write("latency-test-" + i).get();
            long endTime = System.nanoTime();
            
            latencies.add(endTime - startTime);
        }
        
        // Calculate statistics
        latencies.sort(Long::compareTo);
        
        double avgNs = latencies.stream().mapToLong(Long::longValue).average().orElse(0.0);
        long p50Ns = latencies.get((int) (latencies.size() * 0.50));
        long p95Ns = latencies.get((int) (latencies.size() * 0.95));
        long p99Ns = latencies.get((int) (latencies.size() * 0.99));
        
        System.out.println();
        System.out.printf("  Average: %.2f ms%n", avgNs / 1_000_000.0);
        System.out.printf("  P50:     %.2f ms%n", p50Ns / 1_000_000.0);
        System.out.printf("  P95:     %.2f ms%n", p95Ns / 1_000_000.0);
        System.out.printf("  P99:     %.2f ms%n", p99Ns / 1_000_000.0);
        
        System.out.println();
    }
    
    static void runScalabilityBenchmark() throws Exception {
        System.out.println("🔄 Scalability Benchmark");
        System.out.println("========================");
        
        int[] threadCounts = {1, 2, 4, 8, 16};
        int operationsPerThread = 100;
        
        for (int threadCount : threadCounts) {
            System.out.printf("Testing with %d threads...", threadCount);
            
            MockQuorumStorage storage = new MockQuorumStorage(3, 2, 1);
            storage.setAllReplicasHealthy();
            
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch completionLatch = new CountDownLatch(threadCount);
            
            long startTime = System.nanoTime();
            
            for (int t = 0; t < threadCount; t++) {
                final int threadId = t;
                executor.submit(() -> {
                    try {
                        startLatch.await();
                        
                        for (int i = 0; i < operationsPerThread; i++) {
                            storage.write("scalability-t" + threadId + "-op" + i).get();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        completionLatch.countDown();
                    }
                });
            }
            
            startLatch.countDown(); // Start all threads
            completionLatch.await(); // Wait for completion
            
            long endTime = System.nanoTime();
            double durationSeconds = (endTime - startTime) / 1_000_000_000.0;
            int totalOperations = threadCount * operationsPerThread;
            double throughput = totalOperations / durationSeconds;
            
            System.out.printf(" %.2f ops/sec (%d total ops)%n", throughput, totalOperations);
            
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
        
        System.out.println();
    }
    
    static void runSustainedLoadBenchmark() throws Exception {
        System.out.println("🔥 Sustained Load Benchmark");
        System.out.println("===========================");
        
        MockQuorumStorage storage = new MockQuorumStorage(3, 2, 1);
        storage.setAllReplicasHealthy();
        
        int durationSeconds = 10;
        int writerThreads = 4;
        int readerThreads = 2;
        
        System.out.printf("Running sustained load for %d seconds (%d writers, %d readers)...%n",
            durationSeconds, writerThreads, readerThreads);
        
        ExecutorService writerExecutor = Executors.newFixedThreadPool(writerThreads);
        ExecutorService readerExecutor = Executors.newFixedThreadPool(readerThreads);
        
        AtomicLong writeCount = new AtomicLong(0);
        AtomicLong readCount = new AtomicLong(0);
        AtomicLong errorCount = new AtomicLong(0);
        
        long startTime = System.nanoTime();
        long endTime = startTime + (durationSeconds * 1_000_000_000L);
        
        // Start writer threads
        for (int i = 0; i < writerThreads; i++) {
            final int writerId = i;
            writerExecutor.submit(() -> {
                while (System.nanoTime() < endTime) {
                    try {
                        storage.write("sustained-write-" + writerId + "-" + writeCount.get()).get();
                        writeCount.incrementAndGet();
                        Thread.sleep(5); // Small delay
                    } catch (Exception e) {
                        errorCount.incrementAndGet();
                    }
                }
            });
        }
        
        // Start reader threads (mock read operations)
        for (int i = 0; i < readerThreads; i++) {
            final int readerId = i;
            readerExecutor.submit(() -> {
                while (System.nanoTime() < endTime) {
                    try {
                        // Simulate read operation
                        storage.mockRead("sustained-read-" + readerId + "-" + readCount.get()).get();
                        readCount.incrementAndGet();
                        Thread.sleep(10); // Small delay
                    } catch (Exception e) {
                        errorCount.incrementAndGet();
                    }
                }
            });
        }
        
        writerExecutor.shutdown();
        readerExecutor.shutdown();
        
        writerExecutor.awaitTermination(durationSeconds + 5, TimeUnit.SECONDS);
        readerExecutor.awaitTermination(durationSeconds + 5, TimeUnit.SECONDS);
        
        long actualEndTime = System.nanoTime();
        double actualDuration = (actualEndTime - startTime) / 1_000_000_000.0;
        
        double writeThroughput = writeCount.get() / actualDuration;
        double readThroughput = readCount.get() / actualDuration;
        double errorRate = (double) errorCount.get() / (writeCount.get() + readCount.get());
        
        System.out.printf("  Duration:        %.2f seconds%n", actualDuration);
        System.out.printf("  Write ops:       %d (%.2f ops/sec)%n", writeCount.get(), writeThroughput);
        System.out.printf("  Read ops:        %d (%.2f ops/sec)%n", readCount.get(), readThroughput);
        System.out.printf("  Error rate:      %.2f%%n", errorRate * 100);
        
        System.out.println();
    }
    
    // Enhanced Mock implementation with read operations
    static class MockQuorumStorage {
        private final int quorumSize;
        private final int writeQuorumSize;
        private final int readQuorumSize;
        private final AtomicBoolean[] replicaHealth;
        private final AtomicInteger healthyCount;
        
        public MockQuorumStorage(int quorumSize, int writeQuorumSize, int readQuorumSize) {
            this.quorumSize = quorumSize;
            this.writeQuorumSize = writeQuorumSize;
            this.readQuorumSize = readQuorumSize;
            this.replicaHealth = new AtomicBoolean[quorumSize];
            this.healthyCount = new AtomicInteger(quorumSize);
            
            for (int i = 0; i < quorumSize; i++) {
                replicaHealth[i] = new AtomicBoolean(true);
            }
        }
        
        public CompletableFuture<Void> write(String data) {
            List<CompletableFuture<Void>> writeFutures = new ArrayList<>();
            
            for (int i = 0; i < quorumSize; i++) {
                final int replicaIndex = i;
                CompletableFuture<Void> future;
                
                if (replicaHealth[i].get()) {
                    // Simulate write with variable delay based on replica
                    int delay = (i == 0) ? 1 : (i == 1) ? 2 : 5; // Primary faster
                    future = CompletableFuture.runAsync(() -> {
                        try {
                            Thread.sleep(delay);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    });
                } else {
                    future = CompletableFuture.failedFuture(
                        new RuntimeException("Replica " + replicaIndex + " failed"));
                }
                
                writeFutures.add(future);
            }
            
            return waitForMajorityVoid(writeFutures, writeQuorumSize);
        }
        
        public CompletableFuture<String> mockRead(String key) {
            // Try primary first, then fallback to secondaries
            if (replicaHealth[0].get()) {
                return CompletableFuture.supplyAsync(() -> {
                    try {
                        Thread.sleep(1); // Fast primary read
                        return "data-for-" + key;
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                });
            } else {
                // Try secondary replicas
                for (int i = 1; i < quorumSize; i++) {
                    if (replicaHealth[i].get()) {
                        final int delay = (i == 1) ? 3 : 6; // Secondary slower
                        return CompletableFuture.supplyAsync(() -> {
                            try {
                                Thread.sleep(delay);
                                return "data-for-" + key;
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new RuntimeException(e);
                            }
                        });
                    }
                }
                return CompletableFuture.failedFuture(new RuntimeException("All replicas failed"));
            }
        }
        
        public void setReplicaHealth(int replicaIndex, boolean healthy) {
            boolean wasHealthy = replicaHealth[replicaIndex].getAndSet(healthy);
            if (wasHealthy != healthy) {
                if (healthy) {
                    healthyCount.incrementAndGet();
                } else {
                    healthyCount.decrementAndGet();
                }
            }
        }
        
        public void setAllReplicasHealthy() {
            for (int i = 0; i < quorumSize; i++) {
                replicaHealth[i].set(true);
            }
            healthyCount.set(quorumSize);
        }
        
        public boolean isHealthy() {
            return healthyCount.get() >= writeQuorumSize;
        }
        
        private static CompletableFuture<Void> waitForMajorityVoid(List<CompletableFuture<Void>> futures, int majorityCount) {
            if (majorityCount > futures.size()) {
                return CompletableFuture.failedFuture(
                    new IllegalArgumentException("Majority count cannot exceed future count"));
            }
            
            CompletableFuture<Void> result = new CompletableFuture<>();
            
            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger failureCount = new AtomicInteger(0);
            Object lock = new Object();
            
            for (CompletableFuture<Void> future : futures) {
                future.whenComplete((value, ex) -> {
                    synchronized (lock) {
                        if (ex != null) {
                            failureCount.incrementAndGet();
                            if (futures.size() - failureCount.get() < majorityCount) {
                                if (!result.isDone()) {
                                    result.completeExceptionally(new RuntimeException(
                                        "Cannot reach majority: " + failureCount.get() + " failures"));
                                }
                            }
                        } else {
                            successCount.incrementAndGet();
                            if (successCount.get() >= majorityCount) {
                                if (!result.isDone()) {
                                    result.complete(null);
                                }
                            }
                        }
                    }
                });
            }
            
            return result;
        }
    }
}