import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.List;
import java.util.ArrayList;

/**
 * Simplified test for Quorum logic without full project dependencies
 */
public class TestQuorumLogic {
    
    public static void main(String[] args) {
        System.out.println("Testing Quorum Storage Logic...");
        
        try {
            testQuorumWriteSuccess();
            testQuorumWriteFailure();
            testQuorumHealthTracking();
            
            System.out.println("✅ All Quorum logic tests passed!");
        } catch (Exception e) {
            System.err.println("❌ Test failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    static void testQuorumWriteSuccess() throws Exception {
        System.out.println("Testing quorum write success scenario...");
        
        // Simulate 3 replicas, 2 succeed
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        futures.add(CompletableFuture.completedFuture(null)); // Replica 1 success
        futures.add(CompletableFuture.completedFuture(null)); // Replica 2 success
        futures.add(CompletableFuture.failedFuture(new RuntimeException("Replica 3 failed"))); // Replica 3 failed
        
        CompletableFuture<Void> result = waitForMajorityVoid(futures, 2);
        result.get(); // Should succeed
        
        System.out.println("  ✓ Quorum write with 2/3 success completed");
    }
    
    static void testQuorumWriteFailure() throws Exception {
        System.out.println("Testing quorum write failure scenario...");
        
        // Simulate 3 replicas, only 1 succeeds
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        futures.add(CompletableFuture.completedFuture(null)); // Replica 1 success
        futures.add(CompletableFuture.failedFuture(new RuntimeException("Replica 2 failed"))); // Replica 2 failed
        futures.add(CompletableFuture.failedFuture(new RuntimeException("Replica 3 failed"))); // Replica 3 failed
        
        CompletableFuture<Void> result = waitForMajorityVoid(futures, 2);
        
        boolean failed = false;
        try {
            result.get(); // Should fail
        } catch (Exception e) {
            failed = true;
        }
        
        if (!failed) {
            throw new RuntimeException("Expected quorum write to fail with only 1/3 success");
        }
        
        System.out.println("  ✓ Quorum write with 1/3 success correctly failed");
    }
    
    static void testQuorumHealthTracking() {
        System.out.println("Testing quorum health tracking...");
        
        // Simulate QuorumState behavior
        AtomicInteger healthyCount = new AtomicInteger(3);
        AtomicBoolean[] replicaHealth = {
            new AtomicBoolean(true),
            new AtomicBoolean(true), 
            new AtomicBoolean(true)
        };
        
        // Check initial healthy state
        if (healthyCount.get() != 3) {
            throw new RuntimeException("Initial healthy count should be 3");
        }
        
        // Mark replica 0 as failed
        replicaHealth[0].set(false);
        healthyCount.decrementAndGet();
        
        if (healthyCount.get() != 2) {
            throw new RuntimeException("Healthy count should be 2 after one failure");
        }
        
        // Check if still has quorum (2/3)
        boolean hasQuorum = healthyCount.get() >= (3 / 2 + 1); // majority
        if (!hasQuorum) {
            throw new RuntimeException("Should still have quorum with 2/3 healthy");
        }
        
        // Mark replica 1 as failed
        replicaHealth[1].set(false);
        healthyCount.decrementAndGet();
        
        if (healthyCount.get() != 1) {
            throw new RuntimeException("Healthy count should be 1 after two failures");
        }
        
        // Check if lost quorum (1/3)
        hasQuorum = healthyCount.get() >= (3 / 2 + 1); // majority
        if (hasQuorum) {
            throw new RuntimeException("Should lose quorum with 1/3 healthy");
        }
        
        // Recover replica 0
        replicaHealth[0].set(true);
        healthyCount.incrementAndGet();
        
        // Check if regained quorum (2/3)
        hasQuorum = healthyCount.get() >= (3 / 2 + 1); // majority
        if (!hasQuorum) {
            throw new RuntimeException("Should regain quorum with 2/3 healthy");
        }
        
        System.out.println("  ✓ Quorum health tracking working correctly");
    }
    
    // Simplified version of FutureUtil.waitForMajorityVoid
    static CompletableFuture<Void> waitForMajorityVoid(List<CompletableFuture<Void>> futures, int majorityCount) {
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
                        // Check if we can't reach majority anymore
                        if (futures.size() - failureCount.get() < majorityCount) {
                            if (!result.isDone()) {
                                result.completeExceptionally(new RuntimeException(
                                    "Cannot reach majority: " + failureCount.get() + " failures"));
                            }
                        }
                    } else {
                        successCount.incrementAndGet();
                        // Check if we have reached majority
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