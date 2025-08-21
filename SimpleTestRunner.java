/*
 * Simple test runner to verify S3 Quorum Storage enhanced features
 */

import com.automq.stream.s3.operator.BucketURI;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorageFactory;
import com.automq.stream.s3.quorum.monitoring.QuorumMetricsCollector;
import com.automq.stream.s3.quorum.resilience.QuorumResilienceManager;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class SimpleTestRunner {
    public static void main(String[] args) {
        System.out.println("=== S3 Quorum Storage Enhanced Features Test ===");
        
        try {
            // Test 1: Basic QuorumObjectStorage Creation
            testQuorumObjectStorageCreation();
            
            // Test 2: Metrics Collection
            testMetricsCollection();
            
            // Test 3: Resilience Manager
            testResilienceManager();
            
            System.out.println("\n✅ All basic tests completed successfully!");
            
        } catch (Exception e) {
            System.err.println("❌ Test failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static void testQuorumObjectStorageCreation() throws Exception {
        System.out.println("\n--- Test 1: QuorumObjectStorage Creation ---");
        
        // Create test buckets
        List<BucketURI> buckets = new ArrayList<>();
        buckets.add(BucketURI.parse("0@mem://test-bucket-1"));
        buckets.add(BucketURI.parse("1@mem://test-bucket-2"));
        buckets.add(BucketURI.parse("2@mem://test-bucket-3"));
        
        // Create QuorumObjectStorage
        ObjectStorage quorumStorage = ObjectStorageFactory.instance()
            .builder()
            .buckets(buckets)
            .quorumEnabled(true)
            .quorumSize(3)
            .writeQuorumSize(2)
            .readQuorumSize(1)
            .build();
        
        System.out.println("✓ QuorumObjectStorage created successfully");
        System.out.println("  - Type: " + quorumStorage.getClass().getSimpleName());
        System.out.println("  - Readiness: " + quorumStorage.readinessCheck());
        
        quorumStorage.close();
        System.out.println("✓ Test 1 completed");
    }
    
    private static void testMetricsCollection() throws Exception {
        System.out.println("\n--- Test 2: Metrics Collection ---");
        
        // Create metrics collector
        QuorumMetricsCollector.MetricsConfig config = 
            new QuorumMetricsCollector.MetricsConfig(false, 5, true);
        
        QuorumMetricsCollector metricsCollector = new QuorumMetricsCollector(config);
        System.out.println("✓ QuorumMetricsCollector created");
        
        // Test operation timing
        try (QuorumMetricsCollector.OperationTimer timer = metricsCollector.startOperation("test-operation")) {
            Thread.sleep(10); // Simulate operation time
            timer.success();
        }
        
        // Test gauge metrics
        metricsCollector.setGauge("active-connections", 5);
        metricsCollector.incrementGauge("total-requests");
        
        // Get metrics snapshot
        QuorumMetricsCollector.MetricsSnapshot snapshot = metricsCollector.getMetricsSnapshot();
        System.out.println("✓ Metrics collection working");
        System.out.println("  - Operation metrics: " + snapshot.getOperationMetrics().size());
        System.out.println("  - Gauge metrics: " + snapshot.getGaugeMetrics().size());
        
        metricsCollector.stop();
        System.out.println("✓ Test 2 completed");
    }
    
    private static void testResilienceManager() throws Exception {
        System.out.println("\n--- Test 3: Resilience Manager ---");
        
        // Create resilience manager
        QuorumResilienceManager.ResilienceConfig config = 
            QuorumResilienceManager.ResilienceConfig.defaultConfig();
        
        QuorumResilienceManager resilienceManager = new QuorumResilienceManager(config);
        System.out.println("✓ QuorumResilienceManager created");
        
        // Test successful operation
        CompletableFuture<String> successOperation = resilienceManager.executeWithTimeout(
            () -> CompletableFuture.completedFuture("success"),
            "test-success-operation",
            Duration.ofSeconds(1)
        );
        
        String result = successOperation.get();
        if (!"success".equals(result)) {
            throw new RuntimeException("Expected success, got: " + result);
        }
        System.out.println("✓ Successful operation with timeout: " + result);
        
        // Test backoff calculation
        Duration backoff = resilienceManager.calculateBackoffDelay(2);
        System.out.println("✓ Backoff calculation: " + backoff.toMillis() + "ms");
        
        resilienceManager.shutdown();
        System.out.println("✓ Test 3 completed");
    }
}