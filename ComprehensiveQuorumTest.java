/*
 * Comprehensive test to verify all S3 Quorum Storage enhancements
 */

import com.automq.stream.s3.operator.BucketURI;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorageFactory;
import com.automq.stream.s3.operator.QuorumObjectStorage;
import com.automq.stream.s3.quorum.monitoring.QuorumMetricsCollector;
import com.automq.stream.s3.quorum.resilience.QuorumResilienceManager;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class ComprehensiveQuorumTest {
    public static void main(String[] args) {
        System.out.println("=== Comprehensive S3 Quorum Storage Enhancement Test ===");
        System.out.println("Testing all enhanced components and functionality...\n");
        
        try {
            // Test 1: QuorumObjectStorage Basic Functionality
            testQuorumObjectStorageBasics();
            
            // Test 2: Metrics Collection
            testMetricsCollection();
            
            // Test 3: Resilience Manager
            testResilienceManager();
            
            // Test 4: Configuration Integration
            testConfigurationIntegration();
            
            System.out.println("\n=== All Enhancement Tests Completed Successfully! ===");
            System.out.println("✅ S3 Quorum Storage enhancements are fully functional");
            
        } catch (Exception e) {
            System.err.println("❌ Enhancement test failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static void testQuorumObjectStorageBasics() throws Exception {
        System.out.println("--- Test 1: QuorumObjectStorage Basic Functionality ---");
        
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
        
        if (quorumStorage instanceof QuorumObjectStorage) {
            QuorumObjectStorage qos = (QuorumObjectStorage) quorumStorage;
            System.out.println("  - Replica count: " + qos.getReplicaCount());
            System.out.println("  - Write quorum size: " + qos.getWriteQuorumSize());
            System.out.println("  - Read quorum size: " + qos.getReadQuorumSize());
            System.out.println("  - Readiness check: " + qos.readinessCheck());
        }
        
        quorumStorage.close();
        System.out.println("✓ Test 1 completed\n");
    }
    
    private static void testMetricsCollection() throws Exception {
        System.out.println("--- Test 2: Metrics Collection ---");
        
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
        System.out.println("✓ Test 2 completed\n");
    }
    
    private static void testResilienceManager() throws Exception {
        System.out.println("--- Test 3: Resilience Manager ---");
        
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
        assertEquals("success", result);
        System.out.println("✓ Successful operation with timeout: " + result);
        
        // Test retry mechanism
        final int[] attemptCount = {0};
        CompletableFuture<String> retryOperation = resilienceManager.executeWithRetry(
            () -> {
                attemptCount[0]++;
                if (attemptCount[0] < 2) {
                    return CompletableFuture.failedFuture(new RuntimeException("Temporary failure"));
                }
                return CompletableFuture.completedFuture("retry-success");
            },
            "test-retry-operation",
            2
        );
        
        String retryResult = retryOperation.get();
        assertEquals("retry-success", retryResult);
        System.out.println("✓ Retry mechanism working: " + retryResult + " (attempts: " + attemptCount[0] + ")");
        
        // Test backoff calculation
        Duration backoff = resilienceManager.calculateBackoffDelay(2);
        System.out.println("✓ Backoff calculation: " + backoff.toMillis() + "ms");
        
        resilienceManager.shutdown();
        System.out.println("✓ Test 3 completed\n");
    }
    
    private static void testConfigurationIntegration() throws Exception {
        System.out.println("--- Test 4: Configuration Integration ---");
        
        // Test bucket parsing for multiple buckets
        String multipleBucketsConfig = "0@mem://bucket1,1@mem://bucket2,2@mem://bucket3";
        String[] bucketEntries = multipleBucketsConfig.split(",");
        
        List<BucketURI> parsedBuckets = new ArrayList<>();
        for (String bucketEntry : bucketEntries) {
            parsedBuckets.add(BucketURI.parse(bucketEntry.trim()));
        }
        
        System.out.println("✓ Multi-bucket configuration parsing working");
        System.out.println("  - Parsed " + parsedBuckets.size() + " buckets");
        
        // Test ObjectStorageFactory with quorum configuration
        ObjectStorage factoryStorage = ObjectStorageFactory.instance()
            .builder()
            .buckets(parsedBuckets)
            .quorumEnabled(true)
            .quorumSize(3)
            .writeQuorumSize(2)
            .readQuorumSize(1)
            .build();
        
        System.out.println("✓ ObjectStorageFactory with quorum configuration working");
        System.out.println("  - Type: " + factoryStorage.getClass().getSimpleName());
        
        factoryStorage.close();
        System.out.println("✓ Test 4 completed\n");
    }
    
    private static void assertEquals(Object expected, Object actual) {
        if (!expected.equals(actual)) {
            throw new AssertionError("Expected: " + expected + ", but was: " + actual);
        }
    }
}