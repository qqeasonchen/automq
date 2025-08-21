/*
 * Simple test to verify QuorumObjectStorage functionality
 */

import com.automq.stream.s3.operator.BucketURI;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorageFactory;
import com.automq.stream.s3.operator.QuorumObjectStorage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class SimpleQuorumTest {
    public static void main(String[] args) {
        System.out.println("=== Simple S3 Quorum Storage Test ===");
        
        try {
            // Create test buckets (memory-based for testing)
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
            }
            
            // Test write operation
            System.out.println("\n--- Testing Write Operation ---");
            String testKey = "test-object-" + System.currentTimeMillis();
            ByteBuf testData = Unpooled.copiedBuffer("Hello S3 Quorum Storage!".getBytes());
            
            CompletableFuture<ObjectStorage.WriteResult> writeFuture = quorumStorage.write(
                new ObjectStorage.WriteOptions(), testKey, testData);
            
            ObjectStorage.WriteResult writeResult = writeFuture.get();
            System.out.println("✓ Write operation completed");
            System.out.println("  - Result bucket: " + writeResult.bucket());
            
            // Test read operation
            System.out.println("\n--- Testing Read Operation ---");
            CompletableFuture<ByteBuf> readFuture = quorumStorage.read(
                new ObjectStorage.ReadOptions(), testKey);
            
            ByteBuf readData = readFuture.get();
            System.out.println("✓ Read operation completed");
            System.out.println("  - Data length: " + readData.readableBytes());
            
            byte[] readBytes = new byte[readData.readableBytes()];
            readData.readBytes(readBytes);
            System.out.println("  - Data content: " + new String(readBytes));
            
            // Test readiness check
            System.out.println("\n--- Testing Readiness Check ---");
            boolean ready = quorumStorage.readinessCheck();
            System.out.println("✓ Readiness check: " + ready);
            
            // Cleanup
            quorumStorage.close();
            testData.release();
            readData.release();
            
            System.out.println("\n=== All tests passed! ===");
            
        } catch (Exception e) {
            System.err.println("❌ Test failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}