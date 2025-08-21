import kafka.server.KafkaConfig;
import kafka.log.stream.s3.ConfigUtils;
import com.automq.stream.s3.Config;

import java.util.Properties;

public class TestQuorumConfig {
    public static void main(String[] args) {
        try {
            // Load the real configuration file
            Properties props = new Properties();
            try (java.io.FileInputStream fis = new java.io.FileInputStream("config/kraft-s3-quorum-real-test.properties")) {
                props.load(fis);
            }
            
            System.out.println("=== Original Properties ===");
            System.out.println("s3.stream.quorum.enabled = " + props.getProperty("s3.stream.quorum.enabled"));
            System.out.println("s3.stream.quorum.size = " + props.getProperty("s3.stream.quorum.size"));
            System.out.println("s3.stream.quorum.write.size = " + props.getProperty("s3.stream.quorum.write.size"));
            System.out.println("s3.stream.quorum.read.size = " + props.getProperty("s3.stream.quorum.read.size"));
            System.out.println("s3.data.buckets = " + props.getProperty("s3.data.buckets"));
            
            // Create KafkaConfig
            KafkaConfig kafkaConfig = new KafkaConfig(props);
            
            System.out.println("\n=== KafkaConfig.originals() ===");
            System.out.println("s3.stream.quorum.enabled = " + kafkaConfig.originals().get("s3.stream.quorum.enabled"));
            System.out.println("s3.stream.quorum.size = " + kafkaConfig.originals().get("s3.stream.quorum.size"));
            System.out.println("s3.stream.quorum.write.size = " + kafkaConfig.originals().get("s3.stream.quorum.write.size"));
            System.out.println("s3.stream.quorum.read.size = " + kafkaConfig.originals().get("s3.stream.quorum.read.size"));
            System.out.println("s3.data.buckets = " + kafkaConfig.originals().get("s3.data.buckets"));
            
            // Convert to S3 Config
            Config s3Config = ConfigUtils.to(kafkaConfig);
            
            System.out.println("\n=== S3 Config Results ===");
            System.out.println("Quorum Enabled: " + s3Config.quorumEnabled());
            System.out.println("Quorum Size: " + s3Config.quorumSize());
            System.out.println("Write Quorum Size: " + s3Config.writeQuorumSize());
            System.out.println("Read Quorum Size: " + s3Config.readQuorumSize());
            System.out.println("Data Buckets: " + s3Config.dataBuckets());
            System.out.println("Data Buckets size: " + (s3Config.dataBuckets() != null ? s3Config.dataBuckets().size() : "null"));
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}