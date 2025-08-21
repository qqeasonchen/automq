import kafka.server.KafkaConfig;
import kafka.log.stream.s3.ConfigUtils;
import com.automq.stream.s3.Config;

import java.util.Properties;

public class QuorumConfigTest {
    public static void main(String[] args) {
        try {
            // Create test properties
            Properties props = new Properties();
            props.setProperty("node.id", "1");
            props.setProperty("process.roles", "broker,controller");
            props.setProperty("controller.quorum.voters", "1@localhost:9093");
            props.setProperty("listeners", "PLAINTEXT://:9092,CONTROLLER://:9093");
            props.setProperty("log.dirs", "/tmp/test-logs");
            
            // Add quorum properties
            props.setProperty("s3.stream.quorum.enabled", "true");
            props.setProperty("s3.stream.quorum.size", "3");
            props.setProperty("s3.stream.quorum.write.size", "2");
            props.setProperty("s3.stream.quorum.read.size", "1");
            
            // Add S3 configuration
            props.setProperty("s3.data.buckets", "0@s3://test-bucket?region=us-east-1&endpoint=http://localhost:9000");
            props.setProperty("elasticstream.enable", "true");
            
            // Create KafkaConfig
            KafkaConfig kafkaConfig = new KafkaConfig(props);
            
            // Convert to S3 Config
            Config s3Config = ConfigUtils.to(kafkaConfig);
            
            // Print results
            System.out.println("Quorum Enabled: " + s3Config.quorumEnabled());
            System.out.println("Quorum Size: " + s3Config.quorumSize());
            System.out.println("Write Quorum Size: " + s3Config.writeQuorumSize());
            System.out.println("Read Quorum Size: " + s3Config.readQuorumSize());
            System.out.println("Data Buckets: " + s3Config.dataBuckets());
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}