# S3 Quorum Storage Configuration Guide

This document describes how to configure and use the S3 Quorum Storage feature in AutoMQ.

## Overview

S3 Quorum Storage provides high availability and durability by replicating data across multiple S3 regions with configurable quorum requirements. The system supports:

- **Multi-region replication** across 3+ AWS regions
- **Configurable quorum sizes** for read and write operations  
- **Automatic failover** when replicas become unavailable
- **Read repair** to maintain consistency across replicas
- **Flexible configuration** via properties files

## Configuration File Format

The quorum configuration is loaded from a properties file with the following structure:

### Basic Configuration

```properties
# Enable S3 Quorum Storage
automq.s3.quorum.enabled=true

# Quorum Configuration
automq.s3.quorum.size=3
automq.s3.quorum.write.quorum.size=2
automq.s3.quorum.read.quorum.size=1

# Timeouts (in milliseconds)
automq.s3.quorum.write.timeout.ms=30000
automq.s3.quorum.read.timeout.ms=10000

# Read Repair Configuration
automq.s3.quorum.read.repair.enabled=true
automq.s3.quorum.read.repair.timeout.ms=5000
```

### Replica Configuration

Each replica must be configured with its own section:

```properties
# Replica 0 Configuration (Primary - US East 1)
automq.s3.quorum.replica.0.id=0
automq.s3.quorum.replica.0.region=us-east-1
automq.s3.quorum.replica.0.bucket=automq-quorum-primary
automq.s3.quorum.replica.0.endpoint=https://s3.us-east-1.amazonaws.com
automq.s3.quorum.replica.0.role=PRIMARY
automq.s3.quorum.replica.0.priority=100

# Replica 1 Configuration (Secondary - US West 2)
automq.s3.quorum.replica.1.id=1
automq.s3.quorum.replica.1.region=us-west-2
automq.s3.quorum.replica.1.bucket=automq-quorum-secondary-1
automq.s3.quorum.replica.1.endpoint=https://s3.us-west-2.amazonaws.com
automq.s3.quorum.replica.1.role=SECONDARY
automq.s3.quorum.replica.1.priority=50

# Replica 2 Configuration (Secondary - EU West 1)
automq.s3.quorum.replica.2.id=2
automq.s3.quorum.replica.2.region=eu-west-1
automq.s3.quorum.replica.2.bucket=automq-quorum-secondary-2
automq.s3.quorum.replica.2.endpoint=https://s3.eu-west-1.amazonaws.com
automq.s3.quorum.replica.2.role=SECONDARY
automq.s3.quorum.replica.2.priority=25
```

## Configuration Parameters

### Quorum Settings

| Parameter | Description | Default | Required |
|-----------|-------------|---------|----------|
| `automq.s3.quorum.enabled` | Enable/disable quorum storage | `false` | Yes |
| `automq.s3.quorum.size` | Total number of replicas | `3` | Yes |
| `automq.s3.quorum.write.quorum.size` | Minimum replicas for write success | `2` | Yes |
| `automq.s3.quorum.read.quorum.size` | Minimum replicas for read success | `1` | Yes |

### Timeout Settings

| Parameter | Description | Default | Required |
|-----------|-------------|---------|----------|
| `automq.s3.quorum.write.timeout.ms` | Write operation timeout | `30000` | No |
| `automq.s3.quorum.read.timeout.ms` | Read operation timeout | `10000` | No |
| `automq.s3.quorum.read.repair.enabled` | Enable read repair | `true` | No |
| `automq.s3.quorum.read.repair.timeout.ms` | Read repair timeout | `5000` | No |

### Replica Settings

For each replica `N` (starting from 0):

| Parameter | Description | Required |
|-----------|-------------|----------|
| `automq.s3.quorum.replica.N.id` | Unique replica identifier | Yes |
| `automq.s3.quorum.replica.N.region` | AWS region | Yes |
| `automq.s3.quorum.replica.N.bucket` | S3 bucket name | Yes |
| `automq.s3.quorum.replica.N.endpoint` | S3 endpoint URL | Yes |
| `automq.s3.quorum.replica.N.role` | `PRIMARY` or `SECONDARY` | No (default: `SECONDARY`) |
| `automq.s3.quorum.replica.N.priority` | Priority for replica selection | No (default: `0`) |

## AWS Credentials

AWS credentials must be provided via environment variables:

```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
```

## Usage Example

### 1. Create Configuration File

Save the following as `quorum-config.properties`:

```properties
# Enable S3 Quorum Storage
automq.s3.quorum.enabled=true

# 3-replica setup with 2/3 write quorum
automq.s3.quorum.size=3
automq.s3.quorum.write.quorum.size=2
automq.s3.quorum.read.quorum.size=1

# Configure replicas across three regions
automq.s3.quorum.replica.0.id=0
automq.s3.quorum.replica.0.region=us-east-1
automq.s3.quorum.replica.0.bucket=my-app-quorum-primary
automq.s3.quorum.replica.0.endpoint=https://s3.us-east-1.amazonaws.com
automq.s3.quorum.replica.0.role=PRIMARY
automq.s3.quorum.replica.0.priority=100

automq.s3.quorum.replica.1.id=1
automq.s3.quorum.replica.1.region=us-west-2
automq.s3.quorum.replica.1.bucket=my-app-quorum-west
automq.s3.quorum.replica.1.endpoint=https://s3.us-west-2.amazonaws.com
automq.s3.quorum.replica.1.role=SECONDARY
automq.s3.quorum.replica.1.priority=50

automq.s3.quorum.replica.2.id=2
automq.s3.quorum.replica.2.region=eu-west-1
automq.s3.quorum.replica.2.bucket=my-app-quorum-eu
automq.s3.quorum.replica.2.endpoint=https://s3.eu-west-1.amazonaws.com
automq.s3.quorum.replica.2.role=SECONDARY
automq.s3.quorum.replica.2.priority=25
```

### 2. Set Environment Variables

```bash
export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

### 3. Use in Java Code

```java
import com.automq.stream.s3.Config;
import com.automq.stream.s3.quorum.factory.S3QuorumStorageFactory;
import com.automq.stream.s3.quorum.S3QuorumStorage;

// Check if quorum is enabled
if (S3QuorumStorageFactory.shouldUseQuorumStorage("quorum-config.properties")) {
    // Create quorum storage from configuration file
    S3QuorumStorage quorumStorage = S3QuorumStorageFactory.createFromConfig(
        "quorum-config.properties",
        baseConfig,
        writeAheadLog,
        streamManager,
        blockCache,
        storageFailureHandler
    );
    
    // Use quorum storage
    quorumStorage.startup();
    // ... perform operations
    quorumStorage.shutdown();
} else {
    // Use regular S3 storage
    // ...
}
```

## Configuration Validation

The system validates the configuration on load:

- **Minimum 3 replicas**: Quorum storage requires at least 3 replicas
- **Write quorum ≤ total**: Write quorum cannot exceed total replica count  
- **Read quorum ≤ total**: Read quorum cannot exceed total replica count
- **Replica count match**: Number of replica configurations must match `quorum.size`
- **AWS credentials**: Must be available via environment variables

## Best Practices

### 1. Region Selection
- Use geographically distributed regions for better fault tolerance
- Consider network latency between regions for performance
- Ensure all regions support the required S3 features

### 2. Bucket Configuration
- Use separate buckets for each replica to avoid conflicts
- Configure appropriate bucket policies and access controls
- Enable versioning and lifecycle policies as needed

### 3. Quorum Sizing
- **3 replicas**: Use 2/3 write quorum and 1/3 read quorum
- **5 replicas**: Use 3/5 write quorum and 2/5 read quorum  
- Higher write quorum = better consistency, lower availability
- Lower read quorum = better availability, potential stale reads

### 4. Timeout Configuration
- Set write timeouts based on expected network latency
- Configure read repair for eventual consistency
- Monitor timeout rates and adjust as needed

### 5. Monitoring
- Monitor replica health and quorum status
- Track read/write latencies across regions
- Set up alerts for quorum loss scenarios

## Troubleshooting

### Common Issues

**Configuration Error: "Quorum size must be at least 3"**
- Solution: Set `automq.s3.quorum.size=3` or higher

**Runtime Error: "AWS credentials not found"**
- Solution: Set `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment variables

**Timeout Errors During Operations**
- Solution: Increase timeout values or check network connectivity between regions

**Quorum Loss**
- Monitor `QuorumState.hasQuorum()` status
- Investigate failed replicas and network connectivity
- Consider temporary reduction of write quorum if needed

### Debugging

Enable debug logging for quorum operations:

```java
// Enable debug logging for quorum classes
Logger quorumLogger = LoggerFactory.getLogger("com.automq.stream.s3.quorum");
// Configure log level as needed
```

## Configuration Examples

### High Availability Setup (5 regions)
```properties
automq.s3.quorum.size=5
automq.s3.quorum.write.quorum.size=3
automq.s3.quorum.read.quorum.size=2

# Configure 5 replicas across different regions...
```

### Low Latency Setup (3 regions, fast timeouts)
```properties
automq.s3.quorum.write.timeout.ms=10000
automq.s3.quorum.read.timeout.ms=5000
automq.s3.quorum.read.repair.timeout.ms=2000

# 3 replicas in nearby regions...
```

### Development/Testing Setup
```properties
automq.s3.quorum.size=3
automq.s3.quorum.write.quorum.size=2
automq.s3.quorum.read.quorum.size=1

# Use local or single-region setup for testing...
```