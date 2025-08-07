# AutoMQ S3 Quorum Storage

## 概述

AutoMQ S3 Quorum Storage 是一个高可用的存储解决方案，通过在多个S3区域写入3份数据副本来确保数据的持久性和可用性。该实现基于Quorum协议，确保在部分副本失败的情况下仍能保证数据一致性。

## 架构设计

### 核心组件

1. **S3QuorumStorage**: 主要的quorum存储实现
2. **QuorumConfig**: quorum配置管理
3. **ReplicaConfig**: 单个副本配置
4. **QuorumState**: quorum状态管理
5. **S3QuorumStorageFactory**: 工厂类，用于创建quorum存储实例

### 写入流程

```
Producer Request
    ↓
Broker (Leader)
    ↓
S3QuorumStorage
    ↓
并行写入到3个副本
    ↓
等待多数副本确认 (2/3)
    ↓
返回成功响应
```

### 读取流程

```
Consumer Request
    ↓
Broker
    ↓
S3QuorumStorage
    ↓
优先从Primary副本读取
    ↓
如果Primary失败，从Secondary副本读取
    ↓
返回数据
```

## 配置说明

### 启用Quorum Storage

在broker配置中启用quorum storage：

```properties
# 启用quorum storage
automq.s3.quorum.enabled=true

# Quorum配置
automq.s3.quorum.size=3
automq.s3.quorum.write.quorum.size=2
automq.s3.quorum.read.quorum.size=1
```

### 副本配置

配置3个不同区域的S3副本：

```properties
# Primary副本 (us-east-1)
automq.s3.quorum.replica.0.id=0
automq.s3.quorum.replica.0.region=us-east-1
automq.s3.quorum.replica.0.bucket=automq-primary-bucket
automq.s3.quorum.replica.0.role=PRIMARY
automq.s3.quorum.replica.0.priority=100

# Secondary副本1 (us-west-2)
automq.s3.quorum.replica.1.id=1
automq.s3.quorum.replica.1.region=us-west-2
automq.s3.quorum.replica.1.bucket=automq-secondary1-bucket
automq.s3.quorum.replica.1.role=SECONDARY
automq.s3.quorum.replica.1.priority=50

# Secondary副本2 (eu-west-1)
automq.s3.quorum.replica.2.id=2
automq.s3.quorum.replica.2.region=eu-west-1
automq.s3.quorum.replica.2.bucket=automq-secondary2-bucket
automq.s3.quorum.replica.2.role=SECONDARY
automq.s3.quorum.replica.2.priority=25
```

### AWS凭证配置

设置AWS访问凭证：

```bash
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key
```

## 使用示例

### 1. 创建Quorum Storage

```java
// 创建基础配置
Config baseConfig = new Config();
baseConfig.nodeId(1);
baseConfig.walCacheSize(200 * 1024 * 1024); // 200MB

// 创建quorum storage
S3QuorumStorage quorumStorage = S3QuorumStorageFactory.createQuorumStorage(
    baseConfig, writeAheadLog, streamManager, blockCache, storageFailureHandler);
```

### 2. 写入数据

```java
// 创建记录批次
StreamRecordBatch recordBatch = new StreamRecordBatch(streamId, epoch, offset, count, data);

// 写入到quorum storage
CompletableFuture<Void> writeFuture = quorumStorage.append(context, recordBatch);

// 等待写入完成
writeFuture.get(30, TimeUnit.SECONDS);
```

### 3. 读取数据

```java
// 从quorum storage读取
CompletableFuture<ReadDataBlock> readFuture = quorumStorage.read(
    context, streamId, startOffset, endOffset, maxBytes);

// 获取读取结果
ReadDataBlock dataBlock = readFuture.get(10, TimeUnit.SECONDS);
```

## 容错机制

### 写入容错

- **多数确认**: 写入需要至少2个副本确认成功
- **自动重试**: 失败的副本会自动重试
- **超时处理**: 设置写入超时，避免长时间等待

### 读取容错

- **优先读取**: 优先从Primary副本读取
- **故障转移**: Primary失败时自动切换到Secondary副本
- **读取修复**: 支持读取修复机制，确保数据一致性

### 状态监控

```java
// 检查quorum状态
QuorumState quorumState = quorumStorage.getQuorumState();

// 检查健康副本数量
int healthyCount = quorumState.getHealthyReplicaCount();

// 检查是否有quorum
boolean hasQuorum = quorumState.hasQuorum();

// 获取健康副本索引
int[] healthyIndices = quorumState.getHealthyReplicaIndices();
```

## 性能考虑

### 写入性能

- **并行写入**: 同时写入3个副本，提高吞吐量
- **异步处理**: 使用CompletableFuture进行异步处理
- **批量写入**: 支持批量写入，减少网络开销

### 读取性能

- **本地优先**: 优先从最近的副本读取
- **缓存机制**: 利用S3BlockCache提高读取性能
- **连接池**: 复用S3连接，减少连接建立开销

### 网络优化

- **带宽限制**: 可配置网络带宽限制
- **重试策略**: 智能重试策略，避免网络抖动影响
- **超时控制**: 合理的超时设置，避免长时间等待

## 监控和指标

### 关键指标

- **写入延迟**: 从写入请求到多数确认的时间
- **读取延迟**: 从读取请求到数据返回的时间
- **副本健康状态**: 各个副本的健康状态
- **失败率**: 写入和读取的失败率

### 日志监控

```java
// 启用详细日志
LOGGER.setLevel(Level.DEBUG);

// 监控关键事件
LOGGER.info("Quorum write completed with sequence {}", sequence);
LOGGER.warn("Replica {} failed, healthy replicas: {}", replicaIndex, healthyCount);
```

## 最佳实践

### 1. 区域选择

- 选择延迟较低的S3区域
- 考虑成本因素，平衡性能和成本
- 避免选择同一可用区的多个副本

### 2. 配置优化

- 根据网络条件调整超时设置
- 根据数据量调整缓存大小
- 根据并发量调整连接池大小

### 3. 监控告警

- 设置副本健康状态告警
- 监控写入和读取延迟
- 设置失败率告警

### 4. 故障处理

- 定期检查副本健康状态
- 及时处理失败的副本
- 准备故障恢复预案

## 故障排除

### 常见问题

1. **写入超时**
   - 检查网络连接
   - 调整超时设置
   - 检查S3服务状态

2. **副本失败**
   - 检查AWS凭证
   - 检查S3 bucket权限
   - 检查网络连接

3. **读取失败**
   - 检查副本健康状态
   - 检查数据一致性
   - 启用读取修复

### 调试命令

```bash
# 检查AWS凭证
aws sts get-caller-identity

# 测试S3连接
aws s3 ls s3://your-bucket

# 检查网络延迟
ping s3.us-east-1.amazonaws.com
```

## 总结

AutoMQ S3 Quorum Storage 提供了高可用的数据存储解决方案，通过3副本写入确保数据持久性，通过智能的故障转移机制确保服务可用性。合理配置和监控可以显著提高系统的可靠性和性能。 