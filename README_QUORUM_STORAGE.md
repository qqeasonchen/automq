# AutoMQ S3 Quorum Storage

## 概述

AutoMQ S3 Quorum Storage 是一个高可用的 S3 存储解决方案，通过将数据写入多个 S3 副本来确保数据可靠性和一致性。该实现基于分布式共识机制，确保写入操作需要多数副本确认才能成功。

## 核心特性

### 🔒 高可用性
- **Quorum 写入**: 写入需要多数（2/3）副本成功
- **故障容错读取**: 优先从主副本读取，失败时自动切换到其他副本
- **自动健康监控**: 实时监控每个副本的健康状态

### 🌍 多区域支持
- 支持跨不同 AWS 区域的副本配置
- 可配置主副本和次副本的角色
- 灵活的优先级设置

### ⚙️ 配置灵活性
- 可配置的 quorum 大小和超时时间
- 支持读取修复功能
- 向后兼容现有的单副本存储

## 架构设计

```
┌─────────────────┐
│   S3QuorumStorage │
│   (Facade)      │
└─────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────┐
│  S3Storage Replica 0  │  S3Storage Replica 1  │  S3Storage Replica 2  │
│  (Primary)           │  (Secondary)          │  (Secondary)          │
│  us-east-1           │  us-west-2            │  eu-west-1            │
└─────────────────────────────────────────────────┘
```

## 核心组件

### 1. S3QuorumStorage
主要的 Quorum 存储实现，实现了 `Storage` 接口：
- 管理多个 `S3Storage` 副本
- 实现 Quorum 写入逻辑
- 提供故障容错读取
- 支持 `forceUpload` 操作

### 2. QuorumConfig
Quorum 配置类：
- 定义 quorum 大小、写入/读取 quorum 大小
- 配置超时时间和读取修复参数
- 包含 `ReplicaConfig` 列表

### 3. ReplicaConfig
单个副本配置：
- 副本ID、区域、存储桶、端点
- AWS 凭证配置
- 角色（PRIMARY/SECONDARY）和优先级

### 4. QuorumState
Quorum 状态管理：
- 跟踪每个副本的健康状态
- 提供故障计数和恢复逻辑

### 5. S3QuorumStorageFactory
工厂类：
- 创建 `S3QuorumStorage` 实例
- 为每个副本创建独立的 `S3Storage`
- 处理配置和依赖注入

## 使用方法

### 1. 启用 Quorum 存储

在 AutoMQ 配置文件中启用：

```properties
# 启用 Quorum 存储
automq.s3.quorum.enabled=true

# Quorum 配置
automq.s3.quorum.size=3
automq.s3.quorum.write.quorum.size=2
automq.s3.quorum.read.quorum.size=1
automq.s3.quorum.write.timeout.ms=30000
automq.s3.quorum.read.timeout.ms=10000
```

### 2. 配置副本

```properties
# 主副本配置 (us-east-1)
automq.s3.quorum.replica.0.id=0
automq.s3.quorum.replica.0.region=us-east-1
automq.s3.quorum.replica.0.bucket=automq-primary-bucket
automq.s3.quorum.replica.0.endpoint=https://s3.us-east-1.amazonaws.com
automq.s3.quorum.replica.0.role=PRIMARY
automq.s3.quorum.replica.0.priority=100

# 次副本 1 配置 (us-west-2)
automq.s3.quorum.replica.1.id=1
automq.s3.quorum.replica.1.region=us-west-2
automq.s3.quorum.replica.1.bucket=automq-secondary1-bucket
automq.s3.quorum.replica.1.endpoint=https://s3.us-west-2.amazonaws.com
automq.s3.quorum.replica.1.role=SECONDARY
automq.s3.quorum.replica.1.priority=50

# 次副本 2 配置 (eu-west-1)
automq.s3.quorum.replica.2.id=2
automq.s3.quorum.replica.2.region=eu-west-1
automq.s3.quorum.replica.2.bucket=automq-secondary2-bucket
automq.s3.quorum.replica.2.endpoint=https://s3.eu-west-1.amazonaws.com
automq.s3.quorum.replica.2.role=SECONDARY
automq.s3.quorum.replica.2.priority=25
```

### 3. 设置 AWS 凭证

```bash
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key
```

## API 使用示例

### 创建 Quorum 存储

```java
// 创建基础配置
Config baseConfig = new Config();
baseConfig.nodeId(1);
baseConfig.walCacheSize(200 * 1024 * 1024); // 200MB

// 创建 Quorum 存储
S3QuorumStorage quorumStorage = S3QuorumStorageFactory.createQuorumStorage(
    baseConfig, null, null, null, null);

// 启动
quorumStorage.startup();
```

### 写入数据

```java
// 创建数据
String data = "Hello AutoMQ Quorum Storage!";
StreamRecordBatch recordBatch = new StreamRecordBatch(
    1L, // streamId
    1L, // baseOffset
    0L, // lastOffset
    data.length(),
    Unpooled.wrappedBuffer(data.getBytes())
);

// 写入数据（等待多数副本成功）
CompletableFuture<Void> writeFuture = quorumStorage.append(
    AppendContext.DEFAULT, recordBatch);
writeFuture.get(30, TimeUnit.SECONDS);
```

### 读取数据

```java
// 读取数据（故障容错）
CompletableFuture<ReadDataBlock> readFuture = quorumStorage.read(
    FetchContext.DEFAULT, 1L, 0L, 100L, 1024);
ReadDataBlock result = readFuture.get(30, TimeUnit.SECONDS);
```

### 监控状态

```java
QuorumState quorumState = quorumStorage.getQuorumState();
System.out.println("Healthy replicas: " + quorumState.getHealthyReplicaCount());
System.out.println("Has quorum: " + quorumState.hasQuorum());
```

## 故障处理

### 写入故障
- 如果少数副本失败，写入仍然成功
- 如果多数副本失败，写入失败并抛出异常
- 自动标记失败的副本

### 读取故障
- 优先从主副本读取
- 主副本失败时自动切换到次副本
- 所有副本失败时抛出异常

### 健康监控
- 实时监控每个副本的健康状态
- 自动标记和恢复失败的副本
- 提供详细的健康状态信息

## 性能考虑

### 写入性能
- 写入延迟取决于最慢的副本
- 网络带宽消耗增加（3倍）
- 建议使用高带宽网络连接

### 读取性能
- 读取延迟取决于最快的可用副本
- 支持读取修复以提高一致性
- 可配置读取超时时间

### 存储成本
- 存储成本增加（3倍）
- 支持数据压缩和生命周期管理
- 可配置数据保留策略

## 监控和运维

### 关键指标
- Quorum 写入成功率
- 副本健康状态
- 读取延迟和成功率
- 存储使用量

### 日志监控
```java
// 启用详细日志
LOGGER.info("Quorum write completed with sequence {}", sequence);
LOGGER.warn("Write failed for replica {} with sequence {}", replicaIndex, sequence);
LOGGER.error("All replicas failed for read", ex);
```

### 告警配置
- 副本健康状态告警
- Quorum 写入失败告警
- 存储空间不足告警

## 最佳实践

### 1. 网络配置
- 使用高带宽、低延迟的网络连接
- 配置适当的超时时间
- 启用网络重试机制

### 2. 存储配置
- 选择合适的数据保留策略
- 启用数据压缩
- 配置生命周期管理

### 3. 监控配置
- 设置详细的监控指标
- 配置告警规则
- 定期检查健康状态

### 4. 安全配置
- 使用 IAM 角色和策略
- 启用 S3 加密
- 配置访问日志

## 故障排除

### 常见问题

1. **Quorum 写入失败**
   - 检查网络连接
   - 验证 AWS 凭证
   - 检查存储桶权限

2. **读取性能下降**
   - 检查副本健康状态
   - 优化网络配置
   - 调整超时时间

3. **存储成本过高**
   - 启用数据压缩
   - 配置生命周期管理
   - 优化数据保留策略

### 调试命令

```bash
# 检查配置
grep -r "automq.s3.quorum" /path/to/config/

# 检查日志
tail -f /path/to/logs/automq.log | grep -i quorum

# 检查网络连接
ping s3.us-east-1.amazonaws.com
ping s3.us-west-2.amazonaws.com
ping s3.eu-west-1.amazonaws.com
```

## 版本兼容性

- **AutoMQ 版本**: 3.9.0+
- **Java 版本**: 17+
- **AWS SDK**: 最新版本
- **向后兼容**: 完全兼容现有的单副本存储

## 贡献指南

欢迎贡献代码和文档！请遵循以下步骤：

1. Fork 项目
2. 创建功能分支
3. 提交更改
4. 创建 Pull Request

## 许可证

本项目采用 Apache License 2.0 许可证。详见 [LICENSE](LICENSE) 文件。

## 联系方式

- **项目主页**: https://github.com/AutoMQ/automq
- **问题反馈**: https://github.com/AutoMQ/automq/issues
- **文档**: https://docs.automq.com

---

*AutoMQ S3 Quorum Storage - 为您的数据提供企业级的高可用性保障* 