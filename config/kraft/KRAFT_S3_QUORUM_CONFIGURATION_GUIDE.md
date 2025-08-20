# AutoMQ KRaft + S3 Quorum Storage 配置指南

## 配置文件说明

AutoMQ 提供了两个集成配置文件，将 Kafka KRaft 模式与 S3 Quorum Storage 完整结合：

### 1. 生产环境配置
**文件**: `config/kraft-s3-quorum-server.properties`

**特点**:
- ✅ 5 副本高可用 Quorum 配置
- ✅ 多区域部署 (US East, US West, EU, Asia Pacific)
- ✅ 生产级性能优化 (大缓存，高并发)
- ✅ 完整的监控和告警配置
- ✅ 自动故障切换和恢复机制
- ✅ 企业级安全和合规设置

### 2. 开发环境配置
**文件**: `config/kraft-s3-quorum-development.properties`

**特点**:
- ✅ 3 副本简化 Quorum 配置
- ✅ 本地 MinIO 存储支持
- ✅ 调试功能和详细日志
- ✅ 资源使用优化 (小缓存，快速测试)
- ✅ 开发友好的设置和超时配置

## 配置文件结构对比

| 配置项 | 生产环境 | 开发环境 | 说明 |
|--------|----------|----------|------|
| **Quorum 副本数** | 5 | 3 | 生产环境更高可用性 |
| **写 Quorum 大小** | 3 | 2 | 生产环境需要更多确认 |
| **读 Quorum 大小** | 2 | 1 | 生产环境更严格一致性 |
| **超时配置** | 25s/8s | 15s/5s | 开发环境更快响应 |
| **缓存大小** | 512MB/1GB | 64MB/128MB | 生产环境更大缓存 |
| **监控间隔** | 15-60s | 30-60s | 生产环境更频繁监控 |
| **故障切换** | 启用 | 禁用 | 生产环境自动故障处理 |
| **调试日志** | 禁用 | 启用 | 开发环境详细调试信息 |

## 两个配置文件整合的核心组件

### 1. KRaft 基础设置
```properties
# 服务器角色和ID
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093

# 网络监听配置
listeners=PLAINTEXT://:9092,CONTROLLER://:9093
advertised.listeners=PLAINTEXT://localhost:9092,CONTROLLER://localhost:9093
```

### 2. S3 Quorum Storage 核心配置
```properties
# 启用 S3 Quorum 存储
automq.s3.quorum.enabled=true
elasticstream.enable=true

# Quorum 规模和超时
automq.s3.quorum.size=3|5
automq.s3.quorum.write.quorum.size=2|3
automq.s3.quorum.read.quorum.size=1|2
```

### 3. 多副本配置
```properties
# 副本配置模板
automq.s3.quorum.replica.{id}.id={replica_id}
automq.s3.quorum.replica.{id}.region={region}
automq.s3.quorum.replica.{id}.bucket={bucket_name}
automq.s3.quorum.replica.{id}.endpoint={s3_endpoint}
automq.s3.quorum.replica.{id}.role=PRIMARY|SECONDARY
automq.s3.quorum.replica.{id}.priority={priority_value}
```

### 4. S3 存储桶配置
```properties
# 数据存储桶
s3.data.buckets=0@s3://{bucket}?region={region}&authType={auth}&...
s3.ops.buckets=0@s3://{bucket}?region={region}&authType={auth}&...  
s3.wal.path=0@s3://{bucket}?region={region}&authType={auth}&...
```

## 使用方法

### 生产环境部署

1. **环境变量设置**:
```bash
export AWS_ACCESS_KEY_ID=your-production-access-key
export AWS_SECRET_ACCESS_KEY=your-production-secret-key
export AWS_REGION=us-east-1
export AUTOMQ_INSTANCE_ID=prod-instance-001
export AUTOMQ_ENVIRONMENT=production
export KAFKA_HEAP_OPTS="-Xmx4g -Xms4g"
```

2. **创建 S3 存储桶**:
```bash
aws s3 mb s3://automq-prod-primary-us-east-1
aws s3 mb s3://automq-prod-backup-us-west-2
aws s3 mb s3://automq-prod-backup-eu-west-1
aws s3 mb s3://automq-prod-backup-ap-southeast-1
aws s3 mb s3://automq-prod-backup-us-east-2
```

3. **初始化和启动**:
```bash
# 生成集群 UUID
CLUSTER_UUID=$(./bin/kafka-storage.sh random-uuid)

# 格式化存储
./bin/kafka-storage.sh format -t $CLUSTER_UUID -c config/kraft-s3-quorum-server.properties

# 启动服务器
./bin/kafka-server-start.sh config/kraft-s3-quorum-server.properties
```

### 开发环境部署

1. **启动 MinIO**:
```bash
docker run -d --name minio \
  -p 9000:9000 -p 9001:9001 \
  -e "MINIO_ROOT_USER=minioadmin" \
  -e "MINIO_ROOT_PASSWORD=minioadmin" \
  quay.io/minio/minio server /data --console-address ":9001"
```

2. **创建存储桶**:
```bash
# 通过 Web 控制台 (http://localhost:9001) 或使用 MinIO 客户端
mc config host add local http://localhost:9000 minioadmin minioadmin
mc mb local/automq-dev-primary
mc mb local/automq-dev-secondary-1
mc mb local/automq-dev-secondary-2
```

3. **环境变量设置**:
```bash
export AUTOMQ_INSTANCE_ID=dev-instance-001
export AUTOMQ_ENVIRONMENT=development
export AUTOMQ_LOG_LEVEL=DEBUG
```

4. **初始化和启动**:
```bash
# 生成集群 UUID
CLUSTER_UUID=$(./bin/kafka-storage.sh random-uuid)

# 格式化存储
./bin/kafka-storage.sh format -t $CLUSTER_UUID -c config/kraft-s3-quorum-development.properties

# 启动服务器
./bin/kafka-server-start.sh config/kraft-s3-quorum-development.properties
```

## 主要优势

### 1. **配置统一化**
- 单个配置文件包含所有必要设置
- 避免多文件管理复杂性
- 减少配置错误和遗漏

### 2. **功能完整性**
- KRaft 模式的元数据管理
- S3 Quorum 的高可用存储
- 全面的监控和故障处理
- 性能优化和资源管理

### 3. **环境适配性**
- 生产环境：高可用、高性能、安全合规
- 开发环境：快速启动、易于调试、资源节约

### 4. **扩展性**
- 支持多区域部署
- 灵活的副本配置
- 可自定义的性能参数

## 监控和运维

### 关键指标监控
- **Quorum 健康状态**: `automq.s3.quorum.health.check.*`
- **写入性能**: `automq.s3.quorum.write.*`  
- **读取性能**: `automq.s3.quorum.read.*`
- **故障切换**: `automq.s3.quorum.failover.*`

### 告警配置
- 延迟超过 5000ms
- 错误率超过 5%
- 可用性低于 95%

### 日志位置
- 服务器日志: `logs/server.log`
- S3 Stream 日志: `logs/s3stream-*.log`
- 控制器日志: `logs/controller.log`

## 故障排除

### 常见问题
1. **WAL 权限错误**: 确保 S3 访问权限正确配置
2. **副本连接失败**: 检查网络连接和端点配置
3. **性能问题**: 调整缓存大小和并发配置
4. **存储桶访问**: 验证 S3 认证和桶权限

### 调试步骤
1. 检查环境变量设置
2. 验证 S3 连接和权限
3. 查看详细日志输出
4. 使用健康检查接口
5. 监控关键性能指标

通过这两个集成配置文件，AutoMQ 实现了 KRaft 模式与 S3 Quorum Storage 的完美结合，为不同环境提供了最优的配置方案。