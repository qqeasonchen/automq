# AutoMQ S3 Quorum Storage End-to-End Testing Guide

## 概述

本文档详细介绍了AutoMQ S3 Quorum Storage的端到端测试框架、测试用例和运行指南。这些测试确保Quorum存储在各种场景下的可靠性、性能和正确性。

## 🎯 测试目标

### 主要目标
- **功能验证**: 验证多副本写入和读取的正确性
- **故障容错**: 测试各种故障场景下的恢复能力
- **性能评估**: 评估在不同负载下的性能表现
- **配置验证**: 确保各种配置组合的有效性
- **集成测试**: 验证与现有系统的兼容性

### 质量保证
- **数据一致性**: 确保多副本间数据一致
- **高可用性**: 验证单点故障不影响服务
- **性能稳定**: 确保性能满足生产需求
- **配置灵活**: 支持多种部署配置

## 📁 测试结构

```
s3stream/src/test/java/com/automq/stream/s3/quorum/
├── S3QuorumStorageE2ETest.java                    # 基础功能端到端测试
├── S3QuorumStorageFailureRecoveryE2ETest.java     # 故障恢复场景测试
├── S3QuorumStoragePerformanceE2ETest.java         # 性能和压力测试
├── S3QuorumStorageConfigurationE2ETest.java       # 配置验证测试
├── QuorumWriteIntegrationTest.java                # 集成测试
└── S3QuorumStorageE2ETestSuite.java              # 测试套件
```

```
scripts/
└── run-quorum-e2e-tests.sh                        # 测试运行脚本
```

## 🧪 测试分类详解

### 1. 基础功能测试 (S3QuorumStorageE2ETest)

#### 测试用例覆盖
- **基本读写操作**
  - `testBasicWriteAndRead()`: 基本的写入和读取验证
  - 验证数据完整性和副本同步

- **并发写入测试**
  - `testMultipleConcurrentWrites()`: 多个并发写入操作
  - 验证并发安全性和性能

- **大数据处理**
  - `testLargeDataWrites()`: 大数据块(1MB+)的写入测试
  - 验证大数据处理能力

- **健康监控**
  - `testQuorumHealthMonitoring()`: Quorum健康状态监控
  - 验证监控指标的准确性

- **强制上传**
  - `testForceUploadToAllReplicas()`: 强制上传到所有副本
  - 验证数据强制同步功能

#### 技术特点
- 使用内存存储后端进行快速测试
- 真实的S3Storage实例，非Mock对象
- 全面的数据验证和状态检查

### 2. 故障恢复测试 (S3QuorumStorageFailureRecoveryE2ETest)

#### 故障场景覆盖

**单副本故障**
```java
testSingleReplicaFailureDuringWrite()
```
- 模拟单个副本失败
- 验证2/3 Quorum仍能正常工作
- 测试故障检测和标记

**主副本故障切换**
```java
testPrimaryReplicaFailoverDuringRead()
```
- 模拟主副本故障
- 验证自动切换到次副本
- 测试读取故障恢复

**副本恢复测试**
```java
testReplicaRecovery()
```
- 测试故障副本的恢复过程
- 验证健康状态的正确更新
- 测试恢复后的正常操作

**多副本故障**
```java
testTwoReplicasFailure()
```
- 模拟多数副本失败
- 验证Quorum失效时的正确行为
- 测试错误处理机制

**级联故障恢复**
```java
testCascadingFailureRecovery()
```
- 测试复杂的故障-恢复序列
- 验证系统的弹性和恢复能力

**超时处理**
```java
testTimeoutHandlingDuringFailure()
```
- 测试各种超时场景
- 验证超时机制的正确性

#### 故障注入技术
```java
// 故障注入示例
private AtomicBoolean replica1Failing = new AtomicBoolean(false);

when(mockStorage1.append(any(), any()))
    .thenAnswer(invocation -> {
        if (replica1Failing.get()) {
            return CompletableFuture.failedFuture(
                new RuntimeException("Simulated failure"));
        } else {
            return CompletableFuture.completedFuture(null);
        }
    });
```

### 3. 性能测试 (S3QuorumStoragePerformanceE2ETest)

#### 性能指标

**吞吐量测试**
- `testHighThroughputWrites()`: 高吞吐量写入测试
  - 目标: >100 writes/sec
  - 测试1000个并发写入操作

**并发性能**
- `testConcurrentWritesFromMultipleThreads()`: 多线程并发测试
  - 10个线程，每线程100次写入
  - 目标: >50 writes/sec总体吞吐量

**大数据性能**
- `testLargeDataWritePerformance()`: 大数据写入性能
  - 测试1KB到1MB不同大小数据
  - 评估吞吐量(MB/s)

**读取性能**
- `testReadPerformance()`: 读取性能测试
  - 目标: >200 reads/sec
  - 测试1000个并发读取

**混合负载**
- `testMixedWorkloadPerformance()`: 读写混合负载
  - 5个写入线程 + 3个读取线程
  - 测试30秒持续负载

**内存使用**
- `testMemoryUsageDuringHighLoad()`: 内存使用监控
  - 监控内存增长趋势
  - 验证无内存泄漏

#### 性能基准
```java
// 性能断言示例
double throughput = numberOfWrites / durationSeconds;
assertTrue(throughput > 100, 
    "Throughput should be > 100 writes/sec, got: " + throughput);
```

### 4. 配置验证测试 (S3QuorumStorageConfigurationE2ETest)

#### 配置测试覆盖

**有效配置测试**
```java
testValidQuorumConfigurations()
```
- 标准3副本配置 (3,2,1)
- 5副本配置 (5,3,1)
- 单副本配置 (1,1,1)

**无效配置检测**
```java
testInvalidQuorumConfigurations()
```
- 写Quorum大于总副本数
- 零或负数配置
- 不合理的Quorum大小

**副本数量验证**
```java
testMismatchedReplicaCount()
```
- 副本数量与配置不匹配
- 过多或过少副本的处理

**超时配置**
```java
testTimeoutConfigurations()
```
- 极短超时配置测试
- 超长超时配置测试
- 超时触发验证

**副本角色配置**
```java
testReplicaRoleConfigurations()
```
- 主副本唯一性验证
- 角色配置有效性检查

**读修复配置**
```java
testReadRepairConfiguration()
```
- 读修复启用/禁用测试
- 修复超时配置验证

### 5. 集成测试 (QuorumWriteIntegrationTest)

#### 集成场景
- **多次Quorum写入**: 验证连续操作的稳定性
- **故障容错写入**: 单个副本故障时的写入测试
- **Quorum失效处理**: 多数副本故障时的正确失败
- **性能计数验证**: 成功/失败计数的准确性

## 🚀 运行测试

### 快速开始

#### 运行全部测试
```bash
./scripts/run-quorum-e2e-tests.sh
```

#### 运行特定测试类别
```bash
# 基础功能测试
./scripts/run-quorum-e2e-tests.sh --basic

# 故障恢复测试
./scripts/run-quorum-e2e-tests.sh --failure-recovery

# 性能测试
./scripts/run-quorum-e2e-tests.sh --performance

# 配置验证测试
./scripts/run-quorum-e2e-tests.sh --configuration

# 集成测试
./scripts/run-quorum-e2e-tests.sh --integration
```

#### 查看帮助
```bash
./scripts/run-quorum-e2e-tests.sh --help
```

### 使用Gradle直接运行

#### 运行整个测试套件
```bash
./gradlew :s3stream:test --tests "S3QuorumStorageE2ETestSuite"
```

#### 运行单个测试类
```bash
./gradlew :s3stream:test --tests "S3QuorumStorageE2ETest"
./gradlew :s3stream:test --tests "S3QuorumStorageFailureRecoveryE2ETest"
./gradlew :s3stream:test --tests "S3QuorumStoragePerformanceE2ETest"
./gradlew :s3stream:test --tests "S3QuorumStorageConfigurationE2ETest"
```

#### 运行特定测试方法
```bash
./gradlew :s3stream:test --tests "S3QuorumStorageE2ETest.testBasicWriteAndRead"
./gradlew :s3stream:test --tests "S3QuorumStoragePerformanceE2ETest.testHighThroughputWrites"
```

### 环境要求

#### 系统要求
- **Java**: JDK 17或更高版本
- **内存**: 建议4GB以上
- **磁盘**: 100MB可用空间用于测试输出

#### JVM参数调优
```bash
export JAVA_OPTS="-Xmx4g -Xms2g -XX:+UseG1GC"
```

#### 测试超时配置
```bash
-Djunit.jupiter.execution.timeout.default=300s
-Djunit.jupiter.execution.timeout.testable.method.default=300s
```

## 📊 测试报告和分析

### 测试输出位置
```
build/
├── test-results/quorum-e2e/          # 测试结果XML
├── reports/quorum-e2e/               # 自定义报告
│   └── test-summary.txt              # 测试摘要
└── reports/tests/test/               # Gradle HTML报告
    └── index.html                    # 详细测试报告
```

### 性能指标解读

#### 写入性能基准
- **高吞吐量**: >100 writes/sec
- **并发写入**: >50 writes/sec (多线程)
- **大数据写入**: <10秒完成(1MB数据)

#### 读取性能基准
- **读取吞吐量**: >200 reads/sec
- **故障切换时间**: <2秒

#### 内存使用基准
- **内存增长**: <100MB (高负载测试)
- **无内存泄漏**: GC后内存回收

### 故障恢复指标
- **单副本故障**: 服务继续可用
- **恢复时间**: <5秒自动检测和恢复
- **数据一致性**: 100%保证

## 🔧 测试开发指南

### 添加新测试用例

#### 1. 选择合适的测试类
```java
// 基础功能 -> S3QuorumStorageE2ETest
// 故障场景 -> S3QuorumStorageFailureRecoveryE2ETest  
// 性能测试 -> S3QuorumStoragePerformanceE2ETest
// 配置验证 -> S3QuorumStorageConfigurationE2ETest
```

#### 2. 测试方法模板
```java
@Test
@Timeout(value = 60, unit = TimeUnit.SECONDS)
void testNewFeature() throws Exception {
    LOGGER.info("Testing new feature...");
    
    // Setup test data
    String testData = "new-feature-data";
    StreamRecordBatch record = createTestRecord(testData);
    
    // Execute operation
    CompletableFuture<Void> future = quorumStorage.append(
        AppendContext.DEFAULT, record);
    future.get(30, TimeUnit.SECONDS);
    
    // Verify results
    assertTrue(quorumStorage.isHealthy());
    assertEquals(3, quorumStorage.getHealthyReplicaCount());
    
    LOGGER.info("New feature test completed successfully");
}
```

#### 3. Mock设置最佳实践
```java
// 成功响应
when(mockStorage.append(any(), any()))
    .thenReturn(CompletableFuture.completedFuture(null));

// 故障模拟
when(mockStorage.append(any(), any()))
    .thenReturn(CompletableFuture.failedFuture(
        new RuntimeException("Simulated failure")));

// 延迟响应
when(mockStorage.append(any(), any()))
    .thenAnswer(invocation -> {
        CompletableFuture<Void> delayed = new CompletableFuture<>();
        CompletableFuture.delayedExecutor(100, TimeUnit.MILLISECONDS)
            .execute(() -> delayed.complete(null));
        return delayed;
    });
```

### 性能测试编写指南

#### 1. 吞吐量测试模式
```java
long startTime = System.nanoTime();

// 执行操作
for (int i = 0; i < numberOfOperations; i++) {
    // 执行测试操作
}

long endTime = System.nanoTime();
double throughput = numberOfOperations / 
    ((endTime - startTime) / 1_000_000_000.0);

// 断言性能要求
assertTrue(throughput > expectedThroughput);
```

#### 2. 并发测试模式
```java
ExecutorService executor = Executors.newFixedThreadPool(threadCount);
CountDownLatch latch = new CountDownLatch(threadCount);

for (int i = 0; i < threadCount; i++) {
    executor.submit(() -> {
        try {
            // 执行并发操作
        } finally {
            latch.countDown();
        }
    });
}

latch.await(timeout, TimeUnit.SECONDS);
executor.shutdown();
```

### 故障注入模式

#### 1. 简单故障注入
```java
AtomicBoolean failing = new AtomicBoolean(false);

when(mockStorage.operation(any()))
    .thenAnswer(invocation -> {
        if (failing.get()) {
            return CompletableFuture.failedFuture(
                new RuntimeException("Injected failure"));
        }
        return CompletableFuture.completedFuture(result);
    });

// 触发故障
failing.set(true);
```

#### 2. 复杂故障场景
```java
enum FailureState { HEALTHY, SLOW, FAILED, RECOVERED }
AtomicReference<FailureState> state = new AtomicReference<>(HEALTHY);

when(mockStorage.operation(any()))
    .thenAnswer(invocation -> {
        switch (state.get()) {
            case HEALTHY:
                return CompletableFuture.completedFuture(result);
            case SLOW:
                return delayedResult(result, 5000); // 5秒延迟
            case FAILED:
                return CompletableFuture.failedFuture(exception);
            case RECOVERED:
                return CompletableFuture.completedFuture(result);
        }
    });
```

## 🐛 常见问题和解决方案

### 测试失败排查

#### 1. 超时问题
```
症状: 测试超时失败
原因: Mock响应太慢或死锁
解决: 检查Mock设置，增加超时时间
```

#### 2. 内存问题  
```
症状: OutOfMemoryError
原因: 大数据测试或内存泄漏
解决: 增加堆内存，检查资源释放
```

#### 3. 并发问题
```
症状: 并发测试不稳定
原因: 竞态条件或时序问题
解决: 使用CountDownLatch同步，增加重试
```

### 性能测试调优

#### 1. 提高测试速度
- 使用内存后端存储
- 减少不必要的日志输出
- 并行运行独立测试

#### 2. 提高测试稳定性
- 设置合理的超时时间
- 使用重试机制
- 避免硬编码时间依赖

#### 3. 提高测试覆盖率
- 添加边界条件测试
- 覆盖异常路径
- 测试各种配置组合

## 📈 持续集成和自动化

### CI/CD 集成

#### 1. GitHub Actions示例
```yaml
name: S3 Quorum Storage E2E Tests

on: [push, pull_request]

jobs:
  e2e-tests:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v3
    - name: Set up JDK 17
      uses: actions/setup-java@v3
      with:
        java-version: '17'
    - name: Run E2E Tests
      run: ./scripts/run-quorum-e2e-tests.sh
    - name: Upload Test Reports
      uses: actions/upload-artifact@v3
      with:
        name: test-reports
        path: build/reports/
```

#### 2. 定期性能回归测试
```bash
# 每日性能基准测试
0 2 * * * /path/to/run-quorum-e2e-tests.sh --performance
```

### 测试指标监控

#### 1. 关键指标
- 测试通过率
- 平均执行时间
- 性能回归检测
- 代码覆盖率

#### 2. 告警配置
- 测试失败率 > 5%
- 性能下降 > 20%
- 测试执行时间 > 2倍基线

## 🎓 最佳实践总结

### 测试设计原则
1. **独立性**: 每个测试独立，不依赖其他测试
2. **确定性**: 测试结果可重现
3. **快速性**: 测试执行时间合理
4. **清晰性**: 测试意图明确，易于理解

### 代码质量
1. **命名规范**: 使用描述性的测试方法名
2. **文档完善**: 每个测试都有清晰的注释
3. **异常处理**: 妥善处理测试中的异常
4. **资源管理**: 正确释放测试资源

### 维护策略
1. **定期更新**: 随功能更新同步测试
2. **性能监控**: 持续监控测试性能指标
3. **覆盖率分析**: 定期分析测试覆盖率
4. **技术债务**: 及时重构老旧测试代码

---

*本文档持续更新，如有问题或建议，请提交Issue或Pull Request。*