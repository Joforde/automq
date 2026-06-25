# rebalance-storm-bench

模拟 **consumer group rebalance 风暴** 的压测工具：单进程内启动多个消费者线程，全部订阅同一 topic 且共用同一个 `group.id`，通过频繁 `close → 新建 → subscribe → poll → close` 触发持续的整组 rebalance。

预期在 broker 监控面板上看到 `JoinGroup`、`FindCoordinator`、`ListOffsets`、`Metadata` 等请求 QPS 升高。

## 原理

同一 `group.id` 下，任意 1 个成员离开或加入，coordinator 会对**整组**触发 rebalance，其余成员也会重新 `JoinGroup`。若 topic 分区数很大（如 `big-test` 9000 分区），每次 rebalance 还会伴随大量 `ListOffsets` 和 `Metadata` 请求。

```
Worker close → LeaveGroup
  → coordinator 触发 rebalance
  → 其余 99 个成员 JoinGroup
  → 分区分配 → ListOffsets × N
Worker 重建 → FindCoordinator → JoinGroup → ...
```

## 前置条件（本地 AutoMQ）

```bash
~/ai-workspace/scripts/automq-dev.sh status
~/ai-workspace/scripts/automq-dev.sh list-topics

# 若 big-test 不存在：
~/ai-workspace/scripts/automq-dev.sh create-topic --topic big-test --partitions 9000
```

建议先从较少消费者数开始（如 20），确认 broker 稳定后再调到 100。

## 构建

```bash
cd tools/rebalance-storm-bench
mvn -q clean package
```

产物：`target/rebalance-storm-bench.jar`

## 运行

### 推荐：使用启动脚本（自动按 consumers 数分配堆内存）

```bash
chmod +x run.sh
./run.sh consumers=100 alive.ms=2000 restart.delay.ms=500
```

`run.sh` 按 `512MB + 48MB × consumers` 计算 `-Xmx`（100 个 consumer → **-Xms3g -Xmx5g**），并启用 G1、`-Xss512k`（降低 100 个 I/O 线程的栈内存占用）。

### 手动指定 JVM

100 个 consumer 订阅大分区 topic（如 `big-test` 9000 分区）时，建议至少 **4–8GB** 堆：

```bash
java -Xms4g -Xmx8g \
  -XX:+UseG1GC -XX:MaxGCPauseMillis=200 \
  -XX:+HeapDumpOnOutOfMemoryError \
  -Xss512k \
  -jar target/rebalance-storm-bench.jar \
  bootstrap=localhost:9092 \
  topic=big-test \
  consumers=100 \
  group.id=rebalance-storm-bench \
  alive.ms=2000 \
  restart.delay.ms=500
```

| consumers | 建议 -Xmx | 说明 |
|-----------|-----------|------|
| 20 | 2g | 冒烟 / 逐步加压 |
| 50 | 3g | 中等强度 |
| 100 | 5–8g | 默认压测强度；`big-test` 建议 8g |

启动时会打印推荐 JVM 参数，可与实际 `-Xmx` 对照。

环境变量覆盖（`run.sh`）：`CONSUMERS`、`BOOTSTRAP`、`TOPIC`、`GROUP_ID`、`JAVA_OPTS`。

每秒输出：

```
[   1s] restarts/s: 38.2 | rebalances/s: 1,204.5 | total restarts: 38 | total rebalances: 1,204
```

- `restarts/s`：消费者 close+重建次数（每秒）
- `rebalances/s`：`onPartitionsRevoked` 回调次数，更能反映 broker 侧 rebalance 压力

Ctrl+C 优雅停止。

## 参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `bootstrap` | `localhost:9092` | bootstrap servers |
| `topic` | `big-test` | 订阅的 topic |
| `group.id` | `rebalance-storm-bench` | 全部 worker 共用 |
| `consumers` | `100` | worker 线程数 |
| `alive.ms` | `2000` | 每次 consumer 存活多久后 close |
| `restart.delay.ms` | `500` | close 后等待多久再重建 |
| `restart.jitter.ms` | `1000` | 随机额外延迟，避免齐步重启 |
| `stagger.ms` | `50` | 各 worker 初始启动间隔 |
| `poll.ms` | `100` | poll 间隔 |
| `session.timeout.ms` | `10000` | consumer session 超时 |
| `heartbeat.interval.ms` | `3000` | 心跳间隔 |
| `max.poll.interval.ms` | `30000` | max poll 间隔 |
| `<any>=<val>` | — | 透传给 KafkaConsumer 的任意配置 |

调大风暴强度：减小 `alive.ms` 和 `restart.delay.ms`。

## 监控对照

压测时在 Grafana / JMX 观察：

- `Request Throughput`：`JoinGroup`、`FindCoordinator`、`ListOffsets`、`Metadata`
- `NetworkProcessorAvgIdlePercent`：网络线程是否被打满

## 压测后清理

```bash
# 使用本地 kafka 脚本（路径以 automq-dev 安装为准）
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --delete --group rebalance-storm-bench
```

## 注意事项

- **仅用于测试/预发环境**，不要在生产集群运行
- `big-test` 9000 分区 + 100 成员：单次 rebalance 开销极大，建议逐步放大 `consumers`
- 此工具模拟 **rebalance 风暴**，不是 AutoMQ 8-inflight pipeline 压测（那需要 Producer `max.in.flight.requests.per.connection=8`）

## 与 metadata-bench 的关系

| 工具 | 模拟场景 |
|------|----------|
| [metadata-bench](../metadata-bench/) | 多连接、周期性 metadata 刷新 |
| **rebalance-storm-bench** | 同 group 消费者频繁进出 → JoinGroup/ListOffsets/Metadata 连锁 |
