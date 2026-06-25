# metadata-bench

一个可控速率的 **Kafka / AutoMQ Metadata 请求压测小工具**，用来在监控面板上稳定打出
指定的 `Metadata req/s`（例如 ~5000/s）。

## 原理

每次 AdminClient 调用在网络上对应**恰好一个 Metadata 请求**：

| mode        | 调用                 | 说明 |
|-------------|----------------------|------|
| `cluster`   | `describeCluster()`  | 最轻：broker 只返回节点信息（默认，最容易把 QPS 打稳） |
| `alltopics` | `listTopics()`       | 重：broker 返回**全部 topic** 的元数据（最接近真实“元数据刷新”压力） |
| `topics`    | `describeTopics(..)` | 返回指定 topic 列表的元数据 |

由单个 pacer 线程按纳秒间隔**匀速发送**，用 in-flight 信号量做背压，
所以实际速率不会超过目标值；broker 跟不上时会自动降速而不是雪崩。

## 构建

```bash
cd tools/metadata-bench
mvn -q clean package
```

产物：`target/metadata-bench.jar`（fat-jar，含 kafka-clients）。

## 运行

打出 ~5000 req/s，持续 60 秒：

```bash
java -jar target/metadata-bench.jar \
  bootstrap=localhost:9092 \
  rate=5000 \
  mode=cluster \
  duration=60
java -jar target/metadata-bench.jar \
  bootstrap=localhost:9092 \
  rate=5000 \
  mode=topics \
  topics=big-test \
  duration=60
```

每秒输出一行实际速率：

```
[   1s] metadata req/s: ok=5,001 err=0 | inflight=3 | total ok=5,001 err=0
[   2s] metadata req/s: ok=4,998 err=0 | inflight=2 | total ok=9,999 err=0
```

## 常用参数

```
bootstrap=host:9092     bootstrap servers（默认 localhost:9092）
rate=5000               目标 Metadata req/s
mode=cluster            cluster | alltopics | topics
topics=t1,t2            mode=topics 时必填
clients=2               AdminClient 实例数（速率打不上去时调大）
inflight=2000           最大并发在途请求数
duration=0              运行秒数（0 = 一直跑，Ctrl+C 停止）
timeout=10000           请求超时 ms
<any>=<val>             透传给 AdminClient 的任意配置
```

## 带鉴权的集群

任何未识别的 `key=value` 都会透传给 AdminClient，例如 SASL/SSL：

```bash
java -jar target/metadata-bench.jar \
  bootstrap=broker:9093 \
  rate=5000 \
  security.protocol=SASL_SSL \
  sasl.mechanism=PLAIN \
  'sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="u" password="p";'
```

## 调优建议

- 打不到目标速率：调大 `clients`（如 4）和/或 `inflight`（如 5000）。
- 想更贴近真实客户端刷新压力（broker 端更重）：用 `mode=alltopics`。
- 想看 broker 极限：把 `rate` 设得很大（如 100000），观察 `err` 是否上升、`inflight` 是否打满。

> ⚠️ 这是压测工具，请只在测试/预发环境对你自己的集群使用。
