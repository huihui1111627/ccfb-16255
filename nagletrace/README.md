# NagleTrace

一个用 Go 在用户态实现的简化 TCP 传输层，专门用来**系统复现和诊断**
Nagle 小包合并与延迟确认（delayed ACK）互相等待造成的长尾延迟——也就是
生产里那种「九成请求 1ms 以内，剩下一成稳稳卡在 40ms」的现象。

整个栈跑在**虚拟时钟 + 确定性事件驱动**上，没有 goroutine 和真实时间，
因此同一个场景的报文时间线逐字节可复现，40ms 长尾可以稳定重现。

## 它实现了什么

按真实协议栈行为建模的机制（`transport/`）：

| 机制 | 真实对应 | 实现位置 |
| --- | --- | --- |
| 连接、序号、累积确认 | TCP 三次握手 / 序号空间 | `conn.go`, `receive.go` |
| 滑动窗口（接收窗口 + 拥塞窗口） | rwnd / cwnd | `send.go` `pump()` |
| Nagle 小包合并，可按连接关闭 | `TCP_NODELAY` | `send.go` `pump()` |
| 小包攒着等显式刷新 | `TCP_CORK`（含 ~200ms 自动刷新） | `send.go` |
| 延迟确认：攒一会儿或攒够两个包 | Linux 40ms 定时器 + RFC 1122 第二包规则 | `receive.go` `scheduleAck` |
| 快速确认模式 | `TCP_QUICKACK`（一次性 + 持续两种语义） | `receive.go` |
| ACK 搭车 | 有数据要发时 ACK 随数据走 | `send.go` `dataSegment` |
| RTO 重传（RFC 6298 SRTT/RTTVAR）+ 快速重传（3 重复 ACK） | 重传定时器 / dupack | `send.go`, `receive.go` |
| 乱序缓存、零窗口、应用背压 | OOO queue / zero window / pause-read | `receive.go` |

应用写法（`app/`）：分两次写头和体、`writev` 合并写、CORK 攒写、
请求/响应停等、流水线、大响应分块。

网络模拟（`netem/`）：单向延迟、抖动、按概率丢包、乱序、重复，
丢包与 Nagle/延迟确认叠加后会自然产生更怪的现象（RTO 与 40ms 定时器
交替、单包窗口无法快速重传等）。

## 诊断器怎么判断

`diagnose/` 只消费**抓包视角能看到的报文序列**（时间、方向、seq/ack、
flag、长度），识别每一段「发送方带着未确认小字节空闲、被一个到来的 ACK
解锁」的区间，再区分：

- `nagle-delayed-ack`：空闲期间传输层确实记录了 Nagle 持有（第二个小包
  已缓冲但发不出去），接收方的延迟确认定时器握着 ACK——两边互相等；
- `delayed-ack-only`：没有第二个包被持有（停等边界），只有接收方定时器；
- `ack-piggybacked`：ACK 被反向数据搭车带走；
- `rto-retransmit` / `fast-retransmit`：按重复 ACK 数量区分；
- `cork-hold` / `window-full`。

每条结论给出：等在哪个包上、等了多久、被哪个包解锁、**哪一方的哪个设置
引起的**、改哪一边。内部状态事件（`nagle-hold`、`delack-arm`）只用于归因
交叉校验，互等的存在和时长全部来自报文本身。

## 快速开始

```bash
go test ./...                 # 19 个确定性测试
go run ./cmd/nagletrace list  # 内置场景
go run ./cmd/nagletrace run classic-stall
go run ./cmd/nagletrace run mixed-workload      # 90% 快 / 10% 卡 40ms
go run ./cmd/nagletrace run fixed-nodelay       # 修复后长尾消失
go run ./cmd/nagletrace matrix                  # 写法 × 选项 × 丢包 72 组
go run ./cmd/nagletrace serve :8080             # 网页时序图
```

## 核心复现结论

`classic-stall`（默认选项 + 头/体分两次写，停等）：

```
latency: min=42ms p50=82ms max=82ms  tail>=40ms: 20 (100%)
verdict: nagle-delayed-ack (critical)
根因: client Nagle (TCP_NODELAY off) waiting on server delayed-ACK timer
```

`mixed-workload`（90% 合并写 + 内核对空闲路径的快速确认重武装）：

```
~86% 请求 1ms，~14% 稳定 41ms —— 与生产里的长尾形状一致
```

修复对比（同一负载）：

| 改动 | 最大延迟 | 诊断 |
| --- | --- | --- |
| 基线 | 82ms | nagle-delayed-ack |
| 仅客户端 `TCP_NODELAY` | 41ms | 长尾转移到响应方向（服务器 Nagle） |
| 两端 `TCP_NODELAY` | 1ms | application-delay，无互等 |
| 服务器持续 `TCP_QUICKACK` | 2ms | application-delay，无互等 |
| `writev` 合并写 | 41ms | 头/体互等消失，停等边界仍受延迟确认约束 |

工具刻意保留了「只修一边长尾转移」和「writev 不能消除停等边界延迟确认」
这两个真实协议栈才有的细节，而不是把任何修复都渲染成万能药。

## 目录结构

```
clock/       虚拟时钟与定时器堆
netem/       有损链路（延迟/丢包/乱序/重复）
transport/   用户态 TCP-like 传输层
app/         应用负载模型与文本协议
diagnose/    报文时间线诊断器
experiment/  场景预设与组合矩阵
cmd/nagletrace/  CLI 与 Web 服务（内嵌静态页）
```
