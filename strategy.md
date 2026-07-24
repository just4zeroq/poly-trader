# Temporal Arbitrage Strategy — 时间累计套利

> **核心哲学**：放弃方向性预测。通过时间窗口内持续双向买入，利用比例分配压低持仓平均成本，使 Up + Down 的平均成本对 < $1，在结算时无论哪个方向胜出都获得无风险利润。

---

## 一、 策略原理

Polymarket 二元期权（Up/Down）的结算规则：胜方 $1，负方 $0。

**无风险套利条件**：AvgCost(Up) + AvgCost(Down) < $1.00

若持有多份合约，结算收益 = 胜方数量 × $1 - 总成本。只要成本对 < $1，即使两边都持有大量合约，在结算时也能获得正收益。

```
收益 = 胜方数量 × 1.0 - (Up总成本 + Down总成本)
      = Up数量 × 1.0 - (Up总成本 + Down总成本)    [若Up胜]
      = Down数量 × 1.0 - (Up总成本 + Down总成本)  [若Down胜]
```

核心挑战不是预测方向，而是**用时间换成本**——在 15 分钟窗口内持续以 maker 价挂单，利用价格波动让平均成本逐步收敛到 $1 以下。

---

## 二、 全栈数据源架构

| 数据 | 来源 | 方式 | 频率 |
|------|------|------|------|
| 订单簿 (BestBidAsk) | Polymarket CLOB WS | 实时推送 | 毫秒级 |
| 成交数据 | Polymarket CLOB WS | 实时推送 | 毫秒级 |
| 用户订单/成交 | Polymarket User WS | 实时推送 (仅实盘) | 毫秒级 |
| 市场发现 | Gamma API | REST | 窗口开始时 |
| 余额查询 | CLOB REST API | REST | 启动时 / 按需 |

**关键：maker 订单费率为 0%。** taker 费率 7% 将完全吞噬套利利润，因此策略必须使用 post-only 限价单。

---

## 三、 订单定价模型

maker 价格 = best_bid + spread × aggressiveness

其中 aggressiveness (默认 0.2) 控制挂单的激进程度：
- `0.0` = 挂在 bid 上（最低成交概率）
- `1.0` = 挂在 ask 上（最高成交概率，但等同于 taker）

默认值 0.2 提供适中的成交概率，同时确保价格优于中间价。

---

## 四、 三层分配模型

每 tick 按以下三层逻辑决定 Up/Down 各买多少：

### Layer 1 — 基础比例分配

```
up_share   = 1.0 - up_price / (up_price + down_price)
down_share = 1.0 - down_price / (up_price + down_price)
```

便宜的一方获得更多份额。例如 Up=$0.48, Down=$0.52 → up_share=0.52, down_share=0.48。

### Layer 2 — 成本改善奖金

```
若 up_price < avg_cost_up:
    bonus = min((avg_cost_up - up_price) / up_price, 2.0)
    up_share *= (1.0 + bonus)
```

当当前价格低于持仓平均成本时，增加该侧的买入份额，主动拉低平均成本。

### Layer 3 — 归一化与上下限

- 两边各至少 1 张合约
- 总和不超过 per_tick
- 不超过 max_per_side 持仓上限
- 每边最多一个挂单（有 pending order 则跳过该侧）

---

## 五、 风控模型

### 1. 核心风控参数

| 参数 | 默认值 | 作用 |
|------|--------|------|
| `max_pair_cost` | 0.9999 | 成本对超过此值 → 停止加仓，保护已有利润 |
| `min_pair_cost_fills` | 2 | 至少 N 次成交后才启用 max_pair_cost 检查（防止早期误杀） |
| `max_per_side` | 20 | 单边最大持仓 |
| `max_spread` | 0.05 | spread > 5% → 跳过该 tick（流动性不足） |
| `max_drawdown` | -5.0 | 会话回撤超过此值 → 熔断 |
| `stop_on_window_loss` | true | 窗口亏损 → 跳过下一窗口 |

### 2. 时间保护

| 参数 | 默认值 | 作用 |
|------|--------|------|
| `min_remaining_time` | 300s | 窗口剩余 < 5min → 停止新挂单，防止单边成交无法配对 |
| `cancel_min_age` | 30s | 挂单至少存活 30s 才允许撤单重挂（防止频繁撤挂） |

### 3. 价格保护

| 检查 | 条件 | 动作 |
|------|------|------|
| Spread 过滤 | spread > max_spread | 跳过 tick |
| 价格偏差 | |price - last_price| / last_price > max_price_dev | 跳过 tick |
| 成本对检查 | pair_cost > max_pair_cost 且 fills ≥ min_pair_cost_fills | 取消所有挂单，停止加仓 |
| 订单簿质量 | bid/ask 均为正且 ask > bid | 必须满足 |

---

## 六、 熔断机制

### 触发条件（任一即触发）

```
① 会话回撤 > max_drawdown (-5.0 USDC)
② 窗口亏损 (stop_on_window_loss=true)
```

### 处理
- 取消所有挂单
- 剩余窗口时间仅等待结算，不再开新仓
- 若 stop_on_window_loss：跳过下一个窗口

---

## 七、 窗口生命周期

```
┌────────── 窗口开始 ──────────┐
│  市场发现 (Gamma API)         │
│  订阅 WS 订单簿               │
│         ↓                     │
│  ┌── tick loop ──┐            │
│  │ 获取买卖价     │            │
│  │ 检查风控过滤   │            │
│  │ 策略决策       │            │
│  │ 挂 maker 单    │            │
│  │ cancel-replace │ (可选)     │
│  │ 等待成交       │            │
│  └────────────────┘            │
│         ↓                     │
│  min_remaining_time 到达      │
│  → 停止新挂单                  │
│         ↓                     │
│  窗口结束 → 结算               │
│  取消所有挂单                  │
│  根据 WS 中间价判断胜方        │
│  计算 PnL                      │
└────────────────────────────────┘
```

---

## 八、 Cancel-Replace 机制

挂单超过一定时间未成交 → 撤单并以新的 maker 价格重新挂单。

前提条件：
- 挂单存活 ≥ cancel_min_age (30s)
- 新价格偏离原价 ≥ cancel_replace_threshold (10%)
- 窗口剩余时间充足
- 成本对未超限

---

## 九、 纪律与忠告

1. **maker 费率是生命线**：taker 费率 7% × 双边 = 14% 摩擦成本，远超薄利空间。永远使用 post-only 限价单。
2. **时间是朋友，单边是敌人**：策略靠双边累计成本收敛获利，单边持仓无法配对 = 风险敞口。min_remaining_time 是最后防线。
3. **成本对是最重要的指标**：所有操作围绕 pair_cost < $1 展开。风控参数宁可偏保守。
4. **低杠杆低风险**：每 tick $10 级别的投入，max_per_side=20 限制总敞口。这不是暴富策略，是稳定套利。
5. **熔断不是失败，是保护**：连续亏损说明市场条件不适合，应暂停而非加码。
