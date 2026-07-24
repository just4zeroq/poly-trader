# Cheap-Side-Only Strategy — 便宜腿单向做市 + 失衡熔断

> **核心哲学**：不猜方向，只买便宜的那一边。震荡行情便宜边交替出现 → 自然凑成对，赚 `1 − (成本_up + 成本_down)`；趋势行情便宜边始终在一侧 → 失衡到达 K 就停，不再追。

**策略本质**：做空 15 分钟窗口内的已实现波动率 / 赌均值回归。在接近 50/50 的高效二元市场不存在免费的"时间套利"——maker 单的成交背后永远是逆向选择。

---

## 一、 为什么旧策略有根本缺陷

### 旧策略（比例分配 + pair_cost）

```
每 tick 两边都买，便宜边多买
目标：pair_cost = avg_up + avg_down < $1
```

**问题 1**：便宜边买更多 = 主动往正在变差的方向加码。趋势行情（BTC 单边走）下这是加速亏损。

**问题 2**：`pair_cost < $1` 作为盈利条件需要两边等量。`N_up ≠ N_down` 时这个条件不再成立。真正的无风险条件：

```
guaranteed_pnl = min(N_up, N_down) − total_spent > 0
```

**问题 3**：PaperExecutor 用 random + 时长填单，完全绕过 maker 单的逆向选择本质。回测 +$6,477 是幻觉——maker 单总是在"那一边正在变差"时成交。

### 逆向选择的微观结构

```
Up + Down ≈ $1 始终成立（买要付 ask，合计 ask > 1）

震荡行情（price 在 0.50 附近摆）:
  Up 0.50→0.47（此时 Down 0.53），Down 单 0.47 成交 ✓
  Down 0.50→0.47（此时 Up 0.53），Up 单 0.47 成交 ✓
  → 两边都以低价拿货，每对成本 0.94 ✓

趋势行情（BTC 线性拉升，Up→1.0）:
  Up 0.50→1.0，远离你的 0.47 买单，永远不成交 ✗
  Down 0.50→0.0，穿过 0.47 一路到底，买单成交 ✗
  → 只囤了必输的一边 ✗✗
```

---

## 二、 新策略：便宜腿单向 + 失衡熔断

### 每 tick 决策

```
1. 判断便宜边: up_price < down_price → 买 Up，反之买 Down
2. 检查失衡: imbalance = |N_up − N_down|
3. 若便宜边 == 超重边 且 imbalance ≥ K:
     → 趋势模式，只在轻的那边挂单（或跳过）
   否则:
     → 正常模式，在便宜边挂单
```

### 以「对」为记账单位

| 指标 | 公式 | 含义 |
|------|------|------|
| `guaranteed_pairs` | min(N_up, N_down) | 已完成的对数 |
| `imbalance` | |N_up − N_down| | 未配对的单边敞口 |
| `guaranteed_pnl` | guaranteed_pairs − total_spent | 已完成对的锁定利润 |
| `realized_pnl` | (胜方数量) − total_spent | 结算时的实际 PnL |

**`realized_pnl = guaranteed_pnl + (imbalance 若胜方正好是超重边, 否则 0)`**

`guaranteed_pnl` 是对冲后的无风险部分，`imbalance` 部分是方向性赌注。

### 盈亏机制

```
震荡行情（便宜边交替）:
  tick 1: Down 便宜 @ $0.47 → 买 Down 10 张  [inv: 0U, 10D]
  tick 2: Up 便宜 @ $0.47 → 买 Up 10 张     [inv: 10U, 10D]
  → 凑成 10 对，每对成本 $0.94
  → guaranteed_pnl = 10 − $9.40 = +$0.60 ✓

趋势行情（BTC 单边拉，Down 持续便宜）:
  tick 1: Down 便宜 @ $0.48 → 买 Down 10 张  [inv: 0U, 10D, imb=10]
  tick 2: Down 便宜 @ $0.40 → imb=10, cheap=Down, overweight=Down
           → 失衡到 K，停止！不再买 Down
  tick 3: Up 便宜 @ $0.42 → 买 Up 10 张       [inv: 10U, 10D, imb=0]
  ...
  → 最坏情况：BTC 全程单边，imbalance 一直 = K
  → 最多亏 K 张 × 单边成本  ≈ K × $0.50 = K/2 美元
```

---

## 三、 风控参数

| 参数 | 默认值 | 作用 |
|------|--------|------|
| `max_imbalance` (K) | 10 | 硬上限 — 单边敞口超过此值停止加该侧 |
| `max_per_side` | 20 | 单边最大持仓 |
| `max_drawdown` | -5.0 | 会话回撤熔断 |
| `stop_on_window_loss` | true | 窗口亏损 → 跳过下一窗口 |
| `min_remaining_time` | 300s | 窗口剩余 < 5min → 停止新挂单 |

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

## 四、 订单定价模型

maker 价格 = best_bid + spread × aggressiveness

默认值 0.2，确保价格优于中间价的同时有合理的成交概率。

---

## 五、 窗口生命周期

```
┌────────── 窗口开始 ──────────┐
│  市场发现 (Gamma API)         │
│  订阅 WS 订单簿               │
│         ↓                     │
│  ┌── tick loop ──┐            │
│  │ 获取买卖价     │            │
│  │ 判断便宜边     │            │
│  │ 检查 imbalance  │            │
│  │ 挂 maker 单    │(只挂一边)  │
│  │ cancel-replace │ (可选)     │
│  └────────────────┘            │
│         ↓                     │
│  min_remaining_time 到达      │
│  → 停止新挂单                  │
│         ↓                     │
│  窗口结束 → 结算               │
│  取消所有挂单                  │
│  Gamma API 查询胜方           │
│  计算 PnL                      │
└────────────────────────────────┘
```

---

## 六、 纪律与忠告

1. **以「对」为记账单位**：`guaranteed_pnl = pairs − total_spent` 才是锁定的利润。单边成交不是进度，是敞口。
2. **maker 费率是生死线**：taker 费率下任何需要吃单的行为都直接死，只有纯 maker 成交才有活路。
3. **趋势行情不可战胜**：K 是硬上限，到了就认。不要幻想"再买一点就能摊平"。
4. **PaperExecutor 不可信**：random + 时长填单绕过了逆向选择。真正验证需要录制真实盘口数据离线回放。
5. **没有免费的套利**：在接近 50/50 的高效二元市场，唯一真正无风险的只有 ask_up + ask_down < 1.0 的瞬时错价吃单——需要速度、运气、taker 费率。

---

## 七、 进化路线图

```
Phase 1: 便宜腿单向 + 失衡熔断            ← 当前 Phase ✅
         ↓
Phase 2: + complete-the-pair（成交后主动补腿）
         ↓
Phase 3: + 真实盘口回放验证 + 行情分类统计
         ↓
Phase 4: + 盘口错价监控 (ask_sum < 1.0)
```

每个 Phase 在上一 Phase 基础上叠加，Phase 1 的 imbalance 熔断是基础。
