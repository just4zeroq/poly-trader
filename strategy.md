# Strategy Documentation

## Current Strategy: Simple Maker (Phase 1)

### Core Logic

独立 Up/Down 做市，每 tick 分别决定是否买入 Up 和 Down。成交后自动配对，不做原子捆绑。

```
每 tick:
  1. 剩余时间 < min_remaining_time → 跳过
  2. 每边 exposure（已成交 + pending）>= max_per_side → 跳过该 tick
  3. 价格由 engine 的 _resolve_pair_pairs 验证：
     - 数据新鲜（WS < 30s）
     - up_price + down_price < max_pair_sum (默认 0.998)
     - up_price + down_price ≈ 1.0（偏差 < max_price_dev，默认 0.20）
  4. Up 和 Down 各下 per_tick（默认 5）张，受 max_per_side 限制
```

### 配对逻辑

`executors.py` 的 `_create_lot_and_pair()`：
- Up 成交 → 创建 Lot，遍历现有 lots 找未配对的 Down Lot → 配对
- Down 成交 → 创建 Lot，遍历现有 lots 找未配对的 Up Lot → 配对

配对通过 `Lot.paired_qty` 追踪，不修改原始订单。

### 风险控制

| 机制 | 说明 |
|------|------|
| `max_pending_orders` (20 shares) | 所有 pending 订单未成交 shares 数之和上限 |
| `max_per_side` (20) | 每边总 exposure 上限（已成交 + pending 未成交）。超过后不再对该方向下单 |
| `min_remaining_time` (300s) | 窗口结束前停止开新单 |
| `_cancel_stale_pending` | 价格偏离 > cancel_replace_threshold (10%) 时撤单重挂 |
| Kill-switch | `guaranteed_pnl < -pairs × kill_pnl_per_pair` (0.03) 时停止并撤单 |
| `max_drawdown` (-10.0) | 累计 PnL 低于此值时停止所有交易 |
| `stop_on_window_loss` | 上一窗口亏损后跳过下一窗口 |

### 订单生命周期

```
strategy.decide() → Decision[]
    ↓
engine 逐一下单 (no atomic, no role routing)
    ↓
executor.place() → SDK place_limit_order()
    ↓
PendingOrder 存入 ws.pending_orders
    ↓
UserTradeEvent → _handle_trade() → 更新 inventory/cost/lot
    ↓
_create_lot_and_pair() → 自动配对
```

### 环境变量

```
POLY_PER_TICK=5              # 每 tick 每方向下单量
POLY_MAX_PER_SIDE=20         # 单方向持仓上限 + 总 pending shares 上限
POLY_AGGRESSIVENESS=0.2      # 做市激进程度 0-1
POLY_MAX_PAIR_SUM=1.0        # 对价上限（超过不买）
POLY_CANCEL_MIN_AGE=120      # 撤单前最小等待时间
POLY_MIN_REMAINING_TIME=300  # 窗口结束前停止开单
POLY_CANCEL_REPLACE_THRESHOLD=0.40  # 撤单价格偏离阈值
POLY_MIN_ORDER_SIZE=5        # 最小下单量
POLY_KILL_PNL_PER_PAIR=0.03  # 每对亏损阈值
POLY_MAX_DRAWDOWN=-5.0       # 累计 PnL 止损
POLY_STOP_ON_WINDOW_LOSS=True  # 亏损后跳过下一窗口
```

### 环境变量说明

| 变量 | 当前 .env 值 | 说明 |
|------|-------------|------|
| `POLY_PER_TICK` | 5 | 每 tick 每方向下单张数 |
| `POLY_MAX_PER_SIDE` | 20 | 每方向 exposure（已成交 + pending）上限 |
| `POLY_AGGRESSIVENESS` | 0.2 | 做市价 = bid + spread × aggr，越高越激进 |
| `POLY_CANCEL_MIN_AGE` | 120s | 挂单至少等 120s 后才考虑撤单重挂 |
| `POLY_CANCEL_REPLACE_THRESHOLD` | 0.40 (40%) | 价格偏离超过 40% 才撤单 |
| `POLY_MIN_REMAINING_TIME` | 300s | 窗口结束前 300s 停止开新单 |
| `POLY_MAX_PAIR_SUM` | 1.0 | 双向 maker 价之和低于此值才下单 |
| `POLY_MIN_ORDER_SIZE` | 5 | 最小下单张数 |
| `POLY_KILL_PNL_PER_PAIR` | 0.03 | 每对亏损超过 3 美分时触发熔断 |
| `POLY_MAX_DRAWDOWN` | -$5.0 | 累计 PnL 低于 -$5 时停止所有交易 |
| `POLY_STOP_ON_WINDOW_LOSS` | true | 窗口亏损后跳过下一窗口 |

---

## Industry Reference Strategies

### 1. Pure Arbitrage (Polymarket 基础套利)

**原理**：Polymarket 的 YES + NO 价格和恒等于 $1。当和偏离 $1 时，双向买入锁定无风险利润。

```
if up_price + down_price < 1.0:
    profit = 1.0 - (up_price + down_price)
    # 买入 Up + Down，到期无论谁赢都得到 $1
```

这是 Polymarket 上最基础的盈利模式。我们的策略基于此原理，区别在于我们只做 maker（被动挂单），不主动吃单。

**参考**：Polymarket 2024-2025 年数据显示超过 7,000 个市场存在此类套利机会，套利者从中提取了超过 $40M 的无风险利润。

### 2. Traditional Market Making (Spread Capture)

**原理**：在 CLOB 上同时挂买单和卖单，赚取买卖价差。不预测方向，靠提供流动性盈利。

```
bid_price = mid - spread/2    # 挂买单
ask_price = mid + spread/2    # 挂卖单
profit = ask_price - bid_price  # 价差
```

Polymarket 做市商通过挂限价单（maker）赚取价差，并可能获得平台流动性奖励。数据显示 maker side 系统性盈利，taker side 系统性亏损——部分原因是散户对低概率合约（如 $0.01）的"彩票偏好"。

**我们的差异**：当前策略只做买单（BUY），不做卖单（SELL）。这是因为二元期权的特性——到期 payout 固定 $1，卖空没有自然的止盈机制。严格来说我们的策略属于"套利做市"，而非传统做市。

### 3. Avellaneda-Stoikov (库存感知做市)

**核心公式**：

```
reservation_price = mid - inventory_skew × γ × σ² × (T - t)
half_spread = γ × σ² × (T - t) + (2/γ) × ln(1 + γ/k)
bid = reservation_price - half_spread/2
ask = reservation_price + half_spread/2
```

其中：
- `γ` = 风险厌恶系数
- `σ` = 波动率
- `T - t` = 剩余时间
- `k` = 订单到达率
- `inventory_skew` = 当前持仓偏向

当 Up 持仓过多时，reservation_price 下移，自动降低 Up 买入价、提高 Down 买入价，自然恢复平衡。

**适用性**：适用于 Phase 2。当前策略没有库存感知——固定 aggressiveness，不根据持仓偏向调整报价。加入 A-S 后可以更智能地控制双边库存。

### 4. Logit-Space Avellaneda-Stoikov (Polymarket 适配版)

**原理**：将概率 p 映射到 log-odds 空间，避免概率接近 0 或 1 时的数值问题：

```
logit(p) = ln(p / (1-p))
p = 1 / (1 + exp(-logit(p)))
```

在 logit 空间应用 Avellaneda-Stoikov 公式，再映射回概率空间得到最终报价。

**优势**：
- 概率趋近 0 或 1 时数值稳定
- logit 空间符合正态分布假设
- Jump-diffusion 模型可捕获新闻事件导致的信念突变

**参考**：Polymarket Market Making Bible 将此方法描述为"预测市场的 Black-Scholes"。

**适用性**：适用于 Phase 3。对于 BTC 15m 市场，价格通常在 0.1-0.9 范围波动，极端概率较少见，当前 price-space 方法足够。如果是选举类长期预测市场（概率可能到 0.01 或 0.99），Logit 空间优势更明显。

### 5. Order Flow Imbalance (OFI)

**原理**：监控买单和卖单的流量差，预测短期价格方向：

```
OFI = buy_volume - sell_volume
if OFI > threshold:  # 买单压倒性 → 价格可能上涨
    adjust_upward()
elif OFI < -threshold:  # 卖单压倒性 → 价格可能下跌
    adjust_downward()
```

**适用性**：对 15m 窗口偏高级了。OFI 在高频场景（秒级）更有价值，15m 窗口有足够时间响应价格变化。

### 6. Machine Learning Approaches

- **Hidden Markov Models (HMM)**：用于状态管理，处理 Polymarket（秒级延迟）和 CEX（毫秒级延迟）之间的延迟差异
- **Reinforcement Learning (SAC/PPO)**：持续优化 A-S 参数
- **Kalman Filter + EM Algorithm**：校准波动率、跳跃强度等参数

**适用性**：专业做市商（百万美金级别账户）使用的方法。对当前策略来说过度复杂。

---

## Roadmap

```
Phase 1 (Current)     →  Phase 2 (Next)        →  Phase 3 (Future)
──────────────────        ──────────────────        ──────────────────
固定 aggressiveness       库存感知报价 (A-S)         Logit 空间 A-S
无偏向                     aggressiveness 偏向调整    波动率自适应
独立下单                   独立下单不变            Jump-diffusion 模型
max_pending_orders 限制    订单到达率估计            ML 参数优化
```

### Phase 2 思路

在 `decide()` 中引入偏向调整：

```python
# 计算库存偏向
inv_up = ws.inventory["Up"]
inv_down = ws.inventory["Down"]
total = inv_up + inv_down
if total > 0:
    skew = (inv_up - inv_down) / total  # [-1, 1]
else:
    skew = 0.0

# 调整 aggressiveness
up_aggr = cfg.aggressiveness * (1 - skew)     # Up 多时更保守
down_aggr = cfg.aggressiveness * (1 + skew)    # Up 多时 Down 更激进

# 各自计算 maker 价格
up_price = maker_price(up_bid, up_ask, up_aggr)
down_price = maker_price(down_bid, down_ask, down_aggr)
```

这样当 Up 库存过多时，自动提高 Down 的买入价（更容易成交），降低 Up 的买入价（更不容易成交），自然恢复平衡。
