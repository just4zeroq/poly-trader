# Strategy Documentation

## Current Strategy: V4 Predictive Favorite → Hedge → Flat

### Core Logic

基于 BTC 已走路径的 P_fair 预测模型（poly_predict），每窗口循环：
**下「胜率大」的一边（favorite，maker 价）→ 成交后补另一边（补平）→ 持平后新一轮**。

门控只用 `ws.auth_inv`（权威持仓）：WS fill 乐观写穿 + 后台每 2s CLOB 轮询校正。

```
每 tick（V4Strategy.decide）:
  1. elapsed < pred_start_elapsed (60s) → []      # 窗口开始 60s 内任何腿都不下单
  2. 绑定对冲已就绪（favorite 成交 ≥ 4）→ **失衡 ≤ min_order_size + 1 时**在 ≤ 上界处对冲指定先行腿
       - HedgePlan 在 favorite 下单成功时记录：对侧、整单量、max_price = hedge_price_bound − fav_price
       - favorite filled ≥ 4 才触发（round 不 truncate，4.992 算数）
       - light maker 价 > bound → 等待重试，不追价
       - **失衡 > min_order_size + 1（残量并入、跨多轮均价）→ 弃计划，走通用对冲（混均守卫）**
       - 不受最后 3 分钟限制，监控到窗口结束
  3. auth_inv[Up] != auth_inv[Down]（无绑定计划时）→ 补平 light 边
       - 有绑定计划但 favorite 未成交 ≥ 4 时，通用补腿不碰部分成交
       - **失衡 ≤ min_order_size + 1 → 绑定式合成对冲**：max_price = hedge_price_bound − avg_cost_heavy
         （以均成本为锚；重启/计划已消费时等价于计划上界）
       - **失衡 > min_order_size + 1 → 通用对冲**，成本守卫:
         avg_cost_heavy + 补腿后 light 混均 ≤ pair_cost_target_extreme
       - **|imbalance| < min_order_size 不补腿**：子最小量失衡不下单，落回 favorite 逻辑，
         残量并入下一轮（累计后由一次 ≥ min 的对冲锁平）
       - 补腿量 = |imbalance|，受 max_per_side 剩余 room 封顶
       - 不受最后 3 分钟限制，监控到窗口结束
  4. remaining < min_remaining_time (180s) → []  # 最后 3 分钟不下新 favorite
  5. auth_inv 持平 + 有活跃 pending → []           # 防堆叠（顺带撤过期的死 favorite）
  6. auth_inv 持平 + 模型够自信 → Decision(fav, min_order_size, maker价)
       - 反堆叠: 已有待对冲的先行腿（活跃未消费计划）→ 不再下 favorite
       - |P_fair − 0.5| ≥ pred_conf_threshold   # 模型只定方向，不干预价格
       - 暴露守卫: auth_inv[side] >= max_per_side → []
       - 价格上限: price > max_extreme_price (0.90) → []   # 不追高价
       - 不做前向 pair-cost（pair-cost 全在对冲腿检验）
```

对冲腿与指定先行腿订单绑定（`HedgePlan`）：
- **何时记录**：favorite 订单**下单成功时**（`executor.place`）才记录计划——对侧、`max_price = hedge_price_bound − fav_price`（默认 0.998 − fav_price），并绑定该 favorite 的 order_id。**决策时刻不记录**：计划永不先于真实订单存在，SDK 下单失败则无计划。
- **何时触发**：只有该 favorite 单成交 **≥ 4**（round，非 truncate——4.992 计为 5）才触发，防止部分成交过早对冲。
- **路由**：失衡 ≤ min_order_size + 1 → 绑定对冲；> min_order_size + 1（残量并入、跨多轮均价）→ 弃计划，
  走通用混均守卫——此时计划上界锚定的 fav_price 已不是 heavy 边真实均价。
- **下多少**：**实时失衡** `auth_inv[heavy] − auth_inv[light]`（非计划的整单量）——子最小残量并入下一轮后会在 heavy 边累积，只有当前失衡才能一次锁平；普通 favorite 即等于计划量。受 max_per_side room 封顶。
- **以什么价**：触发时取 light 边当前 maker 价；若 `> max_price` 则等待（返回 [] 每 tick 重试），不追价。
- **消费一次**：触发后 `plan.placed = True`，不会重复对冲同一 favorite；在途对冲单通过 pending 检查挡住二次下单。
- **计划失效**：favorite 被撤单/掉单 → 放弃计划，窗口回到正常循环，不卡死。
- 主循环不再有 1s 节流（`min_tick_interval = 0`），触发条件即时响应。

### 定价

maker 价 = `best_bid + spread × aggressiveness`，按 tick size 取整
（`engine._maker_price` → `_resolve_pair_prices`）。每条腿在各自订单簿独立定价，无锚定/对价推导。

**favorite 价格 = 纯 maker 价**：模型 P_fair 只决定方向，不干预价格。favorite 直接以
`book_maker` 挂单（post_only），尽快成交。唯一价格上限是 `max_extreme_price`（0.90）——临近结算的
favorite 补腿几乎必然过不了成本守卫，本质是裸仓赌单，故封顶不追。

**pair-cost 只在对冲腿检验**：先行腿不做前向配对守卫。对冲路径按失衡量路由
（`|imbalance| ≤ min_order_size + 1` → 绑定式；`> min_order_size + 1` → 通用混均）：

- **绑定对冲**（favorite 成交 ≥ 4 且失衡 ≤ min_order_size + 1）：价格上限在 favorite 下单时就定死——`max_price = hedge_price_bound − fav_price`
  （默认 0.998 − fav_price），提前锁定不追价；失衡 > min_order_size + 1 时弃计划走通用守卫。
- **合成绑定对冲**（无绑定计划 + 单单位失衡 ≤ min_order_size + 1）：重启恢复丢计划 / 计划已消费时，
  `max_price = hedge_price_bound − avg_cost_heavy` 以均成本为锚——单笔 favorite 即等价于下单时上界，
  复用同一绑定路径；无成本基准则退化为通用混均守卫（更紧，不会更松）。
- **通用补平**（窗口中途恢复持仓 / 计划已消费且失衡 > min_order_size + 1）：用补腿后的**混均**成本
  `avg_cost_heavy + (light_cost + 补腿量×price)/(补腿量 + 轻边已有量) ≤ pair_cost_target_extreme`（0.99）。
  轻边已有持仓会摊低/摊高真实对子成本，守卫按混均判断——轻边为空时即退化为 `avg_cost_heavy + price`。
  **|imbalance| < min_order_size 时不下单**（子最小量，残量并入下一轮），保证通用补腿量恒 ≥ min_order_size。

补腿过贵 → 保持裸仓，每 tick 重试（成本守卫日志可见），light 边变便宜后自动锁仓。

### 下单总量约束

| 约束 | 值 | 作用 |
|------|-----|------|
| `min_order_size` (5) | 每轮 favorite 下单张数 | 单次开仓量 |
| `max_per_side` (100) | 每边已成交持仓上限 | 补腿量封顶 + favorite 暴露守卫 |
| 补腿 room 封顶 | `max_per_side − auth_inv[light]` | 超仓时补腿不超 room |
| `pred_conf_threshold` (0.05) | 最小 |P_fair−0.5| | 不自信不下单 |
| 防堆叠 | auth_inv 持平时有活跃 pending → 不下单 | 同一时间至多 1 个在途单 |

### 对账

- WS fill 写穿 `ws.auth_inv`（乐观）
- `_position_loop` 每 `positions_interval` (2s) 轮询 CLOB positions，**单调向上合并**：
  `auth_inv` / `inventory` 只向 CLOB 数字上移，**永不抹掉已记录持仓**（data-api 最终一致，
  过期快照可能返回更小的值；抹掉会导致误判空仓 → 反向再下一腿，配对成本 > $1）
- CLOB 显示多于 WS 记录（丢 fill 事件）时，差额**吸收进该边活跃 pending 单**（按 limit 价记成本），
  inventory / auth_inv / pending 三者收敛 —— 幽灵 pending 不再卡死 anti-stack，日志与页面一致
- **重启恢复**（无对冲计划重建）：`load_current_state` 从 CLOB 恢复持仓 + 均价 + 在途单（只留未满单，
  满单/已撤单丢弃），`auth_inv` = 持仓。HedgePlan 只存内存，重启即丢失——不做重建，按失衡量路由：
  单单位失衡（≤ min_order_size + 1）走**合成绑定对冲**（`max_price = hedge_price_bound − avg_cost_heavy`，
  均成本即单笔 favorite 的实际成本，与计划上界等价）；累计失衡（> min_order_size + 1）走**通用对冲**
  （混均成本守卫，比绑定上界更紧 ~0.008）。第一个 tick 即响应，与不重启时窗口中途的恢复路径一致。
  满单 favorite（imbalance = 全量）一次补平，部分成交的 favorite 分步补，终点同样双边锁平。

### 结算

**主流程不做任何结算/PnL/赎回**——只撤单 + 发 WindowEnd。结算完全由独立脚本 `tools/onchain/settle_window.py` 承担，在**下一窗口开始时**运行：

```
python3 poly_trader/tools/onchain/settle_window.py [--slug …] [--interval 5] [--max-attempts 0]
```

1. 由市场时间表推导上一窗口 slug（`{market_slug}-{prev_ts}`），或 `--slug` 覆盖
2. 每 `--interval`（默认 5s）轮询一次 `get_resolved_winner(slug)` 查 Gamma 获胜方
3. 未解决 → 继续轮询（`--max-attempts` 设上限则超限退出，默认一直轮询）
4. 已解决 → `redeem_positions(condition_id)` 赎回赢方持仓（合并互补对）
5. 无 secure client 时只完成解析、跳过赎回；市场已过期则提示链上手动处理

这样主交易循环零链上副作用，结算时机独立可控。

### 订单生命周期

```
strategy.decide() → Decision[]
    ↓
engine._place_decisions() → 逐单 _place_order()
    ↓
executor.place() → SDK place_limit_order() → PendingOrder 入 ws.pending_orders
    ↓
UserTradeEvent → _handle_trade() → 更新 ws.inventory/auth_inv/cost，满单删 pending
    ↓
_position_loop() 每 2s 以 CLOB positions 校正 auth_inv
```

### 环境变量（实际使用）

| 变量 | 默认 | 说明 |
|------|------|------|
| `POLY_MAX_PER_SIDE` | 20 | 每边已成交持仓上限 |
| `POLY_AGGRESSIVENESS` | 0.3 | 做市价 = bid + spread × aggr，越高越激进 |
| `POLY_MIN_ORDER_SIZE` | 5 | favorite 单次下单张数 |
| `POLY_MAX_EXTREME_PRICE` | 0.90 | 单边 best_bid 超过此值视为已settled，跳过 tick；也是 favorite 价格上限（> 此值不追）|
| `POLY_MAX_CONSECUTIVE_FAILURES` | 15 | 连续全拒 tick 数达到后停手（余额耗尽） |
| `POLY_MIN_REMAINING_TIME` | 180s | 窗口结束前停止开新单 |
| `POLY_PAIR_COST_TARGET_EXTREME` | 0.99 | 补腿成本守卫：heavy 均成本 + 补腿后 light 混均超过则跳过 |
| `POLY_PRED_CONF_THRESHOLD` | 0.05 | 最小 |P_fair−0.5| |
| `POLY_PRED_START_ELAPSED` | 60s | 窗口开始多久后可下 favorite |
| `POLY_PRED_BTC_MAX_AGE` | 8s | BTC 缓存价超过此年龄则跳过预测决策 |
| `POLY_FAVORITE_STALE_SECONDS` | 25s | 未成交 favorite 停留超过此秒数 → 撤单重报（仅当新 placement 明显更优：换边 / 重报价 +0.005）；防止一单死单占住整个窗口 |
| `POLY_HEDGE_PRICE_BOUND` | 0.998 | 绑定对冲价格上界：`max_price = hedge_price_bound − fav_price`，favorite 下单时定死 |
| `POLY_POSITIONS_INTERVAL` | 2s | 后台 CLOB 持仓轮询间隔（0 关闭） |
| `POLY_WS_RECONNECT_DELAY` | 3s | WS 重连延迟 |
| `POLY_MIN_TICK_INTERVAL` | 0s | 最小 tick 间隔（0 = 不节流，绑定对冲触发即响应） |
| `POLY_MARKET` | btc-updown-15m | 交易市场 |

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
P_fair 预测 favorite        aggressiveness 偏向调整    波动率自适应
favorite→hedge→flat        独立下单不变            Jump-diffusion 模型
防堆叠（至多 1 在途单）      订单到达率估计            ML 参数优化
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
