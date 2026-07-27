# 双角色下单系统设计

> 版本: Phase 2 — 便宜腿单向 + 配对者常驻

## 一、核心哲学

**不猜方向，只买便宜的那一边。** 震荡行情便宜边交替 → 自然凑成对，赚取 `1 − (cost_up + cost_down)`。趋势行情便宜边始终在一侧 → 配对者尝试关敞口 → 关不上则捡便宜者在 K 暂停，保护下行。

本策略做空已实现波动率 / 赌均值回归。在接近 50/50 的高效二元市场，maker 单成交的背后永远是逆向选择——没有免费的套利，只有可控的风险敞口。

---

## 二、双角色架构

每 tick 两个角色并行决策，有冲突时**配对者优先**：

| 角色 | 职责 | aggressiveness | 价格公式 | 约束 |
|------|------|----------------|----------|------|
| **捡便宜者** | 买当前便宜边，赚取低成本的单边 | 0.2 | `bid + spread × 0.2` | `imbalance < K`、`min_remaining_time`、`max_per_side` |
| **配对者** | 用"成本最低的未配对 lot"去配对，锁定利润 | 0.5 | `min(bid + spread × 0.5, 0.9999 − lot.cost)` | `lot.cost + maker_price < 0.9999`、`max_per_side`、不受时间/窗口剩余约束 |

### 配对者价格公式

```
raw_price = best_bid + spread × 0.5
cap = 0.9999 − selected_lot.cost
maker_price = min(raw_price, cap)

条件: maker_price 必须在 [best_bid, best_ask] 区间内，且 lot.cost + maker_price < 0.9999
不满足 → 不挂单，等下一 tick
```

---

## 三、Lot 记账系统

每个 fill 创建一个独立的 `Lot` 记录，取代简单 `inventory` 聚合：

```python
@dataclass
class Lot:
    lot_id: str       # 唯一标识
    side: str         # "Up" / "Down"
    amount: int       # 原始成交数量
    price: float      # 成交价
    paired_qty: int   # 已配对数量 (0 = 未配对)
    created_at: float # 成交时间戳

    @property
    def unpaired_qty(self) -> int:
        return self.amount - self.paired_qty
```

### Lot 创建

- 每次 fill 创建一个 lot（不合并）
- 创建在 executor 的 fill 处理逻辑中
- 存储在 `WindowState.lots: list[Lot]`

### 配对者选择 lot

1. 从所有 `unpaired_qty > 0` 的 lot 中筛选
2. 按 **成本从低到高** 排序
3. 遍历找到第一个满足 `lot.cost + maker_price < 0.9999` 的
4. lot 成本最低 → cap 最高 → 配对条件最容易满足 → 快速锁定利润

### 配对成交后

```
配对订单成交:
  → PendingOrder.pairing_lot_id → 找到对应 lot
  → lot.paired_qty += fill_size
```

---

## 四、per-tick 决策流

```
tick loop:
  ┌─ 获取 Up/Down 的 best_bid / best_ask ─┐
  │
  ├─ 1. 配对者 ────────────────────────────
  │    a. 遍历 unpaired lots (成本升序)
  │    b. 对每个 lot，计算配对 maker_price
  │       raw = opposite_side.best_bid + spread × 0.5
  │       cap = 0.9999 − lot.cost
  │       price = min(raw, cap)
  │    c. 条件: price 在 [bid, ask] 内 且 lot.cost + price < 0.9999
  │    d. 找到第一个满足的 → 生成 Decision(role="pairing", lot_id=lot.id)
  │       不满足 → 配对者本轮不下单
  │
  ├─ 2. 捡便宜者 ──────────────────────────
  │    a. cheap = Up if up_price < down_price else Down
  │    b. 如果 imbalance >= K AND cheap == overweight:
  │       → 捡便宜者本轮不下单（等待配对者关敞口）
  │    c. 否则: price = cheap_side.best_bid + spread × 0.2
  │    d. 受 min_remaining_time 约束
  │    e. 生成 Decision(role="cheap", lot_id=None)
  │
  ├─ 3. 冲突解决 ──────────────────────────
  │    两个 Decision 指向同一边 → 只保留 role="pairing" 的
  │    不同边 → 都保留
  │
  ├─ 4. pending order 检查 ────────────────
  │    目标边已有 pending order → 跳过
  │    受 max_per_side 约束
  │
  └─ 5. 下单 ───────────────────────────────
       按 Decision.price 挂 post-only limit
```

### Decision 数据模型

```python
@dataclass
class Decision:
    side: str            # "Up" / "Down"
    amount: int          # per_tick
    price: float         # maker 价格（已按角色计算）
    role: str            # "cheap" / "pairing"
    lot_id: str | None   # 配对者绑定的 lot_id；捡便宜为 None
```

---

## 五、参数总览

| 参数 | 值 | 用途 |
|------|-----|------|
| `POLY_PER_TICK` | 5 | 每 tick 每角色下单量 |
| `POLY_MAX_PER_SIDE` | 20 | 单边最大持仓（含 pending） |
| `POLY_MAX_IMBALANCE` (K) | 10 | 捡便宜者停手阈值 `\|N_up − N_down\|` |
| `POLY_AGGRESSIVENESS` | 0.2 | 捡便宜 maker 价偏移 |
| `POLY_PAIRING_AGGRESSIVENESS` | 0.5 | 配对 maker 价偏移 |
| `POLY_MAX_PAIR_COST` | 0.9999 | 配对硬上限 |
| `POLY_CANCEL_MIN_AGE` | 30s | 超时取消（两角色共用） |
| `POLY_MIN_REMAINING_TIME` | 300s | 捡便宜禁挂（配对不受限） |
| `POLY_MAX_DRAWDOWN` | -5.0 | 会话回撤熔断 |
| `POLY_STOP_ON_WINDOW_LOSS` | true | 窗口亏损 → 跳过下一窗口 |

---

## 六、约束矩阵

| 约束 | 捡便宜者 | 配对者 |
|------|---------|--------|
| `max_per_side` | ✓ | ✓ |
| `min_remaining_time` | ✓ 300s 窗口尾禁挂 | 不受限 |
| `max_pair_cost` (0.9999) | 不参与 | ✓ 硬条件过滤 lot |
| `cancel_min_age` (30s) | ✓ | ✓ |
| 每边最多 1 pending | ✓ | ✓ |

---

## 七、Imbalance 与 K 的交互

```
imbalance < K:
  捡便宜: 正常运行
  配对者: 正常运行

imbalance >= K AND cheap == overweight:
  捡便宜: 停止（防止继续增大敞口）
  配对者: 继续尝试配对（等待价格回落到可配对水平）

imbalance >= K AND cheap == underweight:
  捡便宜: 可以运行（便宜边正好是轻边，自然缩小 imbalance）
  配对者: 正常运行
```

---

## 八、guaranteed_pnl 计算

保留近似公式：

```
guaranteed_pairs = min(N_up, N_down)
guaranteed_pnl = guaranteed_pairs − total_spent
```

未启用以 lot 为单位的精确公式（成本/收益可接受范围内近似足够）。

---

## 九、超时取消 + 软删除

- 两角色共享 `cancel_min_age=30s`：订单挂出超过 30s 未成交 → 取消
- 取消使用软删除（`cancelled_at` 标记），延迟 3-5s 后才从 `pending_orders` 删除，防止在途 UserTradeEvent 丢失 fill
- 被取消的配对订单：lot 回到未配对状态，下次 tick 配对者重新选择 lot

---

## 十、窗口生命周期

```
┌────────── 窗口开始 ──────────┐
│  市场发现 (Gamma API)         │
│  订阅 WS 订单簿               │
│         ↓                     │
│  ┌── tick loop ───────────┐   │
│  │ 获取 Up/Down best_bid/ask  │
│  │                          │
│  │ 1. 配对者：遍历 unpaired lots │
│  │    满足条件 → 挂配对单       │
│  │                          │
│  │ 2. 捡便宜者：cheap 边       │
│  │    imb < K OR cheap=轻边 → 挂 │
│  │                          │
│  │ 3. 冲突解决：配对优先       │
│  │ 4. 检查 pending/max_side  │
│  │ 5. 挂单                   │
│  │ 6. 取消超时/过期订单       │
│  └──────────────────────────┘   │
│         ↓                     │
│  min_remaining_time 到达      │
│  → 捡便宜者停手，配对者继续     │
│         ↓                     │
│  窗口结束 → 结算               │
│  取消所有挂单                  │
│  Gamma API 查询胜方           │
│  计算 PnL                     │
└────────────────────────────────┘
```

---

## 十一、与 Phase 1 的区别

| | Phase 1（旧） | Phase 2（新） |
|---|-------------|-------------|
| 下单角色 | 1 个（便宜边） | 2 个（捡便宜 + 配对） |
| 配对机制 | imbalance >= K 才强制补腿 | **每 tick 常驻配对**，成本最低 lot 优先 |
| 记账 | 简单 inventory dict | **Lot 级**，每个 fill 独立追踪 |
| 价格 | 单一 aggressiveness | 两个 aggressiveness（0.2 / 0.5） |
| 补腿量 | per_tick 一步 | per_tick 渐进，lot 锁定配对 |
| K 的周期振荡 | 补完立刻又追 → 死循环 | 配对者常驻 → 持续关敞口 → K 触发频率降低 |
| PendingOrder | 无 lot 绑定 | `pairing_lot_id` 绑定 |
