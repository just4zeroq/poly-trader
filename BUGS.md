# Bug 待修复

> 从 2026-07-29 00:15 实盘日志分析发现

## Bug 1: 完全成交的 pair 永不释放 accumulate [Critical]

**文件**: `platforms/engine.py:_cancel_stale_pending` ~line 857-858

**现象**: 00:16:53 起 `accumulate=20 >= 20 → skip`，整个窗口剩余 11 分钟无法下单

**根因**: pair 双腿成交后 orders 从 `pending_orders` 删除（remaining ≤ 0），但 pair 留在 `ws.pairs`，accumulate 永久不释放

```python
# 当前：识别到 dead pair 但不清理
if not has_up and not has_down:
    continue  # dead pair — no active orders
```

有两个修复点（选其一或都修）：
1. `_cancel_stale_pending`: dead pair 释放 accumulate + 移除 pair
2. `_fuse_pairs`: 双腿均完成的 pair 直接清理（更早更干净）

---

## Bug 2: 部分成交的 pair 永不取消 [High]

**文件**: `platforms/engine.py:_cancel_stale_pending` ~line 861-862

**现象**: pair_1_2 Down 剩余4@0.46，价格漂到 Down=0.11（偏离 0.35）仍挂着不取消

**根因**: 有 fill 的 pair 被硬跳过，即使价格已严重偏离

```python
# 当前：有 fill 就跳过
if pair.up_filled != 0 or pair.down_filled != 0:
    continue
```

**修法**: 对有 fills 的 pair 也允许在价格偏离大且剩余时间少时取消

---

## Bug 3: _fuse_pairs 跳过双腿 pair 但不清理已完成的 [Medium]

**文件**: `platforms/strategy.py:_fuse_pairs` ~line 142

**现象**: 双腿都 filled 的 pair（无 pending orders），`_fuse_pairs` 当做 "both legs" 跳过，accumulate 不释放

**根因**: `has_up and has_down` 的分支只 skip，不检查是否已完成

```python
# 可在此处加：
# has_up and has_down → 若均无 pending order → 已完成 pair，清理并释放 accumulate
```

---

## 预测先腿冒烟（2026-08-04 23:18–23:21，窗口 1785856500，POLY_PREDICTIVE=true）

**结果**: ✅ 通过。完整「先腿 → 配对 → 再先腿」循环实盘跑通，5 轮配对，无 BTC 拉取失败，tick 耗时无劣化。

**观察**（日志 `/tmp/pred_smoke.log`）:
- 特征加载: `[predict] window 1785856500 open=64065.94 prior15=-0.1777% prior1h=+0.6248% sigma5=0.136%` 一次成功。
- 先腿单: 5 次 `[favorite] Down 5 @ 0.68~0.73 P_fair=0.322~0.362 elapsed=189~319s`（模型持续偏向 Down，|P_fair−0.5|≥0.05 均通过）→ `[engine] step3 single Down=... OK`。
- 配对单: 每单成交后下一 tick 即 `[repair] Pair pair_1_N missing Up → 5 @ 0.25~0.30 cost=0.9800`（0.98<0.99 成本门槛内）→ Up 腿成交。
- 防护闸: `Down price 0.7000 too close to pending → skip`（min_price_gap）、`Down exposure 20/24 >= 20 → skip`（max_per_side / max_imbalance）均正常触发。
- 性能: 普通 tick `total=0.1~0.3ms`（P_fair 判定纯数学 µs 级）；下单 tick `total=250~270ms`（SDK HTTP order 调用）——均 <800ms 目标。币价走后台 WS 缓存，`[predict] BTC price fetch failed` 出现 0 次。
- reconcile 正常干预（GHOST ORDER 取消、MISSING FILL 修正），属既有引擎行为。

**遗留观察（非 bug）**:
- 冒烟被 `timeout 240` 掐断时窗口未结束，留下真实敞口 U=25/D=24 及 1 个挂单（pair_1_4 的 Down 剩 1 张）。进程退出后这些单无人管理，窗口结算时自动结算。配对部分锁定 ~2% 成本差（0.98 成本 vs 1.00 面值）。
- ORPHAN fill 警告来自本次 run 之前的历史挂单晚成交，与预测集成无关。
- 先腿价用的是市场 maker 价（Down 0.68~0.73），高于模型隐含 P(Down)=0.64~0.68；模型只用于选边，不用于定价（与计划一致）。
