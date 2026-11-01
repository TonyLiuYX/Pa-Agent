# 50 个 setup 的可执行识别目录

由 recognition/specs.py 与原 Markdown 主记录生成；运行 build.py 重建，勿直接编辑。

规则引擎 v0.2.2：50 个基础路径 + 34 个独立标识的变体。
数值解释均为 editor_operationalization / editor_proposal_not_tested；真实行情验证尚未执行。
变体、早期观测与数据代理不增加 setup 数量，也不继承原研究绩效。

周期以秒表示；0 为实际到达数据组成的事件窗口，source_open 的 0 为真实开盘价格观测。
状态路径按时间推进；same_frame 明示允许与前一步共用兼容观测。新信号端点默认仍等后续观测。多空统一使用 direction × price 坐标。

[运行与输入格式](README.md) · [评价协议](../evaluation/README.md) · [机器目录](catalog.json)

| Setup | 家族 | 识别周期/秒 | 变体 |
|---|---|---|---|
| [SET-0001 · 前一交易区间突破失败后的开盘反转](../setups/SET-0001.md) | range_failure | 60, 180, 300, 900 | intraday_position |
| [SET-0002 · 五分钟开盘区间突破](../setups/SET-0002.md) | clock_formula | 60 |  |
| [SET-0003 · 关键位置吸收后的反转](../setups/SET-0003.md) | flow | 0, 60, 180, 300, 900 |  |
| [SET-0004 · 显著成交重心跌破后的回测延续](../setups/SET-0004.md) | profile | 60, 180, 300, 900 | active_cross |
| [SET-0005 · 内包线突破失败陷阱](../setups/SET-0005.md) | strict_bar | 60, 180, 300, 900 | extended_window |
| [SET-0006 · 趋势中的两段回撤第二次入场](../setups/SET-0006.md) | pullback | 60, 180, 300, 900 | ema_reference |
| [SET-0007 · 三推高潮后的反转](../setups/SET-0007.md) | reversal | 60, 180, 300, 900 | parabolic |
| [SET-0008 · 旧趋势破坏后再测极值的主要反转](../setups/SET-0008.md) | reversal | 60, 180, 300, 900 | lower_low, higher_low, double_test |
| [SET-0009 · 强推进后的首次微型回撤](../setups/SET-0009.md) | pullback | 10, 60, 180, 300 | ross_10s |
| [SET-0010 · 突破后首次回测守住旧边界](../setups/SET-0010.md) | break_retest | 60, 180, 300, 900 | strict_tick_hold, double_test |
| [SET-0011 · 趋势末端最后旗形突破失败](../setups/SET-0011.md) | reversal | 60, 180, 300, 900 |  |
| [SET-0012 · 连续推进后的分数回撤反向短交易](../setups/SET-0012.md) | strict_bar | 60, 180, 300, 900 |  |
| [SET-0013 · 趋势内双内包压缩首次释放](../setups/SET-0013.md) | strict_bar | 60, 180, 300, 900 | inner_edge, close_inside, triple_inside |
| [SET-0014 · 趋势第一次反向冲击后的首回撤](../setups/SET-0014.md) | reversal | 60, 180, 300, 900 |  |
| [SET-0015 · 缩量普通回撤后的成交恢复](../setups/SET-0015.md) | pullback | 60, 180, 300, 900 |  |
| [SET-0016 · 反复逼近固定边界的压力突破](../setups/SET-0016.md) | compression | 60, 180, 300, 900 |  |
| [SET-0017 · 逆趋势实体柱试探失败后的顺势入场](../setups/SET-0017.md) | strict_bar | 60, 180, 300, 900 |  |
| [SET-0018 · 平衡区间边缘的第二次反向信号](../setups/SET-0018.md) | range_failure | 60, 180, 300, 900 |  |
| [SET-0019 · 原始顺势信号止损后快速恢复的延迟入场](../setups/SET-0019.md) | linked_reentry | 60, 180, 300, 900 |  |
| [SET-0020 · 前日低量尾部的次日回价值区](../setups/SET-0020.md) | profile | 60, 180, 300, 900 | source_open, source_0800_BST |
| [SET-0021 · 盘前高点的开盘动量突破](../setups/SET-0021.md) | opening | 60, 180, 300, 900 | tight_flag, sai_gap |
| [SET-0022 · 开盘洗盘后守住盘前支撑](../setups/SET-0022.md) | opening | 60, 180, 300, 900 | early_v |
| [SET-0023 · 弱开盘跌势的 VWAP 收复](../setups/SET-0023.md) | vwap | 60, 180, 300, 900 | direct_reclaim |
| [SET-0024 · VWAP 穿透、偏离失败与回测](../setups/SET-0024.md) | vwap | 0, 1, 5, 10, 60, 180, 300, 900 |  |
| [SET-0025 · 平衡日的 VWAP 过度偏离回归](../setups/SET-0025.md) | vwap | 60, 180, 300, 900 | touch_early |
| [SET-0026 · 历史高成交日 VWAP 压力与量能断崖](../setups/SET-0026.md) | equity_context | 60, 180, 300, 900 |  |
| [SET-0027 · 市场确认后的个股相对强弱交易](../setups/SET-0027.md) | correlated | 60, 180, 300, 900 | public_price_proxy |
| [SET-0028 · 新消息后的第二日关键位接受](../setups/SET-0028.md) | news | 60, 180, 300, 900 | direct_first_break |
| [SET-0029 · 突发新闻的第一轮价格反应](../setups/SET-0029.md) | news | 0, 1, 5, 10 | window90 |
| [SET-0030 · 领导市场先行的滞后市场 IB 突破](../setups/SET-0030.md) | correlated | 0, 60, 180, 300, 900 |  |
| [SET-0031 · 事前预案中的宏观意外与跨市场确认](../setups/SET-0031.md) | news | 0, 60, 180, 300, 900 |  |
| [SET-0032 · 多阶段公告的净信息重新判断](../setups/SET-0032.md) | news | 0, 60, 180, 300, 900 |  |
| [SET-0033 · TPO 台阶的破位与接受](../setups/SET-0033.md) | tpo | 0, 60, 180, 300, 900 |  |
| [SET-0034 · 台阶久攻不破后的反向持仓退出](../setups/SET-0034.md) | tpo | 0, 60, 180, 300, 900 |  |
| [SET-0035 · IB 首侧失败后的反侧突破与回收](../setups/SET-0035.md) | ib | 0, 60, 180, 300, 900 |  |
| [SET-0036 · Footprint P 形上分布边缘保持](../setups/SET-0036.md) | flow_profile | 0, 60, 180, 300, 900 | edge_limit_observation |
| [SET-0037 · 旧价值区重新接受后的横穿](../setups/SET-0037.md) | tpo | 60 | two_bracket_touch, volume_va_proxy |
| [SET-0038 · 当前低量伸展的失败反转回价值](../setups/SET-0038.md) | profile | 60, 180, 300, 900 | double_hold, early_failure |
| [SET-0039 · 同一水平前后成交能量改变的突破](../setups/SET-0039.md) | flow | 0, 60, 180, 300, 900 |  |
| [SET-0040 · 单向伸展末端的主动流耗尽反向短打](../setups/SET-0040.md) | flow | 0, 60, 180, 300, 900 |  |
| [SET-0041 · 阶梯跳价后低量区保持的延续](../setups/SET-0041.md) | flow | 0, 60, 180, 300, 900 |  |
| [SET-0042 · 堆叠对角失衡区的保持与再推进](../setups/SET-0042.md) | footprint | 0, 60, 180, 300, 900 | ratio150 |
| [SET-0043 · 逆势主动失衡失败后的顺势回归](../setups/SET-0043.md) | footprint | 0, 60, 180, 300, 900 |  |
| [SET-0044 · 价格破位而累计 Delta 背离的反转](../setups/SET-0044.md) | footprint | 0, 60, 180, 300, 900 |  |
| [SET-0045 · 冰山受阻、流转向与弱冲洗后的再拍卖](../setups/SET-0045.md) | flow | 0, 60, 180, 300, 900 |  |
| [SET-0046 · 弱多头背景下黏住卖价的向下清仓](../setups/SET-0046.md) | flow | 0, 60, 180, 300, 900 |  |
| [SET-0047 · 厚市场被动承接的缓慢磨过](../setups/SET-0047.md) | flow | 0, 60, 180, 300, 900 |  |
| [SET-0048 · 按同一时刻历史波动定义的日内噪声带动量](../setups/SET-0048.md) | clock_formula | 60 | same_side_band, same_side_vwap |
| [SET-0049 · 首段收益方向驱动的最后半小时](../setups/SET-0049.md) | clock_formula | 60 | joint_sign |
| [SET-0050 · 隔夜方向失败后反穿现金开盘线](../setups/SET-0050.md) | overnight | 60, 180, 300, 900 | early_extreme |

## SET-0001 · 前一交易区间突破失败后的开盘反转

原规则：[v1.0.0](../setups/SET-0001.md)；SHA-256：`311bbb5187dbd86b2b03c61b45e3ed5cb2085f99e13dd45d4a353a84da4a6326`。
原始七轴边界：开盘越界未获跟随；前一session区间边缘；越界→回收→强反向柱；信号柱端点触发；反转极值重破；回到旧区间或新反向趋势；OHLC

### base

必要数据：bars, prior_session.
周期：60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `extreme_break`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 前日区间越界：opening = true；prior_opposite_break = true | level ← prior_entry；risk ← extreme | 路径总限时 |
| 2 | 回收并形成反向信号：prior_reclaim = true；signal = true | trigger ← signal_edge | 路径总限时 |
| 3 | 越过信号端点：directional_extreme > "@trigger" | — | 路径总限时 |
### intraday_position

必要数据：bars, prior_session.
周期：60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `extreme_break`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 前日区间越界：prior_opposite_break = true | level ← prior_entry；risk ← extreme | 路径总限时 |
| 2 | 回收并形成反向信号：prior_reclaim = true；signal = true | trigger ← signal_edge | 路径总限时 |
| 3 | 越过信号端点：directional_extreme > "@trigger" | — | 路径总限时 |

## SET-0002 · 五分钟开盘区间突破

原规则：[v1.0.0](../setups/SET-0002.md)；SHA-256：`5f34188591ed03c1a55d5896cd1f799ba48486e18b23c03df567725225be50e0`。
原始七轴边界：开盘流动性与波动筛选；首5分钟高低；首柱方向→顺向越界；区间端点触发单；模型ATR止损；持至停止条件或收盘；OHLC+前日ATR

### base

必要数据：bars, daily_atr14, liquidity_history.
周期：60 秒；最长未触发时间 390 分钟；失效方式 `atr_touch`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 完整五分钟区间及方向：orb_ready = true；orb_direction = true；orb_liquidity = true | trigger ← orb_edge；risk ← orb_stop | 路径总限时 |
| 2 | 顺向触及区间端点：directional_extreme ≥ "@trigger" | — | 路径总限时 |

已建立路径可用续行周期：0 秒；保留建立周期与实际观测周期。

主版5分钟、单向、10%前14日ATR风险；成交不在此模拟。

## SET-0003 · 关键位置吸收后的反转

原规则：[v1.0.0](../setups/SET-0003.md)；SHA-256：`e2fc437a90a95e2680c21b424cb2a23e493cdded5c393cbab46040ec9916fe39`。
原始七轴边界：关键位置原方向进攻；极值/平衡边缘；大量冲击→低位移→反向接力；反向主动流推进；吸收带穿透并延伸；离开边缘回向平衡；逐笔+DOM

### base

必要数据：bars, classified_trades, depth, key_levels.
周期：0, 60, 180, 300, 900 秒；最长未触发时间 20 分钟；失效方式 `acceptance_close`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 关键位吸收：key_near = true；absorption = true | level ← key；risk ← extreme | 路径总限时 |
| 2 | 反向流接力：flow_turn = true；resume = true | — | 路径总限时 |

## SET-0004 · 显著成交重心跌破后的回测延续

原规则：[v1.0.0](../setups/SET-0004.md)；SHA-256：`6bc0782823f2c4d45dce9f1ce2f2ceb893ec600e0169b0bb82a83bfe6c8216ba`。
原始七轴边界：高价价值接受不足；突出旧VPOC；平衡→主动穿越→回测拒绝；回测后顺原穿越方向推进；旧重心另一侧重新接受；旧重心到新平衡；成交价位分布+OHLC

### base

必要数据：bars, prior_volume_profile, classified_trades.
周期：60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `acceptance_close`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 显著旧VPOC穿越：prominent_poc = true；poc_break = true | level ← poc；risk ← extreme | 路径总限时 |
| 2 | 首次回测拒绝：poc_retest = true；signal = true | — | 路径总限时 |
| 3 | 顺向价格恢复（可与前一步同帧）：resume = true | — | 路径总限时 |
### active_cross

必要数据：bars, prior_volume_profile, classified_trades, ib.
周期：60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `acceptance_close`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 显著旧VPOC穿越：prominent_poc = true；poc_break = true | level ← poc；risk ← extreme | 路径总限时 |
| 2 | VPOC与IB同向穿越：ib_break = true | — | 路径总限时 |

允许同刻判断：两个条件均来自当时已收到的观测。

## SET-0005 · 内包线突破失败陷阱

原规则：[v1.0.0](../setups/SET-0005.md)；SHA-256：`ff653f6da77ef1677078087172395ce8d3b0c8606148f632795fc9edb734b8a7`。
原始七轴边界：趋势/位置背景；内包线两端；内包→突破→回收另一端；反侧触发单且三柱有效；陷阱突破极值再破；回向背景方向；OHLC

### base

必要数据：bars.
周期：60, 180, 300, 900 秒；最长未触发时间 60 分钟；失效方式 `extreme_break`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 严格内包后双端外移：hikkake_start = true | trigger ← hikkake_trigger；risk ← hikkake_risk | 路径总限时 |
| 2 | 三柱内反侧触及：directional_extreme ≥ "@trigger" | — | 3 个完整柱（含末柱） |
### extended_window

必要数据：bars.
周期：60, 180, 300, 900 秒；最长未触发时间 60 分钟；失效方式 `extreme_break`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 严格内包后双端外移：hikkake_start = true | trigger ← hikkake_trigger；risk ← hikkake_risk | 路径总限时 |
| 2 | 三柱内反侧触及：directional_extreme ≥ "@trigger" | — | 6 个完整柱（含末柱） |

## SET-0006 · 趋势中的两段回撤第二次入场

原规则：[v1.0.0](../setups/SET-0006.md)；SHA-256：`d131e5e98068ef4c59842fc283c5888afa73115041b82332dcb4f2debc2be0ab`。
原始七轴边界：已存在趋势；回撤末端/可选均线；回撤→失败恢复→再回撤；第二次顺向信号端点；两段回撤极值重破；趋势再推进；OHLC

### base

必要数据：bars.
周期：60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `extreme_break`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 趋势首回撤：trend ≥ 0.55；pullback = true | risk ← extreme；logical_floor ← trend_origin | 路径总限时 |
| 2 | 第一次恢复：resume = true | — | 路径总限时 |
| 3 | 恢复失败再回撤：pullback = true | — | 路径总限时 |
| 4 | 第二顺向信号：signal = true | trigger ← signal_edge | 路径总限时 |
| 5 | 第二端点触发：directional_extreme > "@trigger" | — | 路径总限时 |
### ema_reference

必要数据：bars.
周期：60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `extreme_break`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 趋势首回撤：trend ≥ 0.55；pullback = true | risk ← extreme；logical_floor ← trend_origin | 路径总限时 |
| 2 | 第一次恢复：resume = true | — | 路径总限时 |
| 3 | 恢复失败再回撤：pullback = true | — | 路径总限时 |
| 4 | 第二顺向信号：signal = true；ema_near = true | trigger ← signal_edge | 路径总限时 |
| 5 | 第二端点触发：directional_extreme > "@trigger" | — | 路径总限时 |

## SET-0007 · 三推高潮后的反转

原规则：[v1.0.0](../setups/SET-0007.md)；SHA-256：`6965a87ae6cf15471e6242832026b3d8fcade9dfcebc9e3e9ef259cb5bb32da9`。
原始七轴边界：趋势末段；第三推进/已知障碍；三推→反向反应；反向强柱端点；第三推极值重破；两段回撤或平衡；OHLC

### base

必要数据：bars.
周期：60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `extreme_break`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 旧趋势末段三推：three_push = true | risk ← extreme | 路径总限时 |
| 2 | 反向信号：signal = true | trigger ← signal_edge | 路径总限时 |
| 3 | 信号端点触发：directional_extreme > "@trigger" | — | 路径总限时 |
### parabolic

必要数据：bars.
周期：60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `extreme_break`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 旧趋势末段三推：three_push = true；parabolic = true | risk ← extreme | 路径总限时 |
| 2 | 反向信号：signal = true | trigger ← signal_edge | 路径总限时 |
| 3 | 信号端点触发：directional_extreme > "@trigger" | — | 路径总限时 |

## SET-0008 · 旧趋势破坏后再测极值的主要反转

原规则：[v1.0.0](../setups/SET-0008.md)；SHA-256：`84e0910da99c1bf36e4de9393d6bb4a104ec8a42189a7cd1e8c34f75b2670404`。
原始七轴边界：既有趋势首次结构破坏；旧趋势极值；破坏→再测→第二反转；第二信号端点；再测极值继续原向推进；区间后可能新趋势；OHLC

### base

必要数据：bars.
周期：60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `extreme_break`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 旧趋势结构破坏：trend_break = true | level ← old_extreme；risk ← extreme | 路径总限时 |
| 2 | 再测旧极值：old_extreme_retest = true | — | 路径总限时 |
| 3 | 第二反转信号：signal = true | trigger ← signal_edge | 路径总限时 |
| 4 | 第二信号端点触发：directional_extreme > "@trigger" | — | 路径总限时 |
### lower_low

必要数据：bars.
周期：60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `extreme_break`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 旧趋势结构破坏：trend_break = true | level ← old_extreme；risk ← extreme | 路径总限时 |
| 2 | 再测旧极值：retest_beyond = true | — | 路径总限时 |
| 3 | 第二反转信号：signal = true | trigger ← signal_edge | 路径总限时 |
| 4 | 第二信号端点触发：directional_extreme > "@trigger" | — | 路径总限时 |
### higher_low

必要数据：bars.
周期：60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `extreme_break`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 旧趋势结构破坏：trend_break = true | level ← old_extreme；risk ← extreme | 路径总限时 |
| 2 | 再测旧极值：retest_inside = true | — | 路径总限时 |
| 3 | 第二反转信号：signal = true | trigger ← signal_edge | 路径总限时 |
| 4 | 第二信号端点触发：directional_extreme > "@trigger" | — | 路径总限时 |
### double_test

必要数据：bars.
周期：60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `extreme_break`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 旧趋势结构破坏：trend_break = true | level ← old_extreme；risk ← extreme | 路径总限时 |
| 2 | 再测旧极值：retest_equal = true | — | 路径总限时 |
| 3 | 第二反转信号：signal = true | trigger ← signal_edge | 路径总限时 |
| 4 | 第二信号端点触发：directional_extreme > "@trigger" | — | 路径总限时 |

## SET-0009 · 强推进后的首次微型回撤

原规则：[v1.0.0](../setups/SET-0009.md)；SHA-256：`ba1e6743c272f35c2df9f12b3fe2e27dd255017cc781b13251d648fd436447d6`。
原始七轴边界：强推进尚无普通回撤；新推进末端；首次浅暂停→迅速恢复；暂停高低点突破+成交恢复；暂停极值破坏/发展为普通回撤；快速续推；OHLC+成交量

### base

必要数据：bars, volume_baseline.
周期：10, 60, 180, 300 秒；最长未触发时间 180 分钟；失效方式 `extreme_break`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 强推进后首次浅暂停：first_micro = true | trigger ← pause_edge；risk ← extreme；logical_floor ← trend_origin | 路径总限时 |
| 2 | 成交恢复并突破暂停：directional_extreme > "@trigger"；rvol ≥ 1.0 | — | 路径总限时 |
### ross_10s

必要数据：bars, volume_baseline.
周期：10 秒；最长未触发时间 180 分钟；失效方式 `extreme_break`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 强推进后首次浅暂停：first_micro = true | trigger ← pause_edge；risk ← extreme；logical_floor ← trend_origin | 路径总限时 |
| 2 | 成交恢复并突破暂停：directional_extreme > "@trigger"；rvol ≥ 1.0 | — | 路径总限时 |

## SET-0010 · 突破后首次回测守住旧边界

原规则：[v1.0.0](../setups/SET-0010.md)；SHA-256：`f62017b77a6b5066901927bcfd83ada62bb69d99478589eae32ba490e064b8b0`。
原始七轴边界：明确边界有效突破；旧边界外侧；突破→首回测→守位续推；回测pivot顺向恢复；回测极值破坏并回收旧区间；外侧延续；OHLC

### base

必要数据：bars, key_levels.
周期：60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `extreme_break`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 先验边界有效突破：key_break = true | level ← key；risk ← extreme | 路径总限时 |
| 2 | 首次回测保持：captured_retest = true；signal = true | — | 路径总限时 |
| 3 | 顺向价格恢复（可与前一步同帧）：resume = true | — | 路径总限时 |
### strict_tick_hold

必要数据：bars, key_levels.
周期：60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `extreme_break`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 先验边界有效突破：key_break = true | level ← key；risk ← extreme | 路径总限时 |
| 2 | 首次回测保持：captured_retest = true；signal = true；strict_hold = true | — | 路径总限时 |
| 3 | 顺向价格恢复（可与前一步同帧）：resume = true | — | 路径总限时 |
### double_test

必要数据：bars, key_levels.
周期：60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `extreme_break`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 先验边界有效突破：key_break = true | level ← key；risk ← extreme | 路径总限时 |
| 2 | 首次回测保持：captured_retest = true；signal = true | — | 路径总限时 |
| 3 | 第一次恢复：resume = true | — | 路径总限时 |
| 4 | 第二次边界保持：captured_retest = true；signal = true | — | 路径总限时 |
| 5 | 顺向价格恢复（可与前一步同帧）：resume = true | — | 路径总限时 |

## SET-0011 · 趋势末端最后旗形突破失败

原规则：[v1.0.0](../setups/SET-0011.md)；SHA-256：`fa3a182633daa375b5a8aea554245d75d90c5c2e144729b04cbb3d605d55825c`。
原始七轴边界：趋势晚段靠近目标；水平末端旗形；旗形→原向突破→失败回收；回收后的反向信号；失败突破极值被再越过；两段反向修正或区间；OHLC

### base

必要数据：bars.
周期：60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `extreme_break`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 晚趋势水平旗形：late_flag = true | level ← flag_edge；risk ← extreme | 路径总限时 |
| 2 | 原向突破失败回收：flag_failure = true | — | 路径总限时 |
| 3 | 反向信号（可与前一步同帧）：signal = true | trigger ← signal_edge | 路径总限时 |
| 4 | 反向信号端点触发：directional_extreme > "@trigger" | — | 路径总限时 |

## SET-0012 · 连续推进后的分数回撤反向短交易

原规则：[v1.0.0](../setups/SET-0012.md)；SHA-256：`70367bdd7df48bf67ee4dbe30d6a7232feab1d168cd1469d0836c1331018211e`。
原始七轴边界：少重叠连续方向柱；刚才推进末端；连续推进→首反色→触发；首反色信号端点；信号极值重破；前段运动部分回撤；OHLC

### base

必要数据：bars.
周期：60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `extreme_break`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 三柱以上推进首逆色：retrace_start = true | trigger ← signal_edge；risk ← signal_risk | 路径总限时 |
| 2 | 反色端点触发：directional_extreme > "@trigger" | — | 路径总限时 |

## SET-0013 · 趋势内双内包压缩首次释放

原规则：[v1.0.0](../setups/SET-0013.md)；SHA-256：`f00c50b378033580d842970f3786040cd70b69be9996fa3cb063831bfb3a6ff4`。
原始七轴边界：已知方向趋势暂停；母柱范围；母柱→两内包→首顺向释放；所选母柱/内柱边界；模式对侧破坏；压缩向趋势扩张；OHLC

### base

必要数据：bars.
周期：60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `extreme_break`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 趋势双内包：trend ≥ 0.55；double_inside = true | trigger ← mother_edge；risk ← mother_risk；logical_floor ← trend_origin | 路径总限时 |
| 2 | 母柱端点首次释放：directional_extreme > "@trigger" | — | 路径总限时 |
### inner_edge

必要数据：bars.
周期：60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `extreme_break`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 趋势双内包：trend ≥ 0.55；double_inside = true | trigger ← inner_edge；risk ← mother_risk；logical_floor ← trend_origin | 路径总限时 |
| 2 | 母柱端点首次释放：directional_extreme > "@trigger" | — | 路径总限时 |
### close_inside

必要数据：bars.
周期：60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `extreme_break`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 趋势双内包：trend ≥ 0.55；close_double_inside = true | trigger ← mother_edge；risk ← mother_risk；logical_floor ← trend_origin | 路径总限时 |
| 2 | 母柱端点首次释放：directional_extreme > "@trigger" | — | 路径总限时 |
### triple_inside

必要数据：bars.
周期：60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `extreme_break`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 趋势双内包：trend ≥ 0.55；triple_inside = true | trigger ← mother_edge；risk ← mother_risk；logical_floor ← trend_origin | 路径总限时 |
| 2 | 母柱端点首次释放：directional_extreme > "@trigger" | — | 路径总限时 |

## SET-0014 · 趋势第一次反向冲击后的首回撤

原规则：[v1.0.0](../setups/SET-0014.md)；SHA-256：`92b4cd1dd4e0b67806b122aa181de4fb1dcfeceb74415f80fc2e524447a9d85b`。
原始七轴边界：旧趋势失衡后强反向冲击；首回撤末端；旧趋势→冲击→首弱回撤→新向恢复；首回撤新向信号；首回撤极值/旧向推进恢复；第二新向冲击；OHLC

### base

必要数据：bars.
周期：60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `extreme_break`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 旧趋势首次强反向冲击：anti_impulse = true | risk ← extreme | 路径总限时 |
| 2 | 新方向首弱回撤：pullback = true；counter_impulse < 0.6 | — | 路径总限时 |
| 3 | 新向信号：signal = true | — | 路径总限时 |
| 4 | 顺向价格恢复：resume = true | — | 路径总限时 |

## SET-0015 · 缩量普通回撤后的成交恢复

原规则：[v1.0.0](../setups/SET-0015.md)；SHA-256：`63994cf78c3dfc7c9a75e3ef8e385228d6bb33bc78922a056a8c83ae382d223c`。
原始七轴边界：非高潮已有趋势；普通回撤支撑区；推进→多柱缩量回撤→价格量恢复；pivot顺向恢复+量回升；支持破坏或逆量扩张；回原趋势极值后延续；OHLC+volume

### base

必要数据：bars, volume_baseline.
周期：60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `extreme_break`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 方向推进：trend ≥ 0.55；impulse ≥ 0.65 | risk ← extreme；logical_floor ← trend_origin | 路径总限时 |
| 2 | 普通缩量回撤：pullback = true；low_volume_pullback = true | — | 路径总限时 |
| 3 | 价量恢复：resume = true；rvol ≥ 1.0 | — | 路径总限时 |

## SET-0016 · 反复逼近固定边界的压力突破

原规则：[v1.0.0](../setups/SET-0016.md)；SHA-256：`4c35ba7941477a1e24b6a7976c64f350907d6dd1b2d679c94b745c4b39ac7e10`。
原始七轴边界：方向背景下持续贴边；事前反复测试固定水平；多次逼近→拒绝缩短→压缩→越界；压缩顺压力边界触发；压缩底破坏/深退；向外扩张；OHLC

### base

必要数据：bars, key_levels.
周期：60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `extreme_break`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 边界压力积累：pressure = true | trigger ← key；risk ← compression_risk | 路径总限时 |
| 2 | 压缩越界：directional_extreme > "@trigger"；compression ≥ 0.4 | — | 路径总限时 |

## SET-0017 · 逆趋势实体柱试探失败后的顺势入场

原规则：[v1.0.0](../setups/SET-0017.md)；SHA-256：`71b11980858e433074d7eeb61c3ad0de3ccab8d2e6f0a9c17370bf83098ddbfd`。
原始七轴边界：清晰既有趋势；逆向大实体柱边缘；逆向柱→端点试探→无第二趋势柱；随后柱原向端点且一柱有效；试探极值被再突破；恢复原趋势；OHLC

### base

必要数据：bars.
周期：60, 180, 300, 900 秒；最长未触发时间 30 分钟；失效方式 `extreme_break`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 趋势逆势实体柱被试探无跟随：trend_bar_failure = true | trigger ← signal_edge；risk ← signal_risk；logical_floor ← trend_origin | 路径总限时 |
| 2 | 下一柱端点触发：directional_extreme > "@trigger" | — | 1 个完整柱（含末柱） |

## SET-0018 · 平衡区间边缘的第二次反向信号

原规则：[v1.0.0](../setups/SET-0018.md)；SHA-256：`58bb0aec8ee66d23317651bae8922d1c1b20be9c94254184fc1c73bc163785c5`。
原始七轴边界：事前已显示双向平衡；当前区间边缘；边缘试探→首反向不足→再测→第二信号；第二逆向信号端点；外侧接受/连续新趋势；回中部及可能对边；OHLC

### base

必要数据：bars.
周期：60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `extreme_break`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 平衡区边缘首次反向：balance ≥ 0.55；range_edge = true；signal = true | risk ← extreme；level ← range_boundary | 路径总限时 |
| 2 | 再次测试边缘：captured_retest = true | — | 路径总限时 |
| 3 | 第二反向信号（可与前一步同帧）：signal = true | trigger ← signal_edge | 路径总限时 |
| 4 | 第二信号端点触发：directional_extreme > "@trigger" | — | 路径总限时 |

## SET-0019 · 原始顺势信号止损后快速恢复的延迟入场

原规则：[v1.0.0](../setups/SET-0019.md)；SHA-256：`c5e5312de81510a1be1d6ba13c76e4ab6d8fbaa927d19e8c63427c7910bcb4e4`。
原始七轴边界：原顺势前提仍在；先前模式典型保护位；原触发→模式止损穿透→快速原向恢复；新原向信号端点；恢复极值破坏/较大背景改变；恢复原计划未达目标；OHLC+原模式参考

### base

必要数据：bars, parent_detection.
周期：60, 180, 300, 900 秒；最长未触发时间 30 分钟；失效方式 `extreme_break`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 父模式触发后保护位被扫：parent_sweep = true | risk ← extreme；logical_floor ← trend_origin | 路径总限时 |
| 2 | 背景保留并迅速恢复（可与前一步同帧）：trend ≥ 0.55；resume = true | trigger ← signal_edge | 路径总限时 |
| 3 | 新端点触发：directional_extreme > "@trigger" | — | 路径总限时 |

## SET-0020 · 前日低量尾部的次日回价值区

原规则：[v1.0.0](../setups/SET-0020.md)；SHA-256：`88579871f611eea4d7f767ce53a8ea6c91e5bf20cc5c75135cbf79ab1489e88c`。
原始七轴边界：前日尾段偏离但旧价值稳定；前日低点附近的次日开盘；尾部低量且新日没有继续接受；次日开盘或08:00量恢复入口；前日极值破坏/尾部新价值形成；途中单印先处理、旧价值目标、当日平仓；OHLC+前日Volume Profile/TPO

### base

必要数据：bars, prior_volume_profile, prior_tpo, prior_session.
周期：60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `acceptance_close`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 前日低量尾部新日近尾开盘：prior_tail_open = true | risk ← prior_tail_risk | 路径总限时 |
| 2 | 尾部不接受且向旧价值恢复（可与前一步同帧）：tail_reject = true；resume = true | — | 路径总限时 |
### source_open

必要数据：prior_volume_profile, prior_tpo, prior_session, price_observation.
周期：0 秒；最长未触发时间 180 分钟；失效方式 `acceptance_close`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 近前日低量尾部开盘：prior_tail_open = true；instant_open = true | risk ← prior_tail_risk；observed_price ← directional_price；price_basis ← price_basis | 路径总限时 |
| 2 | 实际开盘价观测：instant_open = true | — | 路径总限时 |

允许同刻判断：两个条件均来自当时已收到的观测。
### source_0800_BST

必要数据：bars, prior_volume_profile, prior_tpo, prior_session, volume_baseline.
周期：60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `acceptance_close`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 前日低量尾部新日近尾开盘：prior_tail_open = true | risk ← prior_tail_risk | 路径总限时 |
| 2 | 尾部不接受且向旧价值恢复（可与前一步同帧）：tail_reject = true；resume = true；london_0800 = true；volume_increase ≥ 1.2 | — | 路径总限时 |

## SET-0021 · 盘前高点的开盘动量突破

原规则：[v1.0.0](../setups/SET-0021.md)；SHA-256：`57a9ad84ceb2e99e38bd712074c82de549bda6b51faaedc195fe35111ccb053d`。
原始七轴边界：新闻/跳空与盘前参与；开盘前已知的PMH；紧盘前范围、开盘近高点、买入动量；开盘直接穿越PMH或紧旗高点；突破回落选定结构/无即时延续；第一轮弱化分批、时间退出、现金收盘平仓；盘前OHLC+成交量+新闻

### base

必要数据：bars, premarket, news, volume_baseline.
周期：60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `extreme_break`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 催化跳空近盘前高点：opening = true；catalyst_gap = true；near_pm_edge = true | trigger ← pm_edge；risk ← signal_risk | 路径总限时 |
| 2 | 盘前高动量突破（可与前一步同帧）：directional_extreme > "@trigger"；impulse ≥ 0.65；rvol ≥ 1 | — | 路径总限时 |
### tight_flag

必要数据：bars, premarket, news, volume_baseline.
周期：60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `extreme_break`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 催化跳空近盘前高点：opening = true；catalyst_gap = true；near_pm_edge = true；compression ≥ 0.5 | trigger ← tight_flag_edge；risk ← signal_risk | 路径总限时 |
| 2 | 盘前高动量突破（可与前一步同帧）：directional_extreme > "@trigger"；impulse ≥ 0.65；rvol ≥ 1 | — | 路径总限时 |
### sai_gap

必要数据：bars, premarket, news, volume_baseline.
周期：60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `extreme_break`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 催化跳空近盘前高点：opening = true；sai_catalyst_gap = true；near_pm_edge = true | trigger ← pm_edge；risk ← signal_risk | 路径总限时 |
| 2 | 盘前高动量突破（可与前一步同帧）：directional_extreme > "@trigger"；impulse ≥ 0.65；rvol ≥ 1 | — | 路径总限时 |

## SET-0022 · 开盘洗盘后守住盘前支撑

原规则：[v1.0.0](../setups/SET-0022.md)；SHA-256：`96065e33077ef63a160a5b14b7547e0adc080f29ad87121d53185cf3ac1942f8`。
原始七轴边界：可做多背景、尚未建立熊趋势；PML上方已知盘前支撑；开盘急跌失败、复测仍保持；复测后的恢复/越过复测棒高点；支撑下方被接受；VWAP先处理、PMH后续、当日平仓；盘前OHLC+成交量

### base

必要数据：bars, premarket.
周期：60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `extreme_break`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 开盘洗盘测试既有盘前支撑：opening = true；pm_wash = true | level ← pm_support；risk ← extreme | 路径总限时 |
| 2 | 复测保持：captured_retest = true；signal = true | trigger ← signal_edge | 路径总限时 |
| 3 | 复测棒高触发：directional_extreme > "@trigger" | — | 路径总限时 |
### early_v

必要数据：bars, premarket.
周期：60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `extreme_break`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 开盘洗盘测试既有盘前支撑：opening = true；pm_wash = true | level ← pm_support；risk ← extreme | 路径总限时 |
| 2 | 首轮支撑恢复：signal = true；resume = true | — | 路径总限时 |

## SET-0023 · 弱开盘跌势的 VWAP 收复

原规则：[v1.0.0](../setups/SET-0023.md)；SHA-256：`86ac2daf6117c845863891687fe82d9b7de19b9f083080fdf217a14e5516caf1`。
原始七轴边界：开盘较弱下跌、熊趋势尚未确立；固定现金session VWAP；跌势弱化并重新收在VWAP上；确认棒之后越过其高点/原版恢复买；选定VWAP强失守或结构低点；PMH/HOD参考、结构跟踪、当天结束；逐棒OHLC+成交量+因果VWAP

### base

必要数据：bars, exact_vwap.
周期：60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `acceptance_close`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 早期弱跌：opening = true；weak_open_drop = true | risk ← extreme | 路径总限时 |
| 2 | VWAP上收盘确认（可与前一步同帧）：vwap_reclaim = true | trigger ← signal_edge | 路径总限时 |
| 3 | 下一棒高触发：directional_extreme > "@trigger" | — | 1 个完整柱（含末柱） |
### direct_reclaim

必要数据：bars, exact_vwap.
周期：60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `acceptance_close`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 早期弱跌：opening = true；weak_open_drop = true | risk ← extreme | 路径总限时 |
| 2 | VWAP上收盘确认（可与前一步同帧）：vwap_reclaim = true | trigger ← signal_edge | 路径总限时 |

## SET-0024 · VWAP 穿透、偏离失败与回测

原规则：[v1.0.0](../setups/SET-0024.md)；SHA-256：`332fbdec4738e55a7c815862a75557746fb0737f4b174b8d6ede20b9cd1c51b5`。
原始七轴边界：开盘已转弱并清出隔夜多头参考；VWAP上方的失败偏离启动点；穿透+再探高放量但失败+返回；最后推进起点的回测失败做空；偏离高点重新被接受；日低/新低或低处吸收、更高低点退出；VWAP+逐笔/Footprint与OHLC

### base

必要数据：bars, exact_vwap, classified_trades, prior_session.
周期：0, 1, 5, 10, 60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `acceptance_close`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 清隔夜参考并穿透VWAP：overnight_failure = true；vwap_cross = true | level ← impulse_origin；risk ← extreme | 路径总限时 |
| 2 | 偏离再探失败：vwap_deviation_fail = true；flow_activity ≥ 1 | — | 路径总限时 |
| 3 | 启动位置回测拒绝：captured_retest = true；flow_resume = true | — | 路径总限时 |

## SET-0025 · 平衡日的 VWAP 过度偏离回归

原规则：[v1.0.0](../setups/SET-0025.md)；SHA-256：`8f62f8fd2d4338a45e94e9de7f8a31834795e1cb3a91a4f1c34b9001623eb933`。
原始七轴边界：已辨认双向平衡且VWAP较平；均价外异常偏离区域；伸展后无法持续、新极值失败；动量衰退后局部结构反穿（编辑版）；失败极值外/新趋势接受；VWAP目标、先验空间检查、当日结束；因果VWAP+OHLC+波动尺度

### base

必要数据：bars, exact_vwap.
周期：60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `acceptance_close`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 平VWAP平衡伸展：balance ≥ 0.55；flat_vwap = true；vwap_stretch = true | risk ← extreme | 路径总限时 |
| 2 | 新极值失败：failed_extension = true | — | 路径总限时 |
| 3 | 局部结构反穿：resume = true | — | 路径总限时 |
### touch_early

必要数据：bars, exact_vwap.
周期：60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `acceptance_close`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 平VWAP平衡伸展：balance ≥ 0.55；flat_vwap = true；vwap_stretch = true | risk ← extreme | 路径总限时 |
| 2 | 已观测的伸展位置：vwap_stretch = true | — | 路径总限时 |

允许同刻判断：两个条件均来自当时已收到的观测。

## SET-0026 · 历史高成交日 VWAP 压力与量能断崖

原规则：[v1.0.0](../setups/SET-0026.md)；SHA-256：`76fb47081a132aec4a0ec4aa951dc95a83512d6d77aa5fac166b0a179b67e1fd`。
原始七轴边界：低流通股催化大gap、历史大成交供给；先前最高成交日VWAP压力；上攻失败+当日量能断崖；拒绝后短线趋势破坏卖出；拒绝高点外/持续大买量接受上线；下方支撑和衰退分批、当日回补；历史逐日VWAP+当日量/价+借券条件

### base

必要数据：bars, historical_vwap, equity_metadata, news, volume_baseline.
周期：60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `extreme_break`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 低流通催化gap触历史供给：boulevard_context = true；historical_vwap_reject = true | risk ← extreme | 路径总限时 |
| 2 | 量能断崖：volume_cliff = true | — | 路径总限时 |
| 3 | 反弹结构破坏：resume = true | — | 路径总限时 |

## SET-0027 · 市场确认后的个股相对强弱交易

原规则：[v1.0.0](../setups/SET-0027.md)；SHA-256：`3a4e09b7e17a3f6897a6bba2f21d795e7d378ba8177461900bd2f9181b710b51`。
原始七轴边界：市场方向与个股日线条件；个股支撑/突破位相对SPY回调；指数弱时个股保持强、随后市场确认；市场方向确认+股票自身恢复/突破；股票支撑破坏、相对强弱消失；跟个股结构并监控指数、当天平仓；同步个股/指数OHLC+成交量

### base

必要数据：bars, synced_market, daily_context, volume_baseline.
周期：60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `extreme_break`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 日线相容且相对强弱成立：daily_aligned = true；relative_strength ≥ 0.6 | risk ← extreme；logical_floor ← trend_origin | 路径总限时 |
| 2 | 市场同向确认：market_confirm = true | — | 路径总限时 |
| 3 | 个股结构恢复（可与前一步同帧）：resume = true | — | 路径总限时 |
### public_price_proxy

必要数据：bars, synced_market, daily_context, volume_baseline.
周期：60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `extreme_break`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 日线相容且相对强弱成立：daily_aligned = true；relative_strength ≥ 0.6 | risk ← extreme；logical_floor ← trend_origin | 路径总限时 |
| 2 | 市场同向确认：market_confirm = true | — | 路径总限时 |
| 3 | 个股结构恢复（可与前一步同帧）：resume = true | — | 路径总限时 |

公开价格代理；不提供1OP/1OSI复刻。

## SET-0028 · 新消息后的第二日关键位接受

原规则：[v1.0.0](../setups/SET-0028.md)；SHA-256：`dbfaab87115c12db81f6ab6d0bbc026782ca22cf300cf13eb2ab50fa2777b30c`。
原始七轴边界：前一日真实新消息及参与；前日已知关键价格、次日上方接受区；次日强穿+随后保持整理；上方整理后再恢复（编辑可复盘版）；上方接受区/关键位被持续压回；下一阻力分批、结构跟踪、当天退出；新闻时间+前日价位+当日量/价

### base

必要数据：bars, news, prior_session, volume_baseline.
周期：60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `extreme_break`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 前日新消息及次日关键位突破：second_day_news = true；second_day_break = true | level ← second_day_edge；risk ← extreme | 路径总限时 |
| 2 | 上方整理接受：accepted = true；compression ≥ 0.3 | — | 路径总限时 |
| 3 | 顺向价格恢复：resume = true | — | 路径总限时 |
### direct_first_break

必要数据：bars, news, prior_session, volume_baseline.
周期：60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `extreme_break`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 前日新消息及次日关键位突破：second_day_news = true；second_day_break = true | level ← second_day_edge；risk ← extreme | 路径总限时 |
| 2 | 首轮越过关键位：directional_price > "@level" | — | 路径总限时 |

允许同刻判断：两个条件均来自当时已收到的观测。

## SET-0029 · 突发新闻的第一轮价格反应

原规则：[v1.0.0](../setups/SET-0029.md)；SHA-256：`a377d4840dceac2e2d473aafcd299f18ec8c3c8d1c9a3cceb256809706fc3787`。
原始七轴边界：可核实刚发生的新信息；第一反应棒与即时成交位置；方向性价格反应且在短窗口；首60秒推进主动进入/90秒独立版；首反应棒极值或首个明显弱化；初轮2–3分钟衰退退出、当日平仓；可靠新闻时间戳+短周期OHLC/成交

### base

必要数据：bars, news, short_bars.
周期：0, 1, 5, 10 秒；最长未触发时间 1 分钟；失效方式 `extreme_break`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 实际到达后首轮方向反应：news_fresh60 = true；impulse ≥ 0.65 | risk ← news_risk | 路径总限时 |
| 2 | 首60秒内继续推进：news_fresh60 = true；resume = true | — | 路径总限时 |
### window90

必要数据：bars, news, short_bars.
周期：0, 1, 5, 10 秒；最长未触发时间 1.5 分钟；失效方式 `extreme_break`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 实际到达后首轮方向反应：news_fresh90 = true；impulse ≥ 0.65 | risk ← news_risk | 路径总限时 |
| 2 | 首60秒内继续推进：news_fresh90 = true；resume = true | — | 路径总限时 |

## SET-0030 · 领导市场先行的滞后市场 IB 突破

原规则：[v1.0.0](../setups/SET-0030.md)；SHA-256：`3750b32aa3bf009e680992f1f44da16a97398f5ff8b88d0d22305f90f9d58f42`。
原始七轴边界：相关市场方向一致并有可核实先行者；滞后市场自己的固定IB边界；领导先动+滞后单向推进及自身流确认；滞后产品破IB并在外保持再续行；自身恢复IB/先行方向被否认；结构目标、相关性复核、当日平仓；同步双市场OHLC+量+DOM

### base

必要数据：bars, synced_market, classified_trades, ib.
周期：0, 60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `extreme_break`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 事前领导关系成立且先行突破：leader_break = true | risk ← extreme | 路径总限时 |
| 2 | 本产品IB外突破接受：ib_break = true；accepted = true | level ← ib_edge | 路径总限时 |
| 3 | 本产品流确认续行：flow_resume = true；resume = true | — | 路径总限时 |

## SET-0031 · 事前预案中的宏观意外与跨市场确认

原规则：[v1.0.0](../setups/SET-0031.md)；SHA-256：`6427eacabc4c5ed7ee1c29b00314a7f87fcbcb647d569bbffc7ab5e841b8059d`。
原始七轴边界：预定宏观发布与已锁定共识分支；发布前价格/敏感产品；真实意外+多个敏感产品响应相容；信息分支与可交易价格同向时进入；价格不响应/跨市场否认+最大价格风险；初轮流衰退、下一信息前评估、当日结束；原始公告+事前共识+同步价格/DOM

### base

必要数据：bars, news, consensus, synced_market.
周期：0, 60, 180, 300, 900 秒；最长未触发时间 15 分钟；失效方式 `extreme_break`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 预案分支匹配实际意外：macro_surprise = true | risk ← news_risk | 路径总限时 |
| 2 | 多个敏感产品方向相容（可与前一步同帧）：cross_confirm = true；resume = true | — | 路径总限时 |

## SET-0032 · 多阶段公告的净信息重新判断

原规则：[v1.0.0](../setups/SET-0032.md)；SHA-256：`f7289122cd9fa85cd988c75acd07ec9e9866bede49fea2d650b68dcd1abbb0d4`。
原始七轴边界：第一段事件反应已发生；第二条信息/后续阶段与其响应位；新增维度改变净含义、旧方向被否认；新内容+新方向真实接受后重新入场；新解释/新恢复区被否认、独立风险边界；每笔分记、信息迭代与当日结束；分阶段原文与到达时间+同步价/流

### base

必要数据：bars, news, consensus, synced_market.
周期：0, 60, 180, 300, 900 秒；最长未触发时间 60 分钟；失效方式 `extreme_break`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 新增阶段改变事前净分支：reassessment = true | risk ← news_risk；level ← signal_edge | 路径总限时 |
| 2 | 新方向真实接受：accepted = true；cross_confirm = true | — | 路径总限时 |
| 3 | 顺向价格恢复：resume = true | — | 路径总限时 |

## SET-0033 · TPO 台阶的破位与接受

原规则：[v1.0.0](../setups/SET-0033.md)；SHA-256：`7e5692a77530214ee0d1369e81ac8da3647523a04cbb5f36dbc1df553ca7e004`。
原始七轴边界：先验Profile台阶尚未解决；多个TPO时段同边缘；朝边缘推进并突破外侧接受；跨出台阶且保持/流确认；回台阶内重新接受；下一拍卖参考、漂移失败退出、当天结束；TPO分段及DOM/量价

### base

必要数据：bars, tpo, classified_trades.
周期：0, 60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `acceptance_close`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 冻结多时段TPO台阶被突破：ledge_break = true | level ← ledge；risk ← extreme | 路径总限时 |
| 2 | 外侧接受且流确认：accepted = true；flow_resume = true | — | 路径总限时 |

## SET-0034 · 台阶久攻不破后的反向持仓退出

原规则：[v1.0.0](../setups/SET-0034.md)；SHA-256：`2894f484e4b7764c5d5eec57aad0ceef87447ff20540e40d5af07c2f000dd836`。
原始七轴边界：旧价值内原方向反复台阶失败；台阶背后范围的相反边缘；反复失败+相反方向实际加速；相反范围边缘突破且速度恢复；相反突破回区间/原台阶被解决；短促退出流分批、速度衰减撤、当日结束；TPO旧价值+OHLC/成交速度/DOM

### base

必要数据：bars, tpo, prior_tpo, classified_trades.
周期：0, 60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `acceptance_close`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 旧价值台阶反复失败：ledge_failed = true | risk ← extreme；trigger ← range_break_edge | 路径总限时 |
| 2 | 相反范围破位加速（可与前一步同帧）：directional_extreme > "@trigger"；flow_activity ≥ 1.3 | — | 路径总限时 |

## SET-0035 · IB 首侧失败后的反侧突破与回收

原规则：[v1.0.0](../setups/SET-0035.md)；SHA-256：`7c3863ce5f2c031b28b48f263dc1f3c6c28636fe485df99681f2a0ebd5b1b76f`。
原始七轴边界：IB已固定且首侧拍卖失败；IB相反边缘及回收区；下破失败→反侧突破→回收仍接受；反侧边缘回收后的恢复进入；回收枢轴/重新接受IB内；前日相反极值、流衰退、当日结束；固定IB+分session Profile+短价/DOM

### base

必要数据：bars, ib, prior_volume_profile, classified_trades.
周期：0, 60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `acceptance_close`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | IB首侧失败：ib_first_failure = true | risk ← extreme | 路径总限时 |
| 2 | 反侧突破（可与前一步同帧）：ib_break = true | level ← ib_edge | 路径总限时 |
| 3 | 反侧回收保持：captured_retest = true；signal = true | — | 路径总限时 |
| 4 | 流恢复：flow_resume = true；resume = true | — | 路径总限时 |

## SET-0036 · Footprint P 形上分布边缘保持

原规则：[v1.0.0](../setups/SET-0036.md)；SHA-256：`6f50db1e077ded7a76460e6ad75a3eaf4317aab7a77790ee60e71ac41797c3ce`。
原始七轴边界：强冲后的更高微观接受；上部厚成交分布下沿/LVN；高处积量、下沿保持/吸收；尽量靠分布下沿保持后买入；上部分布边缘被持续穿回；上方拍卖参考/接受跟踪、当天结束；实时Footprint成交分布

### base

必要数据：bars, classified_trades, volume_profile.
周期：0, 60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `acceptance_close`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 强冲后高处分布形成：micro_p = true | level ← distribution_edge；risk ← extreme | 路径总限时 |
| 2 | 分布边缘测试保持：captured_retest = true；absorption = true | — | 路径总限时 |
| 3 | 顺向价格恢复：resume = true | — | 路径总限时 |
### edge_limit_observation

必要数据：bars, classified_trades, volume_profile.
周期：0, 60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `acceptance_close`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 强冲后高处分布形成：micro_p = true | level ← distribution_edge；risk ← extreme | 路径总限时 |
| 2 | 分布边缘测试保持：captured_retest = true；absorption = true | — | 路径总限时 |

仅标边缘保持的识别时点，不表示限价成交。

## SET-0037 · 旧价值区重新接受后的横穿

原规则：[v1.0.0](../setups/SET-0037.md)；SHA-256：`546bede6aaee6c88f2f68c9b81a2b0e30b9c630a31021aa8fde8013ec368c5aa`。
原始七轴边界：前日价值固定、今日外侧开盘；旧VA入口边及相反边；返回并满足因果内部接受窗口；严格收盘满60分钟后向对边（本版）；入口边外重新接受；相反VA边、空间检查、session结束；前日TPO VA+分钟收盘+时钟

### base

必要数据：bars, prior_tpo, prior_session.
周期：60 秒；最长未触发时间 390 分钟；失效方式 `acceptance_close`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 外开盘返回旧TPO价值：value_return = true | level ← value_entry；risk ← value_risk | 路径总限时 |
| 2 | 连续内部分钟收盘满60分钟：value_inside60 = true | — | 路径总限时 |
### two_bracket_touch

必要数据：bars, prior_tpo, prior_session, tpo.
周期：60 秒；最长未触发时间 390 分钟；失效方式 `acceptance_close`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 外开盘返回旧TPO价值：value_return = true | level ← value_entry；risk ← value_risk | 路径总限时 |
| 2 | 连续内部分钟收盘满60分钟：value_two_brackets = true | — | 路径总限时 |
### volume_va_proxy

必要数据：bars, prior_volume_profile, prior_session.
周期：60 秒；最长未触发时间 390 分钟；失效方式 `acceptance_close`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 外开盘返回旧TPO价值：volume_value_return = true | level ← volume_value_entry；risk ← volume_value_risk | 路径总限时 |
| 2 | 连续内部分钟收盘满60分钟：volume_value_inside60 = true | — | 路径总限时 |

## SET-0038 · 当前低量伸展的失败反转回价值

原规则：[v1.0.0](../setups/SET-0038.md)；SHA-256：`90625a4c3ac4b763537cf31ab9eb6d7e3a2b1b8f78aec68273cd9a7d1b1312f9`。
原始七轴边界：当前快速伸展、旧价值未跟移；伸展末端与既有价值；新极值失败+恢复不能延续；失败后的结构反穿/双持版本；失败极值外、新价值接受；旧价值/途中节点、计划段结束；同场所实时Volume Profile+量价

### base

必要数据：bars, volume_profile, classified_trades.
周期：60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `acceptance_close`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 低量伸展旧价值未迁移：profile_stretch = true | risk ← extreme；level ← prior_value_poc | 路径总限时 |
| 2 | 新极值失败：failed_extension = true | — | 路径总限时 |
| 3 | 恢复失败后反穿：resume = true | — | 路径总限时 |
### double_hold

必要数据：bars, volume_profile, classified_trades.
周期：60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `acceptance_close`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 低量伸展旧价值未迁移：profile_stretch = true | risk ← extreme；level ← extreme | 路径总限时 |
| 2 | 新极值失败：failed_extension = true | — | 路径总限时 |
| 3 | 首次保持后反弹：resume = true | — | 路径总限时 |
| 4 | 第二次极值保持：captured_retest = true | — | 路径总限时 |
| 5 | 恢复失败后反穿：resume = true | — | 路径总限时 |
### early_failure

必要数据：bars, volume_profile, classified_trades.
周期：60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `acceptance_close`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 低量伸展旧价值未迁移：profile_stretch = true | risk ← extreme；level ← prior_value_poc | 路径总限时 |
| 2 | 新极值失败：failed_extension = true | — | 路径总限时 |

## SET-0039 · 同一水平前后成交能量改变的突破

原规则：[v1.0.0](../setups/SET-0039.md)；SHA-256：`ec035fc97e0d33d593c7a0723a1c2b285309e82cf5159d83d03f899853eddc0a`。
原始七轴边界：同一已知边界先前低能量未破；旧失败水平及新薄量区；新攻击真实量/速度与持价优于旧；越界且主动消耗/买方保持；放量不推进或薄区被穿回；下一参考及流衰减、session结束；DOM实际成交+相对量+先验Profile

### base

必要数据：bars, classified_trades, depth, volume_profile, key_levels.
周期：0, 60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `acceptance_close`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 同位低能量失败：key_near = true；weak_attack = true | level ← key；energy ← attack_energy；risk ← extreme | 路径总限时 |
| 2 | 新攻击能量改善：attack_energy > "@energy"；flow_resume = true | — | 路径总限时 |
| 3 | 边界越过：directional_extreme > "@level" | — | 路径总限时 |

## SET-0040 · 单向伸展末端的主动流耗尽反向短打

原规则：[v1.0.0](../setups/SET-0040.md)；SHA-256：`ad67ae46e0c8ab23210a11c30f0e79595d9c5183c3a759ad22526f722640b796`。
原始七轴边界：持续单向伸展末端；最后外侧探测极值；原主动推进衰退、反向轮转快；原方向无法重启后的反向流触发；极值外/原主动性恢复；一次短打、最近反向点、迅速退出；逐笔与DOM动态+短价格

### base

必要数据：bars, classified_trades, depth.
周期：0, 60, 180, 300, 900 秒；最长未触发时间 5 分钟；失效方式 `acceptance_close`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 原方向末端耗尽：exhaustion = true | risk ← extreme | 路径总限时 |
| 2 | 无法重启后反向轮转：flow_turn = true；resume = true | — | 路径总限时 |

## SET-0041 · 阶梯跳价后低量区保持的延续

原规则：[v1.0.0](../setups/SET-0041.md)；SHA-256：`af68795c63c89ec2278babcf406107ae40aeabb4f0eb600cb9f3155ab46740f8`。
原始七轴边界：关键位置有方向压力；跳价后留下的薄成交区；实际阶梯跳过+反向流少；跳价或薄区保持后续推版本；薄区补成交并被穿回；下一拍卖参考、反向流返回即撤、当日结束；DOM/逐笔动态+volume at price

### base

必要数据：bars, classified_trades, depth, volume_profile.
周期：0, 60, 180, 300, 900 秒；最长未触发时间 15 分钟；失效方式 `acceptance_close`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 真实阶梯跳价留下薄区：jump = true | level ← jump_edge；risk ← jump_risk | 路径总限时 |
| 2 | 薄区反向测试保持：captured_retest = true；thin_hold = true | — | 路径总限时 |
| 3 | 再推进：flow_resume = true；resume = true | — | 路径总限时 |

## SET-0042 · 堆叠对角失衡区的保持与再推进

原规则：[v1.0.0](../setups/SET-0042.md)；SHA-256：`827977d28214728a0feec60cd868ea120607c0b32f7dafaa383b18807f9658c5`。
原始七轴边界：主动方向成交已形成可固定区域；连续多档对角失衡区；堆叠后反向测试不穿区；测试保持后局部恢复进入；反侧被接受或对向主动区持续；下一结构/对向压力、当日结束；正确Bid/Ask分类与对角Footprint

### base

必要数据：bars, classified_trades.
周期：0, 60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `acceptance_close`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 同向连续对角堆叠区形成：stacked = true | level ← imbalance_edge；risk ← imbalance_risk | 路径总限时 |
| 2 | 固定区回测保持：captured_retest = true | — | 路径总限时 |
| 3 | 顺向价格恢复：resume = true | — | 路径总限时 |
### ratio150

必要数据：bars, classified_trades.
周期：0, 60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `acceptance_close`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 同向连续对角堆叠区形成：stacked150 = true | level ← imbalance150_edge；risk ← imbalance150_risk | 路径总限时 |
| 2 | 固定区回测保持：captured_retest = true | — | 路径总限时 |
| 3 | 顺向价格恢复：resume = true | — | 路径总限时 |

## SET-0043 · 逆势主动失衡失败后的顺势回归

原规则：[v1.0.0](../setups/SET-0043.md)；SHA-256：`632acfa3e35d3ec07c21543c3458afd0225a871e25a813706f1f763991ccbd04`。
原始七轴边界：旧趋势清晰且有独立逆势尝试；该次逆势主动失衡区；主动尝试不续、区被价格否认；反穿逆势区后的旧趋势恢复；失败尝试极值/逆势重新接受；旧趋势下一参考、对向接受退出、当日结束；正确对角失衡+价格趋势

### base

必要数据：bars, classified_trades.
周期：0, 60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `acceptance_close`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 清晰趋势中逆势主动堆叠：trend ≥ 0.55；counter_stacked = true | level ← counter_zone_edge；risk ← counter_zone_risk；logical_floor ← trend_origin | 路径总限时 |
| 2 | 反穿逆势区：directional_price > "@level" | — | 路径总限时 |
| 3 | 顺向价格恢复：resume = true | — | 路径总限时 |

## SET-0044 · 价格破位而累计 Delta 背离的反转

原规则：[v1.0.0](../setups/SET-0044.md)；SHA-256：`fe9d9ded3aecfb29e9d0b262ea2deee8ca169d7c99685b0aabffece8ef6a1138`。
原始七轴边界：先验范围被破但推进拖沓；失败破位外的微观反向结构；价格更低而同窗CVD逐步更高；小结构反向破位后进入；失败极值/原方向重新接受；小范围先处理、原范围后目标、当日结束；同步OHLC+同reset累计Delta

### base

必要数据：bars, classified_trades.
周期：0, 60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `acceptance_close`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 价格破位但同窗CVD背离：cvd_divergence = true | risk ← extreme；trigger ← range_break_edge | 路径总限时 |
| 2 | 微观反向结构破位：directional_extreme > "@trigger" | — | 路径总限时 |

## SET-0045 · 冰山受阻、流转向与弱冲洗后的再拍卖

原规则：[v1.0.0](../setups/SET-0045.md)；SHA-256：`287a4932b41eb0dede6e6a69ca8544689b1dd371db5b76072eb5cd55eb0ee67f`。
原始七轴边界：已知供给受阻且负转正流已发生；旧供给下的第二次弱冲洗低点；供给→跌→流转正→再受阻→弱冲洗；弱冲洗保持后买入恢复（编辑版）；冲洗低点外/强卖重新接受低价；旧供给先目标、重新评估突破、当日结束；Footprint买卖归类+多段价/量比较

### base

必要数据：bars, classified_trades, depth.
周期：0, 60, 180, 300, 900 秒；最长未触发时间 60 分钟；失效方式 `acceptance_close`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 供给受阻：supply_block = true | level ← supply；risk ← extreme | 路径总限时 |
| 2 | 第一次下跌：pullback = true | energy ← counter_energy | 路径总限时 |
| 3 | 流转正：flow_turn = true | — | 路径总限时 |
| 4 | 再受供给阻挡：captured_retest = true | — | 路径总限时 |
| 5 | 第二次弱冲洗：weak_flush = true；counter_energy < "@energy" | risk ← signal_risk | 路径总限时 |
| 6 | 顺向价格恢复：resume = true | — | 路径总限时 |

## SET-0046 · 弱多头背景下黏住卖价的向下清仓

原规则：[v1.0.0](../setups/SET-0046.md)；SHA-256：`f5ffe24d4ea6bedd03fd8c6b8d1e34993b562a745eaeb248d871b1294eece2a6`。
原始七轴边界：前日平衡高收、现金上攻失败；较大平衡外侧与未测隔夜低；黏卖价+bid轻易成交+反弹浅；连续预期DOM行为确认时空；失败高外/买方恢复与反弹加深；隔夜低参考、每次更深反弹分批、当日结束；分session Profile+DOM/逐笔动态

### base

必要数据：bars, depth, classified_trades, prior_volume_profile, prior_session.
周期：0, 60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `acceptance_close`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 弱多背景现金上攻失败：weak_long_context = true；overnight_failure = true | risk ← extreme | 路径总限时 |
| 2 | 黏卖价真实成交并浅反弹：sticky_offer = true；shallow_bounce = true | — | 路径总限时 |
| 3 | 卖流延续：flow_resume = true；resume = true | — | 路径总限时 |

## SET-0047 · 厚市场被动承接的缓慢磨过

原规则：[v1.0.0](../setups/SET-0047.md)；SHA-256：`24262428011069da8b0a14329cfb3950c2926098f7548dc49399864ce18cc7c5`。
原始七轴边界：厚市场开盘下驱动后收复起点；驱动起点与前HVN；持续被动承接、逐档小量消耗且不需爆速；承接未失效时缓慢磨过起点；承接底/HVN破坏并接受下方；下一价格参考、容许慢持但日内平；DOM补单/真实成交+HVN+短价

### base

必要数据：bars, depth, classified_trades, prior_volume_profile.
周期：0, 60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `acceptance_close`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 开盘下驱动后持续被动承接：drive_recovery = true；passive_support = true | level ← drive_origin；risk ← extreme | 路径总限时 |
| 2 | 低爆发缓慢磨过起点：directional_price > "@level"；slow_drift = true；passive_support = true | — | 路径总限时 |

## SET-0048 · 按同一时刻历史波动定义的日内噪声带动量

原规则：[v1.0.0](../setups/SET-0048.md)；SHA-256：`8a484fd8d251a35c5a629e9f863a2559a4600cdff43b78051a1c00e9edbea7a1`。
原始七轴边界：有完整过去14日同刻基准；gap调整的动态上下噪声带；当前同刻价格处于带外；仅HH:00/HH:30长上短下；选定反侧/同侧/VWAP带半小时检查；16:00平仓、仓位/杠杆版本另记；分钟OHLCV+因果14日同刻均值

### base

必要数据：bars, same_time14, previous_close.
周期：60 秒；最长未触发时间 390 分钟；失效方式 `sampled_close`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 完整同刻14日带外采样：noise_break = true | risk ← opposite_band | 路径总限时 |
| 2 | 采样时刻成立：half_hour = true | — | 路径总限时 |

止损后的后续采样可形成新事件；要求价格仍在带外且新的保护位置有效，同一止损帧不再入。
### same_side_band

必要数据：bars, same_time14, previous_close.
周期：60 秒；最长未触发时间 390 分钟；失效方式 `sampled_close`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 完整同刻14日带外采样：noise_break = true | risk ← same_band | 路径总限时 |
| 2 | 采样时刻成立：half_hour = true | — | 路径总限时 |

止损后的后续采样可形成新事件；要求价格仍在带外且新的保护位置有效，同一止损帧不再入。
### same_side_vwap

必要数据：bars, same_time14, previous_close, exact_vwap.
周期：60 秒；最长未触发时间 390 分钟；失效方式 `sampled_close`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 完整同刻14日带外采样：noise_break = true | risk ← vwap_band | 路径总限时 |
| 2 | 采样时刻成立：half_hour = true | — | 路径总限时 |

止损后的后续采样可形成新事件；要求价格仍在带外且新的保护位置有效，同一止损帧不再入。

## SET-0049 · 首段收益方向驱动的最后半小时

原规则：[v1.0.0](../setups/SET-0049.md)；SHA-256：`d35069371662f396c9d9f84b1f1d2623b6b8503d20db215d5d82896a609bde4e`。
原始七轴边界：完整现金日、有昨收和10点价；固定15:30入口/16点退出；首段昨收至10点收益符号；15:30长正短非正；联合版另记；时间退出；价格硬风险仅编辑版；16:00全部平、真实收盘执行成本；昨收+10点+15:30/16点数据与纽约时钟

### base

必要数据：bars, previous_close, first_half_hour.
周期：60 秒；最长未触发时间 390 分钟；失效方式 `extreme_break`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 15:30首段符号已知：last_half_hour = true；early_sign = true | — | 路径总限时 |
| 2 | 固定时刻方向事件：last_half_hour = true | — | 路径总限时 |
### joint_sign

必要数据：bars, previous_close, first_half_hour.
周期：60 秒；最长未触发时间 390 分钟；失效方式 `extreme_break`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 15:30首段符号已知：last_half_hour = true；joint_sign = true | — | 路径总限时 |
| 2 | 固定时刻方向事件：last_half_hour = true | — | 路径总限时 |

## SET-0050 · 隔夜方向失败后反穿现金开盘线

原规则：[v1.0.0](../setups/SET-0050.md)；SHA-256：`b56182e36223591afc5c86929dafc01fd2d3cdb6414aab036dd30a7dc19facad`。
原始七轴边界：隔夜已偏向并等待现金延续；当日现金开盘线及隔夜极值；现金未改善极值、原方向被否认；反穿今日cash open的反向入口；失败隔夜极值外/原方向重新接受；前日cash close缺口路线、当天结束；分cash/overnight OHLC与开盘时钟

### base

必要数据：bars, prior_session, premarket.
周期：60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `extreme_break`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 隔夜方向极值延续失败：overnight_failure = true | risk ← overnight_risk；level ← cash_open | 路径总限时 |
| 2 | 反穿现金开盘线：directional_price > "@level" | — | 路径总限时 |
### early_extreme

必要数据：bars, prior_session, premarket.
周期：60, 180, 300, 900 秒；最长未触发时间 180 分钟；失效方式 `extreme_break`。

| 步骤 | 顺序与条件 | 冻结参考 | 限时 |
|---|---|---|---|
| 1 | 隔夜方向极值延续失败：overnight_failure = true | risk ← overnight_risk；level ← cash_open | 路径总限时 |
| 2 | 隔夜极值失败观测：overnight_failure = true | — | 路径总限时 |

允许同刻判断：两个条件均来自当时已收到的观测。
