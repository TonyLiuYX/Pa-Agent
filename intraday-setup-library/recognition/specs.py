"""Executable rule graph definitions, linked to unchanged Markdown source records.

All qualitative -> numerical translations are editor proposals, not source claims.
`catalog.json` is generated from these definitions and source Markdown.
"""
from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RULE_VERSION = "0.2.2"


def stage(name, *tests, capture=None, deadline_bars=None):
    return {"name": name, "tests": [list(t) for t in tests],
            "capture": capture or {}, "deadline_bars": deadline_bars}


def yes(name):
    return (name, "eq", True)


def gt(name, value):
    return (name, "gt", value)


def ge(name, value):
    return (name, "ge", value)


def lt(name, value):
    return (name, "lt", value)


def rule(number, family, required, stages, *, directions=(1, -1), variants=(), ttl=180,
         cancel=(), scale=None, notes=""):
    return dict(setup_id=f"SET-{number:04}", family=family,
                requirements=["bars", *required], stages=deepcopy(stages), directions=list(directions),
                variants=list(variants), ttl_minutes=ttl, cancel=[list(x) for x in cancel],
                scales=list(scale or (60, 180, 300, 900)), notes=notes)


def definitions():
    S, Y = stage, yes
    impulse = ("impulse", "ge", .65)
    trend = ("trend", "ge", .55)
    restore = S("顺向价格恢复", Y("resume"))
    rules = [
        rule(1, "range_failure", ["prior_session"], [
            S("前日区间越界", Y("opening"), Y("prior_opposite_break"), capture={"level": "prior_entry", "risk": "extreme"}),
            S("回收并形成反向信号", Y("prior_reclaim"), Y("signal"), capture={"trigger": "signal_edge"}),
            S("越过信号端点", gt("directional_extreme", "@trigger"))], variants=("intraday_position",)),
        rule(2, "clock_formula", ["daily_atr14", "liquidity_history"], [
            S("完整五分钟区间及方向", Y("orb_ready"), Y("orb_direction"), Y("orb_liquidity"),
              capture={"trigger": "orb_edge", "risk": "orb_stop"}),
            S("顺向触及区间端点", ge("directional_extreme", "@trigger"))], scale=(60,), ttl=390,
             notes="主版5分钟、单向、10%前14日ATR风险；成交不在此模拟。"),
        rule(3, "flow", ["classified_trades", "depth", "key_levels"], [
            S("关键位吸收", Y("key_near"), Y("absorption"), capture={"level": "key", "risk": "extreme"}),
            S("反向流接力", Y("flow_turn"), Y("resume"))], ttl=20),
        rule(4, "profile", ["prior_volume_profile", "classified_trades"], [
            S("显著旧VPOC穿越", Y("prominent_poc"), Y("poc_break"), capture={"level": "poc", "risk": "extreme"}),
            S("首次回测拒绝", Y("poc_retest"), Y("signal")), restore], variants=("active_cross",)),
        rule(5, "strict_bar", [], [
            S("严格内包后双端外移", Y("hikkake_start"), capture={"trigger": "hikkake_trigger", "risk": "hikkake_risk"}),
            S("三柱内反侧触及", ge("directional_extreme", "@trigger"), deadline_bars=3)],
             variants=("extended_window",), ttl=60),
        rule(6, "pullback", [], [
            S("趋势首回撤", trend, Y("pullback"), capture={"risk": "extreme"}),
            S("第一次恢复", Y("resume")), S("恢复失败再回撤", Y("pullback")),
            S("第二顺向信号", Y("signal"), capture={"trigger": "signal_edge"}),
            S("第二端点触发", gt("directional_extreme", "@trigger"))], variants=("ema_reference",)),
        rule(7, "reversal", [], [S("旧趋势末段三推", Y("three_push"), capture={"risk": "extreme"}),
            S("反向信号", Y("signal"), capture={"trigger": "signal_edge"}),
            S("信号端点触发", gt("directional_extreme", "@trigger"))], variants=("parabolic",)),
        rule(8, "reversal", [], [S("旧趋势结构破坏", Y("trend_break"), capture={"level": "old_extreme", "risk": "extreme"}),
            S("再测旧极值", Y("old_extreme_retest")), S("第二反转信号", Y("signal"), capture={"trigger": "signal_edge"}),
            S("第二信号端点触发", gt("directional_extreme", "@trigger"))], variants=("lower_low", "higher_low", "double_test")),
        rule(9, "pullback", ["volume_baseline"], [S("强推进后首次浅暂停", Y("first_micro"), capture={"trigger": "pause_edge", "risk": "extreme"}),
            S("成交恢复并突破暂停", gt("directional_extreme", "@trigger"), ge("rvol", 1.0))],
             variants=("ross_10s",), scale=(10, 60, 180, 300)),
        rule(10, "break_retest", ["key_levels"], [S("先验边界有效突破", Y("key_break"), capture={"level": "key", "risk": "extreme"}),
            S("首次回测保持", Y("captured_retest"), Y("signal")), restore], variants=("strict_tick_hold", "double_test")),
        rule(11, "reversal", [], [S("晚趋势水平旗形", Y("late_flag"), capture={"level": "flag_edge", "risk": "extreme"}),
            S("原向突破失败回收", Y("flag_failure")), S("反向信号", Y("signal"), capture={"trigger": "signal_edge"}),
            S("反向信号端点触发", gt("directional_extreme", "@trigger"))]),
        rule(12, "strict_bar", [], [S("三柱以上推进首逆色", Y("retrace_start"), capture={"trigger": "signal_edge", "risk": "signal_risk"}),
            S("反色端点触发", gt("directional_extreme", "@trigger"))]),
        rule(13, "strict_bar", [], [S("趋势双内包", trend, Y("double_inside"), capture={"trigger": "mother_edge", "risk": "mother_risk"}),
            S("母柱端点首次释放", gt("directional_extreme", "@trigger"))], variants=("inner_edge", "close_inside", "triple_inside")),
        rule(14, "reversal", [], [S("旧趋势首次强反向冲击", Y("anti_impulse"), capture={"risk": "extreme"}),
            S("新方向首弱回撤", Y("pullback"), lt("counter_impulse", .6)), S("新向信号", Y("signal")), restore]),
        rule(15, "pullback", ["volume_baseline"], [S("方向推进", trend, impulse, capture={"risk": "extreme"}),
            S("普通缩量回撤", Y("pullback"), Y("low_volume_pullback")), S("价量恢复", Y("resume"), ge("rvol", 1.0))]),
        rule(16, "compression", ["key_levels"], [S("边界压力积累", Y("pressure"), capture={"trigger": "key", "risk": "compression_risk"}),
            S("压缩越界", gt("directional_extreme", "@trigger"), ge("compression", .4))]),
        rule(17, "strict_bar", [], [S("趋势逆势实体柱被试探无跟随", Y("trend_bar_failure"), capture={"trigger": "signal_edge", "risk": "signal_risk"}),
            S("下一柱端点触发", gt("directional_extreme", "@trigger"), deadline_bars=1)], ttl=30),
        rule(18, "range_failure", [], [S("平衡区边缘首次反向", ge("balance", .55), Y("range_edge"), Y("signal"), capture={"risk": "extreme", "level": "range_boundary"}),
            S("再次测试边缘", Y("captured_retest")), S("第二反向信号", Y("signal"), capture={"trigger": "signal_edge"}),
            S("第二信号端点触发", gt("directional_extreme", "@trigger"))]),
        rule(19, "linked_reentry", ["parent_detection"], [S("父模式触发后保护位被扫", Y("parent_sweep"), capture={"risk": "extreme"}),
            S("背景保留并迅速恢复", trend, Y("resume"), capture={"trigger": "signal_edge"}),
            S("新端点触发", gt("directional_extreme", "@trigger"))], ttl=30),
        rule(20, "profile", ["prior_volume_profile", "prior_tpo", "prior_session"], [
            S("前日低量尾部新日近尾开盘", Y("prior_tail_open"), capture={"risk": "prior_tail_risk"}),
            S("尾部不接受且向旧价值恢复", Y("tail_reject"), Y("resume"))], variants=("source_open", "source_0800_BST")),
        rule(21, "opening", ["premarket", "news", "volume_baseline"], [
            S("催化跳空近盘前高点", Y("opening"), Y("catalyst_gap"), Y("near_pm_edge"), capture={"trigger": "pm_edge", "risk": "signal_risk"}),
            S("盘前高动量突破", gt("directional_extreme", "@trigger"), impulse, ge("rvol", 1))],
             directions=(1,), variants=("tight_flag", "sai_gap",)),
        rule(22, "opening", ["premarket"], [S("开盘洗盘测试既有盘前支撑", Y("opening"), Y("pm_wash"), capture={"level": "pm_support", "risk": "extreme"}),
            S("复测保持", Y("captured_retest"), Y("signal"), capture={"trigger": "signal_edge"}),
            S("复测棒高触发", gt("directional_extreme", "@trigger"))], directions=(1,), variants=("early_v",)),
        rule(23, "vwap", ["exact_vwap"], [S("早期弱跌", Y("opening"), Y("weak_open_drop"), capture={"risk": "extreme"}),
            S("VWAP上收盘确认", Y("vwap_reclaim"), capture={"trigger": "signal_edge"}),
            S("下一棒高触发", gt("directional_extreme", "@trigger"), deadline_bars=1)], directions=(1,), variants=("direct_reclaim",)),
        rule(24, "vwap", ["exact_vwap", "classified_trades", "prior_session"], [S("清隔夜参考并穿透VWAP", Y("overnight_failure"), Y("vwap_cross"), capture={"level": "impulse_origin", "risk": "extreme"}),
            S("偏离再探失败", Y("vwap_deviation_fail"), ge("flow_activity", 1)),
            S("启动位置回测拒绝", Y("captured_retest"), Y("flow_resume"))]),
        rule(25, "vwap", ["exact_vwap"], [S("平VWAP平衡伸展", ge("balance", .55), Y("flat_vwap"), Y("vwap_stretch"), capture={"risk": "extreme"}),
            S("新极值失败", Y("failed_extension")), S("局部结构反穿", Y("resume"))], variants=("touch_early",)),
        rule(26, "equity_context", ["historical_vwap", "equity_metadata", "news", "volume_baseline"], [
            S("低流通催化gap触历史供给", Y("boulevard_context"), Y("historical_vwap_reject"), capture={"risk": "extreme"}),
            S("量能断崖", Y("volume_cliff")), S("反弹结构破坏", Y("resume"))], directions=(-1,)),
        rule(27, "correlated", ["synced_market", "daily_context", "volume_baseline"], [S("日线相容且相对强弱成立", Y("daily_aligned"), ge("relative_strength", .6), capture={"risk": "extreme"}),
            S("市场同向确认", Y("market_confirm")), S("个股结构恢复", Y("resume"))], variants=("public_price_proxy",)),
        rule(28, "news", ["news", "prior_session", "volume_baseline"], [S("前日新消息及次日关键位突破", Y("second_day_news"), Y("second_day_break"), capture={"level": "second_day_edge", "risk": "extreme"}),
            S("上方整理接受", Y("accepted"), ge("compression", .3)), restore], variants=("direct_first_break",)),
        rule(29, "news", ["news", "short_bars"], [S("实际到达后首轮方向反应", Y("news_fresh60"), impulse, capture={"risk": "news_risk"}),
            S("首60秒内继续推进", Y("news_fresh60"), Y("resume"))], scale=(1, 5, 10), ttl=1,
             variants=("window90",)),
        rule(30, "correlated", ["synced_market", "classified_trades", "ib"], [S("事前领导关系成立且先行突破", Y("leader_break"), capture={"risk": "extreme"}),
            S("本产品IB外突破接受", Y("ib_break"), Y("accepted"), capture={"level": "ib_edge"}),
            S("本产品流确认续行", Y("flow_resume"), Y("resume"))]),
        rule(31, "news", ["news", "consensus", "synced_market"], [S("预案分支匹配实际意外", Y("macro_surprise"), capture={"risk": "news_risk"}),
            S("多个敏感产品方向相容", Y("cross_confirm"), Y("resume"))], ttl=15),
        rule(32, "news", ["news", "consensus", "synced_market"], [S("新增阶段改变事前净分支", Y("reassessment"), capture={"risk": "news_risk", "level": "signal_edge"}),
            S("新方向真实接受", Y("accepted"), Y("cross_confirm")), restore], ttl=60),
        rule(33, "tpo", ["tpo", "classified_trades"], [S("冻结多时段TPO台阶被突破", Y("ledge_break"), capture={"level": "ledge", "risk": "extreme"}),
            S("外侧接受且流确认", Y("accepted"), Y("flow_resume"))]),
        rule(34, "tpo", ["tpo", "prior_tpo", "classified_trades"], [S("旧价值台阶反复失败", Y("ledge_failed"), capture={"risk": "extreme", "trigger": "range_break_edge"}),
            S("相反范围破位加速", gt("directional_extreme", "@trigger"), ge("flow_activity", 1.3))]),
        rule(35, "ib", ["ib", "prior_volume_profile", "classified_trades"], [S("IB首侧失败", Y("ib_first_failure"), capture={"risk": "extreme"}),
            S("反侧突破", Y("ib_break"), capture={"level": "ib_edge"}),
            S("反侧回收保持", Y("captured_retest"), Y("signal")), S("流恢复", Y("flow_resume"), Y("resume"))]),
        rule(36, "flow_profile", ["classified_trades", "volume_profile"], [S("强冲后高处分布形成", Y("micro_p"), capture={"level": "distribution_edge", "risk": "extreme"}),
            S("分布边缘测试保持", Y("captured_retest"), Y("absorption")), restore], variants=("edge_limit_observation",)),
        rule(37, "tpo", ["prior_tpo", "prior_session"], [S("外开盘返回旧TPO价值", Y("value_return"), capture={"level": "value_entry", "risk": "value_risk"}),
            S("连续内部分钟收盘满60分钟", Y("value_inside60"))], scale=(60,), ttl=390,
             variants=("two_bracket_touch", "volume_va_proxy")),
        rule(38, "profile", ["volume_profile", "classified_trades"], [S("低量伸展旧价值未迁移", Y("profile_stretch"), capture={"risk": "extreme", "level": "prior_value_poc"}),
            S("新极值失败", Y("failed_extension")), S("恢复失败后反穿", Y("resume"))], variants=("double_hold", "early_failure")),
        rule(39, "flow", ["classified_trades", "depth", "volume_profile", "key_levels"], [S("同位低能量失败", Y("key_near"), Y("weak_attack"), capture={"level": "key", "energy": "attack_energy", "risk": "extreme"}),
            S("新攻击能量改善", gt("attack_energy", "@energy"), Y("flow_resume")), S("边界越过", gt("directional_extreme", "@level"))]),
        rule(40, "flow", ["classified_trades", "depth"], [S("原方向末端耗尽", Y("exhaustion"), capture={"risk": "extreme"}),
            S("无法重启后反向轮转", Y("flow_turn"), Y("resume"))], ttl=5),
        rule(41, "flow", ["classified_trades", "depth", "volume_profile"], [S("真实阶梯跳价留下薄区", Y("jump"), capture={"level": "jump_edge", "risk": "jump_risk"}),
            S("薄区反向测试保持", Y("captured_retest"), Y("thin_hold")), S("再推进", Y("flow_resume"), Y("resume"))], ttl=15),
        rule(42, "footprint", ["classified_trades"], [S("同向连续对角堆叠区形成", Y("stacked"), capture={"level": "imbalance_edge", "risk": "imbalance_risk"}),
            S("固定区回测保持", Y("captured_retest")), restore], variants=("ratio150",)),
        rule(43, "footprint", ["classified_trades"], [S("清晰趋势中逆势主动堆叠", trend, Y("counter_stacked"), capture={"level": "counter_zone_edge", "risk": "counter_zone_risk"}),
            S("反穿逆势区", gt("directional_price", "@level")), restore]),
        rule(44, "footprint", ["classified_trades"], [S("价格破位但同窗CVD背离", Y("cvd_divergence"), capture={"risk": "extreme", "trigger": "range_break_edge"}),
            S("微观反向结构破位", gt("directional_extreme", "@trigger"))]),
        rule(45, "flow", ["classified_trades", "depth"], [S("供给受阻", Y("supply_block"), capture={"level": "supply", "risk": "extreme"}),
            S("第一次下跌", Y("pullback"), capture={"energy": "counter_energy"}), S("流转正", Y("flow_turn")),
            S("再受供给阻挡", Y("captured_retest")), S("第二次弱冲洗", Y("weak_flush"), lt("counter_energy", "@energy"), capture={"risk": "signal_risk"}), restore],
             directions=(1,), ttl=60),
        rule(46, "flow", ["depth", "classified_trades", "prior_volume_profile", "prior_session"], [S("弱多背景现金上攻失败", Y("weak_long_context"), Y("overnight_failure"), capture={"risk": "extreme"}),
            S("黏卖价真实成交并浅反弹", Y("sticky_offer"), Y("shallow_bounce")), S("卖流延续", Y("flow_resume"), Y("resume"))], directions=(-1,)),
        rule(47, "flow", ["depth", "classified_trades", "prior_volume_profile"], [S("开盘下驱动后持续被动承接", Y("drive_recovery"), Y("passive_support"), capture={"level": "drive_origin", "risk": "extreme"}),
            S("低爆发缓慢磨过起点", gt("directional_price", "@level"), Y("slow_drift"), Y("passive_support"))], directions=(1,)),
        rule(48, "clock_formula", ["same_time14", "previous_close"], [S("完整同刻14日带外采样", Y("noise_break"), capture={"risk": "opposite_band"}),
            S("采样时刻成立", Y("half_hour"))], scale=(60,), ttl=390, variants=("same_side_band", "same_side_vwap")),
        rule(49, "clock_formula", ["previous_close", "first_half_hour"], [S("15:30首段符号已知", Y("last_half_hour"), Y("early_sign")),
            S("固定时刻方向事件", Y("last_half_hour"))], scale=(60,), ttl=390, variants=("joint_sign",)),
        rule(50, "overnight", ["prior_session", "premarket"], [S("隔夜方向极值延续失败", Y("overnight_failure"), capture={"risk": "overnight_risk", "level": "cash_open"}),
            S("反穿现金开盘线", gt("directional_price", "@level"))], variants=("early_extreme",)),
    ]
    # Compatible observations may share a frame; a newly captured signal's
    # endpoint still requires a later observation. Indices are zero based.
    simultaneous = {4: (2,), 10: (2,), 11: (2,), 18: (2,), 19: (1,),
                    20: (1,), 21: (1,), 23: (1,), 27: (2,), 31: (1,),
                    34: (1,), 35: (1,)}
    for r in rules:
        number = int(r["setup_id"].split("-")[1])
        for index in simultaneous.get(number, ()):
            r["stages"][index]["same_frame"] = True
        if number == 2:
            r["continuation_scales"] = [0]
        if number == 24:
            r["scales"] = [1, 5, 10, *r["scales"]]
        if number == 48:
            r["reentry_after_invalidation"] = True
        if r["family"] in {"flow", "flow_profile", "footprint"} or r["setup_id"] in {"SET-0024", "SET-0030", "SET-0031", "SET-0032", "SET-0033", "SET-0034", "SET-0035"}:
            r["scales"] = [0, *r["scales"]]
        if r["setup_id"] == "SET-0029":
            r["scales"] = [0, *r["scales"]]
        r["invalidation_mode"] = "sampled_close" if r["setup_id"] == "SET-0048" else "atr_touch" if r["setup_id"] == "SET-0002" else "acceptance_close" if r["family"] in {"profile", "tpo", "flow", "flow_profile", "footprint", "vwap", "ib"} else "extreme_break"
        if r["setup_id"] in {"SET-0006", "SET-0009", "SET-0013", "SET-0015", "SET-0017", "SET-0019", "SET-0027", "SET-0043"}:
            r["stages"][0]["capture"]["logical_floor"] = "trend_origin"
    return {r["setup_id"]: r for r in rules}


# Variants keep their own identity; a proxy or early observation is never silently
# substituted for the source setup's confirmed path.
VARIANTS = {
    ("SET-0001", "intraday_position"): {"transform": "remove_opening"},
    ("SET-0004", "active_cross"): {"transform": "active_poc"},
    ("SET-0005", "extended_window"): {"deadline": 6},
    ("SET-0006", "ema_reference"): {"transform": "ema"},
    ("SET-0007", "parabolic"): {"transform": "parabolic"},
    ("SET-0008", "lower_low"): {"transform": "retest_beyond"},
    ("SET-0008", "higher_low"): {"transform": "retest_inside"},
    ("SET-0008", "double_test"): {"transform": "retest_equal"},
    ("SET-0009", "ross_10s"): {"transform": "ross"},
    ("SET-0010", "strict_tick_hold"): {"transform": "strict_hold"},
    ("SET-0010", "double_test"): {"transform": "double_retest"},
    ("SET-0013", "inner_edge"): {"trigger": "inner_edge"},
    ("SET-0013", "close_inside"): {"feature": "close_double_inside"},
    ("SET-0013", "triple_inside"): {"feature": "triple_inside"},
    ("SET-0020", "source_open"): {"transform": "source_open"},
    ("SET-0020", "source_0800_BST"): {"transform": "london_0800"},
    ("SET-0021", "tight_flag"): {"transform": "tight_flag"},
    ("SET-0021", "sai_gap"): {"transform": "sai_gap"},
    ("SET-0022", "early_v"): {"transform": "early_v"},
    ("SET-0023", "direct_reclaim"): {"transform": "direct_vwap"},
    ("SET-0025", "touch_early"): {"transform": "early_stretch"},
    ("SET-0027", "public_price_proxy"): {"transform": "public_price"},
    ("SET-0028", "direct_first_break"): {"transform": "direct_news"},
    ("SET-0029", "window90"): {"fresh": "news_fresh90", "ttl": 1.5},
    ("SET-0036", "edge_limit_observation"): {"transform": "distribution_edge"},
    ("SET-0037", "two_bracket_touch"): {"transform": "bracket_touch"},
    ("SET-0037", "volume_va_proxy"): {"transform": "volume_va"},
    ("SET-0038", "double_hold"): {"transform": "double_hold"},
    ("SET-0038", "early_failure"): {"transform": "early_profile_failure"},
    ("SET-0042", "ratio150"): {"feature": "stacked150", "zone_prefix": "imbalance150"},
    ("SET-0048", "same_side_band"): {"risk": "same_band"},
    ("SET-0048", "same_side_vwap"): {"risk": "vwap_band", "require": "exact_vwap"},
    ("SET-0049", "joint_sign"): {"feature": "joint_sign"},
    ("SET-0050", "early_extreme"): {"transform": "early_overnight"},
}


def transform_variant(spec, change):
    name = change.get("transform")
    steps = spec["stages"]
    if name == "remove_opening":
        steps[0]["tests"] = [t for t in steps[0]["tests"] if t[0] != "opening"]
    elif name == "active_poc":
        spec["stages"] = [steps[0], stage("VPOC与IB同向穿越", yes("ib_break"))]
        spec["requirements"].append("ib")
        spec["co_temporal"] = True
    elif name == "ema":
        steps[-2]["tests"].append(list(yes("ema_near")))
    elif name == "parabolic":
        steps[0]["tests"].append(list(yes("parabolic")))
    elif name and name.startswith("retest_"):
        # HL/LL are explicitly distinct versions, not the narrow base retest
        # with an extra predicate that still rejects their wider locations.
        steps[1]["tests"] = [list(yes(name)) if t[0] == "old_extreme_retest" else t
                             for t in steps[1]["tests"]]
    elif name == "ross":
        spec["scales"] = [10]
    elif name == "strict_hold":
        steps[1]["tests"].append(list(yes("strict_hold")))
    elif name == "double_retest":
        spec["stages"] = steps[:2] + [stage("第一次恢复", yes("resume")),
            stage("第二次边界保持", yes("captured_retest"), yes("signal")), steps[-1]]
    elif name == "source_open":
        spec["requirements"] = [r for r in spec["requirements"] if r != "bars"] + ["price_observation"]
        spec["scales"] = [0]
        spec["stages"] = [stage("近前日低量尾部开盘", yes("prior_tail_open"), yes("instant_open"),
                          capture={"risk": "prior_tail_risk", "observed_price": "directional_price", "price_basis": "price_basis"}),
                          stage("实际开盘价观测", yes("instant_open"))]
        spec["co_temporal"] = True
    elif name == "london_0800":
        steps[-1]["tests"].extend([list(yes("london_0800")), list(ge("volume_increase", 1.2))])
        spec["requirements"].append("volume_baseline")
    elif name == "tight_flag":
        steps[0]["capture"]["trigger"] = "tight_flag_edge"
        steps[0]["tests"].append(list(ge("compression", .5)))
    elif name == "sai_gap":
        steps[0]["tests"] = [["sai_catalyst_gap", "eq", True] if t[0] == "catalyst_gap" else t for t in steps[0]["tests"]]
    elif name == "early_v":
        spec["stages"] = [steps[0], stage("首轮支撑恢复", yes("signal"), yes("resume"))]
    elif name == "direct_vwap":
        spec["stages"] = steps[:2]
    elif name == "early_stretch":
        spec["stages"] = [steps[0], stage("已观测的伸展位置", yes("vwap_stretch"))]
        spec["co_temporal"] = True
    elif name == "public_price":
        spec["notes"] += "公开价格代理；不提供1OP/1OSI复刻。"
    elif name == "direct_news":
        spec["stages"] = [steps[0], stage("首轮越过关键位", gt("directional_price", "@level"))]
        spec["co_temporal"] = True
    elif name == "distribution_edge":
        spec["stages"] = steps[:2]
        spec["notes"] += "仅标边缘保持的识别时点，不表示限价成交。"
    elif name == "bracket_touch":
        steps[-1]["tests"] = [list(yes("value_two_brackets"))]
        spec["requirements"].append("tpo")
    elif name == "volume_va":
        spec["requirements"] = ["prior_volume_profile" if r == "prior_tpo" else r for r in spec["requirements"]]
        for st in steps:
            st["tests"] = [["volume_" + t[0], *t[1:]] if t[0] in {"value_return", "value_inside60"} else t for t in st["tests"]]
            st["capture"] = {k: "volume_" + v if v in {"value_entry", "value_risk"} else v for k, v in st["capture"].items()}
        spec["semantic_status"] = "data_proxy"
    elif name == "double_hold":
        steps[0]["capture"]["level"] = "extreme"
        spec["stages"] = steps[:2] + [stage("首次保持后反弹", yes("resume")),
            stage("第二次极值保持", yes("captured_retest")), steps[-1]]
    elif name == "early_profile_failure":
        spec["stages"] = steps[:2]
    elif name == "early_overnight":
        spec["stages"] = [steps[0], stage("隔夜极值失败观测", yes("overnight_failure"))]
        spec["co_temporal"] = True


def catalog():
    out = []
    for setup_id, definition in definitions().items():
        path = ROOT / "setups" / f"{setup_id}.md"
        raw = path.read_text(encoding="utf-8")
        metadata = json.loads(raw.split("---", 2)[1])
        out.append(dict(
            **definition, rule_version=RULE_VERSION, name_zh=metadata["name_zh"],
            source_version=metadata["version"], source_sha256=hashlib.sha256(raw.encode()).hexdigest(),
            source_file=path.relative_to(ROOT).as_posix(), evidence_ids=metadata["evidence_ids"],
            core_invariants=metadata["logic_fingerprint"], source_markets=metadata["markets_observed"],
            condition_provenance="editor_operationalization/editor_proposal_not_tested",
            validation_status="synthetic_only; real_market_validation_not_run",
            variant_status={v: "implemented_experimental" if (setup_id, v) in VARIANTS else "specified_not_implemented"
                            for v in definition["variants"]},
            required_test_scenarios=["ordered_positive", "missing_core", "out_of_order", "missing_data", "expiry", "invalidation"],
        ))
    return out


def load_specs(setup_ids=None, include_variants=False):
    import copy
    rows = catalog()
    chosen = set(setup_ids) if setup_ids else {r["setup_id"] for r in rows}
    if chosen - {r["setup_id"] for r in rows}:
        raise ValueError("unknown setup IDs")
    out = []
    for row in rows:
        if row["setup_id"] not in chosen:
            continue
        out.append(dict(copy.deepcopy(row), variant_id="base"))
        if not include_variants:
            continue
        for variant in row["variants"]:
            change = VARIANTS.get((row["setup_id"], variant))
            if not change:
                continue
            clone = dict(copy.deepcopy(row), variant_id=variant)
            transform_variant(clone, change)
            for st in clone["stages"]:
                if "deadline" in change and st["deadline_bars"] is not None:
                    st["deadline_bars"] = change["deadline"]
                for test in st["tests"]:
                    if "fresh" in change and test[0] == "news_fresh60":
                        test[0] = change["fresh"]
                    if "feature" in change and test[0] in {"double_inside", "stacked", "early_sign"}:
                        test[0] = change["feature"]
                if "trigger" in change and "trigger" in st["capture"]:
                    st["capture"]["trigger"] = change["trigger"]
                if "risk" in change and "risk" in st["capture"]:
                    st["capture"]["risk"] = change["risk"]
                if "zone_prefix" in change:
                    st["capture"] = {k: v.replace("imbalance_", change["zone_prefix"] + "_") for k, v in st["capture"].items()}
            if "ttl" in change:
                clone["ttl_minutes"] = change["ttl"]
            if "require" in change:
                clone["requirements"].append(change["require"])
            out.append(clone)
    return out


def catalog_document():
    """One generated contract includes base definitions and executable variants."""
    return {"rule_version": RULE_VERSION, "setups": catalog(),
            "variant_graphs": [s for s in load_specs(include_variants=True) if s["variant_id"] != "base"]}


def catalog_markdown(document=None):
    doc = document or catalog_document()
    variants = doc["variant_graphs"]
    lines = ["# 50 个 setup 的可执行识别目录", "",
        "由 recognition/specs.py 与原 Markdown 主记录生成；运行 build.py 重建，勿直接编辑。", "",
        f"规则引擎 v{RULE_VERSION}：50 个基础路径 + {len(variants)} 个独立标识的变体。",
        "数值解释均为 editor_operationalization / editor_proposal_not_tested；真实行情验证尚未执行。",
        "变体、早期观测与数据代理不增加 setup 数量，也不继承原研究绩效。", "",
        "周期以秒表示；0 为实际到达数据组成的事件窗口，source_open 的 0 为真实开盘价格观测。",
        "状态路径按时间推进；same_frame 明示允许与前一步共用兼容观测。新信号端点默认仍等后续观测。多空统一使用 direction × price 坐标。", "",
        "[运行与输入格式](README.md) · [评价协议](../evaluation/README.md) · [机器目录](catalog.json)", "",
        "| Setup | 家族 | 识别周期/秒 | 变体 |", "|---|---|---|---|"]
    for s in doc["setups"]:
        lines.append(f'| [{s["setup_id"]} · {s["name_zh"]}](../{s["source_file"]}) | {s["family"]} | '
                     + ", ".join(map(str, s["scales"])) + " | " + ", ".join(s["variants"]) + " |")
    ops = {"eq": "=", "gt": ">", "ge": "≥", "lt": "<", "le": "≤"}
    for base in doc["setups"]:
        lines.extend(["", f'## {base["setup_id"]} · {base["name_zh"]}', "",
            f'原规则：[v{base["source_version"]}](../{base["source_file"]})；SHA-256：`{base["source_sha256"]}`。',
            "原始七轴边界：" + "；".join(base["core_invariants"]), ""])
        for s in [dict(base, variant_id="base"), *[v for v in variants if v["setup_id"] == base["setup_id"]]]:
            lines.extend([f'### {s["variant_id"]}', "",
                "必要数据：" + ", ".join(s["requirements"]) + ".",
                "周期：" + ", ".join(map(str, s["scales"])) + f' 秒；最长未触发时间 {s["ttl_minutes"]} 分钟；'
                + f'失效方式 `{s["invalidation_mode"]}`。', "",
                "| 步骤 | 顺序与条件 | 冻结参考 | 限时 |", "|---|---|---|---|"])
            for i, step in enumerate(s["stages"], 1):
                conditions = "；".join(f"{f} {ops[op]} {json.dumps(v, ensure_ascii=False)}" for f, op, v in step["tests"])
                captures = "；".join(f"{k} ← {v}" for k, v in step["capture"].items()) or "—"
                deadline = f'{step["deadline_bars"]} 个完整柱（含末柱）' if step["deadline_bars"] else "路径总限时"
                timing = "（可与前一步同帧）" if step.get("same_frame") else ""
                lines.append(f'| {i} | {step["name"]}{timing}：{conditions} | {captures} | {deadline} |')
            if s.get("co_temporal"):
                lines.extend(["", "允许同刻判断：两个条件均来自当时已收到的观测。"])
            if s.get("continuation_scales"):
                lines.extend(["", "已建立路径可用续行周期：" + ", ".join(map(str, s["continuation_scales"])) + " 秒；保留建立周期与实际观测周期。"])
            if s.get("reentry_after_invalidation"):
                lines.extend(["", "止损后的后续采样可形成新事件；要求价格仍在带外且新的保护位置有效，同一止损帧不再入。"])
            if s["notes"]:
                lines.extend(["", s["notes"]])
    return "\n".join(lines) + "\n"
