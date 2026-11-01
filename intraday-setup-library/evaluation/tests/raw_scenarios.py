"""Raw market-event scenarios. Never replaces make_frame or injects features.

The fixture archive contains input-only copies of the project's earlier audit.
Every positive expectation must be a complete engine trigger from these events.
"""
from dataclasses import dataclass, replace
from datetime import timedelta
from pathlib import Path
import hashlib
import json

from recognition.config import RecognitionConfig
from recognition.engine import RecognitionEngine
from recognition.models import MarketEvent
from recognition.specs import load_specs
from .support import OPEN, bar, event, session, reference, history14


@dataclass
class RawScenario:
    scenario_id: str
    events: list[MarketEvent]
    setup_ids: tuple[str, ...]
    config: RecognitionConfig | None = None
    expected_graphs: tuple[str, ...] = ()
    provenance: dict | None = None

    def run(self):
        engine = RecognitionEngine(list(self.setup_ids), include_variants=True, config=self.config)
        rows = [r for e in self.events for r in engine.feed(e)]
        return engine, rows


def make_case(name, ids, candles, extra=None, *, preevents=(), config=None, expected=()):
    events = [session(extra), *preevents]
    events += [bar(i, *c) for i, c in enumerate(candles, 1)]
    return RawScenario(name, events, tuple(f"SET-{i:04}" for i in ids), config, tuple(expected),
                       {"scope": "synthetic_raw_market_events", "author": "project_regression"})


def frozen_audit_scenarios():
    rows = json.loads(Path(__file__).with_name("raw_fixtures.json").read_text())
    positives = {
        'audit:SET-0001-S1': ('SET-0001:base', 'SET-0001:intraday_position'),
        'audit:SET-0013-S1': ('SET-0013:close_inside',),
        'audit:SET-0020-S1': ('SET-0020:source_open',),
        'audit:SET-0023-S1': ('SET-0023:base', 'SET-0023:direct_reclaim'),
        'audit:SET-0035-S1': ('SET-0035:base',),
        'audit:SET-0048-S5': ('SET-0048:base', 'SET-0048:same_side_band', 'SET-0048:same_side_vwap'),
        'audit:SET-0050-S1': ('SET-0050:base', 'SET-0050:early_extreme'),
    }
    for row in rows:
        config = row['config']
        if config:
            config = dict(config)
            if 'swing_factors' in config:
                config['swing_factors'] = tuple(config['swing_factors'])
            config = RecognitionConfig(**config).validate()
        yield RawScenario(row['scenario_id'], [MarketEvent.from_dict(e) for e in row['events']],
                          (row['setup_id'],), config, positives.get(row['scenario_id'], ()), row['origin'])


def price_scenarios():
    # Exact four-bar hikkake and its outside-bar counterexample.
    shapes = [(10, 10.4, 9.6, 10.1), (10.1, 10.2, 9.8, 9.95), (9.95, 10.1, 9.5, 9.65), (9.65, 10.3, 9.6, 10.25)]
    yield make_case('hikkake-long', [5], shapes, expected=('SET-0005:base', 'SET-0005:extended_window'))
    yield make_case('hikkake-short', [5], [(20-o,20-l,20-h,20-c) for o,h,l,c in shapes], expected=('SET-0005:base',))
    yield make_case('hikkake-outside-rejected', [5], [*shapes[:2], (9.95,10.5,9.5,9.65), shapes[-1]])
    yield make_case('five-minute-orb', [2], [(10,10+.1*i,9.9,10+.05*i) for i in range(1,6)] + [(10.25,10.8,10.24,10.7)],
                    {'references': {'atr14':reference(10.),'average_daily_volume14':reference(2_000_000)}}, expected=('SET-0002:base',))
    trend=[(100+.1*(i-1),100+.1*i+.03,100+.1*(i-1)-.03,100+.1*i) for i in range(1,13)]
    yield make_case('trend-bar-failure',[17],trend+[(101.2,101.23,100.98,101.),(101.,101.1,100.96,101.08),(101.08,101.2,101.05,101.18)],expected=('SET-0017:base',))
    yield make_case('first-retrace',[12],[(100,100.05,99.85,99.9),(99.9,99.95,99.75,99.8),(99.8,99.85,99.65,99.7),(99.7,99.95,99.65,99.9),(99.9,100.05,99.85,100)],expected=('SET-0012:base',))
    yield make_case('double-inside-release',[13],trend+[(101.2,101.8,100.8,101.3),(101.3,101.6,101.,101.4),(101.4,101.5,101.1,101.45),(101.45,101.9,101.4,101.85)], expected=('SET-0013:base','SET-0013:inner_edge','SET-0013:close_inside'))
    yield make_case('triple-inside-release',[13],trend+[(101.2,102.,100.6,101.3),(101.3,101.8,100.8,101.4),(101.4,101.6,101.,101.45),(101.45,101.5,101.1,101.47),(101.47,102.1,101.4,102.)],expected=('SET-0013:triple_inside',))
    yield make_case('second-entry',[6],trend+[(101.2,101.22,100.9,101.),(101.,101.35,100.98,101.3),(101.3,101.31,101.05,101.1),(101.1,101.32,101.08,101.3),(101.3,101.5,101.25,101.45)],expected=('SET-0006:base',))
    bear=[(101-.1*(i-1),101-.1*(i-1)+.03,101-.1*i-.03,101-.1*i) for i in range(1,13)]
    yield make_case('strong-reversal-first-pullback',[14],bear+[(99.8,100.4,99.77,100.35),(100.35,100.36,100.25,100.3),(100.3,100.55,100.27,100.5),(100.5,100.7,100.47,100.65)],expected=('SET-0014:base',))
    yield make_case('key-break-retest',[10],[(99.85,99.89,99.8,99.85)]*5+[(99.85,100.1,99.83,100.06),(100.01,100.08,99.99,100.06),(100.06,100.16,100.04,100.12)], {'references':{'key_level':reference(100.)}},expected=('SET-0010:base','SET-0010:strict_tick_hold'))
    yield make_case('key-double-retest',[10],[(99.85,99.89,99.8,99.85)]*5+[(99.85,100.1,99.83,100.06),(100.01,100.08,99.99,100.06),(100.06,100.16,100.04,100.12),(100.04,100.15,99.99,100.1),(100.1,100.25,100.07,100.2)],{'references':{'key_level':reference(100.)}},expected=('SET-0010:double_test',))
    tp={'kind':'tpo','venue':'SYN','val':100.,'vah':110.,'poc':105.}
    yield make_case('sixty-minute-value-acceptance',[37],[(99.,103.,99.,102.)]+[(102.,103.,101.,102.)]*59,
                    {'references':{'prior_tpo':reference(tp),'prev_close':reference(105.),'prev_high':reference(111.),'prev_low':reference(99.)}},expected=('SET-0037:base',))
    yield make_case('fixed-last-half-hour',[49],[(100.,100.1,99.9,100.)]*390, {'references':{'prev_close':reference(100.)}},expected=('SET-0049:base',))



def full_history():
    rows=history14()
    clocks=[(OPEN+timedelta(seconds=i)).strftime('%H:%M:%S') for i in range(10,23401,10)]
    minute_clocks=[(OPEN+timedelta(minutes=i)).strftime('%H:%M') for i in range(1,391)]
    for row in rows:
        row['volumes']={clock:1000. for clock in clocks+minute_clocks}
        row['closes']={clock:101. for clock in minute_clocks}
    return rows


def additional_price_scenarios():
    yield make_case('three-push-reversal',[7],[(100,100.05,99.65,99.7),(99.7,100.02,99.65,100),(100,100.02,99.4,99.45),(99.45,99.8,99.42,99.75),(99.75,99.78,99.1,99.15),(99.15,99.5,99.12,99.45),(99.45,99.5,98.8,98.85),(98.85,99.15,98.83,99.1),(99.1,99.65,99.08,99.6)], expected=('SET-0007:base',))
    trend=[(100+.1*(i-1),100+.1*i+.03,100+.1*(i-1)-.03,100+.1*i) for i in range(1,13)]
    # First pullback is kept separate from the later EMA reference variant.
    yield make_case('ordinary-volume-pullback',[15],trend+[(101.2,101.9,101.18,101.85,2000),(101.85,101.86,101.5,101.55,100),(101.55,101.58,101.2,101.25,100),(101.25,101.28,100.9,100.95,100),(100.95,101.6,100.93,101.55,2000)],{'history':full_history()},expected=('SET-0015:base',))
    drive=[(100+i-1,100+i+.05,100+i-1-.05,100+i,2000) for i in range(1,7)]
    yield make_case('first-micro-pause',[9],drive+[(106,106.12,105.95,106.04,1000),(106.04,107.,106.,106.9,2000)],{'history':full_history()},expected=('SET-0009:base',))
    ross=make_case('ten-second-micro-pause',[9],drive+[(106,106.12,105.95,106.04,1000),(106.04,107.,106.,106.9,2000)],{'history':full_history()},expected=('SET-0009:ross_10s',))
    ross.events=[ross.events[0]]+[replace(e,event_time=OPEN+timedelta(seconds=i*10),arrival_time=OPEN+timedelta(seconds=i*10),payload={**e.payload,'seconds':10},event_id=f'ross-{i}') for i,e in enumerate(ross.events[1:],1)]
    yield ross
    yield make_case('boundary-pressure',[16],[(99.5+i*.04,100.01,99.45+i*.04,99.55+i*.04) for i in range(9)]+[(99.87,100.3,99.86,100.25)],{'references':{'key_level':reference(100.)}},expected=('SET-0016:base',))
    yield make_case('range-edge-second-reversal',[18],[(100,100.1,99.9,100.01 if i%2 else 99.99) for i in range(10)]+[(99.96,100.08,99.9,100.07),(100.02,100.07,99.91,100.06),(100.06,100.2,100.02,100.18)],expected=('SET-0018:base',))
    vp={'kind':'volume','venue':'SYN','poc':105.,'val':103.,'vah':108.,'low':100.,'high':110.,'prominence':3.,'lower_tail_fraction':.03,'upper_tail_fraction':.03}
    tp={'kind':'tpo','venue':'SYN','val':103.,'vah':108.,'poc':105.}
    refs={k:reference(v) for k,v in {'prior_volume_profile':vp,'prior_tpo':tp,'prev_high':110.,'prev_low':100.,'prev_close':105.}.items()}
    yield make_case('tail-return-base',[20],[(100.01,100.08,100.,100.04),(100.04,100.1,100.01,100.07),(100.07,100.2,100.05,100.18)],{'references':refs},expected=('SET-0020:base',))
    pm=[bar(-2,100.,100.3,99.,100.2),bar(-1,100.2,100.4,99.5,100.3),bar(0,100.3,100.4,99.8,100.2)]
    yield make_case('premarket-support-wash',[22],[(100.2,100.3,98.98,99.1),(99.1,99.3,99.,99.25),(99.25,99.6,99.2,99.55),(99.55,100.5,99.5,100.45)],{'premarket_complete':True,'references':{'pm_support':reference(99.)}},preevents=pm,expected=('SET-0022:base','SET-0022:early_v'))
    news=event('news',OPEN,{'source':'synthetic_regression','event_key':'catalyst','is_new':True},'catalyst')
    yield make_case('catalyst-premarket-break',[21],[(100.25,100.35,100.2,100.3,2000),(100.3,101.2,100.28,101.15,3000)],{'premarket_complete':True,'history':full_history(),'references':{'prev_close':reference(90.)}},preevents=[*pm,news],expected=('SET-0021:base','SET-0021:sai_gap'))
    yield make_case('boulevard-reject-volume-cliff',[26],[(100.4,100.8,100.2,100.25,3000),(100.25,100.6,100.2,100.3,100),(100.3,100.32,99.8,99.85,1000)],{'history':full_history(),'references':{'prev_close':reference(90.),'historical_vwap':reference(100.5),'float_shares':reference(1_000_000)}},preevents=[news],expected=('SET-0026:base',))
    priornews=event('news',OPEN-timedelta(days=1),{'source':'synthetic_regression','event_key':'prior-release','is_new':True},'prior-release',arrival=OPEN-timedelta(hours=1))
    yield make_case('second-day-acceptance',[28],[(100.2,100.4,100.1,100.3),(100.3,100.8,100.25,100.75),(100.75,100.82,100.68,100.77),(100.77,100.85,100.72,100.8),(100.8,100.95,100.75,100.9),(100.9,101.1,100.85,101.)],{'history':full_history(),'references':{'prior_acceptance_level':reference(100.5),'prev_high':reference(103.),'prev_low':reference(99.),'prev_close':reference(100.)}},preevents=[priornews],expected=('SET-0028:base','SET-0028:direct_first_break'))


def micro_case(name, ids, windows, *, extra=None, expected=(), first_price=100.):
    payload={'coverage':{k:{'complete':True,'start_time':OPEN.isoformat()} for k in ('trades','classified_trades','depth')},**(extra or {})}
    events=[session(payload),event('depth',OPEN,{'mode':'snapshot','bids':[[first_price-.01,1000]],'asks':[[first_price+.01,1000]]},name+'-D0'),event('trade',OPEN,{'price':first_price,'size':10.,'side':'buy'},name+'-T0')]
    for window,trades in enumerate(windows):
        for index,(price,size,side) in enumerate(trades):
            at=OPEN+timedelta(seconds=window+.1+.8*index/max(1,len(trades)-1))
            events.append(event('trade',at,{'price':price,'size':size,'side':side},f'{name}-T{window}-{index}'))
        at=OPEN+timedelta(seconds=window+1)
        price=trades[-1][0]
        events.append(event('depth',at,{'mode':'snapshot','bids':[[price-.01,1000]],'asks':[[price+.01,1000]]},f'{name}-D{window+1}'))
    return RawScenario(name,events,tuple(f'SET-{i:04}' for i in ids),RecognitionConfig(event_window_seconds=1),tuple(expected),{'scope':'synthetic_raw_market_events'})


def flow_scenarios():
    dummy=[(100.,100.,'buy'),(100.,100.,'sell')]
    stack=[(100.,10.,'sell'),(100.01,40.,'buy'),(100.01,10.,'sell'),(100.02,40.,'buy'),(100.02,10.,'sell'),(100.03,40.,'buy')]
    yield micro_case('three-to-one-footprint',[42],[dummy,stack,[(100.015,20.,'buy'),(100.02,30.,'buy')],[(100.04,30.,'buy'),(100.1,50.,'buy')]],expected=('SET-0042:base','SET-0042:ratio150'))
    ratio=[(p,20. if side=='buy' else n,side) for p,n,side in stack]
    yield micro_case('one-point-five-footprint',[42],[dummy,ratio,[(100.015,20.,'buy'),(100.02,30.,'buy')],[(100.04,30.,'buy'),(100.1,50.,'buy')]],expected=('SET-0042:ratio150',))
    yield micro_case('real-gap-thin-hold',[41],[dummy,[(100.02,100.,'buy'),(100.04,100.,'buy'),(100.06,100.,'buy')],[(100.045,100.,'buy'),(100.05,100.,'buy')],[(100.06,100.,'buy'),(100.1,100.,'buy')]],expected=('SET-0041:base',))
    yield micro_case('low-energy-new-attack',[39],[[(100.,200.,'sell'),(100.,200.,'sell')],[(100.,50.,'sell'),(100.,50.,'sell')],[(100.,150.,'buy'),(100.03,150.,'buy')],[(100.03,150.,'buy'),(100.08,150.,'buy')]],extra={'references':{'key_level':reference(100.)}},expected=('SET-0039:base',))
    yield micro_case('exhausted-selling-roll',[40],[[(100.02,300.,'sell'),(100.01,300.,'sell')],[(100.01,20.,'sell'),(100.01,20.,'sell')],[(100.02,100.,'buy'),(100.04,100.,'buy')],[(100.04,100.,'buy'),(100.07,100.,'buy')]],expected=('SET-0040:base',))
    yield micro_case('cvd-price-divergence',[44],[[(100.03,100.,'sell'),(100.02,100.,'sell')],[(100.04,100.,'buy'),(100.01,100.,'buy')],[(100.05,100.,'buy'),(100.09,100.,'buy')]],expected=('SET-0044:base',),first_price=100.03)


def tape_case(name, ids, candles, extra=None, *, expected=()):
    base=make_case(name,ids,candles,{'coverage':{k:{'complete':True,'start_time':OPEN.isoformat()} for k in ('trades','classified_trades','depth')},**(extra or {})},expected=expected)
    events=[base.events[0]]
    for minute,b in enumerate(base.events[1:],1):
        payload=b.payload
        prices=[payload['open'],payload['low'],payload['high'],payload['close']] if payload['close']>=payload['open'] else [payload['open'],payload['high'],payload['low'],payload['close']]
        for index,price in enumerate(prices):
            events.append(event('trade',OPEN+timedelta(minutes=minute-1,seconds=12*(index+1)),{'price':price,'size':max(.01,payload['volume']/4),'side':'buy' if payload['close']>=payload['open'] else 'sell'},f'{name}-T{minute}-{index}'))
        events.append(b)
    base.events=events
    return base


def profile_scenarios():
    vp={'kind':'volume','venue':'SYN','poc':100.,'val':99.,'vah':101.,'low':98.,'high':102.,'prominence':3.,'lower_tail_fraction':.03,'upper_tail_fraction':.03}
    yield tape_case('old-poc-break-retest',[4],[(99.85,99.9,99.8,99.87),(99.87,99.98,99.85,99.9),(99.9,100.2,99.88,100.15),(100.05,100.3,100.01,100.25)],{'ib_minutes':2,'references':{'prior_volume_profile':reference(vp)}},expected=('SET-0004:base','SET-0004:active_cross'))
    tp={'kind':'tpo','venue':'SYN','val':100.,'vah':110.,'poc':105.}
    vp2={**vp,'val':100.,'vah':110.,'poc':105.}
    yield tape_case('tpo-brackets-and-volume-proxy',[37],[(99.,103.,99.,102.)]+[(102.,103.,101.,102.)]*59,{'references':{'prior_tpo':reference(tp),'prior_volume_profile':reference(vp2),'prev_close':reference(105.),'prev_high':reference(111.),'prev_low':reference(99.)}},expected=('SET-0037:two_bracket_touch','SET-0037:volume_va_proxy'))
    shapes=[(100.,100.1,99.9,100.,1000)]*8+[(100.,100.02,99.4,99.5,1),(99.5,99.65,99.3,99.6,1),(99.6,100.2,99.58,100.15,1000)]
    yield make_case('flat-vwap-range-stretch',[25],shapes,expected=('SET-0025:base','SET-0025:touch_early'))
    dummy=[(100.,200.,'buy'),(100.,200.,'sell')]
    yield micro_case('high-impulse-distribution',[36],[[(100.,20.,'buy'),(100.,20.,'sell')],[(100.,20.,'buy'),(100.02,20.,'buy'),(100.04,20.,'buy'),(100.06,20.,'buy'),(100.08,1000.,'buy')],[(100.08,2000.,'sell'),(100.08,2000.,'sell')],[(100.12,2000.,'buy'),(100.18,2000.,'buy')]],expected=('SET-0036:base','SET-0036:edge_limit_observation'))
    yield tape_case('unchanged-value-stretch-reversal',[38],[(100.,100.01,99.99,100.,1000)]*8+[(100.,100.01,99.8,99.8,1),(99.8,99.92,99.7,99.9,1),(99.9,100.12,99.88,100.1,100),(99.9,100.05,99.8,99.95,1),(99.95,100.25,99.9,100.2,100)],expected=('SET-0038:base','SET-0038:early_failure','SET-0038:double_hold'))
    trend=[(100+.1*(i-1),100+.1*i+.03,100+.1*(i-1)-.03,100+.1*i) for i in range(1,26)]
    yield make_case('second-entry-ema',[6],trend+[(102.5,102.52,102.1,102.2),(102.2,102.7,102.18,102.65),(102.65,102.66,101.5,101.6),(101.6,102.,101.55,101.95),(101.95,102.2,101.9,102.15)],expected=('SET-0006:ema_reference',))
    candles=[(100.,100.1,99.9,100.)]*390
    candles=[(100. if i==0 else 101.,101.1,99.9 if i==0 else 100.9,101.) if i<330 else (101.,102.1,100.9,102.) for i in range(390)]
    yield make_case('joint-sign-last-half-hour',[49],candles,{'references':{'prev_close':reference(100.)}},expected=('SET-0049:joint_sign',))


def reversal_scenarios():
    closes=[]
    price=100.
    for cycle in range(3):
        for step in range(8):
            price-=.2
            closes.append(price)
        price+=.5
        closes.append(price)
    closes += [price-.2,price-.4]
    candles=[(100. if i==0 else closes[i-1],max(100. if i==0 else closes[i-1],c)+.05,min(100. if i==0 else closes[i-1],c)-.05,c) for i,c in enumerate(closes)]
    for name,low,variant in [('major-reversal-equal',96.15,'double_test'),('major-reversal-higher-low',96.3,'higher_low'),('major-reversal-lower-low',96.05,'lower_low')]:
        yield make_case(name,[8],candles+[(96.3,97.1,96.25,97.),(96.4,96.9,low,96.8),(96.8,97.15,96.75,97.1),(97.1,97.4,97.05,97.35)],expected=('SET-0008:base','SET-0008:'+variant))
    yield make_case('accelerating-three-push',[7],[(100,100.05,99.65,99.7),(99.7,100.02,99.65,100),(100,100.02,99.4,99.45),(99.45,99.8,99.42,99.75),(99.75,99.78,99.1,99.15),(99.15,99.5,99.12,99.45),(99.45,99.5,98.55,98.6),(98.6,98.95,98.58,98.9),(98.9,99.45,98.88,99.4),(99.4,99.8,99.35,99.75)],expected=('SET-0007:parabolic',))
    bear=[(100.-.1*(i-1),100.-.1*(i-1)+.03,100.-.1*i-.03,100.-.1*i) for i in range(1,16)]
    flag=[(98.5,98.65,98.3,98.45),(98.45,98.65,98.3,98.4),(98.4,98.65,98.3,98.44),(98.44,98.65,98.3,98.42)]
    yield make_case('late-final-flag-failure',[11],bear+flag+[(98.25,98.55,98.2,98.45),(98.45,98.75,98.4,98.7)],expected=('SET-0011:base',))
    trend=[(100+.1*(i-1),100+.1*i+.03,100+.1*(i-1)-.03,100+.1*i) for i in range(1,13)]
    yield make_case('actual-parent-sweep-reentry',[17,19],trend+[(101.2,101.23,100.98,101.),(101.,101.1,100.96,101.08),(101.08,101.2,101.05,101.18),(101.18,101.3,100.94,101.25),(101.25,101.5,101.2,101.45)],expected=('SET-0019:base',))


def cross_case(name, ids, own, benchmark, *, extra=None, news=(), expected=(), tape=False):
    base=tape_case(name,ids,own,{'benchmarks':['B'],**(extra or {})},expected=expected) if tape else make_case(name,ids,own,{'benchmarks':['B'],**(extra or {})},expected=expected)
    other=[replace(session(symbol='B'),event_id=name+'-B-session')]+[replace(bar(i,*c,symbol='B'),event_id=f'{name}-B-{i}',arrival_time=OPEN+timedelta(minutes=i)-timedelta(microseconds=1)) for i,c in enumerate(benchmark,1)]
    # A bar cannot arrive before its close. Same arrival: benchmark first.
    other=[replace(e,arrival_time=e.event_time) if e.kind=='bar' else e for e in other]
    base.events=sorted([*other,*base.events,*news],key=lambda e:(e.arrival_time,e.kind!='session',e.symbol!='B'))
    return base


def cross_scenarios():
    own=[(100+.2*(i-1),100+.2*i+.02,100+.2*(i-1)-.02,100+.2*i) for i in range(1,9)]
    market=[(100+.02*(i-1),100+.02*i+.01,100+.02*(i-1)-.01,100+.02*i) for i in range(1,9)]
    yield cross_case('relative-strength-market-confirm',[27],own,market,extra={'history':full_history(),'references':{'daily_direction':reference(1)}},expected=('SET-0027:base','SET-0027:public_price_proxy'))
    branches=[{'name':'up','conditions':[['x','gt',2]],'surprise':True,'direction_by_symbol':{'SYN':1,'B':1}},{'name':'down','conditions':[['x','lt',2]],'surprise':True,'direction_by_symbol':{'SYN':-1,'B':-1}}]
    consensus=event('consensus',OPEN-timedelta(minutes=10),{'source':'synthetic_regression','event_key':'macro','branches':branches},'macro-plan')
    first=event('news',OPEN+timedelta(minutes=6),{'source':'synthetic_regression','event_key':'macro','values':{'x':3},'stage':1,'is_new':True},'macro-first')
    second=event('news',OPEN+timedelta(minutes=7),{'source':'synthetic_regression','event_key':'macro','values':{'x':1},'stage':2,'is_new':True},'macro-second')
    up=[(100+.1*(i-1),100+.1*i+.02,100+.1*(i-1)-.02,100+.1*i) for i in range(1,10)]
    yield cross_case('macro-original-surprise',[31],up,up,news=[consensus,first],expected=('SET-0031:base',))
    reassess=up[:6]+[(100.6,100.62,100.35,100.4),(100.4,100.42,100.15,100.2),(100.2,100.22,100.05,100.1),(100.1,100.12,99.95,100.),(100.,100.02,99.7,99.75),(99.75,99.77,99.4,99.45)]
    yield cross_case('macro-sequential-reassessment',[32],reassess,reassess,news=[consensus,first,second],expected=('SET-0032:base',))
    own=[(100.,100.25,99.95,100.2)]*7+[(100.2,100.6,100.18,100.55),(100.55,100.9,100.5,100.85)]
    leader=[(100.,100.2,99.95,100.15)]*5+[(100.15,100.5,100.12,100.45),(100.45,100.55,100.4,100.5),(100.5,100.65,100.45,100.6),(100.6,100.75,100.55,100.7)]
    lead=cross_case('calibrated-leader-before-ib-break',[30],own,leader,extra={'ib_minutes':5,'references':{'leadership':reference({'symbol':'B','correlation':.8,'lag_bars':1})}},expected=('SET-0030:base',),tape=True)
    lead.events=[replace(e,payload={**e.payload,'references':{'key_level':reference(100.3)}}) if e.kind=='session' and e.symbol=='B' else e for e in lead.events]
    yield lead


def clock_scenarios():
    vp={'kind':'volume','venue':'SYN','poc':105.,'val':103.,'vah':108.,'low':100.,'high':110.,'prominence':3.,'lower_tail_fraction':.03}
    tp={'kind':'tpo','venue':'SYN','val':103.,'vah':108.,'poc':105.}
    refs={k:reference(v) for k,v in {'prior_volume_profile':vp,'prior_tpo':tp,'prev_high':110.,'prev_low':100.,'prev_close':105.}.items()}
    op=OPEN-timedelta(hours=6,minutes=32)
    history=history14('03:00')
    for row in history:
        row['volumes'].update({'02:59':1000.,'03:00':1000.,'03:01':1000.})
    events=[session({'references':refs,'history':history,'asset_class':'futures'},op=op)]+[bar(i,*c,op=op) for i,c in enumerate([(100.01,100.08,100.,100.04,1000),(100.04,100.1,100.01,100.07,2000),(100.07,100.2,100.05,100.18,3000)],1)]
    yield RawScenario('london-eight-clock',events,('SET-0020',),expected_graphs=('SET-0020:source_0800_BST',),provenance={'scope':'synthetic_clock_compatibility'})
    shapes=[(100.,100.05,99.95,100.)]*6+[(100.,100.5,99.98,100.45),(100.45,100.95,100.43,100.9)]
    for name,news_second,expected in [('first-minute-news',60,('SET-0029:base','SET-0029:window90')),('seventy-second-news',0,('SET-0029:window90',))]:
        events=[session()]
        events+=[replace(bar(i,*c,seconds=10),event_time=OPEN+timedelta(seconds=i*10),arrival_time=OPEN+timedelta(seconds=i*10),event_id=f'{name}-bar{i}') for i,c in enumerate(shapes,1)]
        events.append(event('news',OPEN+timedelta(seconds=news_second),{'source':'synthetic_regression','event_key':name,'is_new':True},name+'-news'))
        events.sort(key=lambda e:(e.arrival_time,e.kind!='session'))
        yield RawScenario(name,events,('SET-0029',),expected_graphs=expected,provenance={'scope':'synthetic_raw_market_events'})


def remaining_scenarios():
    tp={'kind':'tpo','venue':'SYN','val':99.,'vah':101.,'poc':100.}
    tpo_prefix=[(100.,100.02,99.98,100.,1000)]*90
    yield tape_case('three-complete-tpo-ledges',[33],tpo_prefix+[(100.,100.15,99.99,100.1,1000),(100.1,100.17,100.08,100.13,1000),(100.13,100.2,100.11,100.18,1000)],expected=('SET-0033:base',))
    yield tape_case('old-value-tpo-failure',[34],tpo_prefix+[(100.,100.2,99.99,100.15,2000)],{'references':{'prior_tpo':reference(tp)}},expected=('SET-0034:base',))
    drive=[[(100.+.03*i,100.,'buy'),(100.+.03*(i+1),100.,'buy')] for i in range(8)]
    counter=[(100.23,40.,'sell'),(100.24,10.,'buy'),(100.24,40.,'sell'),(100.25,10.,'buy'),(100.25,40.,'sell'),(100.26,10.,'buy')]
    yield micro_case('trend-counter-footprint-cross',[43],[*drive,counter,[(100.26,100.,'buy'),(100.28,100.,'buy')],[(100.3,100.,'buy'),(100.35,100.,'buy')]],expected=('SET-0043:base',))
    yield micro_case('blocked-supply-weak-second-wash',[45],[[(100.,50.,'sell'),(100.,50.,'sell')],[(100.,500.,'buy'),(100.,500.,'buy')],[(99.95,100.,'sell'),(99.9,100.,'sell')],[(99.93,100.,'buy'),(99.96,100.,'buy')],[(99.99,100.,'buy'),(100.,100.,'buy')],[(99.985,100.,'sell'),(99.98,100.,'sell')],[(100.01,100.,'buy'),(100.06,100.,'buy')]],expected=('SET-0045:base',))
    pm=[bar(-2,101.5,101.55,101.,101.1),bar(-1,101.1,101.15,100.6,100.8),bar(0,100.8,100.85,100.4,100.5)]
    refs={'prev_high':reference(102.),'prev_low':reference(100.),'prev_close':reference(101.)}
    case=tape_case('second-vwap-deviation-and-origin-retest',[24],[(100.6,100.65,100.45,100.5,10000),(100.5,100.55,100.05,100.1,100),(100.1,100.7,99.5,100.6,100),(100.6,100.62,99.4,99.65,100),(99.65,100.6,99.6,100.55,100)],{'premarket_complete':True,'references':refs},expected=('SET-0024:base',))
    case.events=sorted([*case.events,*pm],key=lambda e:(e.arrival_time,e.kind!='session'))
    yield case
    vp={'kind':'volume','venue':'SYN','poc':108.,'val':106.,'vah':110.,'low':100.,'high':112.,'prominence':3.,'lower_tail_fraction':.03}
    bearish=[(110.-.2*(i-1),110.-.2*(i-1)+.05,110.-.2*i-.05,110.-.2*i,1000) for i in range(1,13)]
    weak=tape_case('sticky-offer-shallow-bounce',[46],bearish+[(107.6,107.65,107.55,107.58,1000),(107.58,107.63,107.53,107.56,1000),(107.56,107.61,107.1,107.15,1000)],{'premarket_complete':True,'references':{'prior_volume_profile':reference(vp),'prev_high':reference(112.),'prev_low':reference(100.),'prev_close':reference(111.),'prev_close_location':reference(.8)}},expected=('SET-0046:base',))
    pm_up=[bar(-2,110.,110.5,109.9,110.4),bar(-1,110.4,110.8,110.3,110.7),bar(0,110.7,111.,110.6,110.9)]
    extra=[*pm_up]
    for minute in (13,14,15):
        for seconds in (0,24,48):
            extra.append(event('depth',OPEN+timedelta(minutes=minute-1,seconds=seconds),{'mode':'snapshot','bids':[[107.1,1000]],'asks':[[107.7,1000]]},f'offer-{minute}-{seconds}'))
    weak.events=sorted([*weak.events,*extra],key=lambda e:(e.arrival_time,e.kind!='session'))
    yield weak
    recover=tape_case('passive-drive-origin-recovery',[47],[(110.,110.1,99.9,100.,1000),(100.,103.1,99.9,103.,1000),(103.,106.1,102.9,106.,1000),(106.,108.1,105.9,108.,1000),(108.,109.1,107.9,109.,1000),(109.,110.3,108.9,110.2,950),(110.2,111.1,110.05,111.,950)],{'references':{'prior_volume_profile':reference(vp)}},expected=('SET-0047:base',))
    keep=[e for e in recover.events if e.kind!='trade' or (e.event_time-OPEN).total_seconds()<300]
    for minute,opening,support,high,close in ((6,109.,108.9,110.3,110.2),(7,110.2,110.05,111.1,111.)):
        for seconds,size in ((0,100.),(24,90.),(48,80.)):
            keep.append(event('depth',OPEN+timedelta(minutes=minute-1,seconds=seconds),{'mode':'snapshot','bids':[[support,size]],'asks':[[high,1000.]]},f'reload-{minute}-{seconds}'))
        for seconds,price,size,side in ((8,opening,50.,'buy'),(12,support,50.,'sell'),(36,support,50.,'sell'),(50,high,400.,'buy'),(55,close,400.,'buy')):
            keep.append(event('trade',OPEN+timedelta(minutes=minute-1,seconds=seconds),{'price':price,'size':size,'side':side},f'recovery-{minute}-{seconds}'))
    recover.events=sorted(keep,key=lambda e:(e.arrival_time,e.kind!='session'))
    yield recover


def boundary_scenarios():
    shapes=[(10,10.4,9.6,10.1),(10.1,10.2,9.8,9.95),(9.95,10.1,9.5,9.65)]+[(9.7,10.15,9.6,9.75)]*3+[(9.75,10.3,9.7,10.25)]
    yield make_case('hikkake-late-extended-window',[5],shapes,expected=('SET-0005:extended_window',))
    late=next(s for s in frozen_audit_scenarios() if s.scenario_id=='audit:SET-0001-S1')
    late.scenario_id='intraday-position-after-opening'
    late.events=[late.events[0]]+[replace(e,event_time=e.event_time+timedelta(hours=2),arrival_time=e.arrival_time+timedelta(hours=2)) for e in late.events[1:]]
    late.expected_graphs=('SET-0001:intraday_position',)
    yield late
    yield make_case('noise-hold-exceeds-one-tick',[10],[(99.85,99.89,99.8,99.85)]*5+[(99.85,100.1,99.83,100.06),(100.01,100.08,99.985,100.06),(100.06,100.16,100.04,100.12)],{'references':{'key_level':reference(100.)}},expected=('SET-0010:base',))
    trend=[(100+.1*(i-1),100+.1*i+.03,100+.1*(i-1)-.03,100+.1*i) for i in range(1,13)]
    yield make_case('inner-edge-before-mother-edge',[13],trend+[(101.2,101.8,100.8,101.3),(101.3,101.6,101.,101.4),(101.4,101.5,101.1,101.45),(101.45,101.65,101.4,101.6)],expected=('SET-0013:inner_edge',))
    small_gap=next(s for s in additional_price_scenarios() if s.scenario_id=='catalyst-premarket-break')
    small_gap.scenario_id='two-percent-catalyst-gap'
    small_gap.events=[replace(e,payload={**e.payload,'references':{**e.payload.get('references',{}),'prev_close':reference(98.)}}) if e.kind=='session' else e for e in small_gap.events]
    small_gap.expected_graphs=('SET-0021:sai_gap',)
    yield small_gap
    broad=next(s for s in additional_price_scenarios() if s.scenario_id=='catalyst-premarket-break')
    broad.scenario_id='catalyst-without-tight-flag'
    revised=[]
    for e in broad.events:
        if e.kind=='bar' and e.event_time==OPEN+timedelta(minutes=1):
            e=replace(e,payload={**e.payload,'low':98.8})
        if e.kind=='bar' and e.event_time==OPEN+timedelta(minutes=2):
            e=replace(e,payload={**e.payload,'close':101.5,'high':101.55,'notional':101.5*e.payload['volume']})
        revised.append(e)
    broad.events=revised
    broad.expected_graphs=('SET-0021:base',)
    yield broad

    vp={'kind':'volume','venue':'SYN','poc':105.,'val':100.,'vah':110.,'low':98.,'high':112.,'prominence':3.,'lower_tail_fraction':.03}
    tp={'kind':'tpo','venue':'SYN','val':100.,'vah':110.,'poc':105.}
    shapes=[(99.,103.,99.,102.)]+[(102.,103.,101.,102.)]*59
    shapes[29]=(102.,103.,98.9,99.)
    yield tape_case('two-brackets-with-intervening-outside-close',[37],shapes,{'references':{'prior_tpo':reference(tp),'prior_volume_profile':reference(vp),'prev_close':reference(105.),'prev_high':reference(111.),'prev_low':reference(99.)}},expected=('SET-0037:two_bracket_touch',))

def absorption_scenario():
    extra={'coverage':{k:{'complete':True,'start_time':OPEN.isoformat()} for k in ('trades','classified_trades','depth')},'references':{'key_level':reference(100.)}}
    events=[session(extra)]
    for seconds in (0.,.3,.6):
        events.append(event('depth',OPEN+timedelta(seconds=seconds),{'mode':'snapshot','bids':[[100.,1000]],'asks':[[100.01,1000]]},f'D-{seconds}'))
    for index,(seconds,price,size,side) in enumerate(((.6,100,10,'sell'),(1.,100,10,'sell'),(1.2,100,100,'sell'),(1.6,100,100,'sell'),(2.,100.03,100,'buy'),(2.4,100.05,100,'buy'),(2.6,100.07,100,'buy'),(3.,100.09,100,'buy'),(3.6,100.12,100,'buy'))):
        events.append(event('trade',OPEN+timedelta(seconds=seconds),{'price':price,'size':size,'side':side},f'T-{index}'))
    return RawScenario('absorption-roll',events,('SET-0003',),RecognitionConfig(event_window_seconds=1),('SET-0003:base',))


def all_scenarios():
    return [*frozen_audit_scenarios(), *price_scenarios(), *additional_price_scenarios(), *flow_scenarios(), *profile_scenarios(), *reversal_scenarios(), *cross_scenarios(), *clock_scenarios(), *remaining_scenarios(), *boundary_scenarios(), absorption_scenario()]



def neutral_control(scenario, graph):
    """Break the geometric premise while retaining typed original timestamps.

    The fixed-sign SET-0049 has no geometric neutral return: zero still means
    short. Its required previous close is explicitly removed instead.
    """
    rows=[]
    setup=graph.split(':')[0]
    session_event=next(e for e in scenario.events if e.kind=='session' and e.symbol=='SYN')
    refs=session_event.payload.get('references',{})
    price=100.
    if setup=='SET-0020':
        price=refs.get('prior_volume_profile',{}).get('value',{}).get('poc',100.)
    elif setup=='SET-0028':
        price=refs.get('prior_acceptance_level',{}).get('value',refs.get('prev_high',{}).get('value',100.))
    elif refs.get('key_level'):
        price=refs['key_level']['value']
    for e in scenario.events:
        payload=dict(e.payload)
        if e.kind=='session' and setup=='SET-0049':
            payload['references']={k:v for k,v in payload.get('references',{}).items() if k!='prev_close'}
            payload['history']=[]
        elif e.kind=='bar':
            payload.update(open=price,high=price,low=price,close=price)
            if 'notional' in payload:
                payload['notional']=price*payload['volume']
        elif e.kind=='trade':
            payload['price']=price
        elif e.kind=='quote':
            payload.update(bid=price,ask=price)
        elif e.kind=='depth':
            payload.update(bids=[[price-.01,sum(n for p,n in payload['bids'])]],asks=[[price+.01,sum(n for p,n in payload['asks'])]])
        rows.append(replace(e,payload=payload))
    return RawScenario(scenario.scenario_id+':neutral',rows,scenario.setup_ids,scenario.config)


def matched_variant_boundaries(scenario, base_spec, variant_spec, first_by_target):
    """Compare complete triggers for one actual directional observation stream.

    The open-price version has no registered bar scale in common with its base.
    Its source-clock evidence is therefore identified explicitly rather than
    claiming a same-period price-rule comparison.
    """
    base = base_spec['setup_id'] + ':base'
    graph = variant_spec['setup_id'] + ':' + variant_spec['variant_id']
    base_rows = first_by_target.get(base, {})
    variant_rows = first_by_target.get(graph, {})
    common_scales = set(base_spec['scales']) & set(variant_spec['scales'])
    common = {'scenario_id':scenario.scenario_id,
              'input_sha256':hashlib.sha256(json.dumps([e.to_dict() for e in scenario.events],
                  sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest(),
              'input_scope':'full_authored_raw_scenario', 'input_event_count':len(scenario.events),
              'input_last_event_id':scenario.events[-1].event_id,
              'base_registered_scales':base_spec['scales'],
              'variant_registered_scales':variant_spec['scales']}
    evidence = []
    for target in sorted(set(base_rows) | set(variant_rows)):
        symbol, venue, direction, scale = target
        if scale not in common_scales:
            continue
        a, b = base_rows.get(target), variant_rows.get(target)
        comparison = {**common, 'comparison_scope':'same_symbol_venue_direction_scale',
                      'target':dict(symbol=symbol, venue=venue, direction=direction, scale_seconds=scale),
                      'base_first_complete':a, 'variant_first_complete':b}
        if (a is None) != (b is None):
            evidence.append({**comparison,'distinction':'complete_trigger_presence'})
        elif a and b and a['event_index'] != b['event_index']:
            evidence.append({**comparison,'distinction':'causal_trigger_event'})
        elif a and b and a['captures'] != b['captures']:
            evidence.append({**comparison,'distinction':'frozen_capture_geometry'})
    if variant_spec['variant_id'] == 'ross_10s':
        for target in sorted(base_rows):
            symbol, venue, direction, scale = target
            if scale in variant_spec['scales']:
                continue
            evidence.append({**common, 'comparison_scope':'registered_scale_selection',
                'target':dict(symbol=symbol,venue=venue,direction=direction,scale_seconds=scale),
                'base_first_complete':base_rows[target], 'variant_first_complete':None,
                'distinction':'registered_scale_selection',
                'reason':'The base triggered on its registered bar scale; this variant only '
                         'registers 10-second observations. No different predicate behavior on '
                         'the shared 10-second scale is claimed.'})
    if not common_scales and variant_spec['variant_id'] == 'source_open':
        def by_direction(rows):
            out = {}
            for target, row in rows.items():
                key = target[:3]
                if key not in out or row['event_index'] < out[key]['event_index']:
                    out[key] = row
            return out
        base_directions, variant_directions = by_direction(base_rows), by_direction(variant_rows)
        for target in sorted(variant_directions):
            a, b = base_directions.get(target), variant_directions[target]
            evidence.append({**common, 'comparison_scope':'registered_source_clock',
                'target':dict(zip(('symbol','venue','direction'),target)),
                'base_first_complete':a, 'variant_first_complete':b,
                'distinction':'registered_source_clock',
                'reason':'Base bar scales and source open-price scale are disjoint by specification; '
                         'the same symbol/venue/direction source-clock trigger is observed without '
                         'claiming a same-period mechanical boundary.'})
    return evidence


def coverage_matrix(include_negatives=True):
    from .raw_negatives import input_hash, verified_core_controls
    scenarios=all_scenarios()
    core_controls=verified_core_controls(scenarios) if include_negatives else {}
    results=[]
    observed={}
    best={}
    boundaries={}
    semantic_evidence=[]
    scale_equivalence_evidence={}
    for scenario in scenarios:
        engine=RecognitionEngine(list(scenario.setup_ids),include_variants=True,config=scenario.config)
        rows=[]
        first={}
        captures={}
        first_rows={}
        first_by_target={}
        trigger_signatures={}
        for index,e in enumerate(scenario.events):
            batch=engine.feed(e)
            rows+=batch
            for r in batch:
                graph=r.setup_id+':'+r.variant_id
                if r.state=='triggered' and r.data_status=='complete' and graph not in first:
                    first[graph]=index
                    captures[graph]=r.anchor['captures']
                    first_rows[graph]={'event_index':index,'symbol':r.symbol,'venue':r.venue,'direction':r.direction,
                        'scale_seconds':r.scale_seconds,'evidence_cutoff':r.evidence_cutoff,
                        'data_status':r.data_status,'captures':r.anchor['captures'],
                        'conditions':[{k:v for k,v in c.items() if k!='source_ids'} for c in r.conditions
                                      if c['result']=='satisfied']}
                if r.state=='triggered' and r.data_status=='complete':
                    target=(r.symbol,r.venue,r.direction,r.scale_seconds)
                    first_by_target.setdefault(graph,{}).setdefault(target,
                        {'event_index':index,'symbol':r.symbol,'venue':r.venue,'direction':r.direction,
                         'scale_seconds':r.scale_seconds,'evidence_cutoff':r.evidence_cutoff,
                         'data_status':r.data_status,'captures':r.anchor['captures'],
                         'conditions':[{k:v for k,v in c.items() if k!='source_ids'} for c in r.conditions
                                       if c['result']=='satisfied']})
                    trigger_signatures.setdefault(graph,[]).append({'event_index':index,'symbol':r.symbol,
                        'venue':r.venue,'direction':r.direction,'scale_seconds':r.scale_seconds,
                        'captures':r.anchor['captures'],
                        'conditions':[{k:v for k,v in c.items() if k!='source_ids'} for c in r.conditions
                                      if c['result']=='satisfied']})
        complete=set(first)
        for graph in complete:
            observed.setdefault(graph,[]).append(scenario.scenario_id)
            if graph not in best or first[graph]<best[graph][1]:
                best[graph]=(scenario,first[graph])
        for spec in engine.graph.specs:
            graph=spec['setup_id']+':'+spec['variant_id']
            base=spec['setup_id']+':base'
            if spec['variant_id']=='base':
                continue
            comparison={'scenario_id':scenario.scenario_id,'input_sha256':input_hash(scenario),
                        'base_first_complete':first_rows.get(base),'variant_first_complete':first_rows.get(graph)}
            if spec['variant_id']=='public_price_proxy' and graph in complete and base in complete:
                semantic_evidence.append({**comparison,'distinction':'declared_source_identity_only',
                    'observable_trigger_signatures_equal':trigger_signatures[graph]==trigger_signatures[base],
                    'base_complete_trigger_count':len(trigger_signatures[base]),
                    'variant_complete_trigger_count':len(trigger_signatures[graph])})
                continue
            base_spec=next(s for s in engine.graph.specs if s['setup_id']==spec['setup_id'] and s['variant_id']=='base')
            boundaries.setdefault(graph,[]).extend(matched_variant_boundaries(scenario,base_spec,spec,first_by_target))
            if spec['variant_id']=='ross_10s':
                for target in sorted(set(first_by_target.get(base,{}))&set(first_by_target.get(graph,{}))):
                    a,b=first_by_target[base][target],first_by_target[graph][target]
                    signatures=lambda key:[w for w in trigger_signatures[key] if
                        (w['symbol'],w['venue'],w['direction'],w['scale_seconds'])==target]
                    observable=lambda row:{k:v for k,v in row.items() if k not in {'variant_id','scales'}}
                    digest=lambda value:hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()).hexdigest()
                    base_hash,variant_hash=digest(observable(base_spec)),digest(observable(spec))
                    scale_equivalence_evidence.setdefault(graph,[]).append({
                        'scenario_id':scenario.scenario_id,'input_sha256':input_hash(scenario),
                        'input_scope':'full_authored_raw_scenario','input_event_count':len(scenario.events),
                        'target':dict(zip(('symbol','venue','direction','scale_seconds'),target)),
                        'base_first_complete':a,'variant_first_complete':b,
                        'base_complete_trigger_count':len(signatures(base)),
                        'variant_complete_trigger_count':len(signatures(graph)),
                        'observable_trigger_signatures_equal':signatures(base)==signatures(graph),
                        'base_observable_spec_sha256':base_hash,'variant_observable_spec_sha256':variant_hash,
                        'observable_predicates_equal':base_hash==variant_hash})
        results.append({'scenario_id':scenario.scenario_id,'events':len(scenario.events),'complete_graphs':sorted(complete),
                        'expected_graphs':list(scenario.expected_graphs),'expectations_passed':set(scenario.expected_graphs)<=complete,
                        'first_complete_event_indices':first,'verification_level':'raw_pipeline'})
    graphs={}
    for spec in load_specs(include_variants=True):
        graph=spec['setup_id']+':'+spec['variant_id']
        negative=[]
        if include_negatives and spec['variant_id']=='base':
            negative=core_controls.get(graph,[])
        elif include_negatives and graph in best:
            scenario,first_index=best[graph]
            prefix=RawScenario(scenario.scenario_id+':before-trigger',scenario.events[:first_index],scenario.setup_ids,scenario.config)
            for name,control in [('final_observation_absent',prefix),('geometric_premise_removed' if spec['setup_id']!='SET-0049' else 'required_previous_close_removed',neutral_control(scenario,graph))]:
                _,rows=control.run()
                negative.append({'scenario_id':control.scenario_id,'condition_broken':name,'passed':not any(r.setup_id+':'+r.variant_id==graph and r.state=='triggered' and r.data_status=='complete' for r in rows),'verification_level':'raw_pipeline_supplemental_blocking_control',
                                 'input_sha256':input_hash(control),'scope':'Additional absence/data/geometry control, not a known-false core predicate witness.'})
        boundary=boundaries.get(graph,[])
        scale_equivalence=scale_equivalence_evidence.get(graph,[])
        if spec['variant_id']=='ross_10s' and not (scale_equivalence and all(
                w['observable_trigger_signatures_equal'] and w['observable_predicates_equal'] for w in scale_equivalence)):
            boundary=[]
        semantic_valid=False
        if spec['variant_id']=='public_price_proxy':
            base=next(s for s in load_specs(include_variants=False) if s['setup_id']==spec['setup_id'])
            observable=lambda s:{k:v for k,v in s.items() if k not in {'notes','variant_id'}}
            digest=lambda value:hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()).hexdigest()
            base_hash=digest(observable(base)); variant_hash=digest(observable(spec))
            boundary=[{**w,'base_observable_spec_sha256':base_hash,'variant_observable_spec_sha256':variant_hash,
                       'observable_predicates_equal':base_hash==variant_hash,
                       'reason':'Public source identity is retained; proprietary 1OP/1OSI is unavailable. No independent observable price-rule boundary is asserted.'}
                      for w in semantic_evidence]
            semantic_valid=bool(boundary) and all(w['observable_predicates_equal'] and w['observable_trigger_signatures_equal'] for w in boundary)
        graphs[graph]={'positive_scenarios':observed.get(graph,[]),'positive_status':'passed' if graph in observed else 'pending_authored_raw_positive',
                       'negative_scenarios':negative,'negative_status':'passed' if len(negative)==2 and all(n['passed'] for n in negative) else 'not_run' if not include_negatives else 'pending_or_failed',
                       'variant_boundary_scenarios':boundary,'variant_boundary_status':'not_applicable' if spec['variant_id']=='base' or semantic_valid else 'passed' if boundary and spec['variant_id']!='public_price_proxy' else 'pending_authored_raw_boundary',
                       'variant_boundary_scope':'source_identity_same_observable_predicates' if semantic_valid else 'registered_source_clock' if spec['variant_id']=='source_open' else 'registered_scale_selection' if spec['variant_id']=='ross_10s' else 'mechanical' if spec['variant_id']!='base' else 'base',
                       'shared_scale_equivalence_scenarios':scale_equivalence,
                       'verification_level':'raw_pipeline','market_validation':'not_run'}
    return {'graph_count':len(graphs),'raw_positive_graph_count':sum(bool(r['positive_scenarios']) for r in graphs.values()),
            'raw_negative_graph_count':sum(r['negative_status']=='passed' for r in graphs.values()),
            'raw_core_negative_base_count':sum(g.endswith(':base') and r['negative_status']=='passed' for g,r in graphs.items()),
            'raw_core_negative_path_count':sum(len(r['negative_scenarios']) for g,r in graphs.items() if g.endswith(':base')),
            'raw_supplemental_negative_path_count':sum(len(r['negative_scenarios']) for g,r in graphs.items() if not g.endswith(':base')),
            'raw_variant_boundary_count':sum(r['variant_boundary_status']=='passed' for r in graphs.values()),
            'raw_same_target_mechanical_boundary_count':sum(r['variant_boundary_status']=='passed' and r['variant_boundary_scope']=='mechanical' for r in graphs.values()),
            'raw_registered_source_clock_boundary_count':sum(r['variant_boundary_status']=='passed' and r['variant_boundary_scope']=='registered_source_clock' for r in graphs.values()),
            'raw_registered_scale_selection_boundary_count':sum(r['variant_boundary_status']=='passed' and r['variant_boundary_scope']=='registered_scale_selection' for r in graphs.values()),
            'raw_variant_observable_boundary_not_applicable_count':sum(r['variant_boundary_status']=='not_applicable' and r['variant_boundary_scope']=='source_identity_same_observable_predicates' for r in graphs.values()),
            'complete':all(r['positive_status']=='passed' and (not include_negatives or r['negative_status']=='passed') and r['variant_boundary_status']!='pending_authored_raw_boundary' for r in graphs.values()),
            'graphs':graphs,'scenarios':results,'market_validation':'not_run'}


if __name__=='__main__':
    print(json.dumps(coverage_matrix(),ensure_ascii=False,indent=2))
