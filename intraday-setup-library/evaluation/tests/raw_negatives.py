"""Known-false core controls, made exclusively by changing raw observations.

Predicates below are probes of real provider frames (and real candidate captures
when a predicate references an anchor). They never replace provider values.
"""
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta
import hashlib
import json
from pathlib import Path

from recognition.engine import RecognitionEngine, condition
from recognition.models import timestamp


def input_hash(scenario):
    value=[event.to_dict() for event in scenario.events]
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()


MUTATORS=('flat_geometry','mirror_geometry','hold_after_first_stage','flip_trade_sides',
          'constant_depth','historical_volume_high','historical_volume_low',
          'low_profile_prominence','wide_profile_tail','large_float','reverse_daily_context',
          'news_not_new','reverse_consensus_direction','reverse_benchmark_geometry',
          'first_return_positive','off_cash_clock','cash_open_not_crossed',
          'clock_noise_inside','clock_noise_off_sample')


def mutate(scenario, name, context):
    from .raw_scenarios import RawScenario
    symbol=context['symbol']; direction=context['direction']
    own_session=next(e for e in scenario.events if e.kind=='session' and e.symbol==symbol)
    opening=timestamp(own_session.payload['open_time'])
    first_price=next((e.payload.get('open',e.payload.get('price')) for e in scenario.events
                      if e.symbol==symbol and e.kind in {'bar','trade'}),100.)
    level=own_session.payload.get('references',{}).get('key_level',{}).get('value',first_price)
    if context['setup_id']=='SET-0020':
        level=own_session.payload['references']['prior_volume_profile']['value']['poc']
    if context['setup_id']=='SET-0028':
        refs=own_session.payload.get('references',{})
        level=refs.get('prior_acceptance_level',refs.get('prev_high',{'value':first_price}))['value']
    holding=context.get('first_stage_price',first_price)
    first_stage=context.get('first_stage_index',0)
    events=[]
    for index,event in enumerate(scenario.events):
        payload=deepcopy(event.payload)
        selected=event.symbol==symbol
        reflect=(selected and name=='mirror_geometry') or (not selected and event.kind!='session' and name=='reverse_benchmark_geometry')
        flatten=selected and (name=='flat_geometry' or name=='hold_after_first_stage' and index>first_stage)
        if flatten and event.kind in {'bar','trade','quote','depth'}:
            price=holding if name=='hold_after_first_stage' else level
            if event.kind=='bar':
                payload.update(open=price,high=price,low=price,close=price)
                if 'notional' in payload: payload['notional']=price*payload['volume']
            elif event.kind=='trade': payload['price']=price
            elif event.kind=='quote': payload.update(bid=price,ask=price)
            else: payload.update(bids=[[price-.01,sum(n for p,n in payload['bids'])]],asks=[[price+.01,sum(n for p,n in payload['asks'])]])
        elif reflect:
            center=first_price if selected else 100.
            if event.kind=='bar':
                payload.update(open=2*center-event.payload['open'],high=2*center-event.payload['low'],
                               low=2*center-event.payload['high'],close=2*center-event.payload['close'])
                if 'notional' in payload: payload['notional']=2*center*payload['volume']-event.payload['notional']
            elif event.kind=='trade':
                payload['price']=2*center-payload['price']
                payload['side']={'buy':'sell','sell':'buy','unknown':'unknown'}[payload['side']]
            elif event.kind=='quote': payload.update(bid=2*center-event.payload['ask'],ask=2*center-event.payload['bid'])
            elif event.kind=='depth':
                payload.update(bids=[[2*center-p,n] for p,n in event.payload['asks']],asks=[[2*center-p,n] for p,n in event.payload['bids']])
        if selected and name=='flip_trade_sides' and event.kind=='trade':
            payload['side']={'buy':'sell','sell':'buy','unknown':'unknown'}[payload['side']]
        if selected and name=='constant_depth' and event.kind=='depth':
            payload.update(bids=[[p,1.] for p,n in payload['bids']],asks=[[p,1.] for p,n in payload['asks']])
        if selected and event.kind=='session':
            refs=payload.get('references',{})
            if name in {'historical_volume_high','historical_volume_low'}:
                factor=1000. if name.endswith('high') else .001
                for day in payload.get('history',[]):
                    day['volumes']={clock:volume*factor for clock,volume in day.get('volumes',{}).items()}
            if name=='low_profile_prominence' and 'prior_volume_profile' in refs:
                refs['prior_volume_profile']['value']['prominence']=0.
            if name=='wide_profile_tail' and 'prior_volume_profile' in refs:
                refs['prior_volume_profile']['value'].update(lower_tail_fraction=.9,upper_tail_fraction=.9)
            if name=='large_float' and 'float_shares' in refs: refs['float_shares']['value']=1e12
            if name=='reverse_daily_context' and 'daily_direction' in refs: refs['daily_direction']['value']=-direction
            if name=='first_return_positive' and 'prev_close' in refs: refs['prev_close']['value']=first_price*.95
            if name=='off_cash_clock':
                payload['open_time']=(timestamp(payload['open_time'])+timedelta(minutes=1)).isoformat()
                payload['close_time']=(timestamp(payload['close_time'])+timedelta(minutes=1)).isoformat()
                # Preserve observed historical coverage at the shifted cutoff.
                # These are raw, frozen historical observations, not a feature
                # override; the explicit source-session clock is off schedule.
                for day in payload.get('history',[]):
                    for field in ('closes','volumes'):
                        original=dict(day.get(field,{}))
                        for clock,value in original.items():
                            shifted_clock=(datetime.strptime(clock,'%H:%M')+timedelta(minutes=1)).strftime('%H:%M')
                            day[field][shifted_clock]=value
        if (selected and name=='cash_open_not_crossed' and event.kind=='bar'
                and event.event_time-timedelta(seconds=payload['seconds'])==opening):
            premarket=[e for e in scenario.events if e.symbol==symbol and e.kind=='bar' and e.event_time<=opening]
            cash=[e for e in scenario.events if e.symbol==symbol and e.kind=='bar' and e.event_time>opening]
            new_open=min(e.payload['low'] for e in cash)-.1 if direction==-1 else max(e.payload['high'] for e in cash)+.1
            payload['open']=new_open
            payload['low']=min(payload['low'],new_open)
            payload['high']=max(payload['high'],new_open)
            # Maintain an actual rejected overnight extreme while withholding
            # the separate reverse crossing of the actual cash open.
            if direction==-1: payload['high']=max(payload['high'],max(e.payload['high'] for e in premarket))
            else: payload['low']=min(payload['low'],min(e.payload['low'] for e in premarket))
        if selected and name=='news_not_new' and event.kind=='news': payload['is_new']=False
        if selected and name=='reverse_consensus_direction' and event.kind=='consensus':
            for branch in payload.get('branches',[]):
                branch['direction_by_symbol']={s:-d for s,d in branch['direction_by_symbol'].items()}
        shifted=name=='off_cash_clock' and selected
        events.append(replace(event,payload=payload,
                              event_time=event.event_time+timedelta(minutes=1) if shifted else event.event_time,
                              arrival_time=event.arrival_time+timedelta(minutes=1) if shifted else event.arrival_time))
    if name in {'clock_noise_inside','clock_noise_off_sample'}:
        from .raw_scenarios import full_history
        from .support import bar,session,reference,OPEN
        count=30 if name=='clock_noise_inside' else 31
        events=[session({'history':full_history(),'references':{'prev_close':reference(100.)}})]
        for minute in range(1,count+1):
            close=101.5 if name=='clock_noise_off_sample' and minute==count else 100.
            events.append(bar(minute,100.,max(100.1,close),99.9,close))
    return RawScenario(scenario.scenario_id+':'+name,events,scenario.setup_ids,scenario.config,
                       provenance={'scope':'synthetic_raw_market_events','mutation':name,'original_scenario':scenario.scenario_id})


def point_key(index,scale,stage,feature):
    return f'{index}|{scale}|{stage}|{feature}'


def probe(scenario, context, *, wanted=None):
    """Collect known false predicates with all required observations available."""
    engine=RecognitionEngine(list(scenario.setup_ids),include_variants=True,config=scenario.config)
    spec=next(s for s in engine.graph.specs if s['setup_id']==context['setup_id'] and s['variant_id']==context.get('variant_id','base'))
    failures={}; target_triggers=[]
    for index,event in enumerate(scenario.events):
        outputs=engine.feed(event)
        target_triggers.extend(r for r in outputs if r.setup_id==spec['setup_id'] and r.variant_id==spec['variant_id']
                               and r.symbol==context['symbol'] and r.direction==context['direction']
                               and r.state=='triggered' and r.data_status=='complete')
        frames=[f for f in engine.scale_frames.values() if f.symbol==context['symbol'] and f.arrived_at==event.arrival_time
                and f.seconds in spec['scales'] and not (set(f.quality)-{'warmup'})
                and not (set(spec['requirements'])-f.capabilities)]
        for frame in frames:
            candidates=[c for c in engine.graph.active+engine.graph.archive if c.symbol==context['symbol']
                        and c.direction==context['direction'] and c.spec['setup_id']==spec['setup_id']
                        and c.spec['variant_id']==spec['variant_id'] and c.seconds==frame.seconds]
            candidate=candidates[-1] if candidates else None
            values=engine.graph._values(frame,candidate) if candidate else frame.values[context['direction']]
            captures=candidate.captured if candidate else {}
            for stage_index,stage in enumerate(spec['stages']):
                for test in stage['tests']:
                    if wanted is not None and (stage_index,test[0])!=wanted: continue
                    positive=context.get('_positive_points',{}).get(point_key(index,frame.seconds,stage_index,test[0]))
                    if positive is None: continue
                    result,actual,expected=condition(test,values,captures)
                    if result is False:
                        failures[(stage_index,test[0])]={
                            'stage_index':stage_index,'stage':stage['name'],'feature':test[0],'operation':test[1],
                            'actual':actual,'expected':expected,'result':'not_satisfied','known':True,
                            'symbol':frame.symbol,'venue':frame.venue,'direction':context['direction'],
                            'scale_seconds':frame.seconds,'event_index':index,'event_id':event.event_id,
                            'evidence_cutoff':frame.bar.end.isoformat(),'identified_at':frame.arrived_at.isoformat(),
                            'missing_capabilities':[],'capability_status':'complete',
                            'required_capabilities':sorted(spec['requirements']),
                            'available_capabilities':sorted(frame.capabilities),
                            'candidate_id':candidate.detection_id if candidate else None,
                            'captures':deepcopy(captures),'source_ids':list(frame.sources),
                            'positive_counterpart':deepcopy(positive)}
    return {'target_complete_trigger_count':len(target_triggers),'failures':failures}


def positive_context(scenario, graph):
    engine,rows=scenario.run()
    setup,variant=graph.split(':')
    row=next(r for r in rows if r.setup_id==setup and r.variant_id==variant and r.state=='triggered' and r.data_status=='complete')
    spec=next(s for s in engine.graph.specs if s['setup_id']==setup and s['variant_id']==variant)
    first_condition=next(c for c in row.conditions if c['stage']==spec['stages'][0]['name'] and c['result']=='satisfied')
    cutoff=timestamp(first_condition['observed_at'])
    index=next(i for i,e in enumerate(scenario.events) if e.symbol==row.symbol and e.event_time==cutoff
               and e.kind in {'bar','trade','depth','quote'})
    first_event=scenario.events[index]
    price=first_event.payload.get('close',first_event.payload.get('price',100.))
    context={'setup_id':setup,'variant_id':variant,'symbol':row.symbol,'direction':row.direction,
            'first_stage_index':index,'first_stage_price':price,
            'positive_conditions':[{k:v for k,v in c.items() if k!='source_ids'} for c in row.conditions if c['result']=='satisfied']}
    engine=RecognitionEngine(list(scenario.setup_ids),include_variants=True,config=scenario.config)
    points={}
    for event_index,event in enumerate(scenario.events):
        engine.feed(event)
        for frame in engine.scale_frames.values():
            if (frame.symbol!=row.symbol or frame.arrived_at!=event.arrival_time or frame.seconds not in spec['scales']
                    or set(frame.quality)-{'warmup'} or set(spec['requirements'])-frame.capabilities): continue
            candidates=[c for c in engine.graph.active+engine.graph.archive if c.symbol==row.symbol and c.direction==row.direction
                        and c.spec['setup_id']==setup and c.spec['variant_id']==variant and c.seconds==frame.seconds]
            candidate=candidates[-1] if candidates else None
            values=engine.graph._values(frame,candidate) if candidate else frame.values[row.direction]
            captures=candidate.captured if candidate else {}
            for stage_index,stage in enumerate(spec['stages']):
                for test in stage['tests']:
                    passed,actual,expected=condition(test,values,captures)
                    if passed is True:
                        points[point_key(event_index,frame.seconds,stage_index,test[0])]={
                            'event_index':event_index,'event_id':event.event_id,'scale_seconds':frame.seconds,
                            'stage_index':stage_index,'stage':stage['name'],'feature':test[0],'operation':test[1],
                            'actual':actual,'expected':expected,'result':'satisfied','known':True,
                            'evidence_cutoff':frame.bar.end.isoformat(),'missing_capabilities':[]}
    context['_positive_points']=points
    return context


def verified_core_controls(scenarios):
    """Re-run each frozen raw control; stored witnesses are never trusted alone."""
    designs=json.loads(Path(__file__).with_name('raw_negative_designs.json').read_text())
    source_by_id={s.scenario_id:s for s in scenarios}
    contexts={}; results={}
    for design in designs['designs']:
        graph=design['graph']; controls=[]
        for frozen in design['controls']:
            source=source_by_id[frozen['positive_scenario_id']]
            key=(source.scenario_id,graph)
            if key not in contexts: contexts[key]=positive_context(source,graph)
            context=contexts[key]
            control=mutate(source,frozen['mutator'],context)
            wanted=(frozen['witness']['stage_index'],frozen['witness']['feature'])
            actual=probe(control,context,wanted=wanted)
            witness=actual['failures'].get(wanted)
            positive_matches=input_hash(source)==frozen['positive_input_sha256']
            negative_matches=input_hash(control)==frozen['input_sha256']
            witness_matches=witness==frozen['witness']
            target_matches=all(context[k]==v for k,v in frozen['target'].items())
            passed=(positive_matches and negative_matches and witness_matches and target_matches
                    and actual['target_complete_trigger_count']==0)
            controls.append({
                'scenario_id':control.scenario_id,'positive_scenario_id':source.scenario_id,
                'condition_broken':frozen['witness']['feature'],'mutator':frozen['mutator'],
                'passed':passed,'verification_level':'raw_pipeline_known_false_core',
                'target':frozen['target'],'input_sha256':input_hash(control),
                'positive_input_sha256':input_hash(source),'input_hashes_match':positive_matches and negative_matches,
                'frozen_witness_matches':witness_matches,'events':len(control.events),
                'positive_events':len(source.events),'target_complete_trigger_count':actual['target_complete_trigger_count'],
                'witness':witness,'market_validation':'not_run'})
        results[graph]=controls
    return results
