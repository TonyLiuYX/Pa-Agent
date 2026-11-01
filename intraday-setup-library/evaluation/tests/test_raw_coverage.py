"""Acceptance matrix: all predicates are reached through actual market events."""
import json
from pathlib import Path
import unittest

from recognition.registry import FEATURE_REGISTRY, validate_feature_value
from recognition.engine import condition
from recognition.specs import load_specs
from .raw_scenarios import coverage_matrix, all_scenarios


class RawCoverageTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.matrix=coverage_matrix()

    def test_all_eighty_four_graphs_have_complete_raw_positive(self):
        self.assertEqual((self.matrix['graph_count'],self.matrix['raw_positive_graph_count']),(84,84))
        for result in self.matrix['scenarios']:
            with self.subTest(scenario=result['scenario_id']):
                self.assertTrue(result['expectations_passed'])
        for graph,row in self.matrix['graphs'].items():
            with self.subTest(graph=graph):
                self.assertEqual(row['positive_status'],'passed')
                self.assertTrue(row['positive_scenarios'])
                self.assertEqual(row['market_validation'],'not_run')

    def test_fifty_bases_have_two_known_false_core_predicates_from_full_raw_inputs(self):
        self.assertEqual(self.matrix['raw_negative_graph_count'],84)
        self.assertEqual(self.matrix['raw_core_negative_base_count'],50)
        self.assertEqual(self.matrix['raw_core_negative_path_count'],100)
        specs={s['setup_id']+':'+s['variant_id']:s for s in load_specs(include_variants=True)}
        for graph,row in self.matrix['graphs'].items():
            if not graph.endswith(':base'): continue
            with self.subTest(graph=graph):
                controls=row['negative_scenarios']
                self.assertEqual(len(controls),2)
                self.assertEqual(len({c['condition_broken'] for c in controls}),2)
                for control in controls:
                    self.assertTrue(control['passed'])
                    self.assertTrue(control['input_hashes_match'])
                    self.assertTrue(control['frozen_witness_matches'])
                    self.assertEqual(control['verification_level'],'raw_pipeline_known_false_core')
                    self.assertEqual(control['target_complete_trigger_count'],0)
                    self.assertEqual(control['events'],control['positive_events'])
                    self.assertEqual(len(control['input_sha256']),64)
                    witness=control['witness']; positive=witness['positive_counterpart']
                    self.assertTrue(witness['known'])
                    self.assertEqual(witness['capability_status'],'complete')
                    self.assertEqual(witness['missing_capabilities'],[])
                    self.assertFalse(set(witness['required_capabilities'])-set(witness['available_capabilities']))
                    self.assertEqual(witness['direction'],control['target']['direction'])
                    self.assertEqual(witness['symbol'],control['target']['symbol'])
                    self.assertIsNotNone(witness['actual'])
                    self.assertIsNotNone(witness['expected'])
                    self.assertTrue(witness['evidence_cutoff'])
                    self.assertTrue(witness['source_ids'])
                    self.assertTrue(positive['known'])
                    self.assertEqual(positive['result'],'satisfied')
                    for field in ('event_index','scale_seconds','stage_index','feature','operation'):
                        self.assertEqual(witness[field],positive[field])
                    stage=specs[graph]['stages'][witness['stage_index']]
                    test=next(t for t in stage['tests'] if t[0]==witness['feature'])
                    self.assertIs(condition(test,{test[0]:witness['actual']},witness['captures'])[0],False)
                    positive_test=[test[0],test[1],positive['expected']]
                    self.assertIs(condition(positive_test,{test[0]:positive['actual']},{})[0],True)

    def test_variant_controls_are_reported_as_supplemental_absence_or_geometry(self):
        self.assertEqual(self.matrix['raw_supplemental_negative_path_count'],68)
        for graph,row in self.matrix['graphs'].items():
            if graph.endswith(':base'): continue
            with self.subTest(graph=graph):
                self.assertEqual(len(row['negative_scenarios']),2)
                self.assertTrue(all(c['passed'] for c in row['negative_scenarios']))
                self.assertTrue(all(c['verification_level']=='raw_pipeline_supplemental_blocking_control'
                                    for c in row['negative_scenarios']))

    def test_thirty_one_variants_have_same_target_mechanical_boundary_evidence(self):
        self.assertEqual(self.matrix['raw_variant_boundary_count'],33)
        self.assertEqual(self.matrix['raw_same_target_mechanical_boundary_count'],31)
        self.assertEqual(self.matrix['raw_registered_source_clock_boundary_count'],1)
        self.assertEqual(self.matrix['raw_registered_scale_selection_boundary_count'],1)
        self.assertTrue(self.matrix['complete'])
        scenarios={s.scenario_id:s for s in all_scenarios()}
        for graph,row in self.matrix['graphs'].items():
            if graph.endswith(':base') or graph in {'SET-0027:public_price_proxy','SET-0020:source_open','SET-0009:ross_10s'}:
                continue
            with self.subTest(graph=graph):
                self.assertEqual(row['variant_boundary_status'],'passed')
                self.assertEqual(row['variant_boundary_scope'],'mechanical')
                self.assertTrue(row['variant_boundary_scenarios'])
                for witness in row['variant_boundary_scenarios']:
                    self.assertEqual(len(witness['input_sha256']),64)
                    self.assertEqual(witness['comparison_scope'],'same_symbol_venue_direction_scale')
                    self.assertEqual(witness['input_scope'],'full_authored_raw_scenario')
                    scenario=scenarios[witness['scenario_id']]
                    self.assertEqual(witness['input_event_count'],len(scenario.events))
                    self.assertEqual(witness['input_last_event_id'],scenario.events[-1].event_id)
                    target=witness['target']
                    self.assertIn(target['scale_seconds'],witness['base_registered_scales'])
                    self.assertIn(target['scale_seconds'],witness['variant_registered_scales'])
                    base=witness['base_first_complete']; variant=witness['variant_first_complete']
                    for observation in (base,variant):
                        if observation is not None:
                            for field,value in target.items():
                                self.assertEqual(observation[field],value)
                            self.assertEqual(observation['data_status'],'complete')
                            self.assertLess(observation['event_index'],len(scenario.events))
                    if witness['distinction']=='complete_trigger_presence':
                        self.assertNotEqual(base is None,variant is None)
                    elif witness['distinction']=='causal_trigger_event':
                        self.assertNotEqual(base['event_index'],variant['event_index'])
                    elif witness['distinction']=='frozen_capture_geometry':
                        self.assertNotEqual(base['captures'],variant['captures'])
                    else: self.fail('Unverified mechanical distinction')

    def test_direct_first_break_compares_the_actual_long_minute_trigger_path(self):
        witnesses=self.matrix['graphs']['SET-0028:direct_first_break']['variant_boundary_scenarios']
        witness=next(w for w in witnesses if w['scenario_id']=='second-day-acceptance'
                     and w['target']['direction']==1 and w['target']['scale_seconds']==60)
        self.assertEqual(witness['target'],dict(symbol='SYN',venue='SYN',direction=1,scale_seconds=60))
        self.assertEqual(witness['distinction'],'causal_trigger_event')
        self.assertEqual(witness['base_first_complete']['event_index'],6)
        self.assertEqual(witness['variant_first_complete']['event_index'],3)
        self.assertEqual(witness['input_event_count'],8)

    def test_source_open_has_an_explicit_registered_clock_boundary(self):
        row=self.matrix['graphs']['SET-0020:source_open']
        self.assertEqual(row['variant_boundary_status'],'passed')
        self.assertEqual(row['variant_boundary_scope'],'registered_source_clock')
        self.assertTrue(row['variant_boundary_scenarios'])
        for witness in row['variant_boundary_scenarios']:
            self.assertEqual(witness['distinction'],'registered_source_clock')
            self.assertEqual(witness['comparison_scope'],'registered_source_clock')
            self.assertFalse(set(witness['base_registered_scales'])&set(witness['variant_registered_scales']))
            self.assertEqual(witness['variant_registered_scales'],[0])
            self.assertEqual(witness['input_scope'],'full_authored_raw_scenario')
            for kind,scales in (('base',witness['base_registered_scales']),('variant',witness['variant_registered_scales'])):
                observation=witness[kind+'_first_complete']
                if observation is not None:
                    for field,value in witness['target'].items():
                        self.assertEqual(observation[field],value)
                    self.assertIn(observation['scale_seconds'],scales)
                    self.assertEqual(observation['data_status'],'complete')
            self.assertIsNotNone(witness['variant_first_complete'])

    def test_ross_scale_selection_and_shared_ten_second_behavior_are_distinct_claims(self):
        row=self.matrix['graphs']['SET-0009:ross_10s']
        self.assertEqual(row['variant_boundary_status'],'passed')
        self.assertEqual(row['variant_boundary_scope'],'registered_scale_selection')
        witness=next(w for w in row['variant_boundary_scenarios']
                     if w['scenario_id']=='first-micro-pause' and w['target']['scale_seconds']==60)
        self.assertEqual(witness['distinction'],'registered_scale_selection')
        self.assertEqual(witness['comparison_scope'],'registered_scale_selection')
        self.assertEqual(witness['variant_registered_scales'],[10])
        self.assertIn(60,witness['base_registered_scales'])
        self.assertIsNone(witness['variant_first_complete'])
        for field,value in witness['target'].items():
            self.assertEqual(witness['base_first_complete'][field],value)
        self.assertEqual(witness['input_scope'],'full_authored_raw_scenario')
        equivalence=next(w for w in row['shared_scale_equivalence_scenarios']
                         if w['scenario_id']=='ten-second-micro-pause')
        self.assertEqual(equivalence['target']['scale_seconds'],10)
        self.assertTrue(equivalence['observable_predicates_equal'])
        self.assertTrue(equivalence['observable_trigger_signatures_equal'])
        self.assertGreater(equivalence['base_complete_trigger_count'],0)
        self.assertEqual(equivalence['base_complete_trigger_count'],equivalence['variant_complete_trigger_count'])
        self.assertEqual(equivalence['base_first_complete'],equivalence['variant_first_complete'])

    def test_public_price_proxy_retains_identity_without_claiming_a_mechanical_boundary(self):
        self.assertEqual(self.matrix['raw_variant_observable_boundary_not_applicable_count'],1)
        row=self.matrix['graphs']['SET-0027:public_price_proxy']
        self.assertEqual(row['variant_boundary_status'],'not_applicable')
        self.assertEqual(row['variant_boundary_scope'],'source_identity_same_observable_predicates')
        self.assertTrue(row['variant_boundary_scenarios'])
        for witness in row['variant_boundary_scenarios']:
            self.assertTrue(witness['observable_predicates_equal'])
            self.assertTrue(witness['observable_trigger_signatures_equal'])
            self.assertEqual(witness['base_observable_spec_sha256'],witness['variant_observable_spec_sha256'])
            self.assertGreater(witness['base_complete_trigger_count'],0)
            self.assertEqual(witness['base_complete_trigger_count'],witness['variant_complete_trigger_count'])

    def test_registry_covers_every_rule_predicate_capture_and_cancel(self):
        for spec in load_specs(include_variants=True):
            used={t[0] for stage in spec['stages'] for t in stage['tests']}
            used.update(name for stage in spec['stages'] for name in stage['capture'].values())
            used.update(t[0] for t in spec['cancel'])
            with self.subTest(graph=spec['setup_id']+':'+spec['variant_id']):
                self.assertFalse(used-set(FEATURE_REGISTRY))
        self.assertTrue(validate_feature_value('impulse',.5))
        self.assertTrue(validate_feature_value('absorption',None))
        with self.assertRaises(ValueError):
            validate_feature_value('impulse',True)
        with self.assertRaises(ValueError):
            validate_feature_value('absorption',1)

    def test_fourteen_audit_links_preserve_levels_and_frozen_input(self):
        links=json.loads((Path(__file__).resolve().parents[2]/'recognition'/'audit_links.json').read_text())
        self.assertEqual(links['original_case_count'],250)
        self.assertEqual(links['linked_case_count'],14)
        self.assertEqual(len({r['case_id'] for r in links['cases']}),14)
        for row in links['cases']:
            self.assertIn(row['verification_level'],{'raw_pipeline','rule_graph','feature_provider'})
            self.assertIn(row['frozen_input']['kind'],{'synthetic_raw_market_events','injected_normalized_features'})
            self.assertEqual(len(row['frozen_original']['sha256']),64)
            self.assertTrue(row['explanation'])
            self.assertEqual(row['market_validation'],'not_run')
