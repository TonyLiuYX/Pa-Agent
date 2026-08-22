from __future__ import annotations

import json
from pathlib import Path

import pytest

from pa_engine.evaluation import run_offline_evaluation, write_report


ROOT = Path(__file__).resolve().parents[3]
FIXTURE_DIR = ROOT / "tests" / "fixtures" / "phase2" / "g029"
SID_LONG = "pa:nanqiao-range-extreme-limit"
SID_SECOND = "pa:nanqiao-always-in-ema20"


@pytest.fixture(scope="module")
def report() -> dict:
    return run_offline_evaluation().to_dict()


def _goldens() -> dict:
    return json.loads((FIXTURE_DIR / "goldens.json").read_text(encoding="utf-8"))


def _events(result: dict, event_type: str) -> list[dict]:
    return [event["payload"] for event in result["events"] if event.get("event_type") == event_type]


def _single_event(result: dict, event_type: str) -> dict:
    values = _events(result, event_type)
    assert len(values) == 1, (result["scenario_id"], event_type, values)
    return values[0]


def test_report_accepts_with_all_sixteen_criteria(report: dict, tmp_path: Path) -> None:
    assert report["status"] == "ACCEPT"
    assert report["scenario_count"] >= 12
    assert report["network_used"] is False
    assert report["credentials_used"] is False
    assert report["replays"]["runs"] == 2
    assert report["replays"]["identical"] is True
    assert report["replays"]["canonical_hashes"][0] == report["replays"]["canonical_hashes"][1]
    assert len(report["canonical_hash"]) == 64
    assert report["golden_checks"]["pass"] is True
    lineage_golden = report["golden_checks"]["scenarios"]["lineage_immediate_long_tp"]
    assert lineage_golden["pass"] is True
    assert all(lineage_golden["fields"].values())

    rows = report["coverage_matrix"]
    assert [row["id"] for row in rows] == [f"AC{index:02d}" for index in range(1, 17)]
    assert all(row["status"] == "PASS" for row in rows)
    assert report["lineage_assertions"]["pass"] is True
    assert any("structural/reproducibility" in item for item in report["limitations"])
    assert any("profitability" in item for item in report["limitations"])

    path = tmp_path / "g029-report.json"
    write_report(report, path)
    written = json.loads(path.read_text(encoding="utf-8"))
    assert written == report


@pytest.mark.parametrize(
    ("scenario_id", "expected_branch"),
    [
        ("no_entry_N", "no_entry"),
        ("immediate_long_tp", "enter_immediately"),
        ("immediate_short_sl", "enter_immediately"),
        ("wait_long_fill_tp", "wait_for_price"),
        ("wait_short_fill_sl", "wait_for_price"),
        ("dual_touch_stop_first", "enter_immediately"),
        ("wait_expiry", "wait_for_price"),
    ],
)
def test_runtime_branches_and_states_are_exercised(report: dict, scenario_id: str, expected_branch: str) -> None:
    result = report["scenario_results"][scenario_id]
    assert result["provider_outcomes"][0]["branch"] == expected_branch
    final_state = next(iter(result["states"].values())) if scenario_id != "opposite_isolation" else None
    if final_state is not None:
        assert final_state == report["scenario_results"][scenario_id]["states"][SID_LONG]

    if scenario_id == "no_entry_N":
        assert final_state == "sleeping"
        accepted = _single_event(result, "model_decision_accepted")
        assert accepted["next_call_after_seconds"] == 57
        assert not _events(result, "paper_fill")
    elif scenario_id == "wait_expiry":
        assert final_state == "analysis_due"
        expired = _single_event(result, "pending_entry_expired")
        assert expired["resolution_at"] == "2026-03-09T13:33:11Z"
        assert not _events(result, "paper_fill")
    else:
        assert final_state == "analysis_due"
        assert _events(result, "paper_fill")
        assert _events(result, "paper_close")


@pytest.mark.parametrize(
    "scenario_id",
    ["immediate_long_tp", "immediate_short_sl", "wait_long_fill_tp", "wait_short_fill_sl"],
)
def test_independent_decimal_goldens_match_runtime_settlement(report: dict, scenario_id: str) -> None:
    golden = _goldens()["scenarios"][scenario_id]
    result = report["scenario_results"][scenario_id]
    fill = _single_event(result, "paper_fill")
    close = _single_event(result, "paper_close")
    settlement = close["settlement"]
    account = result["accounts"][SID_LONG]

    for key in ("direction", "entry", "quantity", "planned_r", "filled_at"):
        runtime_key = "entry_price" if key == "entry" else key
        assert fill[runtime_key] == golden[key], (scenario_id, key, fill)
    for key in ("closed_at", "exit_reason", "selected_exit"):
        assert close[key] == golden[key]
    assert settlement["gross_pnl"] == golden["gross_pnl"]
    assert settlement["applied_pnl"] == golden["applied_pnl"]
    assert settlement["balance_after"] == golden["balance_after"]
    assert settlement["quantity"] == golden["quantity"]
    assert settlement["paper_math_version"] == "0.1.0"
    assert account["balance"] == golden["balance_after"]
    assert result["states"][SID_LONG] == golden["final_state"]


def test_dual_touch_expiry_and_opposite_direction_goldens(report: dict) -> None:
    dual = report["scenario_results"]["dual_touch_stop_first"]
    dual_golden = _goldens()["scenarios"]["dual_touch_stop_first"]
    close = _single_event(dual, "paper_close")
    assert close["exit_reason"] == dual_golden["exit_reason"]
    assert close["selected_exit"] == dual_golden["selected_exit"]
    assert close["settlement"]["balance_after"] == dual_golden["balance_after"]
    assert close["settlement"]["gross_pnl"] == "-14.925373125"
    assert close["settlement"]["applied_pnl"] == "-14.93"
    assert dual["transitions"][SID_LONG][-1]["reason"] == "position_closed"

    opposite = report["scenario_results"]["opposite_isolation"]
    assert set(opposite["states"]) == {SID_LONG, SID_SECOND}
    assert opposite["states"][SID_LONG] == "open_position"
    assert opposite["states"][SID_SECOND] == "open_position"
    assert opposite["provider_call_count"] == 2
    assert {item["strategy_id"] for item in opposite["provider_outcomes"]} == {SID_LONG, SID_SECOND}
    assert {item["direction"] for item in opposite["provider_outcomes"]} == {"long", "short"}
    assert opposite["accounts"][SID_LONG]["account_id"] == f"paper:{SID_LONG}"
    assert opposite["accounts"][SID_SECOND]["account_id"] == f"paper:{SID_SECOND}"

    same = report["scenario_results"]["same_direction_parallel"]
    assert same["provider_call_count"] == 2
    assert set(same["states"]) == {SID_LONG, SID_SECOND}
    assert set(same["states"].values()) == {"open_position"}
    assert {item["direction"] for item in same["provider_outcomes"]} == {"long"}
    assert same["accounts"][SID_LONG]["account_id"] == f"paper:{SID_LONG}"
    assert same["accounts"][SID_SECOND]["account_id"] == f"paper:{SID_SECOND}"


def test_lineage_ids_are_retained_and_bound_to_each_request(report: dict) -> None:
    lineage = report["lineage_assertions"]
    assert lineage["all_actionable_have_claim_ids"] is True
    assert lineage["all_actionable_have_citation_ids"] is True
    assert lineage["all_actionable_have_iex_market_refs"] is True
    assert lineage["all_immediate_have_current_second_refs"] is True
    assert lineage["future_market_inputs_excluded"] is True
    assert lineage["all_requests_have_profile_packet_and_market_hashes"] is True
    assert lineage["model_boundary_safe"] is True
    assert lineage["event_market_refs"]
    assert all(item["market_feed"] == {"schema_version": "0.1.0", "provider": "alpaca", "feed": "iex", "coverage": "single_exchange"} for item in lineage["event_market_refs"])
    assert all(len(item["second_event_hash"]) == 64 for item in lineage["event_market_refs"])

    for item in lineage["actionable_outcomes"]:
        assert item["strategy_id"] in {SID_LONG, SID_SECOND}
        assert item["candidate_id"] == "moc_" + item["candidate_hash"]
        assert item["claim_ids"]
        assert item["citation_ids"]
        assert all(value.startswith("bar_") for value in item["market_refs"])
        result = report["scenario_results"][item["scenario_id"]]
        request = next(row for row in result["requests"] if row["strategy_id"] == item["strategy_id"])
        assert request["provider"] == "deepseek"
        assert request["model"].startswith("deepseek-")
        assert request["adapter"]
        view_ref = item["shared_market_view_ref"]
        for key in ("market_snapshot_id", "market_snapshot_hash", "observation_frame_id", "observation_frame_hash"):
            assert view_ref[key] == request[key]
        assert request["boundary_issues"] == []
        if item["branch"] == "enter_immediately":
            assert item["second_event_ref"]["event_id"] == request["latest_second_event_id"]
            assert item["second_event_ref"]["event_hash"] == request["latest_second_event_hash"]


def test_scale_failures_recovery_boundaries_and_deadlines(report: dict) -> None:
    scale = report["scale"]
    assert scale["forty"]["registered"] == 40
    assert scale["forty"]["started_jobs"] == 40
    assert scale["forty"]["shared_view_ids"] == ["smv_g029_shared"]
    assert scale["forty"]["duplicate_job_ids"] == 0
    assert scale["two_hundred"]["registered"] == 200
    assert scale["two_hundred"]["claimed"] == 200
    assert scale["two_hundred"]["unique_claimed"] == 200
    assert scale["two_hundred"]["peak_in_flight"] == 7
    assert scale["two_hundred"]["work_conserving_refill"] is True

    retry = report["retry_backoff"]
    assert retry["observed_delays_seconds"] == [5, 10, 20, 40, 60]
    assert retry["expected_delays_seconds"] == [5, 10, 20, 40, 60]
    assert [item["classification"] for item in report["scenario_results"]["provider_retry_backoff"]["provider_outcomes"]] == ["timeout", "rate_limit", "server", "transport", "timeout", "timeout"]
    assert report["scenario_results"]["terminal_auth_no_hot_loop"]["provider_call_count"] == 1
    assert report["scenario_results"]["terminal_auth_no_hot_loop"]["states"][SID_LONG] == "blocked"
    assert report["scenario_results"]["provider_invalid_response"]["states"][SID_LONG] == "blocked"
    assert report["scenario_results"]["provider_invalid_response"]["extra"]["terminal_no_hot_loop"] is True
    assert report["scenario_results"]["provider_invalid_response"]["extra"]["no_paper_entry"] is True

    assert report["recovery"]["entry_exit_status"] == "settled"
    assert report["recovery"]["entry_exit_lineage"]["path_precision"] == "minute_bar_no_path"
    assert report["recovery"]["straddling_not_evaluated"] is True
    assert report["recovery"]["duplicate_late_status"] == "replayed"
    assert report["recovery"]["typed_lineage_tamper_rejected"] is True
    assert report["boundaries"]["pre_watermark_changed"] is True
    assert report["boundaries"]["post_watermark_execution_hash_unchanged"] is True
    assert report["boundaries"]["sip_rejected"] is True
    assert report["derived_seconds"]["pass"] is True
    assert report["derived_seconds"]["batches"]["dual_touch"]["actual"]["volume"] == "8"
    assert report["market_quality"]["gap_blocks_execution"] is True
    assert report["market_quality"]["gap_requires_recovery"] is True
    assert report["market_quality"]["stale_blocks_execution"] is True
    assert report["immediate_drift"]["fresh_price_rejected"] is True
    assert report["immediate_drift"]["reason_binds_both_second_ids"] is True
    assert report["immediate_drift"]["balance_unchanged"] is True
    assert report["immediate_drift"]["no_open_trade"] is True
    assert report["disabled_exhausted"]["disabled_state"] == "disabled"
    assert report["disabled_exhausted"]["exhausted_state"] == "exhausted"
    assert report["disabled_exhausted"]["no_recall"] is True
    assert report["deadlines"]["status"] == "filled"
    assert report["deadlines"]["deadline_not_extended"] is True
    resume = report["session_close"]["open_position_resume"]
    assert resume["open_before_close"] is True
    assert resume["open_at_close"] is True
    assert resume["open_next_session"] is True
    assert resume["state_next_session"] == "open_position"
    assert resume["calls_at_close"] == resume["calls_before_close"] == resume["calls_next_session"]


def test_scheduler_scale_has_real_identity_and_work_conserving_service(report: dict) -> None:
    scale = report["scale"]
    forty = scale["forty"]
    assert len(forty["service_counts"]) == 40
    assert set(forty["service_counts"].values()) == {1}
    assert set(forty["registry_strategy_ids"]) == set(forty["service_counts"])
    assert forty["all_completed"] is True
    assert forty["same_view"] is True
    assert forty["no_duplicate_generation_job"] is True
    assert forty["reentry"]["isolated"] is True
    assert all(first["job_id"] != second["job_id"] for first, second in zip(forty["reentry"]["first"].values(), forty["reentry"]["second"].values()))

    two_hundred = scale["two_hundred"]
    records = two_hundred["service_records"]
    assert len(records) == 200
    assert len({item["strategy_id"] for item in records}) == 200
    assert set(two_hundred["registry_strategy_ids"]) == {item["strategy_id"] for item in records}
    assert all(item["completed"] and item["schedule_generation"] == item["generation"] for item in records)
    assert two_hundred["all_strategy_ids_once"] is True
    assert two_hundred["unique_job_ids"] is True
    assert two_hundred["unique_generation_jobs"] is True
    assert two_hundred["peak_in_flight"] <= two_hundred["configured_max_in_flight"]
    assert two_hundred["work_conserving_refill"] is True


def test_same_and_opposite_strategy_partitions_are_complete(report: dict) -> None:
    isolation = report["isolation"]
    assert isolation["pass"] is True
    for case in isolation["cases"].values():
        assert case["pass"] is True
        per_strategy = case["per_strategy"]
        assert len({item["account_id"] for item in per_strategy.values()}) == 2
        assert len({item["trade_id"] for item in per_strategy.values()}) == 2
        assert {item["generation"] for item in (row["schedule_identity"] for row in per_strategy.values())} == {0}
        assert len({item["open_position_id"] for item in (row["schedule_identity"] for row in per_strategy.values())}) == 2
        assert case["event_strategy_ids"] == sorted({SID_LONG, SID_SECOND})
        assert case["request_strategy_ids"] == sorted({SID_LONG, SID_SECOND})
        assert case["outcome_strategy_ids"] == sorted({SID_LONG, SID_SECOND})


def test_restart_and_recovery_compare_side_effect_multiplicity(report: dict) -> None:
    restart = report["restart"]
    assert restart["restored_pending_or_open"] is True
    assert restart["recovery_gate_observed"] is True
    assert restart["provider_calls_after_redelivery"] == restart["provider_calls_before_redelivery"]
    assert restart["paper_fill_count_after"] == restart["paper_fill_count_before"]
    assert restart["paper_close_count_after"] == restart["paper_close_count_before"]
    assert restart["event_multiplicity_unchanged"] is True
    assert restart["balance_pnl_stable_after_replay"] is True
    assert report["recovery"]["exit_settlement_once"]["one_settlement"] is True
    assert report["recovery"]["exit_settlement_once"]["close_callback_count"] == 1
    assert report["recovery"]["entry_recovery"]["duplicate_no_effect"] is True


def test_recovery_entry_exit_lineage_and_interval_are_typed(report: dict) -> None:
    recovery = report["recovery"]
    entry = recovery["entry_recovery"]
    interval = recovery["whole_interval_recovery"]
    assert entry["status"] == "filled"
    assert entry["filled_price"] == "100"
    assert entry["exact_requested_level"] is True
    assert entry["lineage_exact"] is True
    assert entry["lineage"]["market_feed"]["feed"] == "iex"
    assert entry["lineage"]["path_precision"] == "minute_bar_no_path"
    assert entry["duplicate_status"] == "replayed"
    assert interval["entry_bar_covers_gap"] is True
    assert interval["exit_gap_recorded"] is True
    assert recovery["straddling_not_evaluated"] is True
    assert recovery["typed_lineage_tamper_rejected"] is True


def test_feed_revision_cutover_and_decimal_evidence_are_observed(report: dict) -> None:
    cutover = report["boundaries"]["half_open_cutover"]
    assert cutover["pre_cutover_event_ids"]
    assert cutover["retained_event_ids"]
    assert cutover["pre_cutover_dropped"] is True
    assert cutover["cutover_inclusive_retained"] is True
    revision = report["revision_lineage"]
    assert revision["native_fields"] == {
        "revision_sequence_zero": True,
        "parent_revision_absent": True,
        "validated": True,
        "wrapper_passthrough_equal": True,
        "wrapper_has_only_passthrough_state": True,
    }
    assert all(revision["golden_fields"].values())
    assert revision["feed_session_ingested_one"] is True
    assert revision["shared_view_join_ok"] is True
    assert revision["feed_session_event_hash"] == revision["event_hash"]
    assert revision["shared_view_latest_event_hash"] == revision["event_hash"]
    assert revision["direct_event_hash"] == revision["event_hash"]
    scales = report["decimal_scales"]
    assert scales["assertions"] == {"quantity_8_places": True, "usd_2_places": True, "r_4_places": True}
    assert scales["observed_quantity_scales"] == [-8]
    assert scales["observed_usd_scales"] == [-2]
    assert report["feed_lineage"]["pass"] is True
    assert report["feed_lineage"]["history_sip_mismatch_rejected"] is True
    assert report["feed_lineage"]["aggregator_sip_mismatch_rejected"] is True
    assert report["feed_lineage"]["live_sip_mismatch_typed_or_g021_rejected"] is True


def test_session_startup_and_phase1_protection_are_evidence_backed(report: dict) -> None:
    pending_x = report["session_close"]["pending_x_across_close"]
    assert pending_x["timer_status_at_close"] == "pending"
    assert pending_x["deadline_not_extended"] is True
    assert pending_x["closed_session_no_model_call"] is True
    assert pending_x["no_entry"] is True
    assert pending_x["expired_status"] == "expired"
    assert pending_x["duplicate_expiry_status"] == "replayed"
    assert pending_x["next_session_resume_call_observed"] is True

    startup = report["startup"]
    assert startup["insufficient_startup"]["provider_calls"] == 0
    assert startup["partial_startup"]["provider_calls"] == 0
    assert startup["partial_startup"]["paper_entries"] == 0
    assert startup["partial_startup"]["no_model_hot_loop"] is True
    assert startup["terminal_configuration"]["terminal"] is True
    assert startup["terminal_configuration"]["provider_calls"] == 0
    assert startup["terminal_configuration"]["paper_entries"] == 0

    phase1 = report["phase1_retrieval_goldens"]
    assert phase1["baseline_available"] is True
    assert phase1["case_count"] == 24
    assert phase1["unique_expected_record_count"] == 24
    assert phase1["unchanged"] is True


def test_no_account_or_credential_fields_cross_provider_boundary(report: dict) -> None:
    forbidden = {"credentials", "secret_key", "api_key", "api_secret", "password", "balance", "available_balance", "open_trade", "pending_trade"}

    def keys(value: object) -> set[str]:
        if isinstance(value, dict):
            return {str(key).lower() for key in value} | set().union(*(keys(child) for child in value.values()))
        if isinstance(value, list):
            return set().union(*(keys(child) for child in value))
        return set()

    for result in report["scenario_results"].values():
        assert not (keys(result.get("requests", [])) & forbidden)
        assert not (keys(result.get("provider_outcomes", [])) & forbidden)
        assert all(request["boundary_issues"] == [] for request in result.get("requests", []))
    assert report["tampering"]["recursive_leak_found"] is True
    assert report["tampering"]["grounding_tamper_rejected"] is True
    assert report["tampering"]["packet_tamper_rejected"] is True
    assert report["tampering"]["profile_tamper_rejected"] is True
    assert report["tampering"]["second_lineage_tamper_rejected"] is True
    assert report["tampering"]["provider_calls_before_execution"] == 0
