from __future__ import annotations

import json
from pathlib import Path

from pa_engine.evaluation.__main__ import main


def test_public_cli_replays_and_writes_machine_report(tmp_path: Path, capsys) -> None:
    output = tmp_path / "g029-cli-report.json"
    exit_code = main(["--output", str(output)])
    captured = capsys.readouterr()
    assert exit_code == 0
    stdout_report = json.loads(captured.out)
    file_report = json.loads(output.read_text(encoding="utf-8"))
    assert stdout_report["status"] == "ACCEPT"
    assert file_report == stdout_report
    assert file_report["replays"]["runs"] == 2
    assert all(row["status"] == "PASS" for row in file_report["coverage_matrix"])
