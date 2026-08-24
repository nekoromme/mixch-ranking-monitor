from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from src.fleet_watchdog import (
    Health,
    IncidentEvent,
    Target,
    WatchdogError,
    alert_version_marker,
    build_discord_payload,
    evaluate_target,
    explain_health,
    index_open_incidents,
    issue_body,
    issue_marker,
    load_targets,
    plan_incidents,
)


UTC = timezone.utc
NOW = datetime(2026, 8, 24, 7, 0, tzinfo=UTC)


def stamp(minutes_ago: int) -> str:
    return (NOW - timedelta(minutes=minutes_ago)).isoformat().replace("+00:00", "Z")


def run(
    *,
    minutes_ago: int,
    status: str = "completed",
    conclusion: str | None = "success",
    run_id: int = 1,
) -> dict[str, object]:
    return {
        "id": run_id,
        "created_at": stamp(minutes_ago),
        "updated_at": stamp(minutes_ago),
        "status": status,
        "conclusion": conclusion,
        "html_url": f"https://github.com/example/monitor/actions/runs/{run_id}",
    }


TARGET = Target(
    name="テスト監視",
    repository="example/monitor",
    workflow="monitor.yml",
    max_success_age_minutes=30,
    max_run_minutes=10,
    purpose="テスト用の定期監視",
    outage_impact="テスト通知が遅れる可能性があります。",
    automatic_recovery="次回実行で再確認します。",
)


class FakeClient:
    def __init__(self, runs: list[dict[str, object]], state: str = "active") -> None:
        self.runs = runs
        self.state = state

    def workflow(self, target: Target) -> dict[str, str]:
        return {"state": self.state}

    def workflow_runs(self, target: Target) -> list[dict[str, object]]:
        return self.runs


class HealthEvaluationTests(unittest.TestCase):
    def test_recent_success_is_healthy(self) -> None:
        result = evaluate_target(FakeClient([run(minutes_ago=5)]), TARGET, NOW)
        self.assertTrue(result.healthy)
        self.assertEqual(result.code, "healthy")
        self.assertEqual(result.last_success_age_minutes, 5)

    def test_latest_failure_wins_over_recent_older_success(self) -> None:
        runs = [
            run(minutes_ago=2, conclusion="failure", run_id=2),
            run(minutes_ago=5, conclusion="success", run_id=1),
        ]
        result = evaluate_target(FakeClient(runs), TARGET, NOW)
        self.assertFalse(result.healthy)
        self.assertEqual(result.code, "latest_run_failed")
        self.assertEqual(result.consecutive_failures, 1)

    def test_old_success_is_stale(self) -> None:
        result = evaluate_target(FakeClient([run(minutes_ago=31)]), TARGET, NOW)
        self.assertFalse(result.healthy)
        self.assertEqual(result.code, "success_stale")

    def test_long_running_job_is_stuck(self) -> None:
        runs = [
            run(minutes_ago=11, status="in_progress", conclusion=None, run_id=2),
            run(minutes_ago=15, conclusion="success", run_id=1),
        ]
        result = evaluate_target(FakeClient(runs), TARGET, NOW)
        self.assertFalse(result.healthy)
        self.assertEqual(result.code, "run_stuck")

    def test_disabled_workflow_is_unhealthy(self) -> None:
        result = evaluate_target(FakeClient([], state="disabled_inactivity"), TARGET, NOW)
        self.assertFalse(result.healthy)
        self.assertEqual(result.code, "workflow_disabled")

    def test_no_runs_is_unhealthy(self) -> None:
        result = evaluate_target(FakeClient([]), TARGET, NOW)
        self.assertFalse(result.healthy)
        self.assertEqual(result.code, "never_run")

    def test_first_active_run_gets_grace_period(self) -> None:
        result = evaluate_target(
            FakeClient([run(minutes_ago=2, status="in_progress", conclusion=None)]),
            TARGET,
            NOW,
        )
        self.assertTrue(result.healthy)
        self.assertEqual(result.code, "first_run_in_progress")


class IncidentPlanningTests(unittest.TestCase):
    def health(self, healthy: bool) -> Health:
        return Health(
            target=TARGET,
            healthy=healthy,
            code="healthy" if healthy else "latest_run_failed",
            detail="正常" if healthy else "失敗",
            checked_at=NOW.isoformat(),
        )

    def test_new_failure_opens_once(self) -> None:
        events = plan_incidents([self.health(False)], {})
        self.assertEqual([event.kind for event in events], ["opened"])

    def test_continuing_failure_does_not_repeat(self) -> None:
        open_incidents = {
            TARGET.key: {"number": 12, "body": alert_version_marker()}
        }
        events = plan_incidents([self.health(False)], open_incidents)
        self.assertEqual(events, [])

    def test_old_alert_format_is_updated_once(self) -> None:
        open_incidents = {TARGET.key: {"number": 12, "body": "古い通知"}}
        events = plan_incidents([self.health(False)], open_incidents)
        self.assertEqual(events[0].kind, "updated")
        self.assertEqual(events[0].issue_number, 12)

    def test_recovery_closes_existing_issue(self) -> None:
        open_incidents = {TARGET.key: {"number": 12}}
        events = plan_incidents([self.health(True)], open_incidents)
        self.assertEqual(events[0].kind, "recovered")
        self.assertEqual(events[0].issue_number, 12)

    def test_issue_marker_finds_matching_target(self) -> None:
        issues = [{"number": 7, "body": f"text\n{issue_marker(TARGET)}\nmore"}]
        indexed = index_open_incidents(issues, [TARGET])
        self.assertEqual(indexed[TARGET.key]["number"], 7)


class AlertMessageTests(unittest.TestCase):
    def stale_health(self, age: int = 31) -> Health:
        return Health(
            target=TARGET,
            healthy=False,
            code="success_stale",
            detail="最後の成功が古い",
            checked_at=NOW.isoformat(),
            latest_run_url="https://github.com/example/monitor/actions/runs/1",
            last_success_at=stamp(age),
            last_success_age_minutes=age,
        )

    def test_stale_once_is_warning_and_requires_no_immediate_action(self) -> None:
        explanation = explain_health(self.stale_health())
        self.assertEqual(explanation.severity, "warning")
        self.assertIn("何もしなくてOK", explanation.user_action)
        self.assertIn("2026/08/24", explanation.what_happened)

    def test_repeated_failure_is_critical(self) -> None:
        health = Health(
            target=TARGET,
            healthy=False,
            code="latest_run_failed",
            detail="連続失敗",
            checked_at=NOW.isoformat(),
            latest_run_conclusion="failure",
            consecutive_failures=2,
        )
        explanation = explain_health(health)
        self.assertEqual(explanation.severity, "critical")
        self.assertIn("GitHubを操作する必要はありません", explanation.user_action)

    def test_discord_alert_has_four_plain_language_sections(self) -> None:
        event = IncidentEvent("opened", self.stale_health())
        payload = build_discord_payload([event])
        description = payload["embeds"][0]["description"]
        for heading in ("何が起きた？", "影響", "自動でやること", "おまえがやること"):
            self.assertIn(heading, description)

    def test_issue_uses_current_format_marker(self) -> None:
        body = issue_body(self.stale_health())
        self.assertIn(alert_version_marker(), body)
        self.assertIn("### おまえがやること", body)


class ConfigTests(unittest.TestCase):
    def test_duplicate_target_is_rejected(self) -> None:
        item = {
            "name": "same",
            "repository": "example/monitor",
            "workflow": "monitor.yml",
            "max_success_age_minutes": 10,
            "max_run_minutes": 5,
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "targets.json"
            path.write_text(json.dumps({"targets": [item, item]}), encoding="utf-8")
            with self.assertRaises(WatchdogError):
                load_targets(path)


if __name__ == "__main__":
    unittest.main()
