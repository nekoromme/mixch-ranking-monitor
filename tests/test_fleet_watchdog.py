from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from src.fleet_watchdog import (
    Health,
    IncidentEvent,
    Target,
    WatchdogError,
    action_requirement_marker,
    alert_version_marker,
    apply_redundancy_rules,
    build_discord_payload,
    evaluate_target,
    explain_health,
    index_open_incidents,
    issue_body,
    issue_marker,
    load_targets,
    plan_incidents,
    send_discord,
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

    def test_stale_success_with_fresh_active_recovery_is_temporarily_healthy(self) -> None:
        runs = [
            run(minutes_ago=2, status="queued", conclusion=None, run_id=2),
            run(minutes_ago=31, conclusion="success", run_id=1),
        ]
        result = evaluate_target(FakeClient(runs), TARGET, NOW)
        self.assertTrue(result.healthy)
        self.assertEqual(result.code, "recovery_in_progress")


class RedundancyAndRecoveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.main_target = Target(
            name="本体",
            repository="example/monitor",
            workflow="main.yml",
            max_success_age_minutes=30,
            max_run_minutes=10,
        )
        self.relay_target = Target(
            name="リレー",
            repository="example/monitor",
            workflow="relay.yml",
            max_success_age_minutes=30,
            max_run_minutes=10,
        )
        self.backup_target = Target(
            name="予備の自動復旧",
            repository="example/monitor",
            workflow="watchdog.yml",
            max_success_age_minutes=30,
            max_run_minutes=10,
            user_action_requires_all_unhealthy=(
                self.main_target.key,
                self.relay_target.key,
            ),
        )

    def health(self, target: Target, healthy: bool) -> Health:
        return Health(
            target=target,
            healthy=healthy,
            code="healthy" if healthy else "success_stale",
            detail="正常" if healthy else "停止",
            checked_at=NOW.isoformat(),
            last_success_at=stamp(100),
            last_success_age_minutes=100,
        )

    def test_backup_alert_is_suppressed_while_real_monitor_paths_are_healthy(self) -> None:
        results = apply_redundancy_rules(
            [
                self.health(self.main_target, True),
                self.health(self.relay_target, True),
                self.health(self.backup_target, False),
            ]
        )
        backup = results[2]
        self.assertEqual(backup.user_action_blocked_by, ("本体", "リレー"))
        self.assertFalse(explain_health(backup).requires_user_action)
        self.assertEqual(
            build_discord_payload([IncidentEvent("opened", backup)])["embeds"],
            [],
        )

    def test_backup_alert_is_actionable_when_all_real_monitor_paths_are_down(self) -> None:
        results = apply_redundancy_rules(
            [
                self.health(self.main_target, False),
                self.health(self.relay_target, False),
                self.health(self.backup_target, False),
            ]
        )
        self.assertEqual(results[2].user_action_blocked_by, ())
        self.assertTrue(explain_health(results[2]).requires_user_action)



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
            TARGET.key: {
                "number": 12,
                "body": f"{alert_version_marker()}\n{action_requirement_marker(False)}",
            }
        }
        events = plan_incidents([self.health(False)], open_incidents)
        self.assertEqual(events, [])

    def test_warning_escalation_to_action_required_updates_once(self) -> None:
        critical = Health(
            target=TARGET,
            healthy=False,
            code="latest_run_failed",
            detail="連続失敗",
            checked_at=NOW.isoformat(),
            latest_run_conclusion="failure",
            consecutive_failures=2,
        )
        open_incidents = {
            TARGET.key: {
                "number": 12,
                "body": f"{alert_version_marker()}\n{action_requirement_marker(False)}",
            }
        }
        events = plan_incidents([critical], open_incidents)
        self.assertEqual([event.kind for event in events], ["updated"])

    def test_action_required_incident_does_not_repeat(self) -> None:
        critical = Health(
            target=TARGET,
            healthy=False,
            code="latest_run_failed",
            detail="連続失敗",
            checked_at=NOW.isoformat(),
            latest_run_conclusion="failure",
            consecutive_failures=2,
        )
        open_incidents = {
            TARGET.key: {
                "number": 12,
                "body": f"{alert_version_marker()}\n{action_requirement_marker(True)}",
            }
        }
        self.assertEqual(plan_incidents([critical], open_incidents), [])

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

    def critical_health(self) -> Health:
        return Health(
            target=TARGET,
            healthy=False,
            code="latest_run_failed",
            detail="連続失敗",
            checked_at=NOW.isoformat(),
            latest_run_url="https://github.com/example/monitor/actions/runs/2",
            latest_run_conclusion="failure",
            consecutive_failures=2,
        )

    def test_stale_once_is_warning_and_requires_no_immediate_action(self) -> None:
        explanation = explain_health(self.stale_health())
        self.assertEqual(explanation.severity, "warning")
        self.assertIn("何もしなくてOK", explanation.user_action)
        self.assertIn("2026/08/24", explanation.what_happened)

    def test_repeated_failure_is_critical(self) -> None:
        explanation = explain_health(self.critical_health())
        self.assertEqual(explanation.severity, "critical")
        self.assertTrue(explanation.requires_user_action)
        self.assertEqual(explanation.user_action, "この通知をそのままわたしに送ってください。")

    def test_actionable_discord_alert_has_only_situation_and_user_action(self) -> None:
        event = IncidentEvent("opened", self.critical_health())
        payload = build_discord_payload([event])
        embed = payload["embeds"][0]
        description = embed["description"]
        for heading in ("何が起きた", "おまえがやること"):
            self.assertIn(heading, description)
        for removed_heading in ("影響", "自動でやること"):
            self.assertNotIn(removed_heading, description)
        self.assertEqual(embed["url"], self.critical_health().latest_run_url)

    def test_warning_and_recovery_do_not_create_discord_embeds(self) -> None:
        warning = IncidentEvent("opened", self.stale_health())
        recovery = IncidentEvent(
            "recovered",
            Health(
                target=TARGET,
                healthy=True,
                code="healthy",
                detail="正常",
                checked_at=NOW.isoformat(),
                last_success_at=stamp(1),
            ),
        )
        self.assertEqual(build_discord_payload([warning, recovery])["embeds"], [])

    @patch("src.fleet_watchdog.urllib.request.urlopen")
    def test_non_actionable_event_does_not_call_discord(self, urlopen) -> None:
        send_discord(
            "https://discord.example/webhook",
            [IncidentEvent("opened", self.stale_health())],
        )
        urlopen.assert_not_called()

    def test_issue_uses_current_format_marker(self) -> None:
        body = issue_body(self.stale_health())
        self.assertIn(alert_version_marker(), body)
        self.assertIn("### 何が起きた", body)
        self.assertIn("### おまえがやること", body)
        self.assertNotIn("### 影響", body)
        self.assertNotIn("### 自動でやること", body)


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
