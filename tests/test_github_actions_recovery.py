from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

from src.github_actions_recovery import (
    GitHubApiError,
    MONITOR_WORKFLOW,
    RELAY_WORKFLOW,
    WatchdogConfig,
    WorkflowRun,
    dispatch_with_retry,
    is_monitor_stale,
    latest_successful_run,
    load_watchdog_state,
    parse_workflow_runs,
    recent_active_run,
    run_watchdog,
    save_watchdog_state,
)


NOW = datetime(2026, 8, 18, 8, 30, tzinfo=timezone.utc)


def workflow_run(
    run_id: int,
    age_minutes: int,
    *,
    status: str = "completed",
    conclusion: str = "success",
) -> WorkflowRun:
    return WorkflowRun(
        run_id=run_id,
        created_at=NOW - timedelta(minutes=age_minutes),
        status=status,
        conclusion=conclusion,
        url=f"https://github.com/example-owner/mixch-ranking-monitor/actions/runs/{run_id}",
    )


def watchdog_config(state_file: Path, *, dry_run: bool = False) -> WatchdogConfig:
    return WatchdogConfig(
        repository="example-owner/mixch-ranking-monitor",
        github_token="test-token",
        discord_webhook_url="https://discord.com/api/webhooks/1/test",
        state_file=state_file,
        stale_minutes=12,
        reminder_minutes=60,
        relay_active_max_minutes=20,
        request_timeout_seconds=10,
        dry_run=dry_run,
    )


class FakeClient:
    """ネット接続なしで監視番の判断と起動内容を確認する偽GitHub API。"""

    def __init__(
        self,
        monitor_runs: list[WorkflowRun] | None = None,
        relay_runs: list[WorkflowRun] | None = None,
    ) -> None:
        self.monitor_runs = monitor_runs or []
        self.relay_runs = relay_runs or []
        self.dispatched: list[tuple[str, dict[str, str]]] = []
        self.dispatch_errors: dict[str, list[GitHubApiError]] = {}
        self.list_error: GitHubApiError | None = None

    def list_workflow_runs(self, workflow_file: str) -> list[WorkflowRun]:
        if self.list_error is not None:
            raise self.list_error
        return (
            list(self.monitor_runs)
            if workflow_file == MONITOR_WORKFLOW
            else list(self.relay_runs)
        )

    def dispatch_workflow(
        self,
        workflow_file: str,
        *,
        inputs: Mapping[str, str] | None = None,
    ) -> None:
        errors = self.dispatch_errors.get(workflow_file, [])
        if errors:
            raise errors.pop(0)
        self.dispatched.append((workflow_file, dict(inputs or {})))


class RunParsingTests(unittest.TestCase):
    def test_parses_and_selects_latest_success(self) -> None:
        payload = {
            "workflow_runs": [
                {
                    "id": 1,
                    "created_at": "2026-08-18T08:00:00Z",
                    "status": "completed",
                    "conclusion": "success",
                    "html_url": "https://github.com/example/run/1",
                },
                {
                    "id": 2,
                    "created_at": "2026-08-18T08:20:00Z",
                    "status": "completed",
                    "conclusion": "failure",
                    "html_url": "https://github.com/example/run/2",
                },
                {
                    "id": 3,
                    "created_at": "2026-08-18T08:10:00Z",
                    "status": "completed",
                    "conclusion": "success",
                    "html_url": "https://github.com/example/run/3",
                },
            ]
        }

        parsed = parse_workflow_runs(payload)
        latest = latest_successful_run(parsed)

        self.assertEqual(3, len(parsed))
        self.assertIsNotNone(latest)
        self.assertEqual(3, latest.run_id if latest else None)

    def test_stale_boundary_is_strictly_older_than_limit(self) -> None:
        exactly_twelve_minutes = workflow_run(1, 12)
        thirteen_minutes = workflow_run(2, 13)

        self.assertFalse(is_monitor_stale(exactly_twelve_minutes, NOW, 12))
        self.assertTrue(is_monitor_stale(thirteen_minutes, NOW, 12))

    def test_ignores_relay_that_has_been_queued_too_long(self) -> None:
        fresh = workflow_run(1, 5, status="queued", conclusion="")
        stuck = workflow_run(2, 30, status="queued", conclusion="")

        active = recent_active_run([stuck, fresh], NOW, max_age_minutes=20)

        self.assertEqual(1, active.run_id if active else None)


class DispatchRetryTests(unittest.TestCase):
    def test_retries_transient_503_with_progressive_delays(self) -> None:
        client = FakeClient()
        client.dispatch_errors[MONITOR_WORKFLOW] = [
            GitHubApiError("temporary", status_code=503),
            GitHubApiError("temporary", status_code=503),
        ]
        delays: list[float] = []

        dispatch_with_retry(
            client,
            MONITOR_WORKFLOW,
            backoff_seconds=(5, 15, 30),
            sleep=delays.append,
        )

        self.assertEqual([5.0, 15.0], delays)
        self.assertEqual([(MONITOR_WORKFLOW, {})], client.dispatched)

    def test_does_not_retry_permission_error(self) -> None:
        client = FakeClient()
        client.dispatch_errors[MONITOR_WORKFLOW] = [
            GitHubApiError("forbidden", status_code=403)
        ]
        delays: list[float] = []

        with self.assertRaises(GitHubApiError):
            dispatch_with_retry(
                client,
                MONITOR_WORKFLOW,
                backoff_seconds=(5, 15),
                sleep=delays.append,
            )

        self.assertEqual([], delays)
        self.assertEqual([], client.dispatched)


class WatchdogTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.state_file = Path(self.temporary.name) / "state.json"
        self.notifications: list[dict[str, Any]] = []

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def notify(
        self, _url: str, payload: Mapping[str, Any], _timeout: float
    ) -> None:
        self.notifications.append(dict(payload))

    def test_healthy_monitor_does_nothing(self) -> None:
        client = FakeClient(
            monitor_runs=[workflow_run(1, 5)],
            relay_runs=[workflow_run(2, 2, status="queued", conclusion="")],
        )

        result = run_watchdog(
            watchdog_config(self.state_file),
            now=NOW,
            client=client,  # type: ignore[arg-type]
            sleep=lambda _seconds: None,
            notify=self.notify,
        )

        self.assertEqual(0, result)
        self.assertEqual([], client.dispatched)
        self.assertEqual([], self.notifications)
        self.assertFalse(load_watchdog_state(self.state_file)["incident_open"])

    def test_stale_monitor_restarts_monitor_and_relay_and_alerts_once(self) -> None:
        client = FakeClient(monitor_runs=[workflow_run(1, 20)])
        config = watchdog_config(self.state_file)

        first_result = run_watchdog(
            config,
            now=NOW,
            client=client,  # type: ignore[arg-type]
            sleep=lambda _seconds: None,
            notify=self.notify,
        )

        self.assertEqual(0, first_result)
        self.assertEqual(
            [MONITOR_WORKFLOW, RELAY_WORKFLOW],
            [workflow for workflow, _inputs in client.dispatched],
        )
        self.assertEqual(1, len(self.notifications))
        self.assertIn("停止しています", self.notifications[0]["embeds"][0]["title"])
        self.assertTrue(load_watchdog_state(self.state_file)["incident_open"])

        # 10分後も停止中なら再起動は再試行するが、Discord通知は1時間抑制する。
        run_watchdog(
            config,
            now=NOW + timedelta(minutes=10),
            client=client,  # type: ignore[arg-type]
            sleep=lambda _seconds: None,
            notify=self.notify,
        )
        self.assertEqual(1, len(self.notifications))

        # 初回通知から1時間を超えた場合だけ、停止継続をもう一度知らせる。
        run_watchdog(
            config,
            now=NOW + timedelta(minutes=61),
            client=client,  # type: ignore[arg-type]
            sleep=lambda _seconds: None,
            notify=self.notify,
        )
        self.assertEqual(2, len(self.notifications))
        self.assertIn("継続", self.notifications[1]["content"])

    def test_existing_active_relay_is_not_duplicated(self) -> None:
        active_relay = workflow_run(9, 4, status="waiting", conclusion="")
        client = FakeClient(
            monitor_runs=[workflow_run(1, 20)],
            relay_runs=[active_relay],
        )

        run_watchdog(
            watchdog_config(self.state_file),
            now=NOW,
            client=client,  # type: ignore[arg-type]
            sleep=lambda _seconds: None,
            notify=self.notify,
        )

        self.assertEqual([MONITOR_WORKFLOW], [item[0] for item in client.dispatched])
        description = self.notifications[0]["embeds"][0]["description"]
        self.assertIn("既存リレーが稼働中", description)

    def test_recent_active_monitor_is_allowed_to_finish(self) -> None:
        active_monitor = workflow_run(8, 2, status="in_progress", conclusion="")
        client = FakeClient(
            monitor_runs=[workflow_run(1, 20), active_monitor],
            relay_runs=[workflow_run(9, 1, status="waiting", conclusion="")],
        )

        result = run_watchdog(
            watchdog_config(self.state_file),
            now=NOW,
            client=client,  # type: ignore[arg-type]
            sleep=lambda _seconds: None,
            notify=self.notify,
        )

        self.assertEqual(0, result)
        self.assertEqual([], client.dispatched)
        self.assertEqual([], self.notifications)

    def test_recovery_notification_closes_incident(self) -> None:
        state = load_watchdog_state(self.state_file)
        state.update(
            {
                "incident_open": True,
                "incident_started_at": "2026-08-18T08:00:00Z",
                "incident_kind": "monitor_stale",
                "last_alerted_at": "2026-08-18T08:00:00Z",
            }
        )
        save_watchdog_state(self.state_file, state)
        client = FakeClient(monitor_runs=[workflow_run(20, 2)])

        result = run_watchdog(
            watchdog_config(self.state_file),
            now=NOW,
            client=client,  # type: ignore[arg-type]
            sleep=lambda _seconds: None,
            notify=self.notify,
        )

        self.assertEqual(0, result)
        self.assertEqual(1, len(self.notifications))
        self.assertIn("自動復旧", self.notifications[0]["embeds"][0]["title"])
        saved = load_watchdog_state(self.state_file)
        self.assertFalse(saved["incident_open"])
        self.assertEqual("2026-08-18T08:30:00Z", saved["last_recovered_at"])

    def test_github_status_check_failure_is_alerted_and_saved(self) -> None:
        client = FakeClient()
        client.list_error = GitHubApiError("service unavailable", status_code=503)

        result = run_watchdog(
            watchdog_config(self.state_file),
            now=NOW,
            client=client,  # type: ignore[arg-type]
            sleep=lambda _seconds: None,
            notify=self.notify,
        )

        self.assertEqual(1, result)
        self.assertEqual(1, len(self.notifications))
        self.assertIn("GitHubへ接続できません", self.notifications[0]["embeds"][0]["title"])
        saved = json.loads(self.state_file.read_text(encoding="utf-8"))
        self.assertTrue(saved["incident_open"])
        self.assertEqual("github_status_check_failed", saved["incident_kind"])

    def test_manual_dry_run_has_no_side_effects(self) -> None:
        client = FakeClient(monitor_runs=[workflow_run(1, 30)])

        result = run_watchdog(
            watchdog_config(self.state_file, dry_run=True),
            now=NOW,
            client=client,  # type: ignore[arg-type]
            sleep=lambda _seconds: None,
            notify=self.notify,
        )

        self.assertEqual(0, result)
        self.assertEqual([], client.dispatched)
        self.assertEqual([], self.notifications)
        self.assertFalse(self.state_file.exists())

    def test_manual_dry_run_keeps_no_side_effects_when_github_is_unavailable(self) -> None:
        client = FakeClient()
        client.list_error = GitHubApiError("service unavailable", status_code=503)

        result = run_watchdog(
            watchdog_config(self.state_file, dry_run=True),
            now=NOW,
            client=client,  # type: ignore[arg-type]
            sleep=lambda _seconds: None,
            notify=self.notify,
        )

        self.assertEqual(1, result)
        self.assertEqual([], client.dispatched)
        self.assertEqual([], self.notifications)
        self.assertFalse(self.state_file.exists())


if __name__ == "__main__":
    unittest.main()
