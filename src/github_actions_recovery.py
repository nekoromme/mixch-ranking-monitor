#!/usr/bin/env python3
"""GitHub Actionsの5分リレーを止まりにくくし、停止時は自動復旧する。

このモジュールには二つの役割がある。

``dispatch``
    リレーから別のワークフローを起動する。GitHub APIの一時的な503や
    429に対して、5秒・15秒・30秒・60秒と待ち時間を延ばしながら再試行する。

``watchdog``
    独立した定期実行から、監視本体の最後の成功時刻を確認する。一定時間
    成功がなければ監視本体とリレーを再起動し、Discordへ停止を通知する。
    正常な監視が戻ったことを次回確認できた時点で、復旧通知を送る。

外部Pythonパッケージを使わず、GitHub Actions標準のPythonだけで動作する。
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlparse
from urllib.request import Request, urlopen


LOGGER = logging.getLogger("mixch-actions-recovery")
JST = timezone(timedelta(hours=9))

GITHUB_API_BASE = "https://api.github.com"
MONITOR_WORKFLOW = "ranking-monitor.yml"
RELAY_WORKFLOW = "relay.yml"
ACTIVE_RUN_STATUSES = frozenset(
    {"queued", "in_progress", "waiting", "pending", "requested"}
)

DEFAULT_STALE_MINUTES = 12
DEFAULT_REMINDER_MINUTES = 60
DEFAULT_RELAY_ACTIVE_MAX_MINUTES = 20
DEFAULT_REQUEST_TIMEOUT_SECONDS = 10.0

# 1回目は即時実行するため、ここには「次の試行まで待つ秒数」だけを並べる。
# 合計110秒待つので、今回のような30～40秒程度の503を吸収できる。
DEFAULT_DISPATCH_BACKOFF_SECONDS = (5.0, 15.0, 30.0, 60.0)

WATCHDOG_STATE_VERSION = 1
DISCORD_ALERT_COLOR = 0xE74C3C
DISCORD_RECOVERY_COLOR = 0x2ECC71


class RecoveryError(RuntimeError):
    """自動復旧処理を安全に続行できない場合。"""


class StateError(RecoveryError):
    """監視番の状態ファイルが壊れている場合。"""


class GitHubApiError(RecoveryError):
    """GitHub REST APIから正常な応答を得られなかった場合。"""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        retry_after: float | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.retry_after = retry_after

    @property
    def retryable(self) -> bool:
        """時間を置けば直る可能性があるエラーだけを再試行対象にする。"""

        return (
            self.status_code is None
            or self.status_code == 429
            or (self.status_code == 403 and self.retry_after is not None)
            or (self.status_code is not None and 500 <= self.status_code <= 599)
        )


class DiscordError(RecoveryError):
    """停止・復旧通知をDiscordへ送れなかった場合。"""


@dataclass(frozen=True, slots=True)
class WorkflowRun:
    """停止判定に必要なGitHub Actions実行履歴の最小情報。"""

    run_id: int
    created_at: datetime
    status: str
    conclusion: str
    url: str


@dataclass(frozen=True, slots=True)
class WatchdogConfig:
    """環境変数から読み込む停止監視の設定。"""

    repository: str
    github_token: str
    discord_webhook_url: str
    state_file: Path
    stale_minutes: int
    reminder_minutes: int
    relay_active_max_minutes: int
    request_timeout_seconds: float
    dry_run: bool

    @classmethod
    def from_environment(cls) -> "WatchdogConfig":
        repository = os.getenv("GITHUB_REPOSITORY", "").strip()
        if repository.count("/") != 1:
            raise RecoveryError(
                "GITHUB_REPOSITORYが owner/repository の形式ではありません"
            )

        github_token = (
            os.getenv("GH_TOKEN", "").strip()
            or os.getenv("GITHUB_TOKEN", "").strip()
        )
        if not github_token:
            raise RecoveryError("GH_TOKENまたはGITHUB_TOKENが未設定です")

        webhook_url = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
        _validate_discord_webhook_url(webhook_url)

        return cls(
            repository=repository,
            github_token=github_token,
            discord_webhook_url=webhook_url,
            state_file=Path(
                os.getenv("WATCHDOG_STATE_FILE", "watchdog-state.json")
            ),
            stale_minutes=_read_int(
                "WATCHDOG_STALE_MINUTES",
                DEFAULT_STALE_MINUTES,
                minimum=7,
                maximum=120,
            ),
            reminder_minutes=_read_int(
                "WATCHDOG_REMINDER_MINUTES",
                DEFAULT_REMINDER_MINUTES,
                minimum=15,
                maximum=1_440,
            ),
            relay_active_max_minutes=_read_int(
                "WATCHDOG_RELAY_ACTIVE_MAX_MINUTES",
                DEFAULT_RELAY_ACTIVE_MAX_MINUTES,
                minimum=10,
                maximum=120,
            ),
            request_timeout_seconds=_read_float(
                "GITHUB_API_TIMEOUT_SECONDS",
                DEFAULT_REQUEST_TIMEOUT_SECONDS,
                minimum=3,
                maximum=60,
            ),
            dry_run=_read_bool("WATCHDOG_DRY_RUN", False),
        )


class GitHubApiClient:
    """今回必要なGitHub Actions APIだけを呼び出す小さなクライアント。"""

    def __init__(
        self,
        repository: str,
        token: str,
        timeout_seconds: float = DEFAULT_REQUEST_TIMEOUT_SECONDS,
    ) -> None:
        self.repository = repository
        self.token = token
        self.timeout_seconds = timeout_seconds

    def list_workflow_runs(
        self, workflow_file: str, per_page: int = 20
    ) -> list[WorkflowRun]:
        encoded_workflow = quote(workflow_file, safe="")
        payload = self._request_json(
            "GET",
            f"/repos/{self.repository}/actions/workflows/"
            f"{encoded_workflow}/runs?per_page={per_page}",
        )
        if not isinstance(payload, dict):
            raise GitHubApiError("GitHubの実行履歴がJSONオブジェクトではありません")
        return parse_workflow_runs(payload)

    def dispatch_workflow(
        self,
        workflow_file: str,
        *,
        inputs: Mapping[str, str] | None = None,
    ) -> None:
        encoded_workflow = quote(workflow_file, safe="")
        body: dict[str, Any] = {"ref": "main"}
        if inputs:
            body["inputs"] = dict(inputs)
        self._request_json(
            "POST",
            f"/repos/{self.repository}/actions/workflows/"
            f"{encoded_workflow}/dispatches",
            body,
        )

    def _request_json(
        self,
        method: str,
        path: str,
        body: Mapping[str, Any] | None = None,
    ) -> Any:
        data = None
        if body is not None:
            data = json.dumps(body, ensure_ascii=False).encode("utf-8")

        request = Request(
            f"{GITHUB_API_BASE}{path}",
            data=data,
            method=method,
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
                "User-Agent": "MixChannelMonitorRecovery/1.0",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )

        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                raw = response.read()
                # workflow_dispatchの正常応答は204で本文がない。
                return json.loads(raw.decode("utf-8")) if raw else None
        except HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            message = _github_error_message(raw) or exc.reason or "不明なエラー"
            raise GitHubApiError(
                f"GitHub APIがHTTP {exc.code}を返しました: {message}",
                status_code=exc.code,
                retry_after=_retry_after_seconds(exc.headers),
            ) from exc
        except (URLError, TimeoutError, OSError) as exc:
            raise GitHubApiError(
                f"GitHub APIへ接続できませんでした ({type(exc).__name__})"
            ) from exc
        except json.JSONDecodeError as exc:
            raise GitHubApiError("GitHub APIのJSON応答を解析できませんでした") from exc


def parse_workflow_runs(payload: Mapping[str, Any]) -> list[WorkflowRun]:
    """GitHub APIの大きな応答から、必要な実行情報だけを検証して抜き出す。"""

    raw_runs = payload.get("workflow_runs")
    if not isinstance(raw_runs, list):
        raise GitHubApiError("GitHubの実行履歴にworkflow_runsがありません")

    runs: list[WorkflowRun] = []
    for item in raw_runs:
        if not isinstance(item, dict):
            continue
        try:
            runs.append(
                WorkflowRun(
                    run_id=int(item["id"]),
                    created_at=_parse_timestamp(str(item["created_at"])),
                    status=str(item.get("status") or ""),
                    conclusion=str(item.get("conclusion") or ""),
                    url=str(item.get("html_url") or ""),
                )
            )
        except (KeyError, TypeError, ValueError, StateError):
            LOGGER.warning("不正なGitHub実行履歴を1件読み飛ばしました")
    return runs


def latest_successful_run(runs: Sequence[WorkflowRun]) -> WorkflowRun | None:
    """成功した監視のうち、開始時刻が最も新しいものを返す。"""

    successful = [run for run in runs if run.conclusion == "success"]
    return max(successful, key=lambda run: run.created_at, default=None)


def recent_active_run(
    runs: Sequence[WorkflowRun],
    now: datetime,
    max_age_minutes: int,
) -> WorkflowRun | None:
    """待機中・実行中で、古すぎないリレーを返す。

    何時間もqueuedのまま固まった実行を「正常」と誤認しないよう、作成からの
    最大時間も確認する。
    """

    oldest = now - timedelta(minutes=max_age_minutes)
    active = [
        run
        for run in runs
        if run.status in ACTIVE_RUN_STATUSES and run.created_at >= oldest
    ]
    return max(active, key=lambda run: run.created_at, default=None)


def is_monitor_stale(
    latest_success: WorkflowRun | None,
    now: datetime,
    stale_minutes: int,
) -> bool:
    """最後の正常監視から設定時間以上経過しているか判定する。"""

    return latest_success is None or (
        latest_success.created_at < now - timedelta(minutes=stale_minutes)
    )


def dispatch_with_retry(
    client: GitHubApiClient,
    workflow_file: str,
    *,
    inputs: Mapping[str, str] | None = None,
    backoff_seconds: Sequence[float] = DEFAULT_DISPATCH_BACKOFF_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """一時的なGitHub API障害に耐えながらワークフローを起動する。"""

    attempts = len(backoff_seconds) + 1
    for index in range(attempts):
        try:
            client.dispatch_workflow(workflow_file, inputs=inputs)
            LOGGER.info(
                "%sを起動しました（試行%d/%d）", workflow_file, index + 1, attempts
            )
            return
        except GitHubApiError as exc:
            is_last = index == attempts - 1
            if is_last or not exc.retryable:
                raise

            delay = (
                exc.retry_after
                if exc.retry_after is not None
                else float(backoff_seconds[index])
            )
            # 異常なRetry-Afterでワークフロー全体が何時間も固まらないよう上限を設ける。
            delay = min(max(delay, 1.0), 120.0)
            LOGGER.warning(
                "%sの起動に失敗しました（試行%d/%d）: %s。%.0f秒後に再試行します",
                workflow_file,
                index + 1,
                attempts,
                exc,
                delay,
            )
            sleep(delay)


def new_watchdog_state() -> dict[str, Any]:
    return {
        "version": WATCHDOG_STATE_VERSION,
        "incident_open": False,
        "incident_started_at": None,
        "incident_kind": None,
        "last_alerted_at": None,
        "last_restart_attempt_at": None,
        "last_recovered_at": None,
    }


def load_watchdog_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return new_watchdog_state()
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise StateError("監視番の状態ファイルを読み込めません") from exc
    if not isinstance(state, dict) or state.get("version") != WATCHDOG_STATE_VERSION:
        raise StateError("監視番の状態ファイルの形式が不正です")

    expected = new_watchdog_state()
    expected.update(state)
    return expected


def save_watchdog_state(path: Path, state: Mapping[str, Any]) -> None:
    """途中で書き込みが切れてJSONが壊れないよう、一時ファイルから置き換える。"""

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(dict(state), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def should_send_incident_alert(
    state: Mapping[str, Any], now: datetime, reminder_minutes: int
) -> bool:
    """新規停止、または長時間継続時だけ通知し、定期実行ごとの連投を防ぐ。"""

    previous = state.get("last_alerted_at")
    if not previous:
        return True
    try:
        alerted_at = _parse_timestamp(str(previous))
    except StateError:
        return True
    return alerted_at <= now - timedelta(minutes=reminder_minutes)


def run_watchdog(
    config: WatchdogConfig,
    *,
    now: datetime | None = None,
    client: GitHubApiClient | None = None,
    sleep: Callable[[float], None] = time.sleep,
    notify: Callable[[str, Mapping[str, Any], float], None] | None = None,
) -> int:
    """停止を検知して復旧を試み、必要なDiscord通知と状態保存を行う。"""

    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    state = load_watchdog_state(config.state_file)
    api = client or GitHubApiClient(
        config.repository,
        config.github_token,
        config.request_timeout_seconds,
    )
    notifier = notify or post_discord

    try:
        monitor_runs = api.list_workflow_runs(MONITOR_WORKFLOW)
        relay_runs = api.list_workflow_runs(RELAY_WORKFLOW)
    except GitHubApiError as exc:
        # 手動dry-runでは「何もしない」という約束を、GitHub障害時も守る。
        if config.dry_run:
            LOGGER.error("dry-run: GitHub Actionsの状態を確認できませんでした: %s", exc)
            return 1
        return _handle_status_check_failure(
            config, state, now, exc, notifier=notifier
        )

    latest_success = latest_successful_run(monitor_runs)
    active_monitor = recent_active_run(
        monitor_runs,
        now,
        config.stale_minutes,
    )
    active_relay = recent_active_run(
        relay_runs,
        now,
        config.relay_active_max_minutes,
    )
    stale = is_monitor_stale(latest_success, now, config.stale_minutes)

    if config.dry_run:
        LOGGER.info(
            "dry-run: 最終成功=%s, 停止判定=%s, 稼働中監視=%s, 稼働中リレー=%s",
            _describe_run(latest_success),
            stale,
            _describe_run(active_monitor),
            _describe_run(active_relay),
        )
        return 0

    # 最後の成功は古くても、新しい監視が待機中・実行中なら、その結果を待つ。
    # ここで復旧通知を送ると「まだ成功していないのに復旧扱い」になるため、
    # incident_openは閉じず、次回の監視番へ判断を持ち越す。
    if stale and active_monitor is not None:
        LOGGER.info(
            "新しい監視が稼働中のため結果を待ちます: %s",
            _describe_run(active_monitor),
        )
        save_watchdog_state(config.state_file, state)
        return 0

    if not stale:
        LOGGER.info("監視は正常です: %s", _describe_run(latest_success))
        return _close_incident_if_recovered(
            config,
            state,
            now,
            latest_success,
            notifier=notifier,
        )

    is_new_incident = not bool(state.get("incident_open"))
    if is_new_incident:
        state["incident_open"] = True
        state["incident_started_at"] = _format_timestamp(now)
        state["incident_kind"] = "monitor_stale"
        state["last_alerted_at"] = None

    state["last_restart_attempt_at"] = _format_timestamp(now)
    monitor_result = _attempt_dispatch(
        api,
        MONITOR_WORKFLOW,
        inputs={"dry_run": "false", "test_webhook": "false"},
        sleep=sleep,
    )

    if active_relay is None:
        relay_result = _attempt_dispatch(api, RELAY_WORKFLOW, sleep=sleep)
    else:
        relay_result = f"既存リレーが稼働中のため追加起動なし（{active_relay.url}）"

    notification_failed = False
    if should_send_incident_alert(state, now, config.reminder_minutes):
        payload = build_incident_payload(
            config.repository,
            latest_success,
            active_relay,
            now,
            monitor_result,
            relay_result,
            is_reminder=not is_new_incident,
        )
        try:
            notifier(
                config.discord_webhook_url,
                payload,
                config.request_timeout_seconds,
            )
            state["last_alerted_at"] = _format_timestamp(now)
        except DiscordError as exc:
            notification_failed = True
            LOGGER.error("停止通知をDiscordへ送れませんでした: %s", exc)

    save_watchdog_state(config.state_file, state)

    # 監視本体か既存リレーのどちらかが動けるなら、次回の監視番で結果を確認する。
    monitor_started = monitor_result.startswith("起動予約成功")
    relay_available = active_relay is not None or relay_result.startswith("起動予約成功")
    return 0 if (monitor_started or relay_available) and not notification_failed else 1


def _handle_status_check_failure(
    config: WatchdogConfig,
    state: dict[str, Any],
    now: datetime,
    error: GitHubApiError,
    *,
    notifier: Callable[[str, Mapping[str, Any], float], None],
) -> int:
    """GitHub API自体を確認できない場合も、可能ならDiscordへ知らせる。"""

    is_new_incident = not bool(state.get("incident_open"))
    if is_new_incident:
        state["incident_open"] = True
        state["incident_started_at"] = _format_timestamp(now)
        state["incident_kind"] = "github_status_check_failed"
        state["last_alerted_at"] = None

    if should_send_incident_alert(state, now, config.reminder_minutes):
        payload = build_status_check_failure_payload(
            config.repository,
            now,
            error,
            is_reminder=not is_new_incident,
        )
        try:
            notifier(
                config.discord_webhook_url,
                payload,
                config.request_timeout_seconds,
            )
            state["last_alerted_at"] = _format_timestamp(now)
        except DiscordError as notify_error:
            LOGGER.error("GitHub確認失敗をDiscordへ送れませんでした: %s", notify_error)

    save_watchdog_state(config.state_file, state)
    LOGGER.error("GitHub Actionsの状態を確認できませんでした: %s", error)
    return 1


def _close_incident_if_recovered(
    config: WatchdogConfig,
    state: dict[str, Any],
    now: datetime,
    latest_success: WorkflowRun | None,
    *,
    notifier: Callable[[str, Mapping[str, Any], float], None],
) -> int:
    if not state.get("incident_open"):
        save_watchdog_state(config.state_file, state)
        return 0

    payload = build_recovery_payload(
        config.repository,
        latest_success,
        state.get("incident_started_at"),
        now,
    )
    try:
        notifier(
            config.discord_webhook_url,
            payload,
            config.request_timeout_seconds,
        )
    except DiscordError as exc:
        # 復旧通知に失敗した場合はincident_openを残し、次回もう一度通知する。
        LOGGER.error("復旧通知をDiscordへ送れませんでした: %s", exc)
        save_watchdog_state(config.state_file, state)
        return 1

    state["incident_open"] = False
    state["incident_started_at"] = None
    state["incident_kind"] = None
    state["last_alerted_at"] = None
    state["last_restart_attempt_at"] = None
    state["last_recovered_at"] = _format_timestamp(now)
    save_watchdog_state(config.state_file, state)
    return 0


def _attempt_dispatch(
    client: GitHubApiClient,
    workflow_file: str,
    *,
    inputs: Mapping[str, str] | None = None,
    sleep: Callable[[float], None],
) -> str:
    try:
        dispatch_with_retry(
            client,
            workflow_file,
            inputs=inputs,
            sleep=sleep,
        )
        return "起動予約成功"
    except GitHubApiError as exc:
        LOGGER.error("%sを自動起動できませんでした: %s", workflow_file, exc)
        return f"起動予約失敗: {_truncate(str(exc), 300)}"


def build_incident_payload(
    repository: str,
    latest_success: WorkflowRun | None,
    active_relay: WorkflowRun | None,
    now: datetime,
    monitor_result: str,
    relay_result: str,
    *,
    is_reminder: bool,
) -> dict[str, Any]:
    title = "🚨 MixChannel勢い監視が停止しています"
    content = "監視停止が継続しています。再度、自動復旧を試みました。" if is_reminder else (
        "監視停止を検知したため、自動復旧を開始しました。"
    )
    if latest_success is None:
        latest_text = "正常終了した監視を確認できません"
    else:
        age_minutes = max(int((now - latest_success.created_at).total_seconds() // 60), 0)
        latest_text = (
            f"[{_format_jst(latest_success.created_at)}]({latest_success.url})"
            f"（{age_minutes}分前）"
        )

    relay_text = (
        f"稼働中: [実行履歴]({active_relay.url})"
        if active_relay is not None
        else "稼働中のリレーなし"
    )
    actions_url = f"https://github.com/{repository}/actions"
    return {
        "username": "MixChannel停止監視",
        "content": content,
        "allowed_mentions": {"parse": []},
        "embeds": [
            {
                "title": title,
                "description": (
                    f"**最後の正常監視**\n{latest_text}\n\n"
                    f"**5分リレー**\n{relay_text}\n\n"
                    f"**監視本体の再起動**\n{monitor_result}\n\n"
                    f"**リレーの再起動**\n{relay_result}\n\n"
                    f"[GitHub Actionsを確認]({actions_url})"
                ),
                "color": DISCORD_ALERT_COLOR,
                "timestamp": _format_timestamp(now),
                "footer": {"text": "同じ停止中の通常通知は1時間に1回まで"},
            }
        ],
    }


def build_status_check_failure_payload(
    repository: str,
    now: datetime,
    error: Exception,
    *,
    is_reminder: bool,
) -> dict[str, Any]:
    content = (
        "GitHub Actionsの状態確認失敗が継続しています。"
        if is_reminder
        else "GitHub Actionsの状態を確認できませんでした。"
    )
    return {
        "username": "MixChannel停止監視",
        "content": f"⚠️ {content}",
        "allowed_mentions": {"parse": []},
        "embeds": [
            {
                "title": "監視番がGitHubへ接続できません",
                "description": (
                    f"{_truncate(str(error), 900)}\n\n"
                    f"[GitHub Actionsを確認](https://github.com/{repository}/actions)"
                ),
                "color": DISCORD_ALERT_COLOR,
                "timestamp": _format_timestamp(now),
                "footer": {"text": "同じ異常の通常通知は1時間に1回まで"},
            }
        ],
    }


def build_recovery_payload(
    repository: str,
    latest_success: WorkflowRun | None,
    incident_started_at: Any,
    now: datetime,
) -> dict[str, Any]:
    latest_text = (
        f"[{_format_jst(latest_success.created_at)}]({latest_success.url})"
        if latest_success is not None
        else "時刻不明"
    )
    duration_text = "不明"
    if incident_started_at:
        try:
            started_at = _parse_timestamp(str(incident_started_at))
            minutes = max(int((now - started_at).total_seconds() // 60), 0)
            duration_text = f"約{minutes}分"
        except StateError:
            pass

    return {
        "username": "MixChannel停止監視",
        "content": "✅ MixChannel勢い監視の正常動作を再確認しました。",
        "allowed_mentions": {"parse": []},
        "embeds": [
            {
                "title": "自動復旧を確認しました",
                "description": (
                    f"**最新の正常監視**\n{latest_text}\n\n"
                    f"**停止検知から復旧確認まで**\n{duration_text}\n\n"
                    f"[GitHub Actionsを確認](https://github.com/{repository}/actions)"
                ),
                "color": DISCORD_RECOVERY_COLOR,
                "timestamp": _format_timestamp(now),
            }
        ],
    }


def post_discord(
    webhook_url: str,
    payload: Mapping[str, Any],
    timeout_seconds: float,
) -> None:
    """Discordの一時的な送信制限や5xxに対して最大3回再試行する。"""

    _validate_discord_webhook_url(webhook_url)
    data = json.dumps(dict(payload), ensure_ascii=False).encode("utf-8")
    delays = (2.0, 5.0)
    for attempt in range(3):
        request = Request(
            webhook_url,
            data=data,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "User-Agent": "MixChannelMonitorRecovery/1.0",
            },
        )
        try:
            with urlopen(request, timeout=timeout_seconds) as response:
                status = getattr(response, "status", 204)
                if 200 <= status < 300:
                    return
                raise DiscordError(f"DiscordがHTTP {status}を返しました")
        except HTTPError as exc:
            retryable = exc.code == 429 or 500 <= exc.code <= 599
            if retryable and attempt < len(delays):
                delay = _retry_after_seconds(exc.headers) or delays[attempt]
                time.sleep(min(max(delay, 1.0), 30.0))
                continue
            raise DiscordError(f"DiscordがHTTP {exc.code}を返しました") from exc
        except (URLError, TimeoutError, OSError) as exc:
            if attempt < len(delays):
                time.sleep(delays[attempt])
                continue
            raise DiscordError(
                f"Discordへ接続できませんでした ({type(exc).__name__})"
            ) from exc

    raise DiscordError("Discord通知の再試行回数を超えました")


def _dispatch_command(args: argparse.Namespace) -> int:
    repository = os.getenv("GITHUB_REPOSITORY", "").strip()
    token = os.getenv("GH_TOKEN", "").strip() or os.getenv("GITHUB_TOKEN", "").strip()
    if repository.count("/") != 1 or not token:
        raise RecoveryError("GITHUB_REPOSITORYとGH_TOKENが必要です")

    inputs: dict[str, str] = {}
    for value in args.input:
        if "=" not in value:
            raise RecoveryError("--inputは name=value の形式で指定してください")
        key, input_value = value.split("=", 1)
        if not key:
            raise RecoveryError("--inputの名前が空です")
        inputs[key] = input_value

    client = GitHubApiClient(
        repository,
        token,
        _read_float(
            "GITHUB_API_TIMEOUT_SECONDS",
            DEFAULT_REQUEST_TIMEOUT_SECONDS,
            minimum=3,
            maximum=60,
        ),
    )
    dispatch_with_retry(client, args.workflow, inputs=inputs or None)
    return 0


def _watchdog_command(_args: argparse.Namespace) -> int:
    return run_watchdog(WatchdogConfig.from_environment())


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    dispatch = subparsers.add_parser(
        "dispatch", description="GitHubワークフローを段階的に再試行して起動する"
    )
    dispatch.add_argument("workflow", help="例: ranking-monitor.yml")
    dispatch.add_argument(
        "--input",
        action="append",
        default=[],
        help="workflow_dispatchの入力。name=value形式で複数指定できる",
    )
    dispatch.set_defaults(handler=_dispatch_command)

    watchdog = subparsers.add_parser(
        "watchdog", description="監視停止を検知し、自動復旧とDiscord通知を行う"
    )
    watchdog.set_defaults(handler=_watchdog_command)
    return parser


def _github_error_message(raw: str) -> str:
    try:
        payload = json.loads(raw)
        if isinstance(payload, dict) and payload.get("message"):
            return str(payload["message"])
    except json.JSONDecodeError:
        pass
    return _truncate(" ".join(raw.split()), 500)


def _retry_after_seconds(headers: Any) -> float | None:
    if not headers:
        return None
    raw = headers.get("Retry-After")
    if not raw:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _validate_discord_webhook_url(value: str) -> None:
    parsed = urlparse(value)
    allowed_hosts = {
        "discord.com",
        "www.discord.com",
        "discordapp.com",
        "www.discordapp.com",
        "canary.discord.com",
        "ptb.discord.com",
    }
    if (
        parsed.scheme != "https"
        or parsed.hostname not in allowed_hosts
        or not parsed.path.startswith("/api/webhooks/")
    ):
        raise RecoveryError(
            "DISCORD_WEBHOOK_URLがDiscordのWebhook URLではありません"
        )


def _read_int(name: str, default: int, minimum: int, maximum: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise RecoveryError(f"{name}は整数で指定してください") from exc
    if not minimum <= value <= maximum:
        raise RecoveryError(f"{name}は{minimum}～{maximum}で指定してください")
    return value


def _read_float(name: str, default: float, minimum: float, maximum: float) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise RecoveryError(f"{name}は数値で指定してください") from exc
    if not minimum <= value <= maximum:
        raise RecoveryError(f"{name}は{minimum}～{maximum}で指定してください")
    return value


def _read_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name, "").strip().lower()
    if not raw:
        return default
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    raise RecoveryError(f"{name}はtrueまたはfalseで指定してください")


def _parse_timestamp(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise StateError(f"時刻形式が不正です: {value!r}") from exc
    if parsed.tzinfo is None:
        raise StateError(f"時刻にタイムゾーンがありません: {value!r}")
    return parsed.astimezone(timezone.utc)


def _format_timestamp(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )


def _format_jst(value: datetime) -> str:
    return value.astimezone(JST).strftime("%Y-%m-%d %H:%M:%S JST")


def _describe_run(run: WorkflowRun | None) -> str:
    if run is None:
        return "なし"
    return f"id={run.run_id}, status={run.status}, created_at={_format_timestamp(run.created_at)}"


def _truncate(value: str, limit: int) -> str:
    return value if len(value) <= limit else value[: limit - 1] + "…"


def configure_logging() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def main(argv: Sequence[str] | None = None) -> int:
    configure_logging()
    parser = build_argument_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.handler(args))
    except Exception as exc:  # noqa: BLE001 - Actionsログへ原因を必ず残す
        LOGGER.exception("自動復旧処理に失敗しました: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
