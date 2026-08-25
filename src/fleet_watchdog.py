"""GitHub Actions で動く監視ツール群を、まとめて外側から監視する。

外部パッケージを使わず、GitHub の公式 API と Discord Webhook だけで動く。
異常の重複通知を防ぐ状態保存には GitHub Issue を利用するため、壊れやすい
キャッシュや、複数実行が衝突しやすい JSON の自動コミットは必要ない。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable


UTC = timezone.utc
JST = timezone(timedelta(hours=9))
API_VERSION = "2022-11-28"
USER_AGENT = "nekoromme-fleet-watchdog/1.0"
ISSUE_PREFIX = "[監視ツール異常]"
ALERT_FORMAT_VERSION = "3"


class WatchdogError(RuntimeError):
    """監視番そのものが処理を続けられない場合の例外。"""


@dataclass(frozen=True)
class Target:
    """監視する GitHub Actions ワークフロー1件分の設定。"""

    name: str
    repository: str
    workflow: str
    max_success_age_minutes: int
    max_run_minutes: int
    purpose: str = "監視対象の定期処理"
    outage_impact: str = "この監視対象の通知や更新が遅れる可能性があります。"
    automatic_recovery: str = "次回の定期実行で自動的に再確認します。"

    @property
    def key(self) -> str:
        return f"{self.repository}/{self.workflow}"

    @property
    def actions_url(self) -> str:
        return (
            f"https://github.com/{self.repository}/actions/workflows/"
            f"{urllib.parse.quote(self.workflow)}"
        )


@dataclass(frozen=True)
class Health:
    """1件のワークフローについて出した健康診断結果。"""

    target: Target
    healthy: bool
    code: str
    detail: str
    checked_at: str
    latest_run_url: str | None = None
    latest_run_status: str | None = None
    latest_run_conclusion: str | None = None
    last_success_at: str | None = None
    last_success_age_minutes: int | None = None
    consecutive_failures: int = 0

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["target"] = asdict(self.target)
        return data


@dataclass(frozen=True)
class IncidentEvent:
    """Issue と Discord に反映すべき、新規異常または復旧。"""

    kind: str  # "opened"、"updated" または "recovered"
    health: Health
    issue_number: int | None = None


@dataclass(frozen=True)
class AlertExplanation:
    """通知を読んだ人が、状況と次の行動を迷わないための説明。"""

    severity: str
    icon: str
    label: str
    headline: str
    what_happened: str
    impact: str
    automatic_action: str
    user_action: str
    color: int


def parse_github_time(value: str | None) -> datetime | None:
    """GitHub の ISO 8601 時刻をタイムゾーン付き datetime にする。"""

    if not value:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def age_minutes(now: datetime, value: str | None) -> int | None:
    parsed = parse_github_time(value)
    if parsed is None:
        return None
    # API 側と runner 側の時計が数秒ずれて未来になる場合は、0分として扱う。
    return max(0, int((now - parsed).total_seconds() // 60))


def format_jst(value: str | None) -> str:
    """機械向けUTCではなく、ユーザーがそのまま読める日本時間へ変換する。"""

    parsed = parse_github_time(value)
    if parsed is None:
        return "不明"
    return parsed.astimezone(JST).strftime("%Y/%m/%d %H:%M")


class GitHubClient:
    """標準ライブラリだけで GitHub REST API を呼ぶ小さなクライアント。"""

    def __init__(self, token: str = "", api_url: str = "https://api.github.com") -> None:
        self.token = token.strip()
        self.api_url = api_url.rstrip("/")

    def request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
    ) -> Any:
        url = f"{self.api_url}{path}"
        data = None if payload is None else json.dumps(payload).encode("utf-8")
        headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": API_VERSION,
            "User-Agent": USER_AGENT,
        }
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        if data is not None:
            headers["Content-Type"] = "application/json"

        # 読み取りだけは一時的な 5xx や通信切断を計3回まで再試行する。
        # Issue 作成などの書き込みは、応答だけ失われた時の二重作成を避けるため
        # 自動再試行しない。
        delays = (0, 2, 6) if method == "GET" else (0,)
        last_error: Exception | None = None
        for attempt, delay in enumerate(delays):
            if delay:
                time.sleep(delay)
            request = urllib.request.Request(url, data=data, headers=headers, method=method)
            try:
                with urllib.request.urlopen(request, timeout=20) as response:
                    body = response.read()
                    return json.loads(body) if body else {}
            except urllib.error.HTTPError as exc:
                body = exc.read().decode("utf-8", errors="replace")[:1000]
                last_error = WatchdogError(
                    f"GitHub API {method} {path} が HTTP {exc.code}: {body}"
                )
                retryable = exc.code == 429 or 500 <= exc.code < 600
                if not retryable or attempt == len(delays) - 1:
                    raise last_error from exc
            except (urllib.error.URLError, TimeoutError) as exc:
                last_error = WatchdogError(
                    f"GitHub API {method} {path} への接続に失敗: {exc}"
                )
                if attempt == len(delays) - 1:
                    raise last_error from exc

        raise WatchdogError(str(last_error or "GitHub API で不明なエラー"))

    def workflow(self, target: Target) -> dict[str, Any]:
        workflow = urllib.parse.quote(target.workflow, safe="")
        return self.request(
            "GET", f"/repos/{target.repository}/actions/workflows/{workflow}"
        )

    def workflow_runs(self, target: Target) -> list[dict[str, Any]]:
        workflow = urllib.parse.quote(target.workflow, safe="")
        result = self.request(
            "GET",
            f"/repos/{target.repository}/actions/workflows/{workflow}/runs?per_page=30",
        )
        return list(result.get("workflow_runs", []))

    def open_issues(self, repository: str) -> list[dict[str, Any]]:
        result = self.request("GET", f"/repos/{repository}/issues?state=open&per_page=100")
        return [item for item in result if "pull_request" not in item]

    def create_issue(self, repository: str, title: str, body: str) -> dict[str, Any]:
        return self.request(
            "POST", f"/repos/{repository}/issues", {"title": title, "body": body}
        )

    def update_issue(self, repository: str, number: int, title: str, body: str) -> None:
        self.request(
            "PATCH",
            f"/repos/{repository}/issues/{number}",
            {"title": title, "body": body},
        )

    def comment_issue(self, repository: str, number: int, body: str) -> None:
        self.request("POST", f"/repos/{repository}/issues/{number}/comments", {"body": body})

    def close_issue(self, repository: str, number: int) -> None:
        self.request("PATCH", f"/repos/{repository}/issues/{number}", {"state": "closed"})


def load_targets(path: Path) -> list[Target]:
    """設定を読み、設定ミスを監視開始前にはっきり失敗させる。"""

    raw = json.loads(path.read_text(encoding="utf-8"))
    items = raw.get("targets")
    if not isinstance(items, list) or not items:
        raise WatchdogError("targets 設定が空です")

    targets: list[Target] = []
    keys: set[str] = set()
    for index, item in enumerate(items, start=1):
        try:
            target = Target(
                name=str(item["name"]).strip(),
                repository=str(item["repository"]).strip(),
                workflow=str(item["workflow"]).strip(),
                max_success_age_minutes=int(item["max_success_age_minutes"]),
                max_run_minutes=int(item["max_run_minutes"]),
                purpose=str(item.get("purpose") or "監視対象の定期処理").strip(),
                outage_impact=str(
                    item.get("outage_impact")
                    or "この監視対象の通知や更新が遅れる可能性があります。"
                ).strip(),
                automatic_recovery=str(
                    item.get("automatic_recovery")
                    or "次回の定期実行で自動的に再確認します。"
                ).strip(),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise WatchdogError(f"targets の {index} 件目が不正です: {exc}") from exc

        if (
            not target.name
            or "/" not in target.repository
            or not target.workflow
            or not target.purpose
            or not target.outage_impact
            or not target.automatic_recovery
        ):
            raise WatchdogError(f"targets の {index} 件目に空欄または不正なリポジトリ名があります")
        if target.max_success_age_minutes <= 0 or target.max_run_minutes <= 0:
            raise WatchdogError(f"targets の {index} 件目の時間は1以上にしてください")
        if target.key in keys:
            raise WatchdogError(f"監視対象が重複しています: {target.key}")
        keys.add(target.key)
        targets.append(target)
    return targets


def _run_time(run: dict[str, Any]) -> datetime:
    return parse_github_time(run.get("created_at")) or datetime.min.replace(tzinfo=UTC)


def evaluate_target(client: GitHubClient, target: Target, now: datetime) -> Health:
    """ワークフローの無効化・停止・失敗・固まりを順番に判定する。"""

    checked_at = now.astimezone(UTC).isoformat()
    try:
        workflow = client.workflow(target)
        runs = sorted(client.workflow_runs(target), key=_run_time, reverse=True)
    except Exception as exc:  # 1対象の通信エラーで、残りの監視を捨てない。
        return Health(
            target=target,
            healthy=False,
            code="api_error",
            detail=f"GitHubから実行状況を取得できません: {exc}",
            checked_at=checked_at,
        )

    workflow_state = str(workflow.get("state", "unknown"))
    if workflow_state != "active":
        return Health(
            target=target,
            healthy=False,
            code="workflow_disabled",
            detail=f"ワークフローが有効ではありません（state={workflow_state}）",
            checked_at=checked_at,
        )

    if not runs:
        return Health(
            target=target,
            healthy=False,
            code="never_run",
            detail="実行履歴が1件もありません",
            checked_at=checked_at,
        )

    latest = runs[0]
    latest_url = latest.get("html_url")
    latest_status = latest.get("status")
    latest_conclusion = latest.get("conclusion")

    # 「実行中」のまま制限時間を大きく超えた固まりを検知する。
    active_runs = [run for run in runs if run.get("status") in {"queued", "in_progress", "waiting"}]
    if active_runs:
        oldest_active = min(active_runs, key=_run_time)
        active_age = age_minutes(now, oldest_active.get("created_at")) or 0
        if active_age > target.max_run_minutes:
            return Health(
                target=target,
                healthy=False,
                code="run_stuck",
                detail=(
                    f"実行中のまま {active_age} 分経過しています"
                    f"（許容 {target.max_run_minutes} 分）"
                ),
                checked_at=checked_at,
                latest_run_url=oldest_active.get("html_url"),
                latest_run_status=str(oldest_active.get("status")),
                latest_run_conclusion=oldest_active.get("conclusion"),
            )

    completed = [run for run in runs if run.get("status") == "completed"]
    latest_completed = completed[0] if completed else None
    successes = [run for run in completed if run.get("conclusion") == "success"]
    last_success = successes[0] if successes else None

    # 直近の完了が失敗なら、古い成功が新しくても異常として扱う。
    if latest_completed and latest_completed.get("conclusion") != "success":
        failures = 0
        for run in completed:
            if run.get("conclusion") == "success":
                break
            failures += 1
        conclusion = str(latest_completed.get("conclusion") or "unknown")
        return Health(
            target=target,
            healthy=False,
            code="latest_run_failed",
            detail=f"直近の完了結果が {conclusion}（連続 {failures} 回）です",
            checked_at=checked_at,
            latest_run_url=latest_completed.get("html_url"),
            latest_run_status=str(latest_completed.get("status")),
            latest_run_conclusion=conclusion,
            last_success_at=last_success.get("updated_at") if last_success else None,
            last_success_age_minutes=(
                age_minutes(now, last_success.get("updated_at")) if last_success else None
            ),
            consecutive_failures=failures,
        )

    if last_success is None:
        # 初回実行が今まさに動いている時だけは、完了まで猶予を与える。
        if active_runs:
            return Health(
                target=target,
                healthy=True,
                code="first_run_in_progress",
                detail="初回実行中です",
                checked_at=checked_at,
                latest_run_url=latest_url,
                latest_run_status=str(latest_status),
                latest_run_conclusion=latest_conclusion,
            )
        return Health(
            target=target,
            healthy=False,
            code="never_succeeded",
            detail="成功した実行履歴がありません",
            checked_at=checked_at,
            latest_run_url=latest_url,
            latest_run_status=str(latest_status),
            latest_run_conclusion=latest_conclusion,
        )

    success_at = last_success.get("updated_at") or last_success.get("created_at")
    success_age = age_minutes(now, success_at)
    if success_age is None or success_age > target.max_success_age_minutes:
        return Health(
            target=target,
            healthy=False,
            code="success_stale",
            detail=(
                f"最後の成功から {success_age if success_age is not None else '不明'} 分経過"
                f"（許容 {target.max_success_age_minutes} 分）"
            ),
            checked_at=checked_at,
            latest_run_url=latest_url,
            latest_run_status=str(latest_status),
            latest_run_conclusion=latest_conclusion,
            last_success_at=success_at,
            last_success_age_minutes=success_age,
        )

    return Health(
        target=target,
        healthy=True,
        code="healthy",
        detail=f"正常（最後の成功から {success_age} 分）",
        checked_at=checked_at,
        latest_run_url=latest_url,
        latest_run_status=str(latest_status),
        latest_run_conclusion=latest_conclusion,
        last_success_at=success_at,
        last_success_age_minutes=success_age,
    )


def issue_marker(target: Target) -> str:
    digest = hashlib.sha256(target.key.encode("utf-8")).hexdigest()[:20]
    return f"<!-- fleet-watchdog:{digest} -->"


def alert_version_marker() -> str:
    return f"<!-- fleet-watchdog-alert-version:{ALERT_FORMAT_VERSION} -->"


def _conclusion_ja(value: str | None) -> str:
    return {
        "failure": "失敗",
        "cancelled": "キャンセル",
        "timed_out": "時間切れ",
        "action_required": "追加操作待ち",
        "startup_failure": "開始失敗",
        "stale": "停止扱い",
        "skipped": "スキップ",
        "neutral": "判定なし",
    }.get(str(value), str(value or "不明"))


def explain_health(result: Health) -> AlertExplanation:
    """機械的な判定コードを、判断と行動が分かる日本語へ変換する。"""

    name = result.target.name
    if result.healthy:
        success = (
            f"最後の正常終了は {format_jst(result.last_success_at)} です。"
            if result.last_success_at
            else result.detail
        )
        return AlertExplanation(
            severity="recovery",
            icon="🟢",
            label="復旧",
            headline=f"{name}は復旧しました",
            what_happened=f"正常に戻りました。{success}",
            impact="現在は正常です。通知や更新が遅れる可能性は解消しました。",
            automatic_action="このまま通常どおり監視を続けます。",
            user_action="何もしなくてOKです。",
            color=0x2ECC71,
        )

    common_wait = "何もしなくてOKです。"
    common_manual = "この通知をそのままわたしに送ってください。"

    severity = "warning"
    label = "様子見"
    icon = "⚠️"
    color = 0xF39C12
    impact = result.target.outage_impact
    automatic = (
        f"{result.target.automatic_recovery} "
        "全体監視番も15分おきに復旧を確認します。"
    )
    action = common_wait

    if result.code == "api_error":
        headline = f"{name}の状態確認に失敗しました"
        what = "GitHubから状態を取得できませんでした。監視対象の停止は未確認です。"
        impact = "現時点では実害不明です。次の確認結果を待ちます。"
        automatic = "全体監視番が15分後にもう一度確認します。"
    elif result.code == "success_stale":
        age = result.last_success_age_minutes
        headline = f"{name}が予定どおり動いていません"
        what = (
            f"最後の正常終了は {format_jst(result.last_success_at)}。"
            f"{age if age is not None else '不明'}分動いておらず、"
            f"通常の待ち時間{result.target.max_success_age_minutes}分を超えました。"
        )
        if age is not None and age > result.target.max_success_age_minutes * 2:
            severity, label, icon, color, action = (
                "critical",
                "対応が必要",
                "🔴",
                0xE74C3C,
                common_manual,
            )
    elif result.code == "latest_run_failed":
        conclusion = _conclusion_ja(result.latest_run_conclusion)
        headline = f"{name}の直近実行が{conclusion}しました"
        what = f"直近の実行が{conclusion}しました（連続{result.consecutive_failures}回）。"
        if result.consecutive_failures >= 2:
            severity, label, icon, color, action = (
                "critical",
                "対応が必要",
                "🔴",
                0xE74C3C,
                common_manual,
            )
    elif result.code == "run_stuck":
        headline = f"{name}の処理が長引いています"
        what = result.detail
    elif result.code == "workflow_disabled":
        headline = f"{name}の定期実行が無効になっています"
        what = "GitHubの定期実行が無効になっています。"
        severity, label, icon, color, action = (
            "critical",
            "対応が必要",
            "🔴",
            0xE74C3C,
            common_manual,
        )
        automatic = "全体監視番だけでは有効化できないため、自動復旧はできません。"
    elif result.code == "never_run":
        headline = f"{name}に実行履歴がありません"
        what = "定期実行が一度も動いた記録を確認できません。"
        severity, label, icon, color, action = (
            "critical",
            "対応が必要",
            "🔴",
            0xE74C3C,
            common_manual,
        )
    elif result.code == "never_succeeded":
        headline = f"{name}が一度も正常終了していません"
        what = "正常終了した記録を一度も確認できません。"
        severity, label, icon, color, action = (
            "critical",
            "対応が必要",
            "🔴",
            0xE74C3C,
            common_manual,
        )
    else:
        headline = f"{name}で異常を検知しました"
        what = result.detail

    return AlertExplanation(
        severity=severity,
        icon=icon,
        label=label,
        headline=headline,
        what_happened=what,
        impact=impact,
        automatic_action=automatic,
        user_action=action,
        color=color,
    )


def index_open_incidents(issues: Iterable[dict[str, Any]], targets: Iterable[Target]) -> dict[str, dict[str, Any]]:
    """開いている Issue を監視対象キーへ対応付ける。"""

    indexed: dict[str, dict[str, Any]] = {}
    target_list = list(targets)
    for issue in issues:
        body = str(issue.get("body") or "")
        for target in target_list:
            if issue_marker(target) in body:
                indexed[target.key] = issue
                break
    return indexed


def plan_incidents(results: Iterable[Health], open_incidents: dict[str, dict[str, Any]]) -> list[IncidentEvent]:
    """新規異常・通知形式の更新・復旧だけを抽出する。"""

    events: list[IncidentEvent] = []
    for result in results:
        existing = open_incidents.get(result.target.key)
        if not result.healthy and existing is None:
            events.append(IncidentEvent("opened", result))
        elif (
            not result.healthy
            and existing is not None
            and alert_version_marker() not in str(existing.get("body") or "")
        ):
            events.append(IncidentEvent("updated", result, int(existing["number"])))
        elif result.healthy and existing is not None:
            events.append(IncidentEvent("recovered", result, int(existing["number"])))
    return events


def issue_body(result: Health) -> str:
    explanation = explain_health(result)
    run_link = result.latest_run_url or result.target.actions_url
    return "\n".join(
        [
            issue_marker(result.target),
            alert_version_marker(),
            f"## {explanation.icon} {explanation.label}：{explanation.headline}",
            "",
            "### 何が起きた",
            explanation.what_happened,
            "",
            "### おまえがやること",
            explanation.user_action,
            "",
            f"[詳しい実行履歴を開く]({run_link})",
            "",
            "<details><summary>技術情報</summary>",
            "",
            f"- 目的: {result.target.purpose}",
            f"- リポジトリ: `{result.target.repository}`",
            f"- ワークフロー: `{result.target.workflow}`",
            f"- 種類: `{result.code}`",
            f"- 元の判定: {result.detail}",
            f"- 検知時刻: {format_jst(result.checked_at)}（日本時間）",
            "",
            "</details>",
            "",
            "復旧を確認すると、このIssueは監視番が自動で閉じます。",
        ]
    )


def build_discord_payload(
    events: Iterable[IncidentEvent], test: bool = False
) -> dict[str, Any]:
    """Discord通知を「何が起きた」「ユーザーがやること」だけで組み立てる。"""

    embeds: list[dict[str, Any]] = []
    if test:
        embeds.append(
            {
                "title": "✅ 通知テスト成功",
                "description": (
                    "**何が起きた**\nDiscordへの通知経路は正常です。\n\n"
                    "**おまえがやること**\n何もしなくてOKです。"
                ),
                "color": 0x2ECC71,
            }
        )
    else:
        for event in events:
            explanation = explain_health(event.health)
            run_url = event.health.latest_run_url or event.health.target.actions_url
            embeds.append(
                {
                    "title": (
                        f"{explanation.icon} {explanation.label}："
                        f"{explanation.headline}"
                    )[:256],
                    "description": (
                        f"**何が起きた**\n{explanation.what_happened}\n\n"
                        f"**おまえがやること**\n{explanation.user_action}"
                    )[:4096],
                    "url": run_url,
                    "color": explanation.color,
                }
            )

    return {
        "username": "監視ツール監視番",
        "allowed_mentions": {"parse": []},
        "embeds": embeds[:10],
    }


def send_discord(webhook_url: str, events: Iterable[IncidentEvent], test: bool = False) -> None:
    """状態変化を1通へまとめる。Webhook URL 自体はログへ絶対に出さない。"""

    event_list = list(events)
    if not test and not event_list:
        return
    payload = build_discord_payload(event_list, test=test)
    request = urllib.request.Request(
        webhook_url,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json", "User-Agent": USER_AGENT},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            if response.status not in {200, 204}:
                raise WatchdogError(f"Discord通知が HTTP {response.status} でした")
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
        raise WatchdogError(f"Discord通知に失敗しました: {exc}") from exc


def apply_incidents(
    client: GitHubClient,
    home_repository: str,
    events: Iterable[IncidentEvent],
) -> None:
    for event in events:
        result = event.health
        if event.kind == "opened":
            client.create_issue(
                home_repository,
                f"{ISSUE_PREFIX} {result.target.name}",
                issue_body(result),
            )
            print(f"INCIDENT_OPENED target={result.target.key} code={result.code}")
        elif event.kind == "updated":
            assert event.issue_number is not None
            client.update_issue(
                home_repository,
                event.issue_number,
                f"{ISSUE_PREFIX} {result.target.name}",
                issue_body(result),
            )
            print(f"INCIDENT_UPDATED target={result.target.key} code={result.code}")
        else:
            assert event.issue_number is not None
            explanation = explain_health(result)
            client.comment_issue(
                home_repository,
                event.issue_number,
                (
                    f"## {explanation.icon} {explanation.headline}\n\n"
                    f"{explanation.what_happened}\n\n"
                    f"**おまえがやること:** {explanation.user_action}\n\n"
                    f"確認時刻: {format_jst(result.checked_at)}（日本時間）"
                ),
            )
            client.close_issue(home_repository, event.issue_number)
            print(f"INCIDENT_RECOVERED target={result.target.key}")


def markdown_summary(results: Iterable[Health]) -> str:
    lines = [
        "# 監視ツール群の健康診断",
        "",
        "| 状態 | 対象 | 判定 | 最後の成功 |",
        "|---|---|---|---|",
    ]
    for result in results:
        state = "✅ 正常" if result.healthy else "❌ 異常"
        detail = result.detail.replace("|", "\\|").replace("\n", " ")
        success = result.last_success_at or "－"
        lines.append(
            f"| {state} | [{result.target.name}]({result.target.actions_url}) "
            f"| {detail} | {success} |"
        )
    return "\n".join(lines) + "\n"


def write_outputs(results: list[Health], report_path: Path) -> None:
    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "healthy": all(result.healthy for result in results),
        "targets": [result.to_dict() for result in results],
    }
    report_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )

    summary_path = os.getenv("GITHUB_STEP_SUMMARY", "").strip()
    if summary_path:
        with Path(summary_path).open("a", encoding="utf-8") as handle:
            handle.write(markdown_summary(results))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="GitHub上の監視ツール群をまとめて監視")
    parser.add_argument("--targets", type=Path, default=Path("fleet_targets.json"))
    parser.add_argument("--report", type=Path, default=Path("fleet-watchdog-report.json"))
    parser.add_argument("--dry-run", action="store_true", help="Issue・Discordを変更しない")
    parser.add_argument("--test-notification", action="store_true", help="Discordへテスト送信")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    token = os.getenv("GH_TOKEN") or os.getenv("GITHUB_TOKEN") or ""
    home_repository = os.getenv("GITHUB_REPOSITORY", "nekoromme/mixch-ranking-monitor")
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
    client = GitHubClient(token=token, api_url=os.getenv("GITHUB_API_URL", "https://api.github.com"))

    try:
        targets = load_targets(args.targets)
        if args.test_notification:
            if not webhook_url:
                raise WatchdogError("DISCORD_WEBHOOK_URL がないためテスト通知できません")
            send_discord(webhook_url, [], test=True)
            print("DISCORD_TEST_OK")

        now = datetime.now(UTC)
        results = [evaluate_target(client, target, now) for target in targets]
        for result in results:
            level = "OK" if result.healthy else "ERROR"
            print(
                f"FLEET_WATCHDOG level={level} target={result.target.key} "
                f"code={result.code} detail={json.dumps(result.detail, ensure_ascii=False)}"
            )
            if not result.healthy:
                explanation = explain_health(result)
                print(
                    f"::warning title={explanation.headline}::"
                    f"{explanation.what_happened} {explanation.user_action}"
                )
        write_outputs(results, args.report)

        # dry-run でも既存 Issue は読み、何が新規通知・復旧になるかまでは確認する。
        open_issues = client.open_issues(home_repository)
        indexed = index_open_incidents(open_issues, targets)
        events = plan_incidents(results, indexed)

        if args.dry_run:
            for event in events:
                print(f"DRY_RUN event={event.kind} target={event.health.target.key}")
        else:
            # Discord送信が失敗した場合はIssueを変えない。次回も同じ状態変化を
            # 再送でき、通知だけ取りこぼす事故を防げる。
            if webhook_url and events:
                send_discord(webhook_url, events)
            apply_incidents(client, home_repository, events)

        # 監視対象の異常はIssueとDiscordで扱う。ここを失敗終了にすると、
        # GitHubが「全ジョブ失敗」という意味不明な二重通知を送るため常に成功扱い。
        # 監視番自身が壊れて例外になった時だけ、下の except で終了コード2を返す。
        return 0
    except Exception as exc:
        print(f"FLEET_WATCHDOG_FATAL {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
