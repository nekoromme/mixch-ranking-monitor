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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


UTC = timezone.utc
API_VERSION = "2022-11-28"
USER_AGENT = "nekoromme-fleet-watchdog/1.0"
ISSUE_PREFIX = "[監視ツール異常]"


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

    kind: str  # "opened" または "recovered"
    health: Health
    issue_number: int | None = None


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
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise WatchdogError(f"targets の {index} 件目が不正です: {exc}") from exc

        if not target.name or "/" not in target.repository or not target.workflow:
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
    """新規異常と復旧だけを抽出し、継続中の異常は通知しない。"""

    events: list[IncidentEvent] = []
    for result in results:
        existing = open_incidents.get(result.target.key)
        if not result.healthy and existing is None:
            events.append(IncidentEvent("opened", result))
        elif result.healthy and existing is not None:
            events.append(IncidentEvent("recovered", result, int(existing["number"])))
    return events


def issue_body(result: Health) -> str:
    run_link = result.latest_run_url or result.target.actions_url
    return "\n".join(
        [
            issue_marker(result.target),
            "## 監視ツールの異常を検知",
            "",
            f"- 対象: **{result.target.name}**",
            f"- リポジトリ: `{result.target.repository}`",
            f"- ワークフロー: `{result.target.workflow}`",
            f"- 種類: `{result.code}`",
            f"- 内容: {result.detail}",
            f"- 検知時刻（UTC）: `{result.checked_at}`",
            f"- [Actionsを開く]({run_link})",
            "",
            "復旧を確認すると、このIssueは監視番が自動で閉じます。",
        ]
    )


def send_discord(webhook_url: str, events: Iterable[IncidentEvent], test: bool = False) -> None:
    """状態変化を1通へまとめる。Webhook URL 自体はログへ絶対に出さない。"""

    if test:
        title = "✅ 監視ツール監視番：テスト成功"
        description = "Discordへの通知経路は正常です。"
        color = 0x2ECC71
    else:
        event_list = list(events)
        if not event_list:
            return
        lines: list[str] = []
        has_failure = False
        for event in event_list:
            if event.kind == "opened":
                has_failure = True
                icon = "🔴"
                label = "異常"
            else:
                icon = "🟢"
                label = "復旧"
            run_url = event.health.latest_run_url or event.health.target.actions_url
            lines.append(
                f"{icon} **{label}: {event.health.target.name}**\n"
                f"{event.health.detail}\n[Actionsを開く]({run_url})"
            )
        title = "監視ツール群の状態が変わりました"
        description = "\n\n".join(lines)[:4000]
        color = 0xE74C3C if has_failure else 0x2ECC71

    payload = {
        "username": "監視ツール監視番",
        "allowed_mentions": {"parse": []},
        "embeds": [{"title": title, "description": description, "color": color}],
    }
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
        else:
            assert event.issue_number is not None
            client.comment_issue(
                home_repository,
                event.issue_number,
                (
                    "## 復旧を確認\n\n"
                    f"{result.detail}\n\n"
                    f"確認時刻（UTC）: `{result.checked_at}`"
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

        return 0 if all(result.healthy for result in results) else 1
    except Exception as exc:
        print(f"FLEET_WATCHDOG_FATAL {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
