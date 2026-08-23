#!/usr/bin/env python3
"""ライブランキングZのMixChannel欄を監視してDiscordへ通知する。

外部パッケージを使わず、GitHub Actionsの起動をなるべく短く保つ設計。
しきい値や再通知までの時間は環境変数（GitHubのRepository variables）で
変更できる。
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import AbstractSet, Any, Iterable, Sequence
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen


LOGGER = logging.getLogger("mixch-ranking-monitor")

DEFAULT_MONITOR_URL = "https://live-ranking.com/v/mixch"
DEFAULT_FALLBACK_MONITOR_URL = "https://ikioi-ranking.com/v/mixch"
# 通知しない配信者の初期リスト。
#
# 配信者名は変更できるため、ここでは名前ではなくMixChannelのユーザーIDを
# 固定で登録する。Repository variableのBLOCKED_USER_IDSは、この初期リストへ
# 追加する仕組みなので、既存の設定を消さずに個別追加もできる。
DEFAULT_BLOCKED_USER_IDS = frozenset(
    {
        "14082684",  # 既存の初期ブロック対象
        "17373942",  # うえきあやか
        "18014848",  # 日DXコーラ
        "18504420",  # のうみくん#ﾚｷﾞｭﾗｰﾓﾃﾞ
        "18674264",  # こうぐちまﾙ
    }
)
DEFAULT_THRESHOLD = 150
DEFAULT_COOLDOWN_HOURS = 12.0
DEFAULT_ERROR_COOLDOWN_HOURS = 6.0
DEFAULT_HEARTBEAT_DAYS = 7.0
DEFAULT_TIMEOUT_SECONDS = 30.0
READER_FALLBACK_PREFIX = "https://r.jina.ai/"
MIXCH_ARCHIVES_API = "https://mixch.tv/api-web/users/{user_id}/live_archives"
PUBLIC_ARCHIVE_VISIBILITY = 1
ARCHIVE_PAGE_SIZE = 100
ARCHIVE_CHECK_WORKERS = 4
MAX_ARCHIVE_PAGES = 100
JST = timezone(timedelta(hours=9))

STATE_VERSION = 1
DISCORD_EMBED_COLOR = 0xFF4D87
MAX_EMBEDS_PER_MESSAGE = 5
DISCORD_DESCRIPTION_LIMIT = 3_800
RANKING_LOG_LIMIT = 10


class MonitorError(RuntimeError):
    """監視処理で利用者に知らせるべき異常。"""


class ParseError(MonitorError):
    """監視ページの構造を安全に読み取れなかった場合。"""


class StateError(MonitorError):
    """再通知抑制用の状態ファイルが壊れている場合。"""


class NotificationError(MonitorError):
    """Discord通知に失敗した場合。"""


@dataclass(frozen=True, slots=True)
class Stream:
    """ランキングに掲載されている1配信。"""

    user_id: str
    broadcaster_name: str
    title: str
    url: str
    momentum: int
    rank: int | None
    elapsed_minutes: int | None
    elapsed_text: str


@dataclass(frozen=True, slots=True)
class Config:
    """環境変数から読み込む実行設定。"""

    monitor_url: str
    fallback_monitor_url: str
    momentum_threshold: int
    cooldown_hours: float
    error_cooldown_hours: float
    heartbeat_days: float
    request_timeout_seconds: float
    state_file: Path
    discord_webhook_url: str
    dry_run: bool
    test_webhook: bool
    notify_on_error: bool
    blocked_user_ids: frozenset[str]

    @classmethod
    def from_environment(cls) -> "Config":
        monitor_url = os.getenv("MONITOR_URL", DEFAULT_MONITOR_URL).strip()
        _validate_https_url(monitor_url, "MONITOR_URL")

        fallback_monitor_url = os.getenv(
            "FALLBACK_MONITOR_URL", DEFAULT_FALLBACK_MONITOR_URL
        ).strip()
        _validate_https_url(fallback_monitor_url, "FALLBACK_MONITOR_URL")

        webhook_url = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
        if webhook_url:
            _validate_discord_webhook_url(webhook_url)

        return cls(
            monitor_url=monitor_url,
            fallback_monitor_url=fallback_monitor_url,
            momentum_threshold=_read_int(
                "MOMENTUM_THRESHOLD", DEFAULT_THRESHOLD, minimum=0, maximum=1_000_000
            ),
            cooldown_hours=_read_float(
                "COOLDOWN_HOURS", DEFAULT_COOLDOWN_HOURS, minimum=0.01, maximum=8_760
            ),
            error_cooldown_hours=_read_float(
                "ERROR_NOTIFY_COOLDOWN_HOURS",
                DEFAULT_ERROR_COOLDOWN_HOURS,
                minimum=0.25,
                maximum=168,
            ),
            heartbeat_days=_read_float(
                "HEARTBEAT_DAYS", DEFAULT_HEARTBEAT_DAYS, minimum=1, maximum=30
            ),
            request_timeout_seconds=_read_float(
                "REQUEST_TIMEOUT_SECONDS",
                DEFAULT_TIMEOUT_SECONDS,
                minimum=5,
                maximum=120,
            ),
            state_file=Path(os.getenv("STATE_FILE", "state.json")),
            discord_webhook_url=webhook_url,
            dry_run=_read_bool("DRY_RUN", False),
            test_webhook=_read_bool("TEST_WEBHOOK", False),
            notify_on_error=_read_bool("NOTIFY_ON_ERROR", True),
            # 初期ブロックリストへ、Repository variableで指定したIDを追加する。
            blocked_user_ids=(
                DEFAULT_BLOCKED_USER_IDS | _read_user_id_set("BLOCKED_USER_IDS")
            ),
        )


@dataclass(slots=True)
class _RawStream:
    """HTML解析中だけ使う未検証データ。"""

    user_id: str = ""
    broadcaster_name: str = ""
    title: str = ""
    url: str = ""
    momentum_text: str = ""
    rank_text: str = ""
    elapsed_text: str = ""
    elapsed_title: str = ""


class _RankingParser(HTMLParser):
    """対象ページの各 ``div#livebox`` を読む小さな専用パーサー。

    正規表現だけでHTML全体を切るより、入れ子や文字実体参照に強い。
    一方、外部HTML解析ライブラリを毎回インストールする必要もない。
    """

    _TARGET_FIELDS = {
        "live_rankNum": "rank_text",
        "live_title": "title",
        "live_name": "broadcaster_name",
        "live_viewer": "momentum_text",
    }

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.raw_streams: list[_RawStream] = []
        self.livebox_count = 0
        self.broadcast_count: int | None = None
        self._current: _RawStream | None = None
        self._livebox_div_depth = 0
        self._capture_field: str | None = None
        self._capture_end_tag: str | None = None
        self._capture_same_tag_depth = 0
        self._capture_parts: list[str] = []
        self._header_capture = False
        self._header_parts: list[str] = []

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        attr = {key: value or "" for key, value in attrs}
        classes = set(attr.get("class", "").split())

        if tag == "span" and attr.get("id") == "live_list_header1":
            self._header_capture = True
            self._header_parts = []

        if tag == "div" and attr.get("id") == "livebox":
            # 壊れたHTMLで前のliveboxが閉じていなくても、前項目を捨てずに確定する。
            if self._current is not None:
                self._finish_current()
            self.livebox_count += 1
            self._current = _RawStream(user_id=attr.get("data-uid", ""))
            self._livebox_div_depth = 1
            return

        if self._current is None:
            return

        if tag == "div":
            self._livebox_div_depth += 1

        if self._capture_field and tag == self._capture_end_tag:
            self._capture_same_tag_depth += 1

        if not self._capture_field:
            for class_name, field_name in self._TARGET_FIELDS.items():
                if class_name in classes:
                    self._begin_capture(field_name, tag)
                    break

            # 主サイトでは ``a.live_timenum``、代替サイトでは
            # ``div.live_timenum`` が使われる。タグ名を限定すると、代替サイトへ
            # 切り替わった回だけ配信時間がすべて「不明」になるため、クラス名で拾う。
            if "live_timenum" in classes:
                self._current.elapsed_title = (
                    attr.get("title", "")
                    or attr.get("aria-label", "")
                    or attr.get("data-title", "")
                )
                self._begin_capture("elapsed_text", tag)

        href = attr.get("href", "")
        if href and _extract_mixch_user_id(href):
            self._current.url = href

    def handle_startendtag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        # 対象箇所のvoid要素はテキストを持たないので、URLだけ通常処理に任せる。
        self.handle_starttag(tag, attrs)

    def handle_data(self, data: str) -> None:
        if self._header_capture:
            self._header_parts.append(data)
        if self._capture_field:
            self._capture_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag == "span" and self._header_capture:
            header = _clean_text("".join(self._header_parts))
            match = re.search(r"\[(\d+)人放送中\]", header)
            if match:
                self.broadcast_count = int(match.group(1))
            self._header_capture = False

        if self._current is None:
            return

        if self._capture_field and tag == self._capture_end_tag:
            if self._capture_same_tag_depth > 0:
                self._capture_same_tag_depth -= 1
            else:
                value = _clean_text("".join(self._capture_parts))
                setattr(self._current, self._capture_field, value)
                self._capture_field = None
                self._capture_end_tag = None
                self._capture_parts = []

        if tag == "div":
            self._livebox_div_depth -= 1
            if self._livebox_div_depth <= 0:
                self._finish_current()

    def close(self) -> None:
        super().close()
        if self._current is not None:
            self._finish_current()

    def _begin_capture(self, field_name: str, end_tag: str) -> None:
        self._capture_field = field_name
        self._capture_end_tag = end_tag
        self._capture_same_tag_depth = 0
        self._capture_parts = []

    def _finish_current(self) -> None:
        assert self._current is not None
        self.raw_streams.append(self._current)
        self._current = None
        self._livebox_div_depth = 0
        self._capture_field = None
        self._capture_end_tag = None
        self._capture_same_tag_depth = 0
        self._capture_parts = []


def parse_ranking_page(html: str) -> list[Stream]:
    """ランキングHTMLを検証し、配信一覧へ変換する。"""

    parser = _RankingParser()
    parser.feed(html)
    parser.close()

    streams: list[Stream] = []
    skipped = 0
    for raw in parser.raw_streams:
        try:
            streams.append(_normalise_stream(raw))
        except ParseError as exc:
            skipped += 1
            LOGGER.warning("配信枠を1件読み飛ばしました: %s", exc)

    if parser.broadcast_count is None and parser.livebox_count == 0:
        raise ParseError("ランキングページを識別できる情報がありません")
    if parser.broadcast_count and parser.livebox_count == 0:
        raise ParseError(
            f"放送中は{parser.broadcast_count}件と表示されていますが、配信枠を取得できません"
        )
    if parser.livebox_count and not streams:
        raise ParseError(
            f"配信枠{parser.livebox_count}件の必要項目を1件も読み取れません"
        )
    if parser.livebox_count >= 5 and len(streams) / parser.livebox_count < 0.8:
        raise ParseError(
            "ページ構造の変化が疑われます "
            f"(配信枠={parser.livebox_count}, 解析成功={len(streams)}, 読み飛ばし={skipped})"
        )

    LOGGER.info(
        "ランキングを解析しました: 放送中表示=%s, 配信枠=%d, 解析成功=%d",
        parser.broadcast_count if parser.broadcast_count is not None else "不明",
        parser.livebox_count,
        len(streams),
    )
    return streams


def _normalise_stream(raw: _RawStream) -> Stream:
    user_id = _extract_mixch_user_id(raw.user_id) or _extract_mixch_user_id(raw.url)
    if not user_id:
        raise ParseError("MixChannelユーザーIDがありません")

    url = raw.url or f"https://mixch.tv/u/{user_id}/live"
    # ページに別形式のリンクが混入しても、通知先は正規の配信URLへ揃える。
    url = f"https://mixch.tv/u/{user_id}/live"

    # 実ページには、配信者名を空欄にしている利用者がまれにいる。
    # 枠自体を捨てると高い勢い度を見逃すため、安定したユーザーIDで補う。
    name = _clean_text(raw.broadcaster_name) or f"名称未設定（ID: {user_id}）"

    momentum = _first_integer(raw.momentum_text, "勢い度", user_id)
    rank = _optional_first_integer(raw.rank_text)
    elapsed_minutes = _elapsed_minutes(raw.elapsed_title, raw.elapsed_text)
    elapsed_text = _normalise_elapsed_text(raw.elapsed_text, elapsed_minutes)

    return Stream(
        user_id=user_id,
        broadcaster_name=name,
        title=_clean_text(raw.title) or "（配信タイトルなし）",
        url=url,
        momentum=momentum,
        rank=rank,
        elapsed_minutes=elapsed_minutes,
        elapsed_text=elapsed_text,
    )


def log_top_ranking_snapshot(
    streams: Sequence[Stream], observed_at: datetime, limit: int = RANKING_LOG_LIMIT
) -> None:
    """ランキング上位だけを、後から機械集計できる1行JSONで実行ログへ残す。

    通知対象の選別より前に呼ぶため、盛り上がり度のしきい値、再通知抑制、
    ブロックリストに関係なく、その時点のランキングそのものを記録できる。
    順位を取得できない配信は、ページに現れた順番を保ったまま末尾へ回す。
    """

    if limit <= 0:
        LOGGER.info(
            "ランキング上位ログ: 観測時刻=%s, 記録件数=0",
            _format_timestamp(observed_at),
        )
        return

    # Pythonのsortは同順位で元の順序を保つ。順位不明を最後へ回しつつ、
    # 代替サイト側で順位が欠けてもページ上位から最大limit件を記録する。
    ordered = sorted(
        streams,
        key=lambda stream: (
            stream.rank is None,
            stream.rank if stream.rank is not None else 0,
        ),
    )[:limit]
    observed_at_text = _format_timestamp(observed_at)
    LOGGER.info(
        "ランキング上位ログ: 観測時刻=%s, 記録件数=%d",
        observed_at_text,
        len(ordered),
    )

    for stream in ordered:
        record = {
            "observed_at": observed_at_text,
            "rank": stream.rank,
            "user_id": stream.user_id,
            "broadcaster_name": stream.broadcaster_name,
            "title": stream.title,
            "momentum": stream.momentum,
            "elapsed_minutes": stream.elapsed_minutes,
            "elapsed_text": stream.elapsed_text,
            "profile_url": f"https://mixch.tv/u/{stream.user_id}",
            "live_url": stream.url,
        }
        LOGGER.info(
            "RANKING_TOP10 %s",
            json.dumps(record, ensure_ascii=False, sort_keys=True),
        )


def fetch_ranking(
    primary_url: str, fallback_url: str, timeout_seconds: float
) -> list[Stream]:
    """主サイトと代替サイトを順に試し、正常に解析できたランキングを返す。

    ``live-ranking.com`` は一部のGitHubホスト実行機にHTTP 200かつ空本文を
    返すことがある。まず主サイト、次に同系列の代替サイトへ直接アクセスする。
    両方とも失敗した場合だけ、Jina Reader経由で同じ2サイトを再試行する。

    本文の長さだけでなくランキングとして解析できることまで確認するため、
    ページ構造の変更やエラーページを正常取得と誤認しない。
    """

    repository = os.getenv("GITHUB_REPOSITORY", "OWNER/mixch-ranking-monitor")
    standard_headers = {
        "User-Agent": (
            "Mozilla/5.0 (compatible; MixchRankingMonitor/1.0; "
            f"+https://github.com/{repository})"
        ),
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "ja-JP,ja;q=0.9",
        "Cache-Control": "no-cache",
    }

    sites: list[tuple[str, str]] = [("主サイト", primary_url)]
    if fallback_url != primary_url:
        sites.append(("代替サイト", fallback_url))

    errors: list[str] = []
    for label, url in sites:
        try:
            html = _download_text(
                url, standard_headers, timeout_seconds, f"{label}のランキングページ"
            )
            streams = _parse_fetched_ranking(html, f"{label}の直接応答")
            LOGGER.info("%sからランキングを取得しました", label)
            return streams
        except MonitorError as exc:
            errors.append(f"{label}の直接取得: {exc}")
            LOGGER.warning("%sの直接取得・解析に失敗しました: %s", label, exc)

    LOGGER.warning("両サイトの直接取得に失敗したためHTML退避経路を使います")

    fallback_headers = {
        "User-Agent": "MixchRankingMonitor/1.0",
        "Accept": "text/plain",
        # 5分監視で古いランキングを再利用しない。
        "X-No-Cache": "true",
        # Markdownではなく元ページに近いHTMLを返してもらい、同じ解析器を使う。
        "X-Respond-With": "html",
    }
    for label, url in sites:
        reader_url = f"{READER_FALLBACK_PREFIX}{url}"
        last_error: MonitorError | None = None
        # 2サイト×3回ではワークフローの3分上限を超え得るため、各2回にする。
        # 退避経路は15秒で見切り、代替サイトを試す時間を確保する。
        for attempt in range(1, 3):
            try:
                html = _download_text(
                    reader_url,
                    fallback_headers,
                    min(timeout_seconds, 15.0),
                    f"{label}のHTML退避経路",
                )
                streams = _parse_fetched_ranking(
                    html, f"{label}のHTML退避経路の応答"
                )
                LOGGER.info("%sをHTML退避経路から取得しました", label)
                return streams
            except MonitorError as exc:
                last_error = exc

            if attempt < 2:
                wait_seconds = attempt * 3
                LOGGER.warning(
                    "%sのHTML退避経路に失敗しました（%d/2）: %s。%d秒後に再試行します",
                    label,
                    attempt,
                    last_error,
                    wait_seconds,
                )
                time.sleep(wait_seconds)

        assert last_error is not None
        errors.append(f"{label}のHTML退避経路: {last_error}")

    raise MonitorError("全取得経路が失敗しました / " + " / ".join(errors))


def _parse_fetched_ranking(html: str, label: str) -> list[Stream]:
    """短い応答と解析不能なHTMLを、次の取得経路へ切り替えられる異常にする。"""

    if len(html) < 1_000:
        # 短い応答はサービス側の一時的な利用制限メッセージであることが多い。
        # 公開ページの取得結果だけを最大200文字出し、原因を判別できるようにする。
        preview = _clean_text(html)[:200]
        raise MonitorError(
            f"{label}が短すぎます ({len(html)}文字, 内容={preview!r})"
        )

    try:
        return parse_ranking_page(html)
    except ParseError as exc:
        raise MonitorError(f"{label}をランキングとして解析できません ({exc})") from exc


def _download_text(
    url: str, headers: dict[str, str], timeout_seconds: float, label: str
) -> str:
    """HTTP応答を文字列として読む。URL自体は例外へ含めず、秘密の誤表示を防ぐ。"""

    request = Request(url, headers=headers)

    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            status = getattr(response, "status", 200)
            if status != 200:
                raise MonitorError(f"{label}がHTTP {status}を返しました")
            body = response.read()
            charset = response.headers.get_content_charset() or "utf-8"
    except HTTPError as exc:
        raise MonitorError(f"{label}がHTTP {exc.code}を返しました") from exc
    except (URLError, TimeoutError, OSError) as exc:
        raise MonitorError(f"{label}の取得に失敗しました ({type(exc).__name__})") from exc

    try:
        html = body.decode(charset, errors="strict")
    except (LookupError, UnicodeDecodeError):
        html = body.decode("utf-8", errors="replace")

    return html


def load_state(path: Path) -> dict[str, Any]:
    """通知済み時刻を読む。壊れた状態は重複通知防止のため黙って初期化しない。"""

    if not path.exists():
        return _new_state()

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise StateError("状態ファイルを読み取れません") from exc

    if not isinstance(data, dict):
        raise StateError("状態ファイルの最上位がオブジェクトではありません")
    if data.get("version", STATE_VERSION) != STATE_VERSION:
        raise StateError(f"未対応の状態ファイル版です: {data.get('version')!r}")

    notifications = data.setdefault("notifications", {})
    night_candidates = data.setdefault("night_candidates", {})
    metadata = data.setdefault("metadata", {})
    if (
        not isinstance(notifications, dict)
        or not isinstance(night_candidates, dict)
        or not isinstance(metadata, dict)
    ):
        raise StateError(
            "状態ファイルのnotifications、night_candidatesまたはmetadataが不正です"
        )

    # 時刻が壊れていた場合に「未通知扱い」で大量再送しないよう、先に全部検証する。
    for user_id, record in notifications.items():
        if not isinstance(user_id, str) or not isinstance(record, dict):
            raise StateError("状態ファイルの通知履歴が不正です")
        timestamp = record.get("last_notified_at")
        if not isinstance(timestamp, str):
            raise StateError(f"通知履歴の時刻がありません (user_id={user_id})")
        _parse_timestamp(timestamp)

    for user_id, record in night_candidates.items():
        if not isinstance(user_id, str) or not isinstance(record, dict):
            raise StateError("状態ファイルの夜間候補が不正です")
        if _extract_mixch_user_id(user_id) != user_id:
            raise StateError(f"夜間候補のユーザーIDが不正です: {user_id!r}")
        for key in ("broadcaster_name", "url"):
            if not isinstance(record.get(key), str):
                raise StateError(
                    f"夜間候補の{key}が不正です (user_id={user_id})"
                )
        momentum = record.get("max_momentum")
        if not isinstance(momentum, int) or momentum < 0:
            raise StateError(
                f"夜間候補のmax_momentumが不正です (user_id={user_id})"
            )
        for key in ("first_seen_at", "last_seen_at"):
            timestamp = record.get(key)
            if not isinstance(timestamp, str):
                raise StateError(
                    f"夜間候補の{key}が不正です (user_id={user_id})"
                )
            _parse_timestamp(timestamp)

    for key in ("last_heartbeat_at", "last_error_notified_at"):
        timestamp = metadata.get(key)
        if timestamp is not None:
            if not isinstance(timestamp, str):
                raise StateError(f"metadata.{key}が文字列ではありません")
            _parse_timestamp(timestamp)

    data["version"] = STATE_VERSION
    return data


def save_state(path: Path, state: dict[str, Any]) -> None:
    """途中書き込みを避けて状態ファイルを置き換える。"""

    path.parent.mkdir(parents=True, exist_ok=True)
    state["version"] = STATE_VERSION
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def select_eligible_streams(
    streams: Iterable[Stream],
    state: dict[str, Any],
    threshold: int,
    cooldown_hours: float,
    now: datetime,
    blocked_user_ids: AbstractSet[str] = frozenset(),
) -> list[Stream]:
    """ブロック対象を除き、しきい値と再通知条件を満たす配信だけを返す。"""

    cutoff = now - timedelta(hours=cooldown_hours)
    notifications = state["notifications"]
    eligible: list[Stream] = []

    for stream in streams:
        if stream.user_id in blocked_user_ids:
            continue
        # 「150を超えた」は150を含まない。設定150なら151以上。
        if stream.momentum <= threshold:
            continue
        record = notifications.get(stream.user_id)
        if record:
            last_notified = _parse_timestamp(record["last_notified_at"])
            if last_notified > cutoff:
                continue
        eligible.append(stream)

    return sorted(eligible, key=lambda item: item.momentum, reverse=True)


def select_night_candidates(
    streams: Iterable[Stream],
    state: dict[str, Any],
    threshold: int,
    cooldown_hours: float,
    now: datetime,
    blocked_user_ids: AbstractSet[str] = frozenset(),
) -> list[Stream]:
    """夜間に蓄積する配信を返す。夜間だけ設定値ちょうども対象にする。"""

    cutoff = now - timedelta(hours=cooldown_hours)
    notifications = state["notifications"]
    candidates: list[Stream] = []

    for stream in streams:
        if stream.user_id in blocked_user_ids:
            continue
        # 利用者指定は「150以上」なので、昼間の「150超」と異なり150を含む。
        if stream.momentum < threshold:
            continue
        record = notifications.get(stream.user_id)
        if record:
            last_processed = _parse_timestamp(record["last_notified_at"])
            if last_processed > cutoff:
                continue
        candidates.append(stream)

    return sorted(candidates, key=lambda item: item.momentum, reverse=True)


def accumulate_night_candidates(
    state: dict[str, Any], streams: Iterable[Stream], observed_at: datetime
) -> int:
    """ユーザーIDで重複を潰し、夜間に観測した最大勢い度と最新名を保存する。"""

    records = state["night_candidates"]
    timestamp = _format_timestamp(observed_at)
    changed = 0

    for stream in streams:
        record = records.get(stream.user_id)
        if record is None:
            records[stream.user_id] = {
                "broadcaster_name": stream.broadcaster_name,
                "url": stream.url,
                "max_momentum": stream.momentum,
                "first_seen_at": timestamp,
                "last_seen_at": timestamp,
            }
            changed += 1
            continue

        new_max = max(record["max_momentum"], stream.momentum)
        if (
            record["broadcaster_name"] != stream.broadcaster_name
            or record["url"] != stream.url
            or record["max_momentum"] != new_max
        ):
            record.update(
                {
                    "broadcaster_name": stream.broadcaster_name,
                    "url": stream.url,
                    "max_momentum": new_max,
                    "last_seen_at": timestamp,
                }
            )
            changed += 1

    return changed


def mark_night_candidates_processed(
    state: dict[str, Any], processed_at: datetime
) -> None:
    """朝の一括処理直後に通常通知が重ならないよう、全候補を抑制履歴へ残す。"""

    timestamp = _format_timestamp(processed_at)
    notifications = state["notifications"]
    for user_id, record in state["night_candidates"].items():
        notifications[user_id] = {
            "last_notified_at": timestamp,
            "broadcaster_name": record["broadcaster_name"],
            "url": record["url"],
            "reason": "night_digest_processed",
        }


def mark_notified(
    state: dict[str, Any], streams: Iterable[Stream], notified_at: datetime
) -> None:
    notifications = state["notifications"]
    timestamp = _format_timestamp(notified_at)
    for stream in streams:
        notifications[stream.user_id] = {
            "last_notified_at": timestamp,
            "broadcaster_name": stream.broadcaster_name,
            "url": stream.url,
        }


def maintain_state(
    state: dict[str, Any], now: datetime, cooldown_hours: float, heartbeat_days: float
) -> bool:
    """古い履歴を掃除し、公開リポジトリ停止防止用の週次生存記録を更新する。"""

    changed = False
    notifications = state["notifications"]
    retention = max(timedelta(days=30), timedelta(hours=cooldown_hours * 2))
    cutoff = now - retention
    expired = [
        user_id
        for user_id, record in notifications.items()
        if _parse_timestamp(record["last_notified_at"]) < cutoff
    ]
    for user_id in expired:
        del notifications[user_id]
        changed = True

    metadata = state["metadata"]
    last_heartbeat_text = metadata.get("last_heartbeat_at")
    heartbeat_due = (
        last_heartbeat_text is None
        or _parse_timestamp(last_heartbeat_text)
        <= now - timedelta(days=heartbeat_days)
    )
    if heartbeat_due:
        metadata["last_heartbeat_at"] = _format_timestamp(now)
        changed = True

    return changed


def build_stream_embed(
    stream: Stream, threshold: int, observed_at: datetime
) -> dict[str, Any]:
    description = _truncate(stream.title, 700)
    profile_url = f"https://mixch.tv/u/{stream.user_id}"
    return {
        "title": _truncate(f"🔥 {stream.broadcaster_name}", 256),
        # 通知上部の配信者名はプロフィールへ、下部の明示リンクはライブへ分ける。
        "url": profile_url,
        "description": description,
        "color": DISCORD_EMBED_COLOR,
        "fields": [
            {
                "name": "勢い度",
                "value": f"**{stream.momentum} points**（設定 {threshold} 超）",
                "inline": True,
            },
            {"name": "配信時間", "value": stream.elapsed_text, "inline": True},
            {"name": "配信URL", "value": f"[MixChannelで開く]({stream.url})", "inline": False},
        ],
        "footer": {"text": "ライブランキングZ / MixChannel勢い監視"},
        "timestamp": _format_timestamp(observed_at),
    }


def send_stream_notifications(
    webhook_url: str,
    streams: Sequence[Stream],
    threshold: int,
    observed_at: datetime,
    timeout_seconds: float,
) -> list[Stream]:
    """最大5件ずつDiscordへ送り、送信完了した配信一覧を返す。"""

    _require_webhook(webhook_url)
    sent: list[Stream] = []
    for batch in _chunks(streams, MAX_EMBEDS_PER_MESSAGE):
        payload = {
            "username": "MixChannel勢い監視",
            "content": f"勢い度が **{threshold}を超えた** 配信を検知しました。",
            "allowed_mentions": {"parse": []},
            "embeds": [
                build_stream_embed(stream, threshold, observed_at) for stream in batch
            ],
        }
        _post_discord(webhook_url, payload, timeout_seconds)
        sent.extend(batch)
    return sent


def has_public_archive(user_id: str, timeout_seconds: float) -> bool:
    """MixChannel公式APIをページ送りし、全体公開アーカイブが1件でもあるか調べる。"""

    cursor: int | None = None
    seen_cursors: set[int] = set()
    headers = {
        "User-Agent": "MixchRankingMonitor/1.0",
        "Accept": "application/json",
    }

    for page_number in range(1, MAX_ARCHIVE_PAGES + 1):
        params: dict[str, int] = {"limit": ARCHIVE_PAGE_SIZE}
        if cursor is not None:
            params["cursor"] = cursor
        url = f"{MIXCH_ARCHIVES_API.format(user_id=user_id)}?{urlencode(params)}"
        raw = _download_text(
            url,
            headers,
            min(timeout_seconds, 15.0),
            f"MixChannelアーカイブ一覧 (user_id={user_id}, page={page_number})",
        )

        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise MonitorError(
                f"MixChannelアーカイブ一覧がJSONではありません (user_id={user_id})"
            ) from exc

        archives = data.get("archives") if isinstance(data, dict) else None
        if not isinstance(archives, list):
            raise MonitorError(
                f"MixChannelアーカイブ一覧の形式が不正です (user_id={user_id})"
            )
        if any(
            isinstance(archive, dict)
            and archive.get("visibility") == PUBLIC_ARCHIVE_VISIBILITY
            for archive in archives
        ):
            return True

        if not data.get("has_next"):
            return False

        next_cursor = data.get("next_cursor")
        if not isinstance(next_cursor, int) or next_cursor in seen_cursors:
            raise MonitorError(
                f"MixChannelアーカイブ一覧のページ情報が不正です (user_id={user_id})"
            )
        seen_cursors.add(next_cursor)
        cursor = next_cursor

    raise MonitorError(
        f"MixChannelアーカイブ一覧が{MAX_ARCHIVE_PAGES}ページを超えました "
        f"(user_id={user_id})"
    )


def find_public_archive_profiles(
    candidates: dict[str, dict[str, Any]], timeout_seconds: float
) -> list[dict[str, str]]:
    """公開アーカイブがある夜間候補だけを、最大勢い度順に返す。"""

    if not candidates:
        return []

    results: dict[str, bool] = {}
    errors: list[str] = []
    worker_count = min(ARCHIVE_CHECK_WORKERS, len(candidates))
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = {
            executor.submit(has_public_archive, user_id, timeout_seconds): user_id
            for user_id in candidates
        }
        for future in as_completed(futures):
            user_id = futures[future]
            try:
                results[user_id] = future.result()
            except Exception as exc:  # noqa: BLE001 - 全候補を確認後にまとめて再試行する
                LOGGER.warning(
                    "公開アーカイブ確認に失敗しました: user_id=%s error=%s",
                    user_id,
                    exc,
                )
                errors.append(user_id)

    if errors:
        raise MonitorError(
            "公開アーカイブを確認できない夜間候補があります: "
            + ", ".join(sorted(errors))
        )

    ordered = sorted(
        candidates.items(),
        key=lambda item: item[1]["max_momentum"],
        reverse=True,
    )
    return [
        {
            "user_id": user_id,
            "broadcaster_name": record["broadcaster_name"],
            "profile_url": f"https://mixch.tv/u/{user_id}",
        }
        for user_id, record in ordered
        if results.get(user_id)
    ]


def build_night_digest_descriptions(
    profiles: Sequence[dict[str, str]],
) -> list[str]:
    """名前だけをプロフィールリンクにし、Discordの文字数内へ分割する。"""

    descriptions: list[str] = []
    current_lines: list[str] = []
    current_length = 0

    for profile in profiles:
        line = f"[{profile['broadcaster_name']}]({profile['profile_url']})"
        added_length = len(line) + (1 if current_lines else 0)
        if current_lines and current_length + added_length > DISCORD_DESCRIPTION_LIMIT:
            descriptions.append("\n".join(current_lines))
            current_lines = [line]
            current_length = len(line)
        else:
            current_lines.append(line)
            current_length += added_length

    if current_lines:
        descriptions.append("\n".join(current_lines))
    return descriptions


def send_night_digest_notification(
    webhook_url: str,
    profiles: Sequence[dict[str, str]],
    observed_at: datetime,
    timeout_seconds: float,
) -> None:
    """夜間候補のうち公開アーカイブがある人を朝にまとめて通知する。"""

    if not profiles:
        return
    _require_webhook(webhook_url)
    descriptions = build_night_digest_descriptions(profiles)
    total = len(descriptions)
    for index, description in enumerate(descriptions, start=1):
        title = "🌙 夜間高勢い・公開アーカイブあり"
        if total > 1:
            title += f"（{index}/{total}）"
        payload = {
            "username": "MixChannel勢い監視",
            "content": "夜間に勢い度が設定値以上になった配信者をまとめました。",
            "allowed_mentions": {"parse": []},
            "embeds": [
                {
                    "title": title,
                    "description": description,
                    "color": DISCORD_EMBED_COLOR,
                    "footer": {"text": "名前を押すとMixChannelプロフィールを開きます"},
                    "timestamp": _format_timestamp(observed_at),
                }
            ],
        }
        _post_discord(webhook_url, payload, timeout_seconds)


def send_test_notification(webhook_url: str, timeout_seconds: float) -> None:
    _require_webhook(webhook_url)
    payload = {
        "username": "MixChannel勢い監視",
        "content": "✅ MixChannel勢い監視のテスト通知です。Webhookは正常に動いています。",
        "allowed_mentions": {"parse": []},
    }
    _post_discord(webhook_url, payload, timeout_seconds)


def maybe_send_error_notification(
    webhook_url: str,
    state: dict[str, Any],
    error: Exception,
    now: datetime,
    cooldown_hours: float,
    timeout_seconds: float,
) -> bool:
    """同種を問わず、監視エラー通知を設定時間に1回までに抑える。"""

    if not webhook_url:
        return False
    metadata = state["metadata"]
    previous = metadata.get("last_error_notified_at")
    if previous and _parse_timestamp(previous) > now - timedelta(hours=cooldown_hours):
        LOGGER.warning("エラー通知は抑制時間内のため送りません")
        return False

    payload = {
        "username": "MixChannel勢い監視",
        "content": "⚠️ MixChannel勢い監視でエラーが発生しました。",
        "allowed_mentions": {"parse": []},
        "embeds": [
            {
                "title": "監視処理を完了できませんでした",
                "description": _truncate(str(error), 1_000),
                "color": 0xE74C3C,
                "footer": {"text": "同じ通知は一定時間抑制されます"},
                "timestamp": _format_timestamp(now),
            }
        ],
    }
    _post_discord(webhook_url, payload, timeout_seconds)
    metadata["last_error_notified_at"] = _format_timestamp(now)
    return True


def run(config: Config, now: datetime | None = None) -> int:
    # ``now`` は夜間・朝の境界を副作用なしで試験するため注入可能にする。
    now = now or datetime.now(timezone.utc)
    state: dict[str, Any] | None = None

    try:
        state = load_state(config.state_file)

        if config.test_webhook:
            send_test_notification(
                config.discord_webhook_url, config.request_timeout_seconds
            )
            LOGGER.info("Discordへテスト通知を送りました")

        streams = fetch_ranking(
            config.monitor_url,
            config.fallback_monitor_url,
            config.request_timeout_seconds,
        )
        # 後から初速と伸び方を比較できるよう、通知判定とは独立して毎回記録する。
        log_top_ranking_snapshot(streams, now)

        blocked_count = sum(
            1 for stream in streams if stream.user_id in config.blocked_user_ids
        )

        if _is_night_time(now):
            night_candidates = select_night_candidates(
                streams,
                state,
                config.momentum_threshold,
                config.cooldown_hours,
                now,
                config.blocked_user_ids,
            )
            LOGGER.info(
                "夜間判定: しきい値=%d以上, ブロック除外=%d件, 蓄積候補=%d件, dry_run=%s",
                config.momentum_threshold,
                blocked_count,
                len(night_candidates),
                config.dry_run,
            )
            for stream in night_candidates:
                LOGGER.info(
                    "夜間蓄積候補: user_id=%s name=%s momentum=%d",
                    stream.user_id,
                    stream.broadcaster_name,
                    stream.momentum,
                )

            if not config.dry_run:
                _require_webhook(config.discord_webhook_url)
                accumulate_night_candidates(state, night_candidates, now)
                maintain_state(
                    state, now, config.cooldown_hours, config.heartbeat_days
                )
                save_state(config.state_file, state)
            return 0

        overnight_user_ids = set(state["night_candidates"])
        if overnight_user_ids:
            LOGGER.info(
                "夜間候補%d件の公開アーカイブを朝の一括確認へ回します",
                len(overnight_user_ids),
            )
            public_profiles = find_public_archive_profiles(
                state["night_candidates"], config.request_timeout_seconds
            )
            LOGGER.info(
                "夜間候補の公開アーカイブ確認結果: 対象=%d件, 公開あり=%d件",
                len(overnight_user_ids),
                len(public_profiles),
            )
            if not config.dry_run:
                _require_webhook(config.discord_webhook_url)
                send_night_digest_notification(
                    config.discord_webhook_url,
                    public_profiles,
                    now,
                    config.request_timeout_seconds,
                )
                mark_night_candidates_processed(state, now)
                state["night_candidates"].clear()
                # ランキング取得や昼間通知が後で失敗しても、朝の一括通知を重複させない。
                save_state(config.state_file, state)

        eligible = select_eligible_streams(
            (
                stream
                for stream in streams
                if stream.user_id not in overnight_user_ids
            ),
            state,
            config.momentum_threshold,
            config.cooldown_hours,
            now,
            config.blocked_user_ids,
        )

        LOGGER.info(
            "判定結果: しきい値=%d超, ブロック除外=%d件, 通知候補=%d件, dry_run=%s",
            config.momentum_threshold,
            blocked_count,
            len(eligible),
            config.dry_run,
        )
        for stream in eligible:
            LOGGER.info(
                "通知候補: user_id=%s name=%s momentum=%d elapsed=%s url=%s",
                stream.user_id,
                stream.broadcaster_name,
                stream.momentum,
                stream.elapsed_text,
                stream.url,
            )

        if eligible and not config.dry_run:
            sent: list[Stream] = []
            try:
                for batch in _chunks(eligible, MAX_EMBEDS_PER_MESSAGE):
                    sent_batch = send_stream_notifications(
                        config.discord_webhook_url,
                        batch,
                        config.momentum_threshold,
                        now,
                        config.request_timeout_seconds,
                    )
                    sent.extend(sent_batch)
                    # 後続バッチが失敗しても、送信済み分を状態へ残す。
                    mark_notified(state, sent_batch, now)
                    save_state(config.state_file, state)
            except Exception:
                if sent:
                    LOGGER.warning("通知済み%d件の状態を保存してから終了します", len(sent))
                raise
            LOGGER.info("Discordへ%d件の配信を通知しました", len(sent))
        elif not config.dry_run:
            # 通常運転でWebhookの設定漏れを放置しない。通知候補が出るまで
            # 未設定に気付けない事故を防ぐため、毎回確認する。
            _require_webhook(config.discord_webhook_url)

        if not config.dry_run:
            maintain_state(
                state, now, config.cooldown_hours, config.heartbeat_days
            )
            save_state(config.state_file, state)
        return 0

    except Exception as exc:  # noqa: BLE001 - 監視を黙って落とさないため最上位で集約
        LOGGER.exception("監視処理に失敗しました: %s", exc)
        if state is not None and not config.dry_run and config.notify_on_error:
            try:
                if maybe_send_error_notification(
                    config.discord_webhook_url,
                    state,
                    exc,
                    now,
                    config.error_cooldown_hours,
                    config.request_timeout_seconds,
                ):
                    save_state(config.state_file, state)
            except Exception as notify_exc:  # noqa: BLE001
                LOGGER.error("エラー通知にも失敗しました: %s", notify_exc)
        if state is not None and not config.dry_run:
            try:
                save_state(config.state_file, state)
            except OSError as state_exc:
                LOGGER.error("状態ファイルの保存にも失敗しました: %s", state_exc)
        return 1


def _post_discord(
    webhook_url: str, payload: dict[str, Any], timeout_seconds: float
) -> None:
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = Request(
        webhook_url,
        data=data,
        method="POST",
        headers={"Content-Type": "application/json", "User-Agent": "MixchRankingMonitor/1.0"},
    )

    for attempt in range(1, 4):
        try:
            with urlopen(request, timeout=timeout_seconds) as response:
                status = getattr(response, "status", 204)
                if 200 <= status < 300:
                    return
                raise NotificationError(f"DiscordがHTTP {status}を返しました")
        except HTTPError as exc:
            if exc.code == 429 and attempt < 3:
                retry_after = _discord_retry_after(exc)
                LOGGER.warning(
                    "Discordの送信制限を受けました。%.1f秒後に再試行します", retry_after
                )
                time.sleep(retry_after)
                continue
            raise NotificationError(f"DiscordがHTTP {exc.code}を返しました") from exc
        except (URLError, TimeoutError, OSError) as exc:
            raise NotificationError(
                f"Discordへの接続に失敗しました ({type(exc).__name__})"
            ) from exc

    raise NotificationError("Discord通知の再試行回数を超えました")


def _discord_retry_after(error: HTTPError) -> float:
    header = error.headers.get("Retry-After") if error.headers else None
    if header:
        try:
            return min(max(float(header), 0.5), 10.0)
        except ValueError:
            pass
    try:
        raw = error.read().decode("utf-8", errors="replace")
        value = json.loads(raw).get("retry_after", 1.0)
        seconds = float(value)
        # Discordの応答差異に備え、1000を超える値はミリ秒として扱う。
        if seconds > 1_000:
            seconds /= 1_000
        return min(max(seconds, 0.5), 10.0)
    except (AttributeError, TypeError, ValueError, json.JSONDecodeError):
        return 1.0


def _new_state() -> dict[str, Any]:
    return {
        "version": STATE_VERSION,
        "notifications": {},
        "night_candidates": {},
        "metadata": {},
    }


def _is_night_time(value: datetime) -> bool:
    """日本時間22:00〜翌06:59を夜間として扱う。"""

    hour = value.astimezone(JST).hour
    return hour >= 22 or hour < 7


def _elapsed_minutes(title: str, visible_text: str) -> int | None:
    # 主サイトのtitleは「62分経過」、代替サイトは「62分」。表示文字列は
    # どちらも「1時間2分」のようになる。属性と表示の両方を順に試す。
    for value in (title, visible_text):
        compact = _clean_text(value).replace(" ", "")
        total_minutes_match = re.fullmatch(r"(\d+)分(?:経過)?", compact)
        if total_minutes_match:
            return int(total_minutes_match.group(1))

        hours_match = re.search(r"(\d+)時間", compact)
        minutes_match = re.search(r"(\d+)分", compact)
        if hours_match or minutes_match:
            hours = int(hours_match.group(1)) if hours_match else 0
            minutes = int(minutes_match.group(1)) if minutes_match else 0
            return hours * 60 + minutes
    return None


def _normalise_elapsed_text(text: str, minutes: int | None) -> str:
    if minutes is not None:
        hours, remaining = divmod(minutes, 60)
        return f"{hours}時間{remaining}分" if hours else f"{remaining}分"

    compact = re.sub(r"\s+", "", text)
    return compact or "不明"


def _first_integer(text: str, label: str, user_id: str) -> int:
    value = _optional_first_integer(text)
    if value is None:
        raise ParseError(f"{label}が数値ではありません (user_id={user_id})")
    return value


def _optional_first_integer(text: str) -> int | None:
    match = re.search(r"\d[\d,]*", text)
    return int(match.group(0).replace(",", "")) if match else None


def _extract_mixch_user_id(value: str) -> str | None:
    match = re.search(r"(?:mixch_|/u/)(\d+)(?:/live)?", value)
    if match:
        return match.group(1)
    return value if value.isdigit() else None


def _clean_text(value: str) -> str:
    return " ".join(value.split())


def _truncate(value: str, limit: int) -> str:
    return value if len(value) <= limit else value[: limit - 1] + "…"


def _chunks(items: Sequence[Stream], size: int) -> Iterable[list[Stream]]:
    for start in range(0, len(items), size):
        yield list(items[start : start + size])


def _parse_timestamp(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise StateError(f"状態ファイルの時刻形式が不正です: {value!r}") from exc
    if parsed.tzinfo is None:
        raise StateError(f"状態ファイルの時刻にタイムゾーンがありません: {value!r}")
    return parsed.astimezone(timezone.utc)


def _format_timestamp(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )


def _read_int(name: str, default: int, minimum: int, maximum: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise MonitorError(f"{name}は整数で指定してください") from exc
    if not minimum <= value <= maximum:
        raise MonitorError(f"{name}は{minimum}〜{maximum}で指定してください")
    return value


def _read_float(name: str, default: float, minimum: float, maximum: float) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise MonitorError(f"{name}は数値で指定してください") from exc
    if not minimum <= value <= maximum:
        raise MonitorError(f"{name}は{minimum}〜{maximum}で指定してください")
    return value


def _read_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name, "").strip().lower()
    if not raw:
        return default
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    raise MonitorError(f"{name}はtrueまたはfalseで指定してください")


def _read_user_id_set(name: str) -> frozenset[str]:
    """カンマ・空白・改行区切りのIDまたはMixChannel URLをID集合へ変換する。"""

    raw = os.getenv(name, "").strip()
    if not raw:
        return frozenset()

    user_ids: set[str] = set()
    for token in re.split(r"[\s,;、]+", raw):
        if not token:
            continue
        user_id = _extract_mixch_user_id(token)
        if not user_id:
            raise MonitorError(
                f"{name}に不正な値があります: {token!r}。数字のユーザーIDかMixChannel URLを指定してください"
            )
        user_ids.add(user_id)
    return frozenset(user_ids)


def _validate_https_url(value: str, name: str) -> None:
    parsed = urlparse(value)
    if parsed.scheme != "https" or not parsed.netloc:
        raise MonitorError(f"{name}にはhttps://で始まるURLを指定してください")


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
        raise MonitorError("DISCORD_WEBHOOK_URLがDiscordのWebhook URLではありません")


def _require_webhook(value: str) -> None:
    if not value:
        raise MonitorError(
            "DISCORD_WEBHOOK_URLが未設定です。GitHub ActionsのSecretへ登録してください"
        )


def configure_logging() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def main() -> int:
    configure_logging()
    try:
        config = Config.from_environment()
    except Exception as exc:  # noqa: BLE001
        LOGGER.error("設定の読み込みに失敗しました: %s", exc)
        return 1
    return run(config)


if __name__ == "__main__":
    sys.exit(main())
