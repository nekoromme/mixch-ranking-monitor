from __future__ import annotations

import json
import os
import tempfile
import unittest
from email.message import Message
from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path
from unittest.mock import patch

from src.mixch_monitor import (
    Config,
    MonitorError,
    StateError,
    Stream,
    _is_night_time,
    accumulate_night_candidates,
    build_night_digest_descriptions,
    build_stream_embed,
    fetch_ranking,
    find_public_archive_profiles,
    has_public_archive,
    load_state,
    log_top_ranking_snapshot,
    maintain_state,
    mark_night_candidates_processed,
    mark_notified,
    parse_ranking_page,
    run,
    select_eligible_streams,
    select_night_candidates,
)


FIXTURE = Path(__file__).parent / "fixtures" / "ranking_sample.html"
NOW = datetime(2026, 8, 8, 1, 30, tzinfo=timezone.utc)


def new_state() -> dict:
    return {
        "version": 1,
        "notifications": {},
        "night_candidates": {},
        "metadata": {},
    }


def stream(
    user_id: str,
    momentum: int,
    name: str = "配信者",
    rank: int | None = 1,
    elapsed_minutes: int | None = 75,
) -> Stream:
    return Stream(
        user_id=user_id,
        broadcaster_name=name,
        title="配信タイトル",
        url=f"https://mixch.tv/u/{user_id}/live",
        momentum=momentum,
        rank=rank,
        elapsed_minutes=elapsed_minutes,
        elapsed_text=(
            "不明"
            if elapsed_minutes is None
            else "1時間15分"
            if elapsed_minutes == 75
            else f"{elapsed_minutes}分"
        ),
    )


def config_for_state(path: Path) -> Config:
    return Config(
        monitor_url="https://live-ranking.com/v/mixch",
        fallback_monitor_url="https://ikioi-ranking.com/v/mixch",
        momentum_threshold=150,
        cooldown_hours=12,
        error_cooldown_hours=6,
        heartbeat_days=7,
        request_timeout_seconds=30,
        state_file=path,
        discord_webhook_url="https://discord.com/api/webhooks/1/test",
        dry_run=False,
        test_webhook=False,
        notify_on_error=False,
        blocked_user_ids=frozenset(),
    )


class ParserTests(unittest.TestCase):
    def test_parses_user_id_momentum_and_elapsed_time(self) -> None:
        parsed = parse_ranking_page(FIXTURE.read_text(encoding="utf-8"))

        self.assertEqual(2, len(parsed))
        self.assertEqual("111", parsed[0].user_id)
        self.assertEqual("配信者🔥A", parsed[0].broadcaster_name)
        self.assertEqual("ガチイベ最終日", parsed[0].title)
        self.assertEqual(151, parsed[0].momentum)
        self.assertEqual(329, parsed[0].elapsed_minutes)
        self.assertEqual("5時間29分", parsed[0].elapsed_text)
        self.assertEqual("https://mixch.tv/u/111/live", parsed[0].url)

    def test_raises_when_header_has_streams_but_no_liveboxes(self) -> None:
        html = "<html><span id='live_list_header1'>勢い順 MixChannel [20人放送中]</span></html>"
        with self.assertRaisesRegex(Exception, "配信枠を取得できません"):
            parse_ranking_page(html)

    def test_rejects_unrelated_long_html(self) -> None:
        html = "<html><body>maintenance</body></html>" + " " * 2_000
        with self.assertRaisesRegex(Exception, "識別できる情報がありません"):
            parse_ranking_page(html)

    def test_keeps_stream_with_blank_broadcaster_name(self) -> None:
        html = """
        <span id="live_list_header1">勢い順 MixChannel [1人放送中]</span>
        <div id="livebox" data-uid="mixch_9738940">
          <div class="live_rankNum">1</div>
          <div class="live_title"><a href="https://mixch.tv/u/9738940/live">無名配信</a></div>
          <div class="live_name"><a href="https://mixch.tv/u/9738940/live"></a></div>
          <a class="live_timenum" title="74分経過"><span>1時間</span><span>14分</span></a>
          <div class="live_viewer"><span>180</span><span>points</span></div>
        </div>
        """
        parsed = parse_ranking_page(html)
        self.assertEqual(1, len(parsed))
        self.assertEqual("名称未設定（ID: 9738940）", parsed[0].broadcaster_name)
        self.assertEqual(74, parsed[0].elapsed_minutes)

    def test_parses_elapsed_time_from_fallback_site_div(self) -> None:
        html = """
        <span id="live_list_header1">勢い順 MixChannel [1人放送中]</span>
        <div id="livebox" data-uid="mixch_111">
          <div class="live_rankNum">1</div>
          <div class="live_title"><a href="https://mixch.tv/u/111/live">配信</a></div>
          <div class="live_name"><a href="https://mixch.tv/u/111/live">配信者A</a></div>
          <div class="live_timenum time_separate_notation" title="214分">
            <a href="https://mixch.tv/u/111/live">
              <span class="time_hour_num">3</span><span>時間</span>
              <span class="time_minutes_num">34</span><span>分</span>
            </a>
          </div>
          <div class="live_viewer"><span>180</span><span>ポイント</span></div>
        </div>
        """

        parsed = parse_ranking_page(html)

        self.assertEqual(214, parsed[0].elapsed_minutes)
        self.assertEqual("3時間34分", parsed[0].elapsed_text)


class FetchTests(unittest.TestCase):
    class FakeResponse:
        def __init__(self, body: bytes, status: int = 200) -> None:
            self._body = BytesIO(body)
            self.status = status
            self.headers = Message()
            self.headers["Content-Type"] = "text/html; charset=utf-8"

        def read(self) -> bytes:
            return self._body.read()

        def __enter__(self) -> "FetchTests.FakeResponse":
            return self

        def __exit__(self, *args: object) -> None:
            return None

    def test_uses_alternative_site_when_primary_response_is_empty(self) -> None:
        fixture = FIXTURE.read_bytes() + b" " * 2_000
        responses = [self.FakeResponse(b""), self.FakeResponse(fixture)]

        with patch("src.mixch_monitor.urlopen", side_effect=responses) as mocked:
            streams = fetch_ranking(
                "https://live-ranking.com/v/mixch",
                "https://ikioi-ranking.com/v/mixch",
                30,
            )

        self.assertEqual(["111", "222"], [item.user_id for item in streams])
        self.assertEqual(2, mocked.call_count)
        alternative_request = mocked.call_args_list[1].args[0]
        self.assertEqual(
            "https://ikioi-ranking.com/v/mixch",
            alternative_request.full_url,
        )

    def test_does_not_use_alternative_when_primary_response_is_valid(self) -> None:
        fixture = FIXTURE.read_bytes() + b" " * 2_000
        with patch(
            "src.mixch_monitor.urlopen", return_value=self.FakeResponse(fixture)
        ) as mocked:
            streams = fetch_ranking(
                "https://live-ranking.com/v/mixch",
                "https://ikioi-ranking.com/v/mixch",
                30,
            )

        self.assertEqual(2, len(streams))
        self.assertEqual(1, mocked.call_count)

    def test_uses_alternative_when_primary_html_cannot_be_parsed(self) -> None:
        invalid = b"<html><body>maintenance</body></html>" + b" " * 2_000
        fixture = FIXTURE.read_bytes() + b" " * 2_000
        responses = [self.FakeResponse(invalid), self.FakeResponse(fixture)]

        with patch("src.mixch_monitor.urlopen", side_effect=responses) as mocked:
            streams = fetch_ranking(
                "https://live-ranking.com/v/mixch",
                "https://ikioi-ranking.com/v/mixch",
                30,
            )

        self.assertEqual(2, len(streams))
        self.assertEqual(2, mocked.call_count)

    @patch("src.mixch_monitor.time.sleep", return_value=None)
    def test_retries_reader_after_both_direct_sites_fail(self, _sleep: object) -> None:
        fixture = FIXTURE.read_bytes() + b" " * 2_000
        responses = [
            self.FakeResponse(b""),
            self.FakeResponse(b""),
            self.FakeResponse(b"temporary fallback error"),
            self.FakeResponse(fixture),
        ]

        with patch("src.mixch_monitor.urlopen", side_effect=responses) as mocked:
            streams = fetch_ranking(
                "https://live-ranking.com/v/mixch",
                "https://ikioi-ranking.com/v/mixch",
                30,
            )

        self.assertEqual(2, len(streams))
        self.assertEqual(4, mocked.call_count)
        reader_request = mocked.call_args_list[2].args[0]
        self.assertEqual(
            "https://r.jina.ai/https://live-ranking.com/v/mixch",
            reader_request.full_url,
        )


class EligibilityTests(unittest.TestCase):
    def test_threshold_is_strictly_greater_than_150(self) -> None:
        eligible = select_eligible_streams(
            [stream("150", 150), stream("151", 151)],
            new_state(),
            threshold=150,
            cooldown_hours=12,
            now=NOW,
        )
        self.assertEqual(["151"], [item.user_id for item in eligible])

    def test_same_user_is_suppressed_even_after_name_change(self) -> None:
        state = new_state()
        mark_notified(state, [stream("111", 200, "旧名")], NOW - timedelta(hours=3))

        eligible = select_eligible_streams(
            [stream("111", 300, "新名")],
            state,
            threshold=150,
            cooldown_hours=12,
            now=NOW,
        )
        self.assertEqual([], eligible)

    def test_same_user_is_eligible_after_twelve_hours(self) -> None:
        state = new_state()
        mark_notified(
            state, [stream("111", 200)], NOW - timedelta(hours=12, seconds=1)
        )

        eligible = select_eligible_streams(
            [stream("111", 250)],
            state,
            threshold=150,
            cooldown_hours=12,
            now=NOW,
        )
        self.assertEqual(["111"], [item.user_id for item in eligible])

    def test_blocked_user_id_is_never_eligible(self) -> None:
        eligible = select_eligible_streams(
            [stream("14082684", 999), stream("222", 200)],
            new_state(),
            threshold=150,
            cooldown_hours=12,
            now=NOW,
            blocked_user_ids={"14082684"},
        )
        self.assertEqual(["222"], [item.user_id for item in eligible])


class RankingSnapshotLogTests(unittest.TestCase):
    def test_logs_only_top_ten_in_rank_order_as_json(self) -> None:
        # 入力順を意図的に崩し、順位11・12位と順位不明が記録されないことも確認する。
        streams = [
            stream(str(rank), 300 - rank, f"配信者{rank}", rank=rank)
            for rank in range(12, 0, -1)
        ]
        streams.append(stream("unknown", 999, "順位不明", rank=None))

        with self.assertLogs("mixch-ranking-monitor", level="INFO") as captured:
            log_top_ranking_snapshot(streams, NOW)

        data_lines = [
            line.split("RANKING_TOP10 ", 1)[1]
            for line in captured.output
            if "RANKING_TOP10 " in line
        ]
        records = [json.loads(line) for line in data_lines]

        self.assertEqual(10, len(records))
        self.assertEqual(list(range(1, 11)), [item["rank"] for item in records])
        self.assertEqual("2026-08-08T01:30:00Z", records[0]["observed_at"])
        self.assertEqual("1", records[0]["user_id"])
        self.assertEqual("配信者1", records[0]["broadcaster_name"])
        self.assertEqual(299, records[0]["momentum"])
        self.assertEqual(75, records[0]["elapsed_minutes"])
        self.assertEqual("https://mixch.tv/u/1", records[0]["profile_url"])
        self.assertEqual("https://mixch.tv/u/1/live", records[0]["live_url"])

    def test_logs_all_streams_when_fewer_than_ten_exist(self) -> None:
        with self.assertLogs("mixch-ranking-monitor", level="INFO") as captured:
            log_top_ranking_snapshot(
                [stream("111", 200, rank=1), stream("222", 180, rank=2)],
                NOW,
            )

        data_lines = [line for line in captured.output if "RANKING_TOP10 " in line]
        self.assertEqual(2, len(data_lines))


class NightModeTests(unittest.TestCase):
    def test_night_time_boundaries_use_japan_time(self) -> None:
        self.assertFalse(_is_night_time(datetime(2026, 8, 8, 12, 59, tzinfo=timezone.utc)))
        self.assertTrue(_is_night_time(datetime(2026, 8, 8, 13, 0, tzinfo=timezone.utc)))
        self.assertTrue(_is_night_time(datetime(2026, 8, 8, 21, 59, tzinfo=timezone.utc)))
        self.assertFalse(_is_night_time(datetime(2026, 8, 8, 22, 0, tzinfo=timezone.utc)))

    def test_night_threshold_includes_exactly_150(self) -> None:
        candidates = select_night_candidates(
            [stream("149", 149), stream("150", 150), stream("151", 151)],
            new_state(),
            threshold=150,
            cooldown_hours=12,
            now=NOW,
        )
        self.assertEqual(["151", "150"], [item.user_id for item in candidates])

    def test_night_candidates_respect_blocklist_and_cooldown(self) -> None:
        state = new_state()
        mark_notified(state, [stream("recent", 200)], NOW - timedelta(hours=2))
        candidates = select_night_candidates(
            [stream("recent", 300), stream("blocked", 300), stream("ok", 200)],
            state,
            threshold=150,
            cooldown_hours=12,
            now=NOW,
            blocked_user_ids={"blocked"},
        )
        self.assertEqual(["ok"], [item.user_id for item in candidates])

    def test_accumulation_deduplicates_and_keeps_maximum_momentum(self) -> None:
        state = new_state()
        first = NOW - timedelta(minutes=5)
        accumulate_night_candidates(state, [stream("111", 180, "旧名")], first)
        accumulate_night_candidates(state, [stream("111", 160, "新名")], NOW)

        self.assertEqual(1, len(state["night_candidates"]))
        record = state["night_candidates"]["111"]
        self.assertEqual("新名", record["broadcaster_name"])
        self.assertEqual(180, record["max_momentum"])
        self.assertEqual("2026-08-08T01:25:00Z", record["first_seen_at"])
        self.assertEqual("2026-08-08T01:30:00Z", record["last_seen_at"])

    def test_unchanged_candidate_does_not_rewrite_state(self) -> None:
        state = new_state()
        first = NOW - timedelta(minutes=5)
        accumulate_night_candidates(state, [stream("111", 180, "配信者A")], first)

        changed = accumulate_night_candidates(
            state, [stream("111", 180, "配信者A")], NOW
        )

        self.assertEqual(0, changed)
        self.assertEqual(
            "2026-08-08T01:25:00Z",
            state["night_candidates"]["111"]["last_seen_at"],
        )

    def test_processed_night_candidates_enter_regular_cooldown(self) -> None:
        state = new_state()
        accumulate_night_candidates(state, [stream("111", 180, "配信者A")], NOW)
        mark_night_candidates_processed(state, NOW)

        self.assertEqual(
            "night_digest_processed", state["notifications"]["111"]["reason"]
        )
        self.assertEqual(
            [],
            select_eligible_streams(
                [stream("111", 300)],
                state,
                threshold=150,
                cooldown_hours=12,
                now=NOW + timedelta(minutes=5),
            ),
        )

    def test_night_run_accumulates_without_immediate_notification(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            night_now = datetime(2026, 8, 8, 13, 0, tzinfo=timezone.utc)
            with (
                patch(
                    "src.mixch_monitor.fetch_ranking",
                    return_value=[stream("111", 150, "配信者A")],
                ),
                patch("src.mixch_monitor.log_top_ranking_snapshot") as ranking_log,
                patch("src.mixch_monitor.send_stream_notifications") as immediate,
            ):
                result = run(
                    config_for_state(path),
                    now=night_now,
                )
            state = load_state(path)

        self.assertEqual(0, result)
        ranking_log.assert_called_once()
        self.assertEqual(night_now, ranking_log.call_args.args[1])
        immediate.assert_not_called()
        self.assertIn("111", state["night_candidates"])

    def test_morning_run_sends_digest_once_and_clears_candidates(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            state = new_state()
            accumulate_night_candidates(
                state, [stream("111", 180, "配信者A")], NOW
            )
            path.write_text(json.dumps(state), encoding="utf-8")
            profiles = [
                {
                    "user_id": "111",
                    "broadcaster_name": "配信者A",
                    "profile_url": "https://mixch.tv/u/111",
                }
            ]
            with (
                patch(
                    "src.mixch_monitor.fetch_ranking",
                    return_value=[stream("111", 300, "配信者A")],
                ),
                patch(
                    "src.mixch_monitor.find_public_archive_profiles",
                    return_value=profiles,
                ),
                patch("src.mixch_monitor.send_night_digest_notification") as digest,
                patch("src.mixch_monitor.send_stream_notifications") as immediate,
            ):
                result = run(
                    config_for_state(path),
                    now=datetime(2026, 8, 8, 22, 0, tzinfo=timezone.utc),
                )
            saved = load_state(path)

        self.assertEqual(0, result)
        digest.assert_called_once()
        immediate.assert_not_called()
        self.assertEqual({}, saved["night_candidates"])
        self.assertEqual(
            "night_digest_processed", saved["notifications"]["111"]["reason"]
        )


class ArchiveTests(unittest.TestCase):
    class FakeResponse:
        def __init__(self, payload: dict) -> None:
            self._body = BytesIO(json.dumps(payload).encode("utf-8"))
            self.status = 200
            self.headers = Message()
            self.headers["Content-Type"] = "application/json; charset=utf-8"

        def read(self) -> bytes:
            return self._body.read()

        def __enter__(self) -> "ArchiveTests.FakeResponse":
            return self

        def __exit__(self, *args: object) -> None:
            return None

    def test_public_archive_is_found_on_later_page(self) -> None:
        responses = [
            self.FakeResponse(
                {
                    "archives": [{"visibility": 3}],
                    "has_next": True,
                    "next_cursor": 123,
                }
            ),
            self.FakeResponse(
                {"archives": [{"visibility": 1}], "has_next": False}
            ),
        ]
        with patch("src.mixch_monitor.urlopen", side_effect=responses) as mocked:
            self.assertTrue(has_public_archive("111", 30))

        self.assertEqual(2, mocked.call_count)
        self.assertIn("cursor=123", mocked.call_args_list[1].args[0].full_url)

    def test_restricted_archives_only_are_not_public(self) -> None:
        response = self.FakeResponse(
            {"archives": [{"visibility": 2}, {"visibility": 3}], "has_next": False}
        )
        with patch("src.mixch_monitor.urlopen", return_value=response):
            self.assertFalse(has_public_archive("111", 30))

    def test_public_profiles_keep_momentum_order(self) -> None:
        candidates = {
            "111": {
                "broadcaster_name": "配信者A",
                "url": "https://mixch.tv/u/111/live",
                "max_momentum": 180,
            },
            "222": {
                "broadcaster_name": "配信者B",
                "url": "https://mixch.tv/u/222/live",
                "max_momentum": 250,
            },
        }
        with patch(
            "src.mixch_monitor.has_public_archive",
            side_effect=lambda user_id, _timeout: user_id == "111",
        ):
            profiles = find_public_archive_profiles(candidates, 30)

        self.assertEqual(
            [
                {
                    "user_id": "111",
                    "broadcaster_name": "配信者A",
                    "profile_url": "https://mixch.tv/u/111",
                }
            ],
            profiles,
        )

    def test_digest_contains_only_clickable_profile_names(self) -> None:
        descriptions = build_night_digest_descriptions(
            [
                {
                    "user_id": "111",
                    "broadcaster_name": "配信者A",
                    "profile_url": "https://mixch.tv/u/111",
                }
            ]
        )
        self.assertEqual(
            ["[配信者A](https://mixch.tv/u/111)"], descriptions
        )


class StateTests(unittest.TestCase):
    def test_old_state_gains_empty_night_candidates(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            path.write_text(
                json.dumps({"version": 1, "notifications": {}, "metadata": {}}),
                encoding="utf-8",
            )
            state = load_state(path)
        self.assertEqual({}, state["night_candidates"])

    def test_rejects_corrupted_timestamp_instead_of_resending(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "notifications": {
                            "111": {"last_notified_at": "壊れた時刻"}
                        },
                        "metadata": {},
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            with self.assertRaises(StateError):
                load_state(path)

    def test_heartbeat_updates_weekly_and_old_records_are_removed(self) -> None:
        state = new_state()
        state["metadata"]["last_heartbeat_at"] = (
            NOW - timedelta(days=8)
        ).isoformat()
        mark_notified(state, [stream("old", 200)], NOW - timedelta(days=31))

        changed = maintain_state(state, NOW, cooldown_hours=12, heartbeat_days=7)

        self.assertTrue(changed)
        self.assertNotIn("old", state["notifications"])
        self.assertEqual("2026-08-08T01:30:00Z", state["metadata"]["last_heartbeat_at"])


class NotificationFormattingTests(unittest.TestCase):
    def test_embed_contains_requested_information(self) -> None:
        item = stream("111", 234, "配信者A")
        embed = build_stream_embed(item, threshold=150, observed_at=NOW)

        rendered = json.dumps(embed, ensure_ascii=False)
        self.assertIn("配信者A", rendered)
        self.assertIn("234 points", rendered)
        self.assertIn("1時間15分", rendered)
        self.assertIn("https://mixch.tv/u/111/live", rendered)
        self.assertEqual("https://mixch.tv/u/111", embed["url"])
        self.assertNotIn("順位", [field["name"] for field in embed["fields"]])
        live_url_field = next(
            field for field in embed["fields"] if field["name"] == "配信URL"
        )
        self.assertEqual(
            "[MixChannelで開く](https://mixch.tv/u/111/live)",
            live_url_field["value"],
        )


class ConfigTests(unittest.TestCase):
    def test_defaults_match_requested_values(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            config = Config.from_environment()
        self.assertEqual(150, config.momentum_threshold)
        self.assertEqual(12, config.cooldown_hours)
        self.assertEqual(
            "https://ikioi-ranking.com/v/mixch", config.fallback_monitor_url
        )
        self.assertEqual(
            frozenset(
                {
                    "14082684",
                    "17373942",  # うえきあやか
                    "18014848",  # 日DXコーラ
                    "18504420",  # のうみくん#ﾚｷﾞｭﾗｰﾓﾃﾞ
                    "18674264",  # こうぐちまﾙ
                }
            ),
            config.blocked_user_ids,
        )

    def test_repository_variables_override_defaults(self) -> None:
        with patch.dict(
            os.environ,
            {
                "MOMENTUM_THRESHOLD": "275",
                "COOLDOWN_HOURS": "8",
                "BLOCKED_USER_IDS": (
                    "https://mixch.tv/u/18844927/live\n18856007"
                ),
            },
            clear=True,
        ):
            config = Config.from_environment()
        self.assertEqual(275, config.momentum_threshold)
        self.assertEqual(8, config.cooldown_hours)
        self.assertEqual(
            frozenset(
                {
                    "14082684",
                    "17373942",
                    "18014848",
                    "18504420",
                    "18674264",
                    "18844927",
                    "18856007",
                }
            ),
            config.blocked_user_ids,
        )

    def test_rejects_invalid_blocklist_value(self) -> None:
        with patch.dict(
            os.environ, {"BLOCKED_USER_IDS": "14082684, 配信者名"}, clear=True
        ):
            with self.assertRaisesRegex(MonitorError, "BLOCKED_USER_IDSに不正な値"):
                Config.from_environment()


if __name__ == "__main__":
    unittest.main()
