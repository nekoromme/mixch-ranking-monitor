# 全監視ツール停止監視

GitHub上で動く既存の監視ツールを15分おきに横断確認する「監視番」です。
`mixch-ranking-monitor` 内で動きますが、別リポジトリのワークフローもGitHub APIから直接確認します。

## 現在の対象

| 監視ツール | 確認するワークフロー | 停止とみなす目安 |
|---|---|---:|
| MixChannel勢い監視 | 本体・5分リレー・既存の停止監視 | 最後の成功から25分 |
| トレカ抽選・発売監視 | `tcg-box-monitor-public/monitor.yml` | 最後の成功から10時間 |
| MixChannelアーカイブ監視 | `mixch-archive-monitor-public/schedule.yml` | 最後の成功から13時間 |
| 高額抽選監視（RICOH） | `high-value-lottery-monitor/monitor.yml` | 最後の成功から2時間30分 |

各ツールの元の実行周期には差があります。最長の定期間隔に、GitHub Actionsの混雑遅延を加えた値を目安にしているため、正常な待ち時間を停止と誤認しません。

## 検知する異常

- ワークフローが無効化された
- 直近の実行が失敗・キャンセル・時間切れになった
- 最後の正常終了から、対象ごとの許容時間を超えた
- 実行中のまま、通常の最大処理時間を超えて固まった
- 実行履歴または成功履歴がない
- GitHub APIから対象の状態を取得できない

## 通知と重複防止

異常を初めて検知した時だけ、次の2か所へ記録します。

1. 既存の `DISCORD_WEBHOOK_URL` を使ったDiscord通知
2. このリポジトリの `[監視ツール異常]` Issue

同じ異常が続いても15分おきに連投しません。復旧を確認するとDiscordへ復旧通知を送り、対応するIssueを自動で閉じます。Issueが重複防止状態を兼ねるため、別ブランチへのJSON保存やActionsキャッシュには依存しません。

## 手動テスト

1. GitHubの `Actions` を開く
2. `全監視ツール停止監視` を開く
3. `Run workflow` を押す
4. 初回は `dry_run` をオンのまま実行する

Discord経路も確認する場合は `test_notification` をオンにします。`dry_run` がオンでも、テスト通知だけは1件送られます。

## 対象を増やす時

`fleet_targets.json` の `targets` に次の5項目を追加します。

- `name`: 通知に表示する名前
- `repository`: `所有者/リポジトリ名`
- `workflow`: `.github/workflows/` 内のファイル名
- `max_success_age_minutes`: 最後の成功が古いと判断する分数
- `max_run_minutes`: 実行が固まったと判断する分数

公開リポジトリなら追加のアクセストークンは不要です。非公開リポジトリを対象にする場合は、標準の `GITHUB_TOKEN` では別リポジトリを読めないため、読み取り専用のGitHub Appなどを別途用意する必要があります。

## ローカルテスト

```bash
python3 -m unittest tests.test_fleet_watchdog -v
python3 -m compileall -q src tests
```

実際のGitHub APIを読み、通知とIssue変更をしない確認:

```bash
python3 -m src.fleet_watchdog --dry-run
```
