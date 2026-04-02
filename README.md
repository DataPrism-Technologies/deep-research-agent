# deep-research-agent

Gemini Deep Research を使ってリサーチを自動化し、ジョブごとに Slack へ通知する小さな運用基盤です。

## 背景

DataPrism Technologies株式会社では、製造業・建設業・物流・印刷業向けの AI 実装事業を展開しています。営業リード獲得が代表個人に寄っているため、展示会や交流会のような継続的な接点づくりを仕組み化するのがこのリポジトリの目的です。

最初のユースケースは、東京都・神奈川県・千葉県・埼玉県の対面イベント調査です。将来的には補助金、公募、業界ニュースなどもジョブ追加だけで広げられる構成を目指しています。

## 構成

- `jobs/*.yaml`: 実際に読み込まれる 1 ファイル 1 ジョブの定義
- `jobs/example.yaml`: ジョブ定義のサンプル
- `prompts/prompt_definition.yaml`: Deep Research 用と JSON 整形用の prompt 定義。`system_prompt` と `user_prompt` を分けて管理
- `schemas/research_result.schema.json`: Gemini structured output 用の JSON schema
- `research_agent.py`: 共通ランナー。Gemini Deep Research 実行、結果の JSON 整形、Slack 通知まで担当
- `pyproject.toml`: `uv` と `ruff` の設定
- `.github/workflows/daily.yml`: 毎日実行する GitHub Actions

## 仕組み

Gemini Deep Research は長時間ジョブかつ structured output 非対応のため、このリポジトリでは 2 段構成にしています。

1. `deep-research-pro-preview-12-2025` で調査レポートを生成
2. `gemini-2.5-flash` でそのレポートを strict JSON に整形
3. JSON を Slack Block Kit に変換して通知

この構成により、Deep Research の調査品質を活かしつつ、通知側は安定して扱えるようにしています。

## 必要なもの

- `uv`
- Gemini API key
- ジョブごとの Slack webhook

## セットアップ

```bash
uv sync
```

`.env` を作成します。

```bash
cp .env.sample .env
```

```env
GEMINI_API_KEY=your_gemini_api_key
SLACK_WEBHOOK_EVENT_RESEARCH=https://hooks.slack.com/services/...
```

`research_agent.py` は `python-dotenv` を使って起動時に `.env` を自動で読み込みます。すでにシェル環境に同名の環境変数がある場合は、そちらを優先します。

ジョブのサンプルをコピーして実ジョブを作ります。

```bash
cp jobs/example.yaml jobs/event_research.yaml
```

## 設定

標準では設定ファイルなしで動きます。以下の固定デフォルトを使います。

- ジョブディレクトリ: `jobs`
- prompt 定義: `prompts/prompt_definition.yaml`
- schema: `schemas/research_result.schema.json`

必要な場合だけ `--config` で上書き用 YAML を渡せます。

```yaml
defaults:
  job_directory: "jobs"
  prompt_definition_path: "prompts/prompt_definition.yaml"
  response_schema_path: "schemas/research_result.schema.json"
```

実ジョブファイルの例:

```yaml
name: "event_research"
slack_webhook_env: "SLACK_WEBHOOK_EVENT_RESEARCH"
prompt: |
  調査したいテーマを書く
search_queries:
  - "検索のたたき台 1"
  - "検索のたたき台 2"
```

`search_queries` は必須ではありませんが、あると Deep Research の初期探索方針を組み立てやすくなります。

プロンプトや schema を変えたい場合は、まず `prompts/` と `schemas/` を編集します。必要なら `--config` で別パスに差し替えできます。prompt は YAML で `deep_research` と `structuring` を分け、それぞれ `system_prompt` と `user_prompt` を持つ形にしています。

`jobs/example.yaml` はサンプルとして無視されます。ジョブファイルが 1 つ壊れていても、他の正常なジョブは継続して読み込まれます。壊れたジョブは warning として表示され、そのファイルだけスキップされます。

## 実行方法

全ジョブ実行:

```bash
uv run python research_agent.py
```

特定ジョブだけ実行:

```bash
uv run python research_agent.py --job event_research
```

Slack 送信なしでローカル確認:

```bash
uv run python research_agent.py --dry-run
```

別の設定ファイルを使う:

```bash
uv run python research_agent.py --config path/to/config.yaml
```

## Lint と Format

Lint:

```bash
uv run ruff check .
```

Format:

```bash
uv run ruff format .
```

必要なら自動修正つき lint:

```bash
uv run ruff check . --fix
```

## 挙動

- Deep Research は `deep-research-pro-preview-12-2025`
- JSON 整形は `gemini-2.5-flash`
- Deep Research は非同期で起動し、完了までポーリング
- Slack 通知には token-only の概算コストを表示
- 1 ジョブ失敗しても残りのジョブは継続
- 1 件でも失敗するとプロセスは非ゼロ終了
- `--dry-run` では Slack を送らず JSON を標準出力

## GitHub Actions

ワークフローは毎日 `00:00 UTC` に実行され、手動実行も可能です。少なくとも以下の Secrets が必要です。

- `GEMINI_API_KEY`
- `SLACK_WEBHOOK_EVENT_RESEARCH`

ジョブを追加したら、そのジョブの `slack_webhook_env` に対応する Secret も追加してください。

設定手順:

1. GitHub の `Settings` -> `Secrets and variables` -> `Actions` を開く
2. `GEMINI_API_KEY` を登録する
3. `SLACK_WEBHOOK_EVENT_RESEARCH` を登録する
4. `Actions` タブから `Daily Research` を一度 `Run workflow` で手動実行する

現在の workflow は `uv sync --locked` を使うため、リポジトリにある `uv.lock` を前提にそのまま実行できます。

## 注意点

- Gemini Interactions API と Deep Research agent はベータ/プレビュー要素を含むため、将来スキーマ変更が入る可能性があります。
- Deep Research は通常の生成より時間がかかるため、1 回の実行に数分以上かかることがあります。
