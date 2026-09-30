# agent-transcript

Cursor、Claude Code、Codex、OpenCode、pi、Antigravity CLI の会話ログを、この PC のローカルストアと Cloudflare R2 の両方から読み取る。R2 に置く本文はアップロード前に zstd で圧縮してから暗号化し（`ATZ1` 形式。旧 `ATX1` オブジェクトもそのまま復号できる）、バケットは非公開のまま SigV4 で取得する。

MCP は読み取り専用です。書き込み、削除、他ハーネスへの resume やクローンは出しません。`cwd` を省略しても全リポジトリは返しません。クライアントがワークスペースの root を返したときは、それらが指す一つの git `origin` を正規化した repo key で、ローカルと R2 を同時に返します。root を返さないときは、プロセス起動時の作業ディレクトリの origin を使います。root が複数のリポジトリを指すとき、または origin を解決できないときは失敗します。

## 範囲

取り込むのは [txcript](https://docs.rs/txcript) がローカルストアとして読むセッション、および Antigravity のセッションデータベース（`.db`）です。Claude Chat / ChatGPT のライブ API は対象外です。

同じ `harness` と `session_id` がローカルと R2 の両方にあるときは 1 件にまとめ、新しい方の本文を使います。新しさは `updated_at`、無ければ最終メッセージ時刻、それでも同じならメッセージ数です。そこまで同じで本文が違うセッションは、本文の取得をエラーにします。

## 設定

設定と復号鍵はリポジトリの外に置きます。Windows では `%APPDATA%\agent-transcript\config.toml` と `%APPDATA%\agent-transcript\key` です。復号したオブジェクトのキャッシュは `%LOCALAPPDATA%\agent-transcript\cache` に内容ハッシュ名の `.json` として保存し、最大 512 MiB・最終利用から 30 日で削除します。検索用のスナップショットは `cache/search-index` に保存し、read/write 環境では暗号化したものを R2 の `v1/search` にも保存します。検索インデックスは古いセッションを保持するため LRU で削除しません。更新されたセッションの項目は差し替えられます。キャッシュは平文なので、このディレクトリへのアクセス権に注意してください。`AGENT_TRANSCRIPT_HOME` を置くと、そのディレクトリを設定場所にします。

`config.toml` の `max_bucket_bytes`（省略時は 1 GiB）はバケットの目標上限です。超えてもセッションは削除せず、ingest のスイープが警告を出すだけです。実際の削減は圧縮と、カタログが参照しなくなった旧リビジョンの回収で行います。

R2 の API トークンはバケット専用にします。読み取り専用の PC では Object Read だけのトークンを作り、`mode` を `read` にします。そのモードでは `ingest` と `gc` はリクエストを出さずに失敗します。

```sh
agent-transcript init \
  --account-id ACCOUNT \
  --bucket BUCKET \
  --access-key-id ACCESS_KEY_ID \
  --secret-access-key SECRET \
  --mode readwrite
```

鍵ファイルが既にあるときは作り直さず、その鍵を残します。別の PC で同じアーカイブを読むには、`config.toml` と `key` をその PC の設定ディレクトリへコピーします。鍵は R2 に置きません。

`origin` は認証情報を除き、`git@host:path` や `ssh://` を `https://host/path` にし、ホストだけ小文字にして末尾の `.git` を除いた文字列です。origin を解決できないセッションはアップロードしません。その実行で新たに分かったものは一覧を出して終了コード 1 になり、ファイルも origin も変わっていなければ次の実行は成功します。

```sh
agent-transcript install
agent-transcript ingest
agent-transcript watch
agent-transcript gc
agent-transcript index
agent-transcript mcp
agent-transcript update
```

`install` は、このコマンドを起動した実行ファイルで `watch` をログオン時に起動するタスク `agent-transcript watch` を、タスク スケジューラへ登録してその場でも起動します。`mode` が `read` のときは登録しません。

`update` は公式 GitHub Releases の最新安定版を確認し、現在のバージョンより新しい場合だけこの実行ファイルを更新します（R2 の設定や鍵は変更しません）。対応する配布バイナリと `.sha256` が必要です。Windows x64、Linux x64、macOS x64 / arm64 に対応します。ダウンロードを SHA-256 で照合してから置換するため、**実行ファイルのあるディレクトリへの書き込み権限**が必要です。Windows では実行中の exe を置換できないため、別プロセスが終了を待って置換し、登録済みの watch タスクを一旦止めて再起動します。置換に失敗した場合は元の exe を復元し、`<実行ファイル>.update.log` にエラーを記録します。手動起動した watch や MCP は自動再起動しません。更新は明示的に `update` を実行したときだけ行い、インストール元がソースビルドでも同じプラットフォームの公式リリース版へ置き換えます。リリースがまだ無い場合はエラーになります。

配布は `Cargo.toml` のバージョンに一致する `v<version>` タグを push すると `.github/workflows/release.yml` が上記プラットフォームの実行ファイルと SHA-256 ファイルを公開します。チェックサムは配布元の破損検出用であり、GitHub アカウントやリリース自体の侵害を防ぐ署名ではありません。

`ingest` は MCP が見るのと同じ `local-state.json` のソース記録を使います。ストアを stat だけで走査し、新規・変更のソースは本文を一度だけ読んでメタと内容ハッシュを記録します。状態が `ready` で、内容ハッシュがカタログに無いソースだけを圧縮・暗号化して送ります。カタログにある内容ハッシュは送りません。

`watch` は同じ確認をすぐ 1 回行い、その後は 5 分おきに同じプロセスで繰り返します。優先度は通常より低くします。カタログコミットのたびに、現行リビジョン（および同位タイ）だけを残して古いリビジョンを整理します。参照されなくなったオブジェクトは `gc` で削除します。`ingest` も約 1 時間ごとに同じ掃除を行い、直近 10 分以内に置かれたオブジェクトは別マシンのコミット途中かもしれないので残します。掃除のたびにバケット合計サイズを表示し、`max_bucket_bytes` を超えていれば警告します。

`index` は指定したリポジトリ（省略時は現在の作業ディレクトリ）の全セッションを検索用に抽出し、ローカルの `cache/search-index` に保存します。read/write モードでは、同じスナップショットを暗号化して R2 の `v1/search` にも保存します。`ingest` で変更されたリポジトリも自動的に更新されます。既存環境では、最初は `agent-transcript index` を一度実行してください。R2 のスナップショットを暗号鍵ごと別の read-only PC にコピーすると、その PC でも全履歴を事前構築なしで検索できます。

MCP はリクエストのたびにストアを stat 走査だけして差分を見ます。セッション本文は新規・変更分だけ読み、各ソースのメタと指紋は `local-state.json` に保持するため、定常時はファイルを開きません。`ingest` も同じ記録からアップロード対象を決めます。カタログは `cache/catalog.json` に保持し、5 分ごとに ETag の HEAD だけで確認します。期限切れのときは必ず確認してから応答するので、結果が古いまま返ることはありません。検索インデックスはメモリ上のランタイムを差分更新し（追加・変更・削除分だけ）、結果をローカルスナップショットへ書き戻します。read/write モードでは更新されたスナップショットを R2 にも公開します。`read_session` はローカルのセッションをその場で読むので、常に最新の本文を返します。

## MCP

どのツールも、リポジトリのディレクトリで次のバイナリを stdio 起動します。秘密は MCP 定義に書かず、上のユーザ設定から読みます。

ツールは 3 つです。

- `list_sessions(from?, cwd?, limit?, offset?)`
- `search_sessions(pattern, from?, cwd?)`
- `read_session(id, from?, cwd?)`

`read_session` の範囲は `id#5-12` のように ID へ付けます。`cwd` は記録されたパスとの比較には使わず、そのディレクトリの origin でローカルと R2 の両方を選びます。`cwd` を省略した `list_sessions`、`search_sessions`、`read_session` は、クライアントのワークスペース、または root が無いときはプロセス起動時の作業ディレクトリを使います。どのコマンドも起動直後に作業ディレクトリを復号キャッシュのディレクトリへ移すため、起動元のディレクトリ（エディタのインストール先など）を握り続けてそのツリーの更新を妨げることはありません。

Cursor (`~/.cursor/mcp.json`):

```json
{
  "mcpServers": {
    "agent-transcript": {
      "command": "agent-transcript",
      "args": ["mcp"]
    }
  }
}
```

Claude Code はユーザスコープの `~/.claude.json` に同じ `mcpServers` を足します。プロジェクトの `.mcp.json` には置きません。

Codex (`~/.codex/config.toml`):

```toml
[mcp_servers.agent-transcript]
command = "agent-transcript"
args = ["mcp"]
```

OpenCode のユーザ設定:

```jsonc
{
  "mcp": {
    "servers": {
      "agent-transcript": {
        "type": "local",
        "command": ["agent-transcript", "mcp"]
      }
    }
  }
}
```

pi (`~/.pi/agent/mcp.json`) は Cursor と同じ `mcpServers` です。Antigravity はユーザの MCP 設定に、同じ stdio コマンドを足します。
