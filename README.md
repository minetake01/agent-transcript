# agent-transcript

Cursor、Claude Code、Codex、OpenCode、pi、Antigravity CLI の会話ログを、この PC のローカルストアと Cloudflare R2 の両方から読み取る。R2 に置く本文はアップロード前に暗号化し、バケットは非公開のまま SigV4 で取得する。

MCP は読み取り専用です。書き込み、削除、他ハーネスへの resume やクローンは出しません。`cwd` を省略しても全リポジトリは返しません。プロセスの作業ディレクトリの git `origin` を正規化した repo key で、ローカルと R2 を同時に返します。

## 範囲

取り込むのは [txcript](https://docs.rs/txcript) がローカルストアとして読むセッションです。Antigravity の IDE が書く `.pb` 会話と、Claude Chat / ChatGPT のライブ API は対象外です。

同じ `harness` と `session_id` がローカルと R2 の両方にあるときは 1 件にまとめ、新しい方の本文を使います。新しさは `updated_at`、無ければ最終メッセージ時刻、それでも同じならメッセージ数です。そこまで同じで本文が違うセッションは、本文の取得をエラーにします。

## 設定

設定と復号鍵はリポジトリの外に置きます。Windows では `%APPDATA%\agent-transcript\config.toml` と `%APPDATA%\agent-transcript\key` です。復号キャッシュは `%LOCALAPPDATA%\agent-transcript\cache` です。`AGENT_TRANSCRIPT_HOME` を置くと、そのディレクトリを設定場所にします。

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
agent-transcript mcp
```

`install` は、このコマンドを起動した実行ファイルで `watch` をログオン時に起動するタスク `agent-transcript watch` を、タスク スケジューラへ登録してその場でも起動します。`mode` が `read` のときは登録しません。

`ingest` はハーネスごとにストアの世代を見ます。世代が前回と同じで、記録した内容ハッシュがすべてカタログにあれば、そのストアは開きません。世代が変わったストアだけを discover し、指紋が空か前回と違うソースだけ本文を開きます。指紋が一致してもカタログにその内容ハッシュが無いソースは開き直して送ります。カタログにある内容ハッシュは送りません。

`watch` は同じ確認をすぐ 1 回行い、その後は 5 分おきに同じプロセスで繰り返します。優先度は通常より低くします。カタログが参照しなくなったオブジェクトは `gc` で削除します。

## MCP

どのツールも、リポジトリのディレクトリで次のバイナリを stdio 起動します。秘密は MCP 定義に書かず、上のユーザ設定から読みます。

ツールは 3 つです。

- `list_sessions(from?, cwd?, limit?, offset?)`
- `search_sessions(pattern, from?, cwd?)`
- `read_session(id, from?)`

`read_session` の範囲は `id#5-12` のように ID へ付けます。`cwd` は記録されたパスとの比較には使わず、そのディレクトリの origin でローカルと R2 の両方を選びます。`read_session` のリポジトリは、プロセスの作業ディレクトリです。

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
