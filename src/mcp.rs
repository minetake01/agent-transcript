use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use chrono::Utc;
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::{Json, Parameters};
use rmcp::model::{Implementation, JsonObject, ServerCapabilities, ServerInfo};
use rmcp::schemars::JsonSchema;
use rmcp::{
    tool, tool_handler, tool_router, transport::stdio, ErrorData, Peer, RoleServer, ServerHandler,
    ServiceExt,
};
use serde::{Deserialize, Serialize};
use txcript::search::{Case, Hit, Origin, Query};
use txcript::HarnessId;
use url::Url;

use crate::config;
use crate::error::Error;
use crate::fragment::parse_ref;
use crate::read;
use crate::repo_id::{self, OriginError};
use crate::search_index;
use crate::sessions::{self, find_session, Scope};
use crate::store::R2;

const SEARCH_INDEX_MAX_AGE: i64 = 5 * 60;
const STANDARD_FORMATS: &[&str] = &[
    "date",
    "date-time",
    "duration",
    "email",
    "hostname",
    "idn-email",
    "idn-hostname",
    "ipv4",
    "ipv6",
    "iri",
    "iri-reference",
    "json-pointer",
    "regex",
    "relative-json-pointer",
    "time",
    "uri",
    "uri-reference",
    "uri-template",
    "uuid",
];

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ListSessionsRequest {
    /// Only include this harness. Omit to include every harness in the repository.
    from: Option<String>,
    /// Directory whose git origin selects the repository. Omit to use the client workspace, or the process working directory when the client reports no workspace.
    cwd: Option<String>,
    /// Return at most this many sessions. Omit for no cap.
    limit: Option<usize>,
    /// Skip this many sessions from the newest end first.
    offset: Option<usize>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SearchSessionsRequest {
    /// Literal substring to find.
    pattern: String,
    /// Search only this harness. Omit to search every harness in the repository.
    from: Option<String>,
    /// Directory whose git origin selects the repository. Omit to use the client workspace, or the process working directory when the client reports no workspace.
    cwd: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ReadSessionRequest {
    /// Session id, unambiguous prefix, or exact title, with an optional `#range` (`abc#5-12`).
    id: String,
    /// Only look in this harness. Omit to look across every harness in the repository.
    from: Option<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
struct SessionList {
    total: usize,
    offset: usize,
    sessions: Vec<SessionSummary>,
}

#[derive(Debug, Serialize, JsonSchema)]
struct SessionSummary {
    harness: String,
    id: String,
    timestamp: String,
    title: Option<String>,
    cwd: Option<String>,
    git_branch: Option<String>,
    model: Option<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
struct SearchResults {
    matches: Vec<SearchMatch>,
}

#[derive(Debug, Serialize, JsonSchema)]
struct SearchMatch {
    session: SessionSummary,
    score: u32,
    hits: Vec<SearchHit>,
}

#[derive(Debug, Serialize, JsonSchema)]
struct SearchHit {
    span: std::ops::Range<usize>,
    block: usize,
    origin: &'static str,
    line: String,
    score: u32,
}

struct App {
    r2: R2,
    key: crate::crypto::Key,
    cache_dir: std::path::PathBuf,
    can_write: bool,
    search_indexes: RwLock<HashMap<String, Arc<search_index::Runtime>>>,
    refreshing: std::sync::Mutex<std::collections::HashSet<String>>,
}

#[derive(Clone)]
pub struct ArchiveServer {
    tool_router: ToolRouter<Self>,
    app: Arc<App>,
}

impl ArchiveServer {
    fn new(app: App) -> Self {
        let mut tool_router = Self::tool_router();
        for route in tool_router.map.values_mut() {
            strip_nonstandard_formats(std::sync::Arc::make_mut(&mut route.attr.input_schema));
            if let Some(output) = route.attr.output_schema.as_mut() {
                strip_nonstandard_formats(std::sync::Arc::make_mut(output));
            }
        }
        Self {
            tool_router,
            app: Arc::new(app),
        }
    }
}

#[tool_router]
impl ArchiveServer {
    #[tool(
        description = "List coding-agent sessions for one repository, newest first. Local sessions and the R2 archive are merged; the same harness and session id appear once. `cwd` selects the repository by its git origin, not by the recorded path. Omit `cwd` to use the client workspace, or this process's working directory when the client reports no workspace. Omit `from` to include every harness in that repository. `limit` and `offset` page the merged list; `total` is the count before paging.",
        annotations(title = "List sessions", read_only_hint = true)
    )]
    async fn list_sessions(
        &self,
        Parameters(request): Parameters<ListSessionsRequest>,
        peer: Peer<RoleServer>,
    ) -> Result<Json<SessionList>, ErrorData> {
        let from = parse_from(request.from.as_deref())?;
        refuse_live(from)?;
        let scope = self.open(request.cwd.as_deref(), from, &peer).await?;
        let total = scope.merged.len();
        let offset = request.offset.unwrap_or(0).min(total);
        let sessions = scope
            .merged
            .iter()
            .skip(offset)
            .take(request.limit.unwrap_or(usize::MAX))
            .map(summary)
            .collect();
        Ok(Json(SessionList {
            total,
            offset,
            sessions,
        }))
    }

    #[tool(
        description = "Search coding-agent sessions in one repository for a literal substring. Local sessions and the R2 archive are merged first. `cwd` selects the repository by its git origin. Omit `cwd` to use the client workspace, or this process's working directory when the client reports no workspace. Omit `from` to search every harness in that repository.",
        annotations(title = "Search sessions", read_only_hint = true)
    )]
    async fn search_sessions(
        &self,
        Parameters(request): Parameters<SearchSessionsRequest>,
        peer: Peer<RoleServer>,
    ) -> Result<Json<SearchResults>, ErrorData> {
        let from = parse_from(request.from.as_deref())?;
        refuse_live(from)?;
        let directory = self
            .workspace_directory(request.cwd.as_deref(), &peer)
            .await?;
        let repo_key = self.repo_key(&directory)?;

        // The normal path is entirely local: load the durable snapshot once,
        // then query the in-memory index. No catalog request, local discovery,
        // transcript parsing, or cache-directory scan is needed here.
        if let Some(runtime) = self.search_runtime(&repo_key).await {
            self.refresh_if_stale(&directory, &repo_key, &runtime);
            return Ok(Json(query_runtime(runtime, request.pattern, from)));
        }

        // If this PC has never downloaded the index, try the encrypted R2 copy
        // briefly. The timeout is deliberate: a slow network must not turn a
        // local query into an unbounded wait. The legacy path below preserves
        // correctness when neither copy is available yet.
        let remote = tokio::time::timeout(
            Duration::from_millis(900),
            search_index::load_remote(&self.app.r2, &self.app.key, &self.app.cache_dir, &repo_key),
        )
        .await;
        if let Ok(Ok(Some(snapshot))) = remote {
            let runtime = Arc::new(snapshot.into_runtime());
            self.remember_runtime(&repo_key, Arc::clone(&runtime));
            self.refresh_if_stale(&directory, &repo_key, &runtime);
            return Ok(Json(query_runtime(runtime, request.pattern, from)));
        }

        // Cold path: build a complete local snapshot and answer from it. This
        // is the only path that may read every transcript; subsequent queries
        // use the fast path above.
        // Build the repository-wide snapshot even when this request selected a
        // single harness; otherwise a filtered first query would poison the
        // durable index for later unfiltered searches.
        let scope = self.open_directory(&directory, None).await?;
        let snapshot = search_index::Snapshot::from_scope(
            scope,
            &self.app.r2,
            &self.app.key,
            &self.app.cache_dir,
        )
        .await
        .map_err(tool_error)?;
        if self.app.can_write {
            let plain = snapshot.to_bytes().map_err(tool_error)?;
            let r2 = self.app.r2.clone();
            let key = self.app.key;
            let repo = repo_key.clone();
            tokio::spawn(async move {
                if let Err(error) =
                    search_index::publish_remote_bytes(&r2, &key, &repo, &plain).await
                {
                    eprintln!("agent-transcript: publishing search index failed: {error}");
                }
            });
        }
        let runtime = Arc::new(snapshot.into_runtime());
        self.remember_runtime(&repo_key, Arc::clone(&runtime));
        Ok(Json(query_runtime(runtime, request.pattern, from)))
    }

    #[tool(
        description = "Read one session from the merged local and R2 archive as token-optimized text. `id` is a session id, unambiguous prefix, or exact title. Append `#range` (1-based inclusive, for example `abc#5-12`) to read part of it. Reads over the byte budget are refused with suggested ranges. `from` limits the harness. The repository is the client workspace, or this process's working directory when the client reports no workspace.",
        annotations(title = "Read session", read_only_hint = true)
    )]
    async fn read_session(
        &self,
        Parameters(request): Parameters<ReadSessionRequest>,
        peer: Peer<RoleServer>,
    ) -> Result<String, ErrorData> {
        let from = parse_from(request.from.as_deref())?;
        refuse_live(from)?;
        let mut scope = self.open(None, from, &peer).await?;
        let (view, range) = {
            let (view, range) = find_session(&scope.merged, &request.id).map_err(tool_error)?;
            (view.clone(), range)
        };
        let label = if range.is_some() {
            parse_ref(&request.id).0.to_string()
        } else {
            view.session_id.clone()
        };
        let transcript = scope
            .transcript(&self.app.r2, &self.app.key, &self.app.cache_dir, &view)
            .await
            .map_err(tool_error)?;
        read::render(&label, &transcript, range.as_ref())
            .map_err(|error| ErrorData::invalid_params(error, None))
    }
}

impl ArchiveServer {
    async fn open(
        &self,
        cwd: Option<&str>,
        from: Option<HarnessId>,
        peer: &Peer<RoleServer>,
    ) -> Result<Scope, ErrorData> {
        let directory = self.workspace_directory(cwd, peer).await?;
        self.open_directory(&directory, from).await
    }

    async fn workspace_directory(
        &self,
        cwd: Option<&str>,
        peer: &Peer<RoleServer>,
    ) -> Result<PathBuf, ErrorData> {
        match cwd {
            Some(cwd) => Ok(PathBuf::from(cwd)),
            None => directory_for_omitted_cwd(peer).await.map_err(tool_error),
        }
    }

    fn repo_key(&self, directory: &Path) -> Result<String, ErrorData> {
        repo_id::origin_of(directory).map_err(|error| tool_error(Error::msg(error.to_string())))
    }

    async fn open_directory(
        &self,
        directory: &Path,
        from: Option<HarnessId>,
    ) -> Result<Scope, ErrorData> {
        let directory = directory.to_str().ok_or_else(|| {
            tool_error(Error::msg(format!(
                "workspace path is not Unicode: {}",
                directory.display()
            )))
        })?;
        sessions::open_scope(&self.app.r2, &self.app.key, Some(directory), from)
            .await
            .map_err(tool_error)
    }

    async fn search_runtime(&self, repo_key: &str) -> Option<Arc<search_index::Runtime>> {
        if let Some(runtime) = self
            .app
            .search_indexes
            .read()
            .ok()
            .and_then(|indexes| indexes.get(repo_key).cloned())
        {
            return Some(runtime);
        }
        let snapshot = search_index::load_local(&self.app.cache_dir, repo_key).ok()??;
        let runtime = Arc::new(snapshot.into_runtime());
        self.remember_runtime(repo_key, Arc::clone(&runtime));
        Some(runtime)
    }

    fn refresh_if_stale(&self, directory: &Path, repo_key: &str, runtime: &search_index::Runtime) {
        let age = Utc::now()
            .signed_duration_since(runtime.generated_at())
            .num_seconds();
        if age < SEARCH_INDEX_MAX_AGE {
            return;
        }
        let Ok(mut refreshing) = self.app.refreshing.lock() else {
            return;
        };
        if !refreshing.insert(repo_key.to_string()) {
            return;
        }
        drop(refreshing);

        let app = Arc::clone(&self.app);
        let directory = directory.to_path_buf();
        let repo_key = repo_key.to_string();
        tokio::spawn(async move {
            let result = if app.can_write {
                search_index::build_for_directory(
                    &app.r2,
                    &app.key,
                    &app.cache_dir,
                    &directory,
                    true,
                )
                .await
            } else {
                match search_index::load_remote(&app.r2, &app.key, &app.cache_dir, &repo_key).await
                {
                    Ok(Some(snapshot)) => Ok(snapshot),
                    Ok(None) => {
                        search_index::build_for_directory(
                            &app.r2,
                            &app.key,
                            &app.cache_dir,
                            &directory,
                            false,
                        )
                        .await
                    }
                    Err(error) => Err(error),
                }
            };
            match result {
                Ok(snapshot) => {
                    let runtime = Arc::new(snapshot.into_runtime());
                    if let Ok(mut indexes) = app.search_indexes.write() {
                        indexes.insert(repo_key.clone(), runtime);
                    }
                }
                Err(error) => {
                    eprintln!("agent-transcript: search index refresh failed: {error}");
                }
            }
            if let Ok(mut refreshing) = app.refreshing.lock() {
                refreshing.remove(&repo_key);
            }
        });
    }

    fn remember_runtime(&self, repo_key: &str, runtime: Arc<search_index::Runtime>) {
        if let Ok(mut indexes) = self.app.search_indexes.write() {
            indexes.insert(repo_key.to_string(), runtime);
        }
    }
}

struct ResolvedRoot {
    directory: PathBuf,
    origin: String,
}

async fn directory_for_omitted_cwd(peer: &Peer<RoleServer>) -> crate::Result<PathBuf> {
    if client_has_roots(peer) {
        let roots = list_workspace_roots(peer).await?;
        if !roots.is_empty() {
            return directory_from_roots(&roots);
        }
    }
    let process = std::env::current_dir()?;
    match repo_id::origin_of(&process) {
        Ok(_) => Ok(process),
        Err(error) => Err(Error::msg(error.to_string())),
    }
}

fn client_has_roots(peer: &Peer<RoleServer>) -> bool {
    peer.peer_info()
        .is_some_and(|info| info.capabilities.roots.is_some())
}

// Cursor starts this process in the home directory and reports the open
// workspace through roots/list. A root may be a file URI or a bare path.
#[allow(deprecated)]
async fn list_workspace_roots(peer: &Peer<RoleServer>) -> crate::Result<Vec<String>> {
    let listed = peer
        .list_roots()
        .await
        .map_err(|error| Error::msg(format!("listing workspace roots failed: {error}")))?;
    Ok(listed.roots.into_iter().map(|root| root.uri).collect())
}

fn directory_from_roots(uris: &[String]) -> crate::Result<PathBuf> {
    let mut directories = Vec::new();
    let mut resolved = Vec::new();
    for uri in uris {
        let directory = root_directory(uri)?;
        if let Some(origin) = origin_at(&directory)? {
            resolved.push(ResolvedRoot { directory, origin });
        } else {
            directories.push(directory);
        }
    }
    if resolved.is_empty() {
        return Err(Error::msg(format!(
            "workspace roots do not identify a repository: {}",
            directories
                .iter()
                .map(|path| path.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )));
    }
    one_repository(&resolved)
}

fn origin_at(dir: &Path) -> crate::Result<Option<String>> {
    match repo_id::origin_of(dir) {
        Ok(origin) => Ok(Some(origin)),
        Err(OriginError::NoOrigin { .. }) => Ok(None),
        Err(OriginError::GitMissing) => Err(Error::msg("git is not installed")),
        Err(OriginError::Invalid { message }) => Err(Error::msg(message)),
    }
}

fn root_directory(uri: &str) -> crate::Result<PathBuf> {
    let path = if uri.starts_with("file:") {
        let url = Url::parse(uri).map_err(|error| {
            Error::msg(format!("workspace root `{uri}` is not a file URI: {error}"))
        })?;
        url.to_file_path()
            .map_err(|_| Error::msg(format!("workspace root `{uri}` is not a local path")))?
    } else {
        PathBuf::from(uri)
    };
    if !path.is_absolute() {
        return Err(Error::msg(format!(
            "workspace root `{}` is not absolute",
            path.display()
        )));
    }
    Ok(path)
}

fn one_repository(roots: &[ResolvedRoot]) -> crate::Result<PathBuf> {
    let Some(first) = roots.first() else {
        return Err(Error::msg("workspace roots do not identify a repository"));
    };
    let mut origins: Vec<&str> = roots.iter().map(|root| root.origin.as_str()).collect();
    origins.sort_unstable();
    origins.dedup();
    if origins.len() != 1 {
        return Err(Error::msg(format!(
            "workspace roots identify more than one repository: {}",
            origins.join(", ")
        )));
    }
    Ok(first.directory.clone())
}

#[allow(unknown_lints, clippy::unused_async_trait_impl)]
#[tool_handler(router = self.tool_router)]
impl ServerHandler for ArchiveServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(
                Implementation::new("agent-transcript", env!("CARGO_PKG_VERSION"))
                    .with_title("agent transcript archive")
                    .with_description(
                        "Read local and R2 coding-agent transcripts for the current repository",
                    ),
            )
            .with_instructions(
                "Use list_sessions, search_sessions, and read_session. They read this PC's local sessions and the encrypted R2 archive together. cwd is a directory whose git origin selects the repository; omit it to use the client workspace, or the process working directory when the client reports no workspace. Append #5-12 to a session id to read that message range.",
            )
    }
}

pub async fn serve() -> Result<(), String> {
    let config = config::load_config().map_err(|error| error.to_string())?;
    let key = config::load_key().map_err(|error| error.to_string())?;
    let cache_dir = config::cache_dir().map_err(|error| error.to_string())?;
    let server = ArchiveServer::new(App {
        r2: R2::new(&config),
        key,
        cache_dir,
        can_write: config.mode == config::Mode::Readwrite,
        search_indexes: RwLock::new(HashMap::new()),
        refreshing: std::sync::Mutex::new(std::collections::HashSet::new()),
    });
    let service = server
        .serve(stdio())
        .await
        .map_err(|error| format!("starting MCP stdio server: {error}"))?;
    service
        .waiting()
        .await
        .map_err(|error| format!("running MCP stdio server: {error}"))?;
    Ok(())
}

fn query_runtime(
    runtime: Arc<search_index::Runtime>,
    pattern: String,
    from: Option<HarnessId>,
) -> SearchResults {
    let mut query = Query::substring(pattern);
    query.case = Case::Insensitive;
    query.limit = Some(20);
    query.hits_per_doc = Some(3);
    if let Some(harness) = from {
        query.harnesses = Some(vec![harness]);
    }
    let matches = runtime
        .index
        .query(&query)
        .iter()
        .map(|found| SearchMatch {
            session: SessionSummary {
                harness: found.key.harness.to_string(),
                id: found.key.id.clone(),
                timestamp: found.meta.timestamp.to_rfc3339(),
                title: found.meta.title.clone(),
                cwd: found.meta.cwd.clone(),
                git_branch: found.meta.git_branch.clone(),
                model: found.meta.model.clone(),
            },
            score: found.score,
            hits: found.hits.iter().map(SearchHit::from).collect(),
        })
        .collect();
    SearchResults { matches }
}

fn summary(view: &crate::merge::MergedView) -> SessionSummary {
    SessionSummary {
        harness: view.harness.to_string(),
        id: view.session_id.clone(),
        timestamp: view.started_at.to_rfc3339(),
        title: view.title.clone(),
        cwd: view.cwd.clone(),
        git_branch: view.git_branch.clone(),
        model: view.model.clone(),
    }
}

impl From<&Hit> for SearchHit {
    fn from(hit: &Hit) -> Self {
        Self {
            span: hit.span.0.clone(),
            block: hit.block,
            origin: origin_name(hit.origin),
            line: hit.line.clone(),
            score: hit.score,
        }
    }
}

fn origin_name(origin: Origin) -> &'static str {
    match origin {
        Origin::User => "user",
        Origin::Assistant => "assistant",
        Origin::Thinking => "thinking",
        Origin::ToolUse => "tool_use",
        Origin::ToolResult => "tool_result",
        Origin::Meta => "meta",
    }
}

fn parse_from(from: Option<&str>) -> Result<Option<HarnessId>, ErrorData> {
    from.map(str::parse)
        .transpose()
        .map_err(|error| ErrorData::invalid_params(format!("unknown harness: {error}"), None))
}

fn refuse_live(from: Option<HarnessId>) -> Result<(), ErrorData> {
    if matches!(from, Some(HarnessId::ClaudeChat | HarnessId::ChatGpt)) {
        let name = from.map_or("live source", HarnessId::as_str);
        return Err(ErrorData::invalid_params(
            format!("list, search, and read do not enumerate {name}"),
            None,
        ));
    }
    Ok(())
}

fn tool_error(error: Error) -> ErrorData {
    match error {
        Error::Msg(_)
        | Error::Ambiguous { .. }
        | Error::Schema { .. }
        | Error::Format { .. }
        | Error::Crypto { .. } => ErrorData::invalid_params(error.to_string(), None),
        other => ErrorData::internal_error(other.to_string(), None),
    }
}

fn strip_nonstandard_formats(schema: &mut JsonObject) {
    if schema
        .get("format")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|format| !STANDARD_FORMATS.contains(&format))
    {
        schema.remove("format");
    }
    for value in schema.values_mut() {
        strip_nested_formats(value);
    }
}

fn strip_nested_formats(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(object) => strip_nonstandard_formats(object),
        serde_json::Value::Array(items) => items.iter_mut().for_each(strip_nested_formats),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resolved(directory: &str, origin: &str) -> ResolvedRoot {
        ResolvedRoot {
            directory: PathBuf::from(directory),
            origin: origin.into(),
        }
    }

    #[test]
    fn one_workspace_root_selects_that_repository() {
        let roots = [
            resolved(r"D:\repo", "https://example.com/repo"),
            resolved(r"D:\repo\crate", "https://example.com/repo"),
        ];
        assert_eq!(one_repository(&roots).unwrap(), PathBuf::from(r"D:\repo"));
    }

    #[test]
    fn several_workspace_repositories_are_rejected() {
        let roots = [
            resolved(r"D:\a", "https://example.com/a"),
            resolved(r"D:\b", "https://example.com/b"),
        ];
        let error = one_repository(&roots).unwrap_err().to_string();
        assert!(error.contains("https://example.com/a"), "{error}");
        assert!(error.contains("https://example.com/b"), "{error}");
    }

    #[cfg(windows)]
    #[test]
    fn workspace_root_accepts_a_file_uri_or_a_bare_path() {
        assert_eq!(
            root_directory("file:///D:/Develop/repo").unwrap(),
            PathBuf::from(r"D:\Develop\repo")
        );
        assert_eq!(
            root_directory("file:///D:/My%20Repo").unwrap(),
            PathBuf::from(r"D:\My Repo")
        );
        assert_eq!(
            root_directory(r"D:\Develop\repo").unwrap(),
            PathBuf::from(r"D:\Develop\repo")
        );
    }

    #[test]
    fn a_non_file_workspace_root_is_rejected() {
        let error = root_directory("https://example.com/repo")
            .unwrap_err()
            .to_string();
        assert!(error.contains("not absolute"), "{error}");
    }
}
