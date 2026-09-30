use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use chrono::{DateTime, Utc};
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::{Json, Parameters};
use rmcp::model::{Implementation, JsonObject, ServerCapabilities, ServerConfig};
use rmcp::schemars::JsonSchema;
use rmcp::{
    tool, tool_handler, tool_router, transport::stdio, ErrorData, Peer, RoleServer, ServerHandler,
    ServiceExt,
};
use serde::{Deserialize, Serialize};
use txcript::search::{Case, DocKey, Extracted, Hit, Origin, Query};
use txcript::HarnessId;

use crate::catalog::{Catalog, CATALOG_KEY};
use crate::config;
use crate::error::Error;
use crate::local_state::LocalStore;
use crate::merge::MergedView;
use crate::read;
use crate::remote;
use crate::repo_id::{self, RepoResolution};
use crate::search_index;
use crate::sessions::{self, find_session};
use crate::sources;
use crate::store::R2;

/// Cached catalogs may lag the bucket by this much; an expired cache is
/// re-validated with a HEAD before its first use.
const CATALOG_MAX_AGE: Duration = Duration::from_secs(300);
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
    /// Directory whose git origin selects the repository. Omit to use the client workspace, or the directory the server was launched from when the client reports no workspace.
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
    /// Directory whose git origin selects the repository. Omit to use the client workspace, or the directory the server was launched from when the client reports no workspace.
    cwd: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ReadSessionRequest {
    /// Session id, unambiguous prefix, or exact title, with an optional `#range` (`abc#5-12`).
    id: String,
    /// Only look in this harness. Omit to look across every harness in the repository.
    from: Option<String>,
    /// Directory whose git origin selects the repository. Omit to use the client workspace, or the directory the server was launched from when the client reports no workspace.
    cwd: Option<String>,
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
    /// Directory the process was launched in — the working-directory
    /// fallback for an omitted `cwd`. The process itself re-anchors to the
    /// cache directory at startup so it never pins the launcher's directory.
    launch_dir: PathBuf,
    can_write: bool,
    /// Persistent per-source session records; `diff()` refreshes them with a
    /// stat-only scan on every request. Also owns the shared directory →
    /// repo-key resolution cache.
    local: Mutex<LocalStore>,
    /// The decrypted catalog, refreshed at most every `CATALOG_MAX_AGE` via
    /// an ETag probe.
    catalog: RwLock<CatalogSlot>,
    catalog_sync: tokio::sync::Mutex<()>,
    /// Per-repository search runtimes, incrementally synced per request.
    indexes: RwLock<HashMap<String, Arc<RwLock<search_index::Runtime>>>>,
}

struct CatalogSlot {
    catalog: Catalog,
    etag: Option<String>,
    fetched_at: DateTime<Utc>,
}

/// On-disk catalog cache so a restarted server does not fetch on the first
/// request either — it probes by ETag like any other expired entry.
#[derive(Serialize, Deserialize)]
struct CatalogCache {
    catalog: Catalog,
    etag: Option<String>,
    fetched_at: DateTime<Utc>,
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
        description = "List coding-agent sessions for one repository, newest first. Local sessions and the R2 archive are merged; the same harness and session id appear once. `cwd` selects the repository by its git origin, not by the recorded path. Omit `cwd` to use the client workspace, or the directory the server was launched from when the client reports no workspace. Omit `from` to include every harness in that repository. `limit` and `offset` page the merged list; `total` is the count before paging.",
        annotations(title = "List sessions", read_only_hint = true)
    )]
    async fn list_sessions(
        &self,
        Parameters(request): Parameters<ListSessionsRequest>,
        peer: Peer<RoleServer>,
    ) -> Result<Json<SessionList>, ErrorData> {
        let from = parse_from(request.from.as_deref())?;
        refuse_live(from)?;
        let (_, merged) = self.merged(request.cwd.as_deref(), from, &peer).await?;
        let total = merged.len();
        let offset = request.offset.unwrap_or(0).min(total);
        let sessions = merged
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
        description = "Search coding-agent sessions in one repository for a literal substring. Local sessions and the R2 archive are merged first. `cwd` selects the repository by its git origin. Omit `cwd` to use the client workspace, or the directory the server was launched from when the client reports no workspace. Omit `from` to search every harness in that repository.",
        annotations(title = "Search sessions", read_only_hint = true)
    )]
    async fn search_sessions(
        &self,
        Parameters(request): Parameters<SearchSessionsRequest>,
        peer: Peer<RoleServer>,
    ) -> Result<Json<SearchResults>, ErrorData> {
        let from = parse_from(request.from.as_deref())?;
        refuse_live(from)?;
        // Always merge the whole repository even when this request selects a
        // single harness — the durable index must cover every harness or a
        // filtered first query would poison it for later unfiltered searches.
        let (repo_key, merged) = self.merged(request.cwd.as_deref(), None, &peer).await?;
        let runtime = self
            .sync_runtime(&repo_key, &merged)
            .await
            .map_err(tool_error)?;
        Ok(Json(query_runtime(runtime, request.pattern, from)))
    }

    #[tool(
        description = "Read one session from the merged local and R2 archive as token-optimized text. `id` is a session id, unambiguous prefix, or exact title. Append `#range` (1-based inclusive, for example `abc#5-12`) to read part of it. Reads over the byte budget are refused with suggested ranges. `from` limits the harness. `cwd` selects the repository by its git origin. Omit `cwd` to use the client workspace, or the directory the server was launched from when the client reports no workspace.",
        annotations(title = "Read session", read_only_hint = true)
    )]
    async fn read_session(
        &self,
        Parameters(request): Parameters<ReadSessionRequest>,
        peer: Peer<RoleServer>,
    ) -> Result<String, ErrorData> {
        let from = parse_from(request.from.as_deref())?;
        refuse_live(from)?;
        let (_, merged) = self.merged(request.cwd.as_deref(), from, &peer).await?;
        let (view, matched, range) = find_session(&merged, &request.id).map_err(tool_error)?;
        let label = if range.is_some() {
            matched
        } else {
            &view.session_id
        };
        let transcript =
            sessions::transcript(&self.app.r2, &self.app.key, &self.app.cache_dir, view)
                .await
                .map_err(tool_error)?;
        read::render(label, &transcript, range.as_ref())
            .map_err(|error| ErrorData::invalid_params(error, None))
    }
}

impl ArchiveServer {
    /// Fresh merged views for one request: stat-scanned local records plus
    /// the (at most `CATALOG_MAX_AGE` stale) catalog.
    async fn merged(
        &self,
        cwd: Option<&str>,
        from: Option<HarnessId>,
        peer: &Peer<RoleServer>,
    ) -> Result<(String, Vec<MergedView>), ErrorData> {
        let repo_key = self.request_repo_key(cwd, peer).await?;
        self.ensure_catalog().await?;

        let app = Arc::clone(&self.app);
        let locals = tokio::task::spawn_blocking(move || -> crate::Result<Vec<_>> {
            let mut local = app
                .local
                .lock()
                .map_err(|_| Error::msg("local state lock poisoned"))?;
            local.diff()?;
            let views = sessions::local_views(&local);
            if let Err(error) = local.save_if_dirty() {
                eprintln!("agent-transcript: saving local state: {error}");
            }
            Ok(views)
        })
        .await
        .map_err(|error| tool_error(Error::msg(format!("scanning local sessions: {error}"))))?
        .map_err(tool_error)?;

        let catalog = self
            .app
            .catalog
            .read()
            .map(|slot| slot.catalog.clone())
            .unwrap_or_else(|_| Catalog::empty());
        let merged = sessions::merged(&repo_key, from, &locals, &catalog).map_err(tool_error)?;
        Ok((repo_key, merged))
    }

    /// The catalog cache is fresh enough to serve without a remote call.
    fn catalog_fresh(&self) -> bool {
        self.app.catalog.read().is_ok_and(|slot| {
            Utc::now()
                .signed_duration_since(slot.fetched_at)
                .to_std()
                .is_ok_and(|age| age < CATALOG_MAX_AGE)
        })
    }

    /// Refresh the cached catalog. Fresh entries return immediately; expired
    /// entries get one ETag probe and are fetched only when it changed.
    async fn ensure_catalog(&self) -> Result<(), ErrorData> {
        if self.catalog_fresh() {
            return Ok(());
        }
        let _guard = self.app.catalog_sync.lock().await;
        if self.catalog_fresh() {
            return Ok(());
        }
        let stale_etag = self
            .app
            .catalog
            .read()
            .ok()
            .and_then(|slot| slot.etag.clone());
        match self.app.r2.head_etag(CATALOG_KEY).await {
            Ok(etag) if etag == stale_etag => {
                if let Ok(mut slot) = self.app.catalog.write() {
                    slot.fetched_at = Utc::now();
                }
                self.persist_catalog_cache();
                return Ok(());
            }
            Err(error) => {
                // A probe failure with an expired-but-present cache still
                // serves the cache — correctness of the result is unchanged,
                // it is merely older than intended.
                eprintln!("agent-transcript: catalog probe failed: {error}");
                return Ok(());
            }
            _ => {}
        }
        let (catalog, etag) = remote::load_catalog(&self.app.r2, &self.app.key)
            .await
            .map_err(tool_error)?;
        if let Ok(mut slot) = self.app.catalog.write() {
            *slot = CatalogSlot {
                catalog,
                etag,
                fetched_at: Utc::now(),
            };
        }
        self.persist_catalog_cache();
        Ok(())
    }

    fn persist_catalog_cache(&self) {
        let Ok(slot) = self.app.catalog.read() else {
            return;
        };
        let cache = CatalogCache {
            catalog: slot.catalog.clone(),
            etag: slot.etag.clone(),
            fetched_at: slot.fetched_at,
        };
        let path = self.app.cache_dir.join("catalog.json");
        let result = serde_json::to_vec(&cache)
            .map_err(Error::from)
            .and_then(|bytes| crate::fsutil::atomic_write(&path, &bytes));
        if let Err(error) = result {
            eprintln!("agent-transcript: caching catalog: {error}");
        }
    }

    /// The repository a request addresses: the explicit `cwd`, the client
    /// workspace roots, or the launch directory — resolved through the
    /// shared persistent cache so `git` runs at most once per directory per
    /// retry window.
    async fn request_repo_key(
        &self,
        cwd: Option<&str>,
        peer: &Peer<RoleServer>,
    ) -> Result<String, ErrorData> {
        match cwd {
            // The same normalization the `index` command applies: file URIs,
            // `/d:/…` forms, and relative paths anchored at the launch dir.
            Some(cwd) => self.repo_key_for(&repo_id::scope_directory(
                Some(Path::new(cwd)),
                &self.app.launch_dir,
            )),
            None => self.omitted_repo_key(peer).await,
        }
    }

    /// The repo key for one directory, through the persistent cache.
    fn repo_key_for(&self, directory: &Path) -> Result<String, ErrorData> {
        let mut local = self
            .app
            .local
            .lock()
            .map_err(|_| tool_error(Error::msg("local state lock poisoned")))?;
        let resolution = local.resolve_directory(directory);
        if let Err(error) = local.save_if_dirty() {
            eprintln!("agent-transcript: saving local state: {error}");
        }
        resolution.into_key(directory).map_err(tool_error)
    }

    /// An omitted `cwd` selects the repository from the client's workspace
    /// roots; without roots the launch directory is the answer — a missing
    /// or malformed origin surfaces from the resolution itself.
    async fn omitted_repo_key(&self, peer: &Peer<RoleServer>) -> Result<String, ErrorData> {
        if client_has_roots(peer) {
            let roots = list_workspace_roots(peer).await.map_err(tool_error)?;
            if !roots.is_empty() {
                return self.repo_key_from_roots(&roots);
            }
        }
        self.repo_key_for(&self.app.launch_dir)
    }

    /// The single repository a workspace's roots identify. Roots that
    /// resolve to no repository are tolerated; roots that resolve to
    /// different repositories are rejected.
    fn repo_key_from_roots(&self, uris: &[String]) -> Result<String, ErrorData> {
        let mut resolved = Vec::new();
        let mut unresolved = Vec::new();
        {
            let mut local = self
                .app
                .local
                .lock()
                .map_err(|_| tool_error(Error::msg("local state lock poisoned")))?;
            for uri in uris {
                let directory = root_directory(uri).map_err(tool_error)?;
                match local.resolve_directory(&directory) {
                    RepoResolution::Key(key) => resolved.push(key),
                    RepoResolution::NoOrigin => unresolved.push(directory),
                    RepoResolution::Failed(error) => {
                        return Err(tool_error(Error::msg(error)));
                    }
                }
            }
            if let Err(error) = local.save_if_dirty() {
                eprintln!("agent-transcript: saving local state: {error}");
            }
        }
        one_repository_key(resolved, &unresolved).map_err(tool_error)
    }

    /// The search runtime for a repository — the in-memory instance, the
    /// local snapshot, a bounded remote fetch, or an empty runtime the sync
    /// fills on first use.
    async fn runtime_for(
        &self,
        repo_key: &str,
    ) -> crate::Result<Arc<RwLock<search_index::Runtime>>> {
        if let Some(runtime) = self
            .app
            .indexes
            .read()
            .ok()
            .and_then(|indexes| indexes.get(repo_key).cloned())
        {
            return Ok(runtime);
        }
        if let Some(snapshot) = search_index::load_local(&self.app.cache_dir, repo_key)? {
            let runtime = Arc::new(RwLock::new(snapshot.into_runtime()));
            self.remember_runtime(repo_key, Arc::clone(&runtime));
            return Ok(runtime);
        }
        // Bounded remote fetch: a slow network must not turn a local query
        // into an unbounded wait; the incremental sync below fills whatever
        // the remote copy lacks.
        let remote = tokio::time::timeout(
            Duration::from_millis(900),
            search_index::load_remote(&self.app.r2, &self.app.key, &self.app.cache_dir, repo_key),
        )
        .await;
        let runtime = match remote {
            Ok(Ok(Some(snapshot))) => snapshot.into_runtime(),
            _ => search_index::Runtime::empty(repo_key),
        };
        let runtime = Arc::new(RwLock::new(runtime));
        self.remember_runtime(repo_key, Arc::clone(&runtime));
        Ok(runtime)
    }

    /// Bring the repository's search runtime in line with the merged views:
    /// only new, changed, and removed documents are touched. Changes that
    /// fail to load keep their previous document rather than erroring the
    /// whole query.
    async fn sync_runtime(
        &self,
        repo_key: &str,
        merged: &[MergedView],
    ) -> crate::Result<Arc<RwLock<search_index::Runtime>>> {
        let runtime = self.runtime_for(repo_key).await?;

        let wanted: HashMap<DocKey, (String, &MergedView)> = merged
            .iter()
            .filter(|view| view.pick != crate::merge::Pick::Ambiguous)
            .map(|view| {
                (
                    DocKey {
                        harness: view.harness,
                        id: view.session_id.clone(),
                        source: None,
                    },
                    (sessions::fingerprint_of(view), view),
                )
            })
            .collect();

        let (removes, changes) = {
            let Ok(runtime) = runtime.read() else {
                return Err(Error::msg("search index lock poisoned"));
            };
            let removes: Vec<DocKey> = runtime
                .keys()
                .filter(|key| !wanted.contains_key(*key))
                .cloned()
                .collect();
            let changes: Vec<(DocKey, String, MergedView)> = wanted
                .iter()
                .filter(|(key, (fingerprint, _))| {
                    runtime.fingerprint(key) != Some(fingerprint.as_str())
                })
                .map(|(key, (fingerprint, view))| {
                    (key.clone(), fingerprint.clone(), (*view).clone())
                })
                .collect();
            (removes, changes)
        };

        let mut docs = Vec::with_capacity(changes.len());
        for (key, fingerprint, view) in changes {
            match sessions::transcript(&self.app.r2, &self.app.key, &self.app.cache_dir, &view)
                .await
            {
                Ok(transcript) => {
                    docs.push((key.clone(), fingerprint, Extracted::new(key, &transcript)))
                }
                Err(error) => {
                    eprintln!(
                        "agent-transcript: indexing {} {}: {error}",
                        view.harness, view.session_id
                    );
                }
            }
        }

        let published = {
            let Ok(mut runtime) = runtime.write() else {
                return Err(Error::msg("search index lock poisoned"));
            };
            for key in removes {
                runtime.remove(&key);
            }
            for (key, fingerprint, extracted) in docs {
                runtime.upsert(key, fingerprint, extracted);
            }
            match runtime.persist(&self.app.cache_dir) {
                Ok(wrote) => (wrote && self.app.can_write).then(|| {
                    runtime
                        .to_snapshot()
                        .and_then(|snapshot| snapshot.to_bytes())
                }),
                Err(error) => {
                    eprintln!("agent-transcript: persisting search index: {error}");
                    None
                }
            }
        };
        // Share the refreshed snapshot so read-only PCs download it instead
        // of rebuilding — publish failures never fail the query.
        if let Some(Ok(plain)) = published {
            let r2 = self.app.r2.clone();
            let key = self.app.key;
            let repo_key = repo_key.to_string();
            tokio::spawn(async move {
                if let Err(error) =
                    search_index::publish_remote_bytes(&r2, &key, &repo_key, &plain).await
                {
                    eprintln!("agent-transcript: publishing search index failed: {error}");
                }
            });
        }
        Ok(runtime)
    }

    fn remember_runtime(&self, repo_key: &str, runtime: Arc<RwLock<search_index::Runtime>>) {
        if let Ok(mut indexes) = self.app.indexes.write() {
            indexes.insert(repo_key.to_string(), runtime);
        }
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

fn root_directory(uri: &str) -> crate::Result<PathBuf> {
    // Shared cwd normalization: file URIs and `/d:/…` forms become paths.
    let path = repo_id::normalize_cwd(uri);
    if path.as_os_str().is_empty() || !path.is_absolute() {
        return Err(Error::msg(format!(
            "workspace root `{uri}` is not absolute"
        )));
    }
    Ok(path)
}

/// The single repository a workspace's roots identify, or why the request
/// cannot pick one. Roots that resolve to no repository are tolerated;
/// roots that resolve to different repositories are rejected.
fn one_repository_key(mut resolved: Vec<String>, unresolved: &[PathBuf]) -> crate::Result<String> {
    if resolved.is_empty() {
        return Err(Error::msg(format!(
            "workspace roots do not identify a repository: {}",
            unresolved
                .iter()
                .map(|path| path.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )));
    }
    resolved.sort_unstable();
    resolved.dedup();
    if resolved.len() != 1 {
        return Err(Error::msg(format!(
            "workspace roots identify more than one repository: {}",
            resolved.join(", ")
        )));
    }
    Ok(resolved.into_iter().next().unwrap())
}

#[allow(unknown_lints, clippy::unused_async_trait_impl)]
#[tool_handler(router = self.tool_router)]
impl ServerHandler for ArchiveServer {
    fn get_info(&self) -> ServerConfig {
        // rmcp 3 dual-era: default discover + initialize; do not narrow supported versions.
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(
                Implementation::new("agent-transcript", env!("CARGO_PKG_VERSION"))
                    .with_title("agent transcript archive")
                    .with_description(
                        "Read local and R2 coding-agent transcripts for the current repository",
                    ),
            )
            .with_instructions(
                "Use list_sessions, search_sessions, and read_session. They read this PC's local sessions and the encrypted R2 archive together. cwd is a directory whose git origin selects the repository; omit it to use the client workspace, or the directory the server was launched from when the client reports no workspace. Append #5-12 to a session id to read that message range.",
            )
    }
}

pub async fn serve(launch_dir: &Path) -> Result<(), String> {
    let config = config::load_config().map_err(|error| error.to_string())?;
    let key = config::load_key().map_err(|error| error.to_string())?;
    let cache_dir = config::cache_dir().map_err(|error| error.to_string())?;
    let catalog = std::fs::read(cache_dir.join("catalog.json"))
        .ok()
        .and_then(|bytes| serde_json::from_slice::<CatalogCache>(&bytes).ok())
        .map(|cache| CatalogSlot {
            catalog: cache.catalog,
            etag: cache.etag,
            fetched_at: cache.fetched_at,
        })
        .unwrap_or_else(|| CatalogSlot {
            catalog: Catalog::empty(),
            etag: None,
            fetched_at: DateTime::<Utc>::MIN_UTC,
        });
    let server = ArchiveServer::new(App {
        r2: R2::new(&config),
        key,
        cache_dir: cache_dir.clone(),
        launch_dir: launch_dir.to_path_buf(),
        can_write: config.can_write(),
        local: Mutex::new(LocalStore::load(&cache_dir)),
        catalog: RwLock::new(catalog),
        catalog_sync: tokio::sync::Mutex::new(()),
        indexes: RwLock::new(HashMap::new()),
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
    runtime: Arc<RwLock<search_index::Runtime>>,
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
    let Ok(runtime) = runtime.read() else {
        return SearchResults { matches: vec![] };
    };
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
        timestamp: view.info.started_at.to_rfc3339(),
        title: view.info.title.clone(),
        cwd: view.info.cwd.clone(),
        git_branch: view.info.git_branch.clone(),
        model: view.info.model.clone(),
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
    if let Some(harness) = from {
        if !sources::is_local(harness) {
            return Err(ErrorData::invalid_params(
                format!(
                    "list, search, and read do not enumerate {}",
                    harness.as_str()
                ),
                None,
            ));
        }
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

    #[test]
    fn one_workspace_repository_is_selected() {
        let key = one_repository_key(
            vec![
                "https://example.com/repo".into(),
                "https://example.com/repo".into(),
            ],
            &[],
        )
        .unwrap();
        assert_eq!(key, "https://example.com/repo");
    }

    #[test]
    fn several_workspace_repositories_are_rejected() {
        let error = one_repository_key(
            vec![
                "https://example.com/a".into(),
                "https://example.com/b".into(),
            ],
            &[],
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("https://example.com/a"), "{error}");
        assert!(error.contains("https://example.com/b"), "{error}");
    }

    #[test]
    fn workspace_roots_without_repositories_are_rejected() {
        let error = one_repository_key(vec![], &[PathBuf::from("/plain")])
            .unwrap_err()
            .to_string();
        assert!(error.contains("/plain"), "{error}");
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

    #[test]
    fn read_session_request_deserializes_with_optional_cwd() {
        let json_with_cwd = r#"{"id":"test-123","cwd":"/path/to/repo"}"#;
        let req: ReadSessionRequest = serde_json::from_str(json_with_cwd).unwrap();
        assert_eq!(req.id, "test-123");
        assert_eq!(req.cwd.as_deref(), Some("/path/to/repo"));
        assert_eq!(req.from, None);

        let json_without_cwd = r#"{"id":"test-456"}"#;
        let req: ReadSessionRequest = serde_json::from_str(json_without_cwd).unwrap();
        assert_eq!(req.id, "test-456");
        assert_eq!(req.cwd, None);
    }
}
