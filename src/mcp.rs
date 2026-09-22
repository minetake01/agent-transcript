use std::sync::Arc;

use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::{Json, Parameters};
use rmcp::model::{Implementation, JsonObject, ServerCapabilities, ServerInfo};
use rmcp::schemars::JsonSchema;
use rmcp::{
    tool, tool_handler, tool_router, transport::stdio, ErrorData, ServerHandler, ServiceExt,
};
use serde::{Deserialize, Serialize};
use txcript::search::{Case, DocKey, Hit, Index, Origin, Query};
use txcript::HarnessId;

use crate::config;
use crate::error::Error;
use crate::fragment::parse_ref;
use crate::read;
use crate::sessions::{self, find_session, Scope};
use crate::store::R2;

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
    /// Directory whose git origin selects the repository. Omit to use the process working directory.
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
    /// Directory whose git origin selects the repository. Omit to use the process working directory.
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
        description = "List coding-agent sessions for one repository, newest first. Local sessions and the R2 archive are merged; the same harness and session id appear once. `cwd` selects the repository by its git origin, not by the recorded path. Omit `cwd` to use this process's working directory. Omit `from` to include every harness in that repository. `limit` and `offset` page the merged list; `total` is the count before paging.",
        annotations(title = "List sessions", read_only_hint = true)
    )]
    async fn list_sessions(
        &self,
        Parameters(request): Parameters<ListSessionsRequest>,
    ) -> Result<Json<SessionList>, ErrorData> {
        let from = parse_from(request.from.as_deref())?;
        refuse_live(from)?;
        let scope = self.open(request.cwd.as_deref(), from).await?;
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
        description = "Search coding-agent sessions in one repository for a literal substring. Local sessions and the R2 archive are merged first. `cwd` selects the repository by its git origin. Omit `cwd` to use this process's working directory. Omit `from` to search every harness in that repository.",
        annotations(title = "Search sessions", read_only_hint = true)
    )]
    async fn search_sessions(
        &self,
        Parameters(request): Parameters<SearchSessionsRequest>,
    ) -> Result<Json<SearchResults>, ErrorData> {
        let from = parse_from(request.from.as_deref())?;
        refuse_live(from)?;
        let mut scope = self.open(request.cwd.as_deref(), from).await?;
        let mut index = Index::default();
        for view in scope.merged.clone() {
            let transcript = scope
                .transcript(&self.app.r2, &self.app.key, &self.app.cache_dir, &view)
                .await
                .map_err(tool_error)?;
            index.insert(
                DocKey {
                    harness: view.harness,
                    id: view.session_id.clone(),
                    source: None,
                },
                &transcript,
            );
        }
        let mut query = Query::substring(request.pattern);
        query.case = Case::Insensitive;
        query.limit = Some(20);
        query.hits_per_doc = Some(3);
        if let Some(harness) = from {
            query.harnesses = Some(vec![harness]);
        }
        let matches = index
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
        Ok(Json(SearchResults { matches }))
    }

    #[tool(
        description = "Read one session from the merged local and R2 archive as token-optimized text. `id` is a session id, unambiguous prefix, or exact title. Append `#range` (1-based inclusive, for example `abc#5-12`) to read part of it. Reads over the byte budget are refused with suggested ranges. `from` limits the harness. The repository is this process's working directory.",
        annotations(title = "Read session", read_only_hint = true)
    )]
    async fn read_session(
        &self,
        Parameters(request): Parameters<ReadSessionRequest>,
    ) -> Result<String, ErrorData> {
        let from = parse_from(request.from.as_deref())?;
        refuse_live(from)?;
        let mut scope = self.open(None, from).await?;
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
    async fn open(&self, cwd: Option<&str>, from: Option<HarnessId>) -> Result<Scope, ErrorData> {
        sessions::open_scope(&self.app.r2, &self.app.key, cwd, from)
            .await
            .map_err(tool_error)
    }
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
                "Use list_sessions, search_sessions, and read_session. They read this PC's local sessions and the encrypted R2 archive together. cwd is a directory whose git origin selects the repository; omit it to use the process working directory. Append #5-12 to a session id to read that message range.",
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
