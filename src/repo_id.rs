use std::collections::HashMap;
use std::path::{Path, PathBuf};

use url::Url;

use crate::error::{Error, Result};

/// Normalize a git remote URL into the repo key used for both local and R2 sessions.
pub fn normalize_remote_url(raw: &str) -> Result<String> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Err(Error::msg("origin URL is empty"));
    }
    if raw.contains("://") {
        normalize_url(raw)
    } else {
        normalize_scp(raw)
    }
}

fn normalize_url(raw: &str) -> Result<String> {
    let url = Url::parse(raw)
        .map_err(|error| Error::msg(format!("origin URL `{raw}` is invalid: {error}")))?;
    match url.scheme() {
        "https" | "http" | "ssh" | "git" => {}
        other => {
            return Err(Error::msg(format!(
                "origin URL `{raw}` uses unsupported scheme `{other}`"
            )))
        }
    }
    let host = url
        .host_str()
        .ok_or_else(|| Error::msg(format!("origin URL `{raw}` has no host")))?
        .to_ascii_lowercase();
    let port = match url.port() {
        Some(22 | 80 | 443) | None => None,
        Some(port) => Some(port),
    };
    finish(&host, port, url.path())
}

fn normalize_scp(raw: &str) -> Result<String> {
    let (host_part, path) = raw
        .split_once(':')
        .ok_or_else(|| Error::msg(format!("origin URL `{raw}` is not a git remote")))?;
    let host = host_part
        .rsplit_once('@')
        .map_or(host_part, |(_, host)| host)
        .to_ascii_lowercase();
    finish(&host, None, path)
}

fn finish(host: &str, port: Option<u16>, path: &str) -> Result<String> {
    let path = path.trim_matches('/');
    let path = path.strip_suffix(".git").unwrap_or(path);
    if host.is_empty() || path.is_empty() || path.contains(' ') {
        return Err(Error::msg(format!(
            "origin URL does not identify a repository (host `{host}`, path `{path}`)"
        )));
    }
    match port {
        Some(port) => Ok(format!("https://{host}:{port}/{path}")),
        None => Ok(format!("https://{host}/{path}")),
    }
}

/// Directory whose origin scopes a query. An omitted cwd is the process directory.
pub fn scope_directory(cwd: Option<&Path>, process_cwd: &Path) -> PathBuf {
    match cwd {
        Some(path) if path.is_absolute() => path.to_path_buf(),
        Some(path) => process_cwd.join(path),
        None => process_cwd.to_path_buf(),
    }
}

#[derive(Debug)]
pub enum OriginError {
    GitMissing,
    NoOrigin { dir: String },
    Invalid { message: String },
}

impl std::fmt::Display for OriginError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::GitMissing => formatter.write_str("git is not installed"),
            Self::NoOrigin { dir } => write!(formatter, "cannot resolve origin of {dir}"),
            Self::Invalid { message } => formatter.write_str(message),
        }
    }
}

pub fn origin_of(dir: &Path) -> std::result::Result<String, OriginError> {
    let output = crate::command::new("git")
        .arg("-C")
        .arg(dir)
        .args(["remote", "get-url", "origin"])
        .output()
        .map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                OriginError::GitMissing
            } else {
                OriginError::Invalid {
                    message: error.to_string(),
                }
            }
        })?;
    if !output.status.success() {
        return Err(OriginError::NoOrigin {
            dir: dir.display().to_string(),
        });
    }
    let url = String::from_utf8(output.stdout).map_err(|_| OriginError::Invalid {
        message: "origin URL is not UTF-8".into(),
    })?;
    let url = url.trim();
    if url.is_empty() {
        return Err(OriginError::NoOrigin {
            dir: dir.display().to_string(),
        });
    }
    normalize_remote_url(url).map_err(|error| OriginError::Invalid {
        message: error.to_string(),
    })
}

#[derive(Debug, Clone)]
pub enum SessionRepo {
    Key(String),
    MissingCwd,
    Unresolved(String),
}

#[derive(Debug, Default)]
pub struct RepoCache {
    by_cwd: HashMap<String, SessionRepo>,
}

impl RepoCache {
    pub fn resolve(&mut self, cwd: Option<&str>) -> Result<SessionRepo> {
        let Some(cwd) = cwd.filter(|cwd| !cwd.is_empty()) else {
            return Ok(SessionRepo::MissingCwd);
        };
        if let Some(resolved) = self.by_cwd.get(cwd) {
            return Ok(resolved.clone());
        }
        let resolved = session_repo(Some(cwd))?;
        self.by_cwd.insert(cwd.to_string(), resolved.clone());
        Ok(resolved)
    }
}

pub fn session_repo(cwd: Option<&str>) -> Result<SessionRepo> {
    let Some(cwd) = cwd.filter(|cwd| !cwd.is_empty()) else {
        return Ok(SessionRepo::MissingCwd);
    };
    let path = Path::new(cwd);
    if !path.is_dir() {
        return Ok(SessionRepo::MissingCwd);
    }
    match origin_of(path) {
        Ok(key) => Ok(SessionRepo::Key(key)),
        Err(OriginError::GitMissing) => Err(Error::msg("git is not installed")),
        Err(error) => Ok(SessionRepo::Unresolved(error.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_scp_https_ssh_and_git_urls() {
        assert_eq!(
            normalize_remote_url("git@github.com:Org/Repo.git").unwrap(),
            "https://github.com/Org/Repo"
        );
        assert_eq!(
            normalize_remote_url("https://user:token@GitHub.com/Org/Repo.git").unwrap(),
            "https://github.com/Org/Repo"
        );
        assert_eq!(
            normalize_remote_url("ssh://git@github.com/Org/Repo.git").unwrap(),
            "https://github.com/Org/Repo"
        );
        assert_eq!(
            normalize_remote_url("git://github.com/Org/Repo.git").unwrap(),
            "https://github.com/Org/Repo"
        );
        assert_eq!(
            normalize_remote_url("http://github.com/Org/Repo.git/").unwrap(),
            "https://github.com/Org/Repo"
        );
        assert_eq!(
            normalize_remote_url("https://example.com:8443/Org/Repo.git").unwrap(),
            "https://example.com:8443/Org/Repo"
        );
    }

    #[test]
    fn rejects_empty_and_non_git_urls() {
        assert!(normalize_remote_url("  ").is_err());
        assert!(normalize_remote_url("file:///srv/repo.git").is_err());
        assert!(normalize_remote_url("not a remote").is_err());
    }

    #[test]
    fn omitted_cwd_uses_the_process_directory() {
        let process = Path::new(r"D:\work\repo");
        assert_eq!(scope_directory(None, process), process);
        assert_eq!(
            scope_directory(Some(Path::new("crate")), process),
            PathBuf::from(r"D:\work\repo\crate")
        );
    }
}
