use chrono::{DateTime, Utc};
use txcript::HarnessId;

use crate::error::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Local,
    Remote,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Freshness {
    pub updated_at: Option<DateTime<Utc>>,
    pub last_message_at: Option<DateTime<Utc>>,
    pub message_count: u64,
    pub content_hash: String,
}

#[derive(Debug, Clone)]
pub struct LocalView {
    pub harness: HarnessId,
    pub session_id: String,
    pub repo_key: Option<String>,
    pub freshness: Freshness,
    pub started_at: DateTime<Utc>,
    pub title: Option<String>,
    pub cwd: Option<String>,
    pub git_branch: Option<String>,
    pub model: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RemoteView {
    pub harness: HarnessId,
    pub session_id: String,
    pub repo_key: String,
    pub freshness: Freshness,
    pub object_key: String,
    pub started_at: DateTime<Utc>,
    pub title: Option<String>,
    pub cwd: Option<String>,
    pub git_branch: Option<String>,
    pub model: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pick {
    Local,
    Remote,
    Ambiguous,
}

#[derive(Debug, Clone)]
pub struct MergedView {
    pub harness: HarnessId,
    pub session_id: String,
    pub pick: Pick,
    pub started_at: DateTime<Utc>,
    pub title: Option<String>,
    pub cwd: Option<String>,
    pub git_branch: Option<String>,
    pub model: Option<String>,
    pub sort_at: DateTime<Utc>,
    pub object_key: Option<String>,
    pub content_hash: String,
}

pub fn prefer(left: &Freshness, right: &Freshness) -> Result<Side> {
    let left_time = left.updated_at.or(left.last_message_at);
    let right_time = right.updated_at.or(right.last_message_at);
    match (left_time, right_time) {
        (Some(left_time), Some(right_time)) if left_time != right_time => {
            return Ok(if left_time > right_time {
                Side::Local
            } else {
                Side::Remote
            });
        }
        (Some(_), None) => return Ok(Side::Local),
        (None, Some(_)) => return Ok(Side::Remote),
        _ => {}
    }
    if left.message_count != right.message_count {
        return Ok(if left.message_count > right.message_count {
            Side::Local
        } else {
            Side::Remote
        });
    }
    if left.content_hash == right.content_hash {
        return Ok(Side::Local);
    }
    Err(Error::Ambiguous {
        harness: String::new(),
        session_id: String::new(),
    })
}

pub fn select(
    repo_key: &str,
    from: Option<HarnessId>,
    locals: &[LocalView],
    remotes: &[RemoteView],
) -> Result<Vec<MergedView>> {
    let mut merged = Vec::new();
    let mut used_remote = vec![false; remotes.len()];

    for local in locals.iter().filter(|session| {
        matches_scope(session.harness, session.repo_key.as_deref(), repo_key, from)
    }) {
        let remote_index = remotes.iter().position(|remote| {
            remote.harness == local.harness
                && remote.session_id == local.session_id
                && remote.repo_key == repo_key
        });
        if let Some(index) = remote_index {
            used_remote[index] = true;
            merged.push(merge_pair(local, &remotes[index])?);
        } else {
            merged.push(from_local(local));
        }
    }

    for (remote, used) in remotes.iter().zip(used_remote) {
        if used || !matches_scope(remote.harness, Some(&remote.repo_key), repo_key, from) {
            continue;
        }
        merged.push(from_remote(remote));
    }

    merged.sort_by(|left, right| {
        right
            .sort_at
            .cmp(&left.sort_at)
            .then(left.harness.as_str().cmp(right.harness.as_str()))
            .then(left.session_id.cmp(&right.session_id))
    });
    Ok(merged)
}

fn matches_scope(
    harness: HarnessId,
    session_repo: Option<&str>,
    repo_key: &str,
    from: Option<HarnessId>,
) -> bool {
    session_repo == Some(repo_key) && from.is_none_or(|selected| selected == harness)
}

fn merge_pair(local: &LocalView, remote: &RemoteView) -> Result<MergedView> {
    match prefer(&local.freshness, &remote.freshness) {
        Ok(Side::Local) => Ok(from_local(local)),
        Ok(Side::Remote) => Ok(from_remote(remote)),
        Err(_) => Ok(MergedView {
            harness: local.harness,
            session_id: local.session_id.clone(),
            pick: Pick::Ambiguous,
            started_at: local.started_at,
            title: local.title.clone(),
            cwd: local.cwd.clone(),
            git_branch: local.git_branch.clone(),
            model: local.model.clone(),
            sort_at: sort_time(&local.freshness, local.started_at),
            object_key: Some(remote.object_key.clone()),
            content_hash: String::new(),
        }),
    }
}

fn from_local(local: &LocalView) -> MergedView {
    MergedView {
        harness: local.harness,
        session_id: local.session_id.clone(),
        pick: Pick::Local,
        started_at: local.started_at,
        title: local.title.clone(),
        cwd: local.cwd.clone(),
        git_branch: local.git_branch.clone(),
        model: local.model.clone(),
        sort_at: sort_time(&local.freshness, local.started_at),
        object_key: None,
        content_hash: local.freshness.content_hash.clone(),
    }
}

fn from_remote(remote: &RemoteView) -> MergedView {
    MergedView {
        harness: remote.harness,
        session_id: remote.session_id.clone(),
        pick: Pick::Remote,
        started_at: remote.started_at,
        title: remote.title.clone(),
        cwd: remote.cwd.clone(),
        git_branch: remote.git_branch.clone(),
        model: remote.model.clone(),
        sort_at: sort_time(&remote.freshness, remote.started_at),
        object_key: Some(remote.object_key.clone()),
        content_hash: remote.freshness.content_hash.clone(),
    }
}

fn sort_time(freshness: &Freshness, started_at: DateTime<Utc>) -> DateTime<Utc> {
    freshness
        .updated_at
        .or(freshness.last_message_at)
        .unwrap_or(started_at)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(secs, 0).unwrap()
    }

    fn fresh(updated: Option<i64>, last: Option<i64>, count: u64, hash: &str) -> Freshness {
        Freshness {
            updated_at: updated.map(at),
            last_message_at: last.map(at),
            message_count: count,
            content_hash: hash.into(),
        }
    }

    fn local(repo: Option<&str>, id: &str, freshness: Freshness) -> LocalView {
        LocalView {
            harness: HarnessId::ClaudeCode,
            session_id: id.into(),
            repo_key: repo.map(str::to_string),
            freshness,
            started_at: at(1),
            title: Some(format!("title-{id}")),
            cwd: Some(r"C:\other\path".into()),
            git_branch: None,
            model: None,
        }
    }

    fn remote(repo: &str, id: &str, freshness: Freshness) -> RemoteView {
        RemoteView {
            harness: HarnessId::ClaudeCode,
            session_id: id.into(),
            repo_key: repo.into(),
            freshness,
            object_key: format!("obj-{id}"),
            started_at: at(1),
            title: Some(format!("remote-{id}")),
            cwd: Some("/home/other/repo".into()),
            git_branch: None,
            model: None,
        }
    }

    #[test]
    fn newer_updated_at_supplies_the_body() {
        let chosen = prefer(
            &fresh(Some(10), None, 1, "local"),
            &fresh(Some(4), None, 99, "remote"),
        )
        .unwrap();
        assert_eq!(chosen, Side::Local);
    }

    #[test]
    fn missing_updated_at_uses_the_last_message_time_then_count() {
        let by_message = prefer(
            &fresh(None, Some(2), 5, "local"),
            &fresh(None, Some(8), 1, "remote"),
        )
        .unwrap();
        assert_eq!(by_message, Side::Remote);

        let by_count = prefer(
            &fresh(Some(3), Some(3), 9, "local"),
            &fresh(Some(3), Some(1), 2, "remote"),
        )
        .unwrap();
        assert_eq!(by_count, Side::Local);
    }

    #[test]
    fn identical_times_and_counts_with_different_hashes_are_ambiguous() {
        let error = prefer(
            &fresh(Some(3), Some(3), 4, "local"),
            &fresh(Some(3), Some(3), 4, "remote"),
        )
        .unwrap_err();
        assert!(matches!(error, Error::Ambiguous { .. }));
    }

    #[test]
    fn select_uses_repo_key_and_collapses_one_session() {
        let repo = "https://github.com/Org/Repo";
        let other = "https://github.com/Org/Other";
        let locals = vec![
            local(Some(repo), "same", fresh(Some(10), None, 1, "new")),
            local(
                Some(other),
                "same-path-different-repo",
                fresh(Some(10), None, 1, "x"),
            ),
            local(None, "unscoped", fresh(Some(10), None, 1, "y")),
        ];
        let remotes = vec![
            remote(repo, "same", fresh(Some(2), None, 8, "old")),
            remote(repo, "only-remote", fresh(Some(1), None, 1, "r")),
            remote(other, "elsewhere", fresh(Some(9), None, 1, "z")),
        ];
        let merged = select(repo, None, &locals, &remotes).unwrap();
        assert_eq!(merged.len(), 2);
        let same = merged
            .iter()
            .find(|session| session.session_id == "same")
            .unwrap();
        assert_eq!(same.pick, Pick::Local);
        assert!(merged
            .iter()
            .any(|session| session.session_id == "only-remote"));
        assert!(merged
            .iter()
            .all(|session| session.session_id != "elsewhere"));
        assert!(merged
            .iter()
            .all(|session| session.session_id != "same-path-different-repo"));
    }
}
