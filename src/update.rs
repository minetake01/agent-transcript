//! Explicit, release-based self update. R2 credentials are never used here.
use std::fs;
use std::io::Write;
use std::path::Path;
use std::time::Duration;

use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::error::{Error, Result};

const REPO: &str = "minetake01/agent-transcript";
const MAX_BINARY: usize = 100 * 1024 * 1024;

#[derive(Deserialize)]
struct Release {
    tag_name: String,
    assets: Vec<Asset>,
}

#[derive(Deserialize)]
struct Asset {
    name: String,
    browser_download_url: String,
}

fn asset_name() -> Result<&'static str> {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("windows", "x86_64") => Ok("agent-transcript-x86_64-pc-windows-msvc.exe"),
        ("linux", "x86_64") => Ok("agent-transcript-x86_64-unknown-linux-gnu"),
        ("macos", "x86_64") => Ok("agent-transcript-x86_64-apple-darwin"),
        ("macos", "aarch64") => Ok("agent-transcript-aarch64-apple-darwin"),
        _ => Err(Error::msg("no release binary for this OS/architecture")),
    }
}

fn newer_version(tag: &str, current: &str) -> Result<Option<semver::Version>> {
    let remote = semver::Version::parse(tag.strip_prefix('v').unwrap_or(tag))
        .map_err(|_| Error::msg(format!("invalid release tag: {tag}")))?;
    let local = semver::Version::parse(current).map_err(|e| Error::msg(e.to_string()))?;
    Ok((remote > local).then_some(remote))
}

fn asset_url<'a>(release: &'a Release, name: &str) -> Result<&'a str> {
    let asset = release
        .assets
        .iter()
        .find(|asset| asset.name == name)
        .ok_or_else(|| Error::msg(format!("release {} has no {name}", release.tag_name)))?;
    // Never follow an arbitrary URL provided by the API response. The request
    // may redirect to GitHub's release-asset CDN, but starts on this repository.
    let expected = format!(
        "https://github.com/{REPO}/releases/download/{}/{}",
        release.tag_name, name
    );
    if asset.browser_download_url != expected {
        return Err(Error::msg(format!("unexpected release URL for {name}")));
    }
    Ok(&asset.browser_download_url)
}

fn verify(binary: &[u8], checksum: &str, name: &str) -> Result<()> {
    let lines: Vec<&str> = checksum.lines().collect();
    if lines.len() != 1 {
        return Err(Error::msg("invalid SHA-256 checksum file"));
    }
    let parts: Vec<&str> = lines[0].split_whitespace().collect();
    if parts.len() != 2
        || parts[1].trim_start_matches('*') != name
        || parts[0].len() != 64
        || !parts[0].bytes().all(|b| b.is_ascii_hexdigit())
    {
        return Err(Error::msg("invalid SHA-256 checksum file"));
    }
    let actual = hex::encode(Sha256::digest(binary));
    if !actual.eq_ignore_ascii_case(parts[0]) {
        return Err(Error::msg("release binary failed SHA-256 verification"));
    }
    Ok(())
}

async fn download(client: &reqwest::Client, url: &str, limit: usize) -> Result<Vec<u8>> {
    let mut response = client
        .get(url)
        .send()
        .await
        .and_then(reqwest::Response::error_for_status)
        .map_err(|e| Error::msg(format!("download failed: {e}")))?;
    if response
        .content_length()
        .is_some_and(|len| len > limit as u64)
    {
        return Err(Error::msg("release asset is too large"));
    }
    let mut data = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|e| Error::msg(e.to_string()))?
    {
        if chunk.len() > limit.saturating_sub(data.len()) {
            return Err(Error::msg("release asset is too large"));
        }
        data.extend_from_slice(&chunk);
    }
    Ok(data)
}

pub async fn update() -> Result<String> {
    let client = reqwest::Client::builder()
        .user_agent(concat!("agent-transcript/", env!("CARGO_PKG_VERSION")))
        .timeout(Duration::from_secs(60))
        .build()
        .map_err(|e| Error::msg(e.to_string()))?;
    let url = format!("https://api.github.com/repos/{REPO}/releases/latest");
    let release: Release = client
        .get(url)
        .send()
        .await
        .and_then(reqwest::Response::error_for_status)
        .map_err(|e| Error::msg(format!("cannot check GitHub Releases: {e}")))?
        .json()
        .await
        .map_err(|e| Error::msg(format!("invalid release response: {e}")))?;
    let Some(version) = newer_version(&release.tag_name, env!("CARGO_PKG_VERSION"))? else {
        return Ok(format!(
            "already up to date ({})",
            env!("CARGO_PKG_VERSION")
        ));
    };
    // Only canonical stable tags from the official release workflow are accepted.
    if release.tag_name != format!("v{version}") || !version.pre.is_empty() {
        return Err(Error::msg("release tag must be a stable v<version>"));
    }
    let name = asset_name()?;
    let binary = download(&client, asset_url(&release, name)?, MAX_BINARY).await?;
    let checksum = download(
        &client,
        asset_url(&release, &format!("{name}.sha256"))?,
        1024,
    )
    .await?;
    verify(
        &binary,
        &String::from_utf8(checksum).map_err(|e| Error::msg(e.to_string()))?,
        name,
    )?;
    let exe = std::env::current_exe()?;
    stage_and_replace(&exe, &binary)?;
    #[cfg(windows)]
    return Ok(format!("verified v{version}; replacement will finish after this process exits (see update log next to executable on failure)"));
    #[cfg(not(windows))]
    Ok(format!(
        "updated to v{version}; restart any running watch/MCP processes"
    ))
}

fn stage_and_replace(exe: &Path, bytes: &[u8]) -> Result<()> {
    let dir = exe
        .parent()
        .ok_or_else(|| Error::msg("executable has no parent directory"))?;
    let mut staged = tempfile::Builder::new()
        .prefix(".agent-transcript-update-")
        .suffix(if cfg!(windows) { ".exe" } else { "" })
        .tempfile_in(dir)?;
    staged.write_all(bytes)?;
    staged.as_file().sync_all()?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(staged.path(), fs::Permissions::from_mode(0o755))?;
        staged.persist(exe).map_err(|e| Error::Io(e.error))?;
    }
    #[cfg(windows)]
    {
        // Windows cannot replace its own running executable. Keep the verified
        // file and launch a separate system process to swap it after we exit.
        let (_, path) = staged.keep().map_err(|e| Error::Io(e.error))?;
        if let Err(error) = replace_after_exit(exe, &path) {
            let _ = fs::remove_file(path);
            return Err(error);
        }
    }
    Ok(())
}

#[cfg(windows)]
fn replace_after_exit(exe: &Path, staged: &Path) -> Result<()> {
    use std::process::Stdio;

    // Arguments are passed as separate OS strings, never interpolated into code.
    let mut script = tempfile::Builder::new()
        .prefix("agent-transcript-update-")
        .suffix(".ps1")
        .tempfile()?;
    script.write_all(include_bytes!("../scripts/update-windows.ps1"))?;
    let (_, script_path) = script.keep().map_err(|e| Error::Io(e.error))?;
    let result = crate::command::new("powershell.exe")
        .args([
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
        ])
        .arg(&script_path)
        .arg(std::process::id().to_string())
        .arg(exe)
        .arg(staged)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn();
    if result.is_err() {
        let _ = fs::remove_file(&script_path);
    }
    result.map(|_| ()).map_err(Error::Io)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versions_do_not_downgrade_or_accept_invalid_tags() {
        assert!(newer_version("v0.2.0", "0.1.0").unwrap().is_some());
        assert!(newer_version("v0.1.0", "0.1.0").unwrap().is_none());
        assert!(newer_version("v0.0.9", "0.1.0").unwrap().is_none());
        assert!(newer_version("oops", "0.1.0").is_err());
    }

    #[test]
    fn checksum_rejects_tampering_and_wrong_filename() {
        let name = "agent-transcript.exe";
        let sum = format!("{}  {name}\n", hex::encode(Sha256::digest(b"binary")));
        verify(b"binary", &sum, name).unwrap();
        assert!(verify(b"tampered", &sum, name).is_err());
        assert!(verify(b"binary", &sum, "other.exe").is_err());
        assert!(verify(b"binary", &(sum.clone() + &sum), name).is_err());
    }

    #[test]
    fn rejects_foreign_asset_url() {
        let release = Release {
            tag_name: "v0.2.0".into(),
            assets: vec![Asset {
                name: "binary".into(),
                browser_download_url: "https://example.com/binary".into(),
            }],
        };
        assert!(asset_url(&release, "binary").is_err());
    }

    #[cfg(unix)]
    #[test]
    fn replaces_executable_and_preserves_executable_permission() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let exe = dir.path().join("agent-transcript");
        fs::write(&exe, b"old").unwrap();
        stage_and_replace(&exe, b"new").unwrap();
        assert_eq!(fs::read(&exe).unwrap(), b"new");
        assert_ne!(fs::metadata(&exe).unwrap().permissions().mode() & 0o111, 0);
    }
}
