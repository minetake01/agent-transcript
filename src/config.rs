use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::crypto::{self, Key, KEY_LEN};
use crate::error::{Error, Result};

pub const SCHEMA: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    Read,
    Readwrite,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub schema: u32,
    pub account_id: String,
    pub bucket: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub mode: Mode,
}

impl Config {
    pub fn endpoint(&self) -> String {
        format!(
            "https://{}.r2.cloudflarestorage.com",
            self.account_id.trim()
        )
    }

    pub fn require_write(&self) -> Result<()> {
        if self.mode == Mode::Read {
            Err(Error::msg(
                "mode is read; refusing a request that writes to R2",
            ))
        } else {
            Ok(())
        }
    }
}

pub fn user_home_dir() -> Option<PathBuf> {
    #[cfg(windows)]
    let home = std::env::var_os("USERPROFILE")
        .filter(|v| !v.is_empty())
        .or_else(|| std::env::var_os("HOME"));
    #[cfg(not(windows))]
    let home = std::env::var_os("HOME");
    home.filter(|v| !v.is_empty()).map(PathBuf::from)
}

pub fn home_dir() -> Result<PathBuf> {
    if let Some(home) = std::env::var_os("AGENT_TRANSCRIPT_HOME") {
        return Ok(PathBuf::from(home));
    }
    if let Some(appdata) = std::env::var_os("APPDATA") {
        return Ok(PathBuf::from(appdata).join("agent-transcript"));
    }
    if let Some(config) = std::env::var_os("XDG_CONFIG_HOME") {
        return Ok(PathBuf::from(config).join("agent-transcript"));
    }
    let home =
        std::env::var_os("HOME").ok_or_else(|| Error::msg("cannot locate a home directory"))?;
    Ok(PathBuf::from(home).join(".config").join("agent-transcript"))
}

pub fn cache_dir() -> Result<PathBuf> {
    if let Some(home) = std::env::var_os("AGENT_TRANSCRIPT_HOME") {
        return Ok(PathBuf::from(home).join("cache"));
    }
    if let Some(local) = std::env::var_os("LOCALAPPDATA") {
        return Ok(PathBuf::from(local).join("agent-transcript").join("cache"));
    }
    Ok(home_dir()?.join("cache"))
}

pub fn config_path() -> Result<PathBuf> {
    Ok(home_dir()?.join("config.toml"))
}

pub fn key_path() -> Result<PathBuf> {
    Ok(home_dir()?.join("key"))
}

pub fn load_config() -> Result<Config> {
    let path = config_path()?;
    let text = fs::read_to_string(&path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            Error::msg(format!(
                "config is missing at {} — run `agent-transcript init`",
                path.display()
            ))
        } else {
            Error::Io(error)
        }
    })?;
    let config: Config = toml::from_str(&text).map_err(|error| Error::msg(error.to_string()))?;
    if config.schema != SCHEMA {
        return Err(Error::Schema {
            schema: config.schema,
        });
    }
    if config.account_id.is_empty() || config.bucket.is_empty() {
        return Err(Error::msg("config is missing account_id or bucket"));
    }
    Ok(config)
}

pub fn load_key() -> Result<Key> {
    let path = key_path()?;
    let bytes = fs::read(&path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            Error::msg(format!(
                "encryption key is missing at {} — run `agent-transcript init`",
                path.display()
            ))
        } else {
            Error::Io(error)
        }
    })?;
    let key: Key = bytes.try_into().map_err(|bytes: Vec<u8>| {
        Error::msg(format!(
            "encryption key at {} is {} bytes, expected {KEY_LEN}",
            path.display(),
            bytes.len()
        ))
    })?;
    Ok(key)
}

pub struct InitOptions {
    pub account_id: String,
    pub bucket: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub mode: Mode,
}

pub fn init(options: InitOptions) -> Result<(PathBuf, bool)> {
    let dir = home_dir()?;
    fs::create_dir_all(&dir)?;
    let config = Config {
        schema: SCHEMA,
        account_id: options.account_id,
        bucket: options.bucket,
        access_key_id: options.access_key_id,
        secret_access_key: options.secret_access_key,
        mode: options.mode,
    };
    fs::write(
        config_path()?,
        toml::to_string_pretty(&config).map_err(|error| Error::msg(error.to_string()))?,
    )?;
    let key_file = key_path()?;
    let created = !key_file.exists();
    if created {
        fs::write(&key_file, crypto::generate_key())?;
        restrict_private(&key_file)?;
    }
    restrict_private(&config_path()?)?;
    Ok((key_file, created))
}

fn restrict_private(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}
