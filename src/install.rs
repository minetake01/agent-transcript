use std::path::PathBuf;

use crate::error::{Error, Result};

pub const TASK_NAME: &str = "agent-transcript watch";

pub fn install() -> Result<PathBuf> {
    #[cfg(windows)]
    {
        install_windows()
    }
    #[cfg(not(windows))]
    {
        Err(Error::msg("install registers a Windows logon task"))
    }
}

#[cfg(windows)]
fn install_windows() -> Result<PathBuf> {
    use crate::config::{self, Mode};

    let config = config::load_config()?;
    if config.mode == Mode::Read {
        return Err(Error::msg("mode is read; refusing to schedule watch"));
    }
    let exe = current_executable()?;
    let user = windows_user()?;
    register(TASK_NAME, &exe, &user)?;
    run_schtasks(&["/Run", "/TN", TASK_NAME])?;
    Ok(exe)
}

#[cfg(windows)]
fn current_executable() -> Result<PathBuf> {
    let path = std::env::current_exe()?;
    let text = path.to_str().ok_or_else(|| {
        Error::msg(format!(
            "executable path is not Unicode: {}",
            path.display()
        ))
    })?;
    let plain = text.strip_prefix(r"\\?\").unwrap_or(text);
    Ok(PathBuf::from(plain))
}

#[cfg(windows)]
fn windows_user() -> Result<String> {
    let name = std::env::var("USERNAME").map_err(|_| Error::msg("USERNAME is not set"))?;
    let domain = std::env::var("USERDOMAIN").map_err(|_| Error::msg("USERDOMAIN is not set"))?;
    if name.is_empty() || domain.is_empty() {
        return Err(Error::msg("USERNAME or USERDOMAIN is empty"));
    }
    Ok(format!("{domain}\\{name}"))
}

#[cfg(windows)]
fn register(name: &str, exe: &std::path::Path, user: &str) -> Result<()> {
    use std::fs;

    let path = std::env::temp_dir().join(format!(
        "agent-transcript-{}-{}.xml",
        std::process::id(),
        name.replace(' ', "-")
    ));
    fs::write(&path, utf16_bom(&task_xml(exe, user)))?;
    let created = run_schtasks(&[
        "/Create",
        "/TN",
        name,
        "/XML",
        &path.display().to_string(),
        "/F",
    ]);
    let _ = fs::remove_file(&path);
    created
}

fn task_xml(exe: &std::path::Path, user: &str) -> String {
    let command = xml_escape(&exe.display().to_string());
    let user = xml_escape(user);
    format!(
        r#"<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.3" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo>
    <Description>Upload changed coding-agent transcripts and maintain the durable search index every 5 minutes.</Description>
  </RegistrationInfo>
  <Triggers>
    <LogonTrigger>
      <Enabled>true</Enabled>
      <UserId>{user}</UserId>
    </LogonTrigger>
  </Triggers>
  <Principals>
    <Principal id="Author">
      <UserId>{user}</UserId>
      <LogonType>InteractiveToken</LogonType>
      <RunLevel>LeastPrivilege</RunLevel>
    </Principal>
  </Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <AllowHardTerminate>true</AllowHardTerminate>
    <StartWhenAvailable>true</StartWhenAvailable>
    <RunOnlyIfNetworkAvailable>false</RunOnlyIfNetworkAvailable>
    <AllowStartOnDemand>true</AllowStartOnDemand>
    <Enabled>true</Enabled>
    <Hidden>true</Hidden>
    <RunOnlyIfIdle>false</RunOnlyIfIdle>
    <WakeToRun>false</WakeToRun>
    <ExecutionTimeLimit>PT0S</ExecutionTimeLimit>
    <Priority>8</Priority>
    <RestartOnFailure>
      <Interval>PT1M</Interval>
      <Count>999</Count>
    </RestartOnFailure>
  </Settings>
  <Actions Context="Author">
    <Exec>
      <Command>{command}</Command>
      <Arguments>watch</Arguments>
    </Exec>
  </Actions>
</Task>
"#
    )
}

fn xml_escape(text: &str) -> String {
    let mut escaped = String::with_capacity(text.len());
    for ch in text.chars() {
        match ch {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&apos;"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

#[cfg(windows)]
fn utf16_bom(text: &str) -> Vec<u8> {
    let mut bytes = vec![0xFF, 0xFE];
    for unit in text.encode_utf16() {
        bytes.extend_from_slice(&unit.to_le_bytes());
    }
    bytes
}

#[cfg(windows)]
fn run_schtasks(args: &[&str]) -> Result<()> {
    use std::io::ErrorKind;

    let output = crate::command::new("schtasks")
        .args(args)
        .output()
        .map_err(|error| {
            if error.kind() == ErrorKind::NotFound {
                Error::msg("schtasks is not installed")
            } else {
                Error::Io(error)
            }
        })?;
    if output.status.success() {
        return Ok(());
    }
    let mut detail = output.stderr;
    detail.extend_from_slice(&output.stdout);
    let detail = decode_console(&detail);
    let detail = detail.trim();
    if detail.is_empty() {
        Err(Error::msg(format!(
            "schtasks exited with {}",
            output.status
        )))
    } else {
        Err(Error::msg(format!("schtasks failed: {detail}")))
    }
}

#[cfg(windows)]
fn decode_console(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return String::new();
    }
    unsafe extern "system" {
        fn MultiByteToWideChar(
            code_page: u32,
            flags: u32,
            input: *const u8,
            input_len: i32,
            output: *mut u16,
            output_len: i32,
        ) -> i32;
    }
    const CP_ACP: u32 = 0;
    unsafe {
        let wide_len = MultiByteToWideChar(
            CP_ACP,
            0,
            bytes.as_ptr(),
            bytes.len() as i32,
            std::ptr::null_mut(),
            0,
        );
        if wide_len <= 0 {
            return String::from_utf8_lossy(bytes).into_owned();
        }
        let mut wide = vec![0u16; wide_len as usize];
        MultiByteToWideChar(
            CP_ACP,
            0,
            bytes.as_ptr(),
            bytes.len() as i32,
            wide.as_mut_ptr(),
            wide_len,
        );
        String::from_utf16_lossy(&wide)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logon_task_runs_watch_for_this_user() {
        let xml = task_xml(
            std::path::Path::new(r"C:\Program Files\agent-transcript.exe"),
            r"PC\me",
        );
        assert_eq!(
            xml_text(&xml, "Command"),
            r"C:\Program Files\agent-transcript.exe"
        );
        assert_eq!(xml_text(&xml, "Arguments"), "watch");
        assert_eq!(xml_text(&logon_trigger(&xml), "UserId"), r"PC\me");
        assert!(xml.contains("<LogonType>InteractiveToken</LogonType>"));
        assert!(xml.contains("<RunLevel>LeastPrivilege</RunLevel>"));
        assert!(xml.contains("<Hidden>true</Hidden>"));
        assert!(xml.contains("<MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>"));
        assert!(xml.contains("<ExecutionTimeLimit>PT0S</ExecutionTimeLimit>"));
        assert!(xml.contains("<Priority>8</Priority>"));
        assert!(xml.contains("<Interval>PT1M</Interval>"));
        assert!(xml.contains("<Count>999</Count>"));
        assert!(xml.contains("<DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>"));
        assert!(xml.contains("<StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>"));
    }

    #[test]
    fn command_path_is_xml_escaped() {
        let xml = task_xml(
            std::path::Path::new(r"C:\a&b\agent-transcript.exe"),
            r"PC\me",
        );
        assert_eq!(
            xml_text(&xml, "Command"),
            r"C:\a&amp;b\agent-transcript.exe"
        );
    }

    #[cfg(windows)]
    #[test]
    fn schtasks_registers_the_logon_task() {
        let name = "agent-transcript watch test";
        let _guard = TaskGuard(name);
        let exe = current_executable().expect("current exe");
        let user = windows_user().expect("user");
        register(name, &exe, &user).expect("register");

        let output = crate::command::new("schtasks")
            .args(["/Query", "/TN", name, "/XML"])
            .output()
            .expect("query");
        assert!(
            output.status.success(),
            "{}",
            decode_console(&output.stderr)
        );
        let xml = decode_task_xml(&output.stdout);
        assert_eq!(xml_text(&xml, "Command"), exe.display().to_string());
        assert_eq!(xml_text(&xml, "Arguments"), "watch");
        assert_eq!(xml_text(&logon_trigger(&xml), "UserId"), user);
        assert!(xml.contains("<Hidden>true</Hidden>"));
        assert!(xml.contains("<ExecutionTimeLimit>PT0S</ExecutionTimeLimit>"));
    }

    fn xml_text<'a>(xml: &'a str, tag: &str) -> &'a str {
        let start = format!("<{tag}>");
        let end = format!("</{tag}>");
        let from = xml
            .find(&start)
            .unwrap_or_else(|| panic!("missing {tag} in {xml}"));
        let rest = &xml[from + start.len()..];
        let to = rest
            .find(&end)
            .unwrap_or_else(|| panic!("unclosed {tag} in {xml}"));
        &rest[..to]
    }

    fn logon_trigger(xml: &str) -> String {
        xml_text(xml, "LogonTrigger").to_string()
    }

    #[cfg(windows)]
    fn decode_task_xml(bytes: &[u8]) -> String {
        if bytes.starts_with(&[0xFF, 0xFE]) {
            let units = bytes[2..]
                .chunks_exact(2)
                .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
                .collect::<Vec<_>>();
            return String::from_utf16_lossy(&units);
        }
        String::from_utf8_lossy(bytes).into_owned()
    }

    #[cfg(windows)]
    struct TaskGuard(&'static str);

    #[cfg(windows)]
    impl Drop for TaskGuard {
        fn drop(&mut self) {
            let _ = crate::command::new("schtasks")
                .args(["/Delete", "/TN", self.0, "/F"])
                .output();
        }
    }
}
