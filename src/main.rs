// Task Scheduler starts this process with no console. A console-subsystem
// executable would be given one, and Windows Terminal would show that window
// for the whole lifetime of `watch`.
#![cfg_attr(all(windows, not(test)), windows_subsystem = "windows")]

use std::io::Write;

use clap::{Parser, Subcommand, ValueEnum};

use agent_transcript::config::{self, InitOptions, Mode};

#[derive(Parser)]
#[command(
    name = "agent-transcript",
    about = "Encrypted coding-agent transcript archive on R2"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Write R2 settings and create an encryption key if one does not exist.
    Init {
        #[arg(long)]
        account_id: String,
        #[arg(long)]
        bucket: String,
        #[arg(long)]
        access_key_id: String,
        #[arg(long)]
        secret_access_key: String,
        #[arg(long, value_enum, default_value_t = CliMode::Readwrite)]
        mode: CliMode,
    },
    /// Register the Windows logon task that runs watch.
    Install,
    /// Encrypt new and changed local transcripts and store those revisions in R2.
    Ingest,
    /// Run ingest now, then every five minutes at a reduced priority.
    Watch,
    /// Serve the read-only MCP tools on stdio.
    Mcp,
    /// Delete archived objects that the catalog no longer references.
    Gc,
}

#[derive(Clone, Copy, ValueEnum)]
enum CliMode {
    Read,
    Readwrite,
}

impl From<CliMode> for Mode {
    fn from(mode: CliMode) -> Self {
        match mode {
            CliMode::Read => Mode::Read,
            CliMode::Readwrite => Mode::Readwrite,
        }
    }
}

#[tokio::main]
async fn main() {
    if let Err(error) = prepare_stdio() {
        let _ = writeln!(std::io::stderr(), "agent-transcript: {error}");
        std::process::exit(1);
    }
    if let Err(error) = run().await {
        eprintln!("agent-transcript: {error}");
        std::process::exit(1);
    }
}

/// Stdio for process startup.
///
/// Piped and redirected handles stay in place. A parent console is used when
/// the caller is a terminal. With no console, output is discarded so the logon
/// task does not open a terminal and so `println!` does not abort.
#[derive(Debug, PartialEq, Eq)]
enum StartupStdio {
    Keep,
    ParentConsole,
    Discard,
}

fn startup_stdio(
    handles_ready: bool,
    attach_succeeded: bool,
    has_console_window: bool,
) -> StartupStdio {
    if handles_ready {
        return StartupStdio::Keep;
    }
    // Task Scheduler rejects AttachConsole with access denied and does not
    // leave a console behind. Only a successful attach or an existing console
    // window is safe to write to.
    if attach_succeeded || has_console_window {
        StartupStdio::ParentConsole
    } else {
        StartupStdio::Discard
    }
}

fn prepare_stdio() -> std::io::Result<()> {
    #[cfg(windows)]
    {
        prepare_stdio_windows()
    }
    #[cfg(not(windows))]
    {
        Ok(())
    }
}

#[cfg(windows)]
fn prepare_stdio_windows() -> std::io::Result<()> {
    const STD_INPUT_HANDLE: u32 = 0xFFFF_FFF6;
    const STD_OUTPUT_HANDLE: u32 = 0xFFFF_FFF5;
    const STD_ERROR_HANDLE: u32 = 0xFFFF_FFF4;
    const ATTACH_PARENT_PROCESS: u32 = 0xFFFF_FFFF;

    unsafe extern "system" {
        fn AttachConsole(process_id: u32) -> i32;
        fn GetConsoleWindow() -> *mut core::ffi::c_void;
        fn GetStdHandle(std_handle: u32) -> *mut core::ffi::c_void;
        fn SetStdHandle(std_handle: u32, handle: *mut core::ffi::c_void) -> i32;
        fn CloseHandle(handle: *mut core::ffi::c_void) -> i32;
    }

    fn handle_ready(id: u32) -> bool {
        let handle = unsafe { GetStdHandle(id) };
        !handle.is_null() && handle != (-1isize as *mut core::ffi::c_void)
    }

    fn set_std_handle(id: u32, device: &str) -> std::io::Result<()> {
        use std::fs::OpenOptions;
        use std::os::windows::io::IntoRawHandle;

        let file = OpenOptions::new().read(true).write(true).open(device)?;
        let handle = file.into_raw_handle();
        if unsafe { SetStdHandle(id, handle) } == 0 {
            let error = std::io::Error::last_os_error();
            unsafe { CloseHandle(handle) };
            return Err(error);
        }
        Ok(())
    }

    fn bind_missing(device: &str, id: u32) -> std::io::Result<()> {
        if handle_ready(id) {
            return Ok(());
        }
        set_std_handle(id, device)
    }

    let handles_ready = handle_ready(STD_INPUT_HANDLE)
        && handle_ready(STD_OUTPUT_HANDLE)
        && handle_ready(STD_ERROR_HANDLE);
    if handles_ready {
        return Ok(());
    }

    let attached = unsafe { AttachConsole(ATTACH_PARENT_PROCESS) } != 0;
    let has_console_window = !unsafe { GetConsoleWindow() }.is_null();
    let device = match startup_stdio(false, attached, has_console_window) {
        StartupStdio::Keep => return Ok(()),
        StartupStdio::ParentConsole => ("CONIN$", "CONOUT$"),
        StartupStdio::Discard => ("NUL", "NUL"),
    };
    bind_missing(device.1, STD_ERROR_HANDLE)?;
    bind_missing(device.1, STD_OUTPUT_HANDLE)?;
    bind_missing(device.0, STD_INPUT_HANDLE)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{startup_stdio, StartupStdio};

    #[test]
    fn redirected_stdio_stays_in_place() {
        assert_eq!(startup_stdio(true, false, false), StartupStdio::Keep);
    }

    #[test]
    fn parent_console_receives_output() {
        assert_eq!(
            startup_stdio(false, true, false),
            StartupStdio::ParentConsole
        );
        assert_eq!(
            startup_stdio(false, false, true),
            StartupStdio::ParentConsole
        );
    }

    #[test]
    fn logon_task_discards_output_instead_of_opening_a_console() {
        assert_eq!(startup_stdio(false, false, false), StartupStdio::Discard);
    }
}

async fn run() -> agent_transcript::Result<()> {
    match Cli::parse().command {
        Command::Init {
            account_id,
            bucket,
            access_key_id,
            secret_access_key,
            mode,
        } => {
            let (key_path, created) = config::init(InitOptions {
                account_id,
                bucket,
                access_key_id,
                secret_access_key,
                mode: mode.into(),
            })?;
            if created {
                println!("wrote config and encryption key at {}", key_path.display());
            } else {
                println!(
                    "wrote config and kept the existing encryption key at {}",
                    key_path.display()
                );
            }
            Ok(())
        }
        Command::Install => {
            let exe = agent_transcript::install::install()?;
            println!(
                "registered logon task `{}` for {}",
                agent_transcript::install::TASK_NAME,
                exe.display()
            );
            Ok(())
        }
        Command::Ingest => agent_transcript::ingest::ingest().await,
        Command::Watch => agent_transcript::ingest::watch().await,
        Command::Mcp => agent_transcript::mcp::serve()
            .await
            .map_err(agent_transcript::Error::msg),
        Command::Gc => agent_transcript::ingest::gc().await,
    }
}
