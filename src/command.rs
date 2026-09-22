use std::ffi::OsStr;
use std::process::Command;

/// Start a console program whose output this process captures.
///
/// The logon task has no console. Without this flag, Windows would allocate one
/// for each child and Windows Terminal would display it.
pub fn new(program: impl AsRef<OsStr>) -> Command {
    let mut command = Command::new(program);
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        command.creation_flags(CREATE_NO_WINDOW);
    }
    command
}
