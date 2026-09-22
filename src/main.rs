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
    if let Err(error) = run().await {
        eprintln!("agent-transcript: {error}");
        std::process::exit(1);
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
        Command::Ingest => agent_transcript::ingest::ingest().await,
        Command::Watch => agent_transcript::ingest::watch().await,
        Command::Mcp => agent_transcript::mcp::serve()
            .await
            .map_err(agent_transcript::Error::msg),
        Command::Gc => agent_transcript::ingest::gc().await,
    }
}
