//! Command-line arguments.

use clap::{Args, Parser, Subcommand};
use std::path::PathBuf;

/// Keep files and folders in sync across machines.
#[derive(Parser)]
#[command(name = "rust-sync", version)]
pub struct Cli {
    #[command(subcommand)]
    pub cmd: Cmd,
}

#[derive(Subcommand)]
pub enum Cmd {
    /// Run the sync daemon in the foreground.
    Daemon {
        /// Port to listen on (saved for next time).
        #[arg(long)]
        port: Option<u16>,
        /// Write the log to this file instead of the terminal. It is rotated at 1 MiB.
        #[arg(long)]
        log_file: Option<PathBuf>,
    },
    /// Show this node's ID.
    Id,
    /// Add, list or remove synced files and folders.
    Path {
        #[command(subcommand)]
        cmd: PathCmd,
    },
    /// Add, list or remove nodes.
    Node {
        #[command(subcommand)]
        cmd: NodeCmd,
    },
    /// Start the daemon at login or boot, or stop doing so.
    Service {
        #[command(subcommand)]
        cmd: ServiceCmd,
    },
    /// Show nodes, synced paths and connection state.
    Status,
}

#[derive(Subcommand)]
pub enum PathCmd {
    /// Start syncing a file or folder.
    Add { path: PathBuf },
    /// List synced files and folders.
    #[command(visible_alias = "ls")]
    List,
    /// Stop syncing a file or folder. The files stay on disk.
    #[command(visible_alias = "rm")]
    Remove { path: PathBuf },
}

#[derive(Subcommand)]
pub enum NodeCmd {
    /// Trust another node and sync with it.
    Add(NodeAdd),
    /// List trusted nodes, their addresses and whether they are connected.
    #[command(visible_alias = "ls")]
    List,
    /// Stop trusting a node.
    #[command(visible_alias = "rm")]
    Remove {
        /// The node's ID, as shown by `rust-sync node list`.
        id: String,
    },
}

#[derive(Subcommand)]
pub enum ServiceCmd {
    /// Install a launchd agent that runs the daemon at login, and start it now.
    Install {
        /// Install a system daemon that runs as you from boot, without a login. Needs sudo.
        #[arg(long)]
        system: bool,
    },
    /// Stop the daemon and remove its launchd agent or system daemon.
    Uninstall,
    /// Start the daemon now.
    Start,
    /// Stop the daemon now. It starts again at the next login or boot.
    Stop,
    /// Stop and start the daemon, for example after an upgrade.
    Restart,
}

#[derive(Args)]
pub struct NodeAdd {
    /// Hostname or IP, with an optional :port.
    pub addr: String,
    /// The ID you expect the node to have. Skips the question.
    #[arg(long)]
    pub id: Option<String>,
    /// Trust the node without asking.
    #[arg(long, short)]
    pub yes: bool,
}
