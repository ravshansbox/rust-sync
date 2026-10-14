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

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    fn parse(args: &[&str]) -> Result<Cli, clap::Error> {
        Cli::try_parse_from(std::iter::once("rust-sync").chain(args.iter().copied()))
    }

    #[test]
    fn cli_definition_is_valid() {
        Cli::command().debug_assert();
    }

    #[test]
    fn list_and_remove_aliases_parse() {
        assert!(matches!(parse(&["path", "ls"]).unwrap().cmd, Cmd::Path { cmd: PathCmd::List }));
        assert!(matches!(parse(&["node", "ls"]).unwrap().cmd, Cmd::Node { cmd: NodeCmd::List }));
        let Cmd::Path { cmd: PathCmd::Remove { path } } = parse(&["path", "rm", "a"]).unwrap().cmd else { panic!() };
        assert_eq!(path, PathBuf::from("a"));
        let Cmd::Node { cmd: NodeCmd::Remove { id } } = parse(&["node", "rm", "x"]).unwrap().cmd else { panic!() };
        assert_eq!(id, "x");
    }

    #[test]
    fn node_add_flags_parse() {
        let cli = parse(&["node", "add", "host:1", "--id", "ab", "-y"]).unwrap();
        let Cmd::Node { cmd: NodeCmd::Add(a) } = cli.cmd else { panic!() };
        assert_eq!((a.addr.as_str(), a.id.as_deref(), a.yes), ("host:1", Some("ab"), true));
        let cli = parse(&["node", "add", "host"]).unwrap();
        let Cmd::Node { cmd: NodeCmd::Add(a) } = cli.cmd else { panic!() };
        assert_eq!((a.id, a.yes), (None, false));
        assert!(parse(&["node", "add"]).is_err());
    }

    #[test]
    fn daemon_and_service_install_flags_parse() {
        let cli = parse(&["daemon", "--port", "7000", "--log-file", "x.log"]).unwrap();
        let Cmd::Daemon { port, log_file } = cli.cmd else { panic!() };
        assert_eq!((port, log_file), (Some(7000), Some(PathBuf::from("x.log"))));
        let Cmd::Daemon { port, log_file } = parse(&["daemon"]).unwrap().cmd else { panic!() };
        assert_eq!((port, log_file), (None, None));
        assert!(parse(&["daemon", "--port", "70000"]).is_err());
        let cli = parse(&["service", "install", "--system"]).unwrap();
        assert!(matches!(cli.cmd, Cmd::Service { cmd: ServiceCmd::Install { system: true } }));
        let cli = parse(&["service", "install"]).unwrap();
        assert!(matches!(cli.cmd, Cmd::Service { cmd: ServiceCmd::Install { system: false } }));
    }
}
