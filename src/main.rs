mod config;
mod ctl;
mod daemon;
mod index;
mod log;
mod net;
mod paths;
mod service;

use anyhow::{Context, bail};
use clap::{Args, Parser, Subcommand};
use config::Config;
use ctl::Req;
use net::Msg;
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;
use tokio::time::timeout;

/// Keep files and folders in sync across machines.
#[derive(Parser)]
#[command(name = "rust-sync", version)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
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
    /// Start the daemon at login, or stop doing so.
    Service {
        #[command(subcommand)]
        cmd: ServiceCmd,
    },
    /// Show nodes, synced paths and connection state.
    Status,
}

#[derive(Subcommand)]
enum PathCmd {
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
enum NodeCmd {
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
enum ServiceCmd {
    /// Install a launchd agent that runs the daemon at login, and start it now.
    Install,
    /// Stop the daemon and remove the launchd agent.
    Uninstall,
}

#[derive(Args)]
struct NodeAdd {
    /// Hostname or IP, with an optional :port.
    addr: String,
    /// The ID you expect the node to have. Skips the question.
    #[arg(long)]
    id: Option<String>,
    /// Trust the node without asking.
    #[arg(long, short)]
    yes: bool,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    if let Err(e) = run(Cli::parse().cmd).await {
        eprintln!("error: {e:#}");
        std::process::exit(1);
    }
}

async fn run(cmd: Cmd) -> anyhow::Result<()> {
    match cmd {
        Cmd::Daemon { port, log_file } => daemon::run(port, log_file).await,
        Cmd::Id => {
            println!("{}", net::pretty_id(&my_keys()?.id()));
            Ok(())
        }
        Cmd::Path { cmd: PathCmd::Add { path } } => {
            let abs = std::fs::canonicalize(&path).with_context(|| format!("cannot find {}", path.display()))?;
            let is_dir = abs.is_dir();
            if !is_dir && !abs.is_file() {
                bail!("{} is not a regular file or folder", abs.display());
            }
            let portable = paths::to_portable(&abs)
                .filter(|p| paths::to_local(p).is_some())
                .with_context(|| format!("cannot sync {}", abs.display()))?;
            send(Req::AddPath { path: portable, is_dir }).await
        }
        Cmd::Path { cmd: PathCmd::List } => {
            // The daemon saves every change to the config file, so this is current either way.
            print!("{}", daemon::paths_text(&Config::load(&config_path())?));
            Ok(())
        }
        Cmd::Path { cmd: PathCmd::Remove { path } } => {
            // The path may be gone already, so do not require it to exist.
            let abs = std::fs::canonicalize(&path).unwrap_or_else(|_| std::path::absolute(&path).unwrap_or(path));
            let portable = paths::to_portable(&abs).context("path is not valid UTF-8")?;
            send(Req::RemovePath { path: portable }).await
        }
        Cmd::Node { cmd: NodeCmd::Add(a) } => node_add(a).await,
        Cmd::Node { cmd: NodeCmd::List } => send(Req::ListNodes).await,
        Cmd::Node { cmd: NodeCmd::Remove { id } } => {
            send(Req::RemoveNode { id: net::parse_id(&id).context("not a valid node ID")? }).await
        }
        Cmd::Service { cmd: ServiceCmd::Install } => service::install().await,
        Cmd::Service { cmd: ServiceCmd::Uninstall } => service::uninstall().await,
        Cmd::Status => {
            send(Req::Status).await?;
            let login = if service::installed() { "yes" } else { "no (`rust-sync service install`)" };
            println!("Starts at login: {login}");
            Ok(())
        }
    }
}

fn my_keys() -> anyhow::Result<net::Keys> {
    net::load_or_create_keys(paths::state_dir())
}

fn config_path() -> PathBuf {
    paths::state_dir().join("config.json")
}

/// Send a change to the running daemon. If none is running, edit the config file.
async fn send(req: Req) -> anyhow::Result<()> {
    if let Some(resp) = ctl::call(&req).await? {
        if !resp.ok {
            bail!("{}", resp.text);
        }
        println!("{}", resp.text.trim_end());
        return Ok(());
    }
    let me = my_keys()?.id();
    let mut cfg = Config::load(&config_path())?;
    let text = match req {
        Req::Status => {
            print!("{}", daemon::status_text(&cfg, &me, |_| None, &Default::default()));
            return Ok(());
        }
        Req::ListNodes => {
            print!("{}", daemon::nodes_text(&cfg, |_| None, &Default::default()));
            println!("\n(The daemon is not running, so connection state is unknown.)");
            return Ok(());
        }
        Req::AddPath { path, is_dir } => {
            cfg.set_root(&me, &path, is_dir, false);
            format!("Now syncing {path}")
        }
        Req::RemovePath { path } => {
            let Some(r) = cfg.shared.roots.get(&path).filter(|r| !r.removed) else { bail!("{path} is not synced") };
            let is_dir = r.is_dir;
            cfg.set_root(&me, &path, is_dir, true);
            format!("Stopped syncing {path}. The files stay on disk.")
        }
        Req::AddNode { id, addr } => {
            cfg.set_node(&me, &id, &addr, false);
            format!("Added node {}", net::pretty_id(&id))
        }
        Req::RemoveNode { id } => {
            let Some(addr) = cfg.trusted(&id).map(|n| n.addr.clone()) else { bail!("No such node") };
            cfg.set_node(&me, &id, &addr, true);
            format!("Removed node {}", net::pretty_id(&id))
        }
    };
    cfg.save(&config_path())?;
    println!("{text}\n(The daemon is not running. This takes effect when it starts.)");
    Ok(())
}

/// Connect to the node, show its ID, and trust it once the user confirms.
async fn node_add(a: NodeAdd) -> anyhow::Result<()> {
    let keys = my_keys()?;
    let cfg = Config::load(&config_path())?;
    let addr = config::with_port(&a.addr);
    let expected = a.id.as_deref().map(|s| net::parse_id(s).context("not a valid node ID")).transpose()?;

    println!("Connecting to {addr}...");
    let stream = timeout(Duration::from_secs(10), tokio::net::TcpStream::connect(&addr))
        .await
        .context("timed out")?
        .with_context(|| format!("cannot connect to {addr}. Is `rust-sync daemon` running there?"))?;
    let (mut r, mut w, id) = net::handshake(stream, &keys, true).await?;
    if id == keys.id() {
        bail!("that is this node");
    }
    w.send(&Msg::Hello { port: cfg.port, probe: true, trusts_you: cfg.trusted(&id).is_some() }).await?;
    let trusts_us = matches!(
        timeout(Duration::from_secs(10), r.recv()).await,
        Ok(Ok(Msg::Hello { trusts_you: true, .. }))
    );

    println!("Node ID: {}", net::pretty_id(&id));
    match expected {
        Some(e) if e != id => bail!("that is not the ID you gave ({}). Node not added.", net::pretty_id(&e)),
        Some(_) => {}
        None if a.yes => {}
        None => {
            print!("Check this matches `rust-sync id` on that machine. Trust it? [y/N] ");
            std::io::stdout().flush()?;
            let mut answer = String::new();
            std::io::stdin().read_line(&mut answer)?;
            if !answer.trim().eq_ignore_ascii_case("y") {
                println!("Node not added.");
                return Ok(());
            }
        }
    }
    send(Req::AddNode { id, addr }).await?;
    if !trusts_us {
        println!(
            "\nThat node does not trust this one yet. On that machine, run\n  \
             rust-sync node add <address of this machine>\nand check it shows this ID:\n  {}",
            net::pretty_id(&keys.id())
        );
    }
    Ok(())
}
