//! Running the command-line commands.

use crate::cli::{AliasCmd, Cmd, NodeCmd, PathCmd, ServiceCmd};
use crate::config::{self, Config};
use crate::ctl::{self, Req};
use crate::net;
use crate::{daemon, paths, service};
use anyhow::bail;
use std::collections::BTreeMap;

mod node;
mod path;
use node::{alias_add, node_add, node_ref};
use path::{path_add, path_list, removal_path};

/// Run one command-line command.
pub async fn run(cmd: Cmd) -> anyhow::Result<()> {
    match cmd {
        Cmd::Daemon { port, log_file } => daemon::run(port, log_file).await,
        Cmd::Id => id(),
        Cmd::Path { cmd: PathCmd::Add { path } } => path_add(path).await,
        Cmd::Path { cmd: PathCmd::List } => path_list(),
        Cmd::Path { cmd: PathCmd::Remove { path } } => {
            send(Req::RemovePath { path: removal_path(path)? }).await
        }
        Cmd::Node { cmd: NodeCmd::Add(a) } => node_add(a).await,
        Cmd::Node { cmd: NodeCmd::List } => send(Req::ListNodes).await,
        Cmd::Node { cmd: NodeCmd::Remove { id } } => {
            send(Req::RemoveNode { id: node_ref(&id)? }).await
        }
        Cmd::Node { cmd: NodeCmd::Alias { cmd: AliasCmd::Add { id, addr } } } => alias_add(&id, &addr).await,
        Cmd::Node { cmd: NodeCmd::Alias { cmd: AliasCmd::Remove { id, addr } } } => {
            send(Req::RemoveAlias { id: node_ref(&id)?, addr: config::with_port(&addr) }).await
        }
        Cmd::Service { cmd: ServiceCmd::Install { system } } => service::install(system).await,
        Cmd::Service { cmd: ServiceCmd::Uninstall } => service::uninstall(),
        Cmd::Service { cmd: ServiceCmd::Start } => service::start().await,
        Cmd::Service { cmd: ServiceCmd::Stop } => service::stop().await,
        Cmd::Service { cmd: ServiceCmd::Restart } => service::restart().await,
        Cmd::Status => status().await,
    }
}

/// Print this node's ID.
fn id() -> anyhow::Result<()> {
    outln!("{}", net::pretty_id(&my_keys()?.id()));
    Ok(())
}

/// Print the daemon status, then whether the service starts at login or boot.
async fn status() -> anyhow::Result<()> {
    send(Req::Status).await?;
    match service::starts_at() {
        Some(when) => outln!("Starts at {when}: yes"),
        None => outln!("Starts at login: no (`rust-sync service install`)"),
    }
    Ok(())
}

/// Load this node's keys, creating them on first use.
fn my_keys() -> anyhow::Result<net::Keys> {
    net::load_or_create_keys(paths::state_dir())
}

/// Send a change to the running daemon. If none is running, edit the config file.
async fn send(req: Req) -> anyhow::Result<()> {
    if let Some(resp) = ctl::call(&req).await? {
        if !resp.ok {
            bail!("{}", resp.text);
        }
        outln!("{}", resp.text.trim_end());
        return Ok(());
    }
    send_offline(&req)
}

/// Handle a request without a daemon: print status or nodes from the config file, or apply a change to it.
fn send_offline(req: &Req) -> anyhow::Result<()> {
    let me = my_keys()?.id();
    let path = paths::config_file();
    let mut cfg = Config::load(&path)?;
    let none = BTreeMap::default();
    match req {
        Req::Status => {
            out!("{}", daemon::status_text(&cfg, &me, |_| None, &none));
            return Ok(());
        }
        Req::ListNodes => {
            out!("{}", daemon::nodes_text(&cfg, |_| None, &none));
            outln!("\n(The daemon is not running, so connection state is unknown.)");
            return Ok(());
        }
        _ => {}
    }
    let text = ctl::apply(&mut cfg, &me, req).map_err(anyhow::Error::msg)?;
    cfg.save(&path)?;
    outln!("{text}\n(The daemon is not running. This takes effect when it starts.)");
    Ok(())
}

#[cfg(test)]
mod tests;
