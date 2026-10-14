//! Adding a node after checking its ID.

use super::{my_keys, send};
use crate::cli::NodeAdd;
use crate::config::{self, Config};
use crate::ctl::Req;
use crate::net::{self, Msg};
use crate::paths;
use anyhow::{Context, bail};
use std::time::Duration;
use tokio::time::timeout;

/// Connect to the node, show its ID, and trust it once the user confirms.
pub(super) async fn node_add(a: NodeAdd) -> anyhow::Result<()> {
    let keys = my_keys()?;
    let cfg = Config::load(&paths::config_file())?;
    let addr = config::with_port(&a.addr);
    let expected = a.id.as_deref().map(node_id).transpose()?;

    let (id, trusts_us) = probe(&addr, &keys, &cfg).await?;

    outln!("Node ID: {}", net::pretty_id(&id));
    match expected {
        Some(e) if e != id => bail!("that is not the ID you gave ({}). Node not added.", net::pretty_id(&e)),
        Some(_) => {}
        None if a.yes => {}
        None => {
            out!("Check this matches `rust-sync id` on that machine. Trust it? [y/N] ");
            let mut answer = String::new();
            std::io::stdin().read_line(&mut answer)?;
            if !answer.trim().eq_ignore_ascii_case("y") {
                outln!("Node not added.");
                return Ok(());
            }
        }
    }
    send(Req::AddNode { id, addr }).await?;
    if !trusts_us {
        outln!(
            "\nThat node does not trust this one yet. On that machine, run\n  \
             rust-sync node add <address of this machine>\nand check it shows this ID:\n  {}",
            net::pretty_id(&keys.id())
        );
    }
    Ok(())
}

pub(super) fn node_id(s: &str) -> anyhow::Result<String> {
    net::parse_id(s).context("not a valid node ID")
}

/// Connect to the node at addr and return its ID and whether it trusts this node.
async fn probe(addr: &str, keys: &net::Keys, cfg: &Config) -> anyhow::Result<(String, bool)> {
    outln!("Connecting to {addr}...");
    let stream = timeout(Duration::from_secs(10), tokio::net::TcpStream::connect(addr))
        .await
        .context("timed out")?
        .with_context(|| format!("cannot connect to {addr}. Is `rust-sync daemon` running there?"))?;
    let (mut r, mut w, id) = net::handshake(stream, keys, true).await?;
    if id == keys.id() {
        bail!("that is this node");
    }
    w.send(&Msg::Hello { port: cfg.port, probe: true, trusts_you: cfg.trusted(&id).is_some() }).await?;
    let trusts_us = matches!(
        timeout(Duration::from_secs(10), r.recv()).await,
        Ok(Ok(Msg::Hello { trusts_you: true, .. }))
    );
    Ok((id, trusts_us))
}
