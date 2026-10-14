use super::Mode;
use crate::ctl;
use anyhow::{Context, bail};
use std::process::Command;
use std::time::Duration;

pub(super) fn bootstrap(mode: Mode) -> anyhow::Result<()> {
    let plist = mode.plist();
    launchctl(mode, &["bootstrap", &mode.domain()?, plist.to_str().context("path is not valid UTF-8")?])
}

pub(super) fn bootstrap_with_retry(mode: Mode) -> anyhow::Result<()> {
    if bootstrap(mode).is_err() {
        // launchd sometimes refuses with "5: Input/output error", for example while a
        // job with the same label is still unloading. Clear it and try once more.
        unload(mode)?;
        std::thread::sleep(Duration::from_secs(1));
        bootstrap(mode)?;
    }
    Ok(())
}

/// Whether launchd has the job loaded (whether or not it is running).
pub(super) fn loaded(mode: Mode) -> anyhow::Result<bool> {
    let out = Command::new("launchctl").args(["print", &mode.target()?]).output().context("cannot run launchctl")?;
    Ok(out.status.success())
}

/// Unload the job and wait until the daemon has exited.
pub(super) fn unload(mode: Mode) -> anyhow::Result<()> {
    // Fails harmlessly if the job is not loaded.
    let _ = mode.launchctl(&["bootout", &mode.target()?]);
    for _ in 0..50 {
        if !ctl::socket_path()?.exists() {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    Ok(())
}

/// Ask the daemon to exit. Fails harmlessly if it is not running.
pub(super) fn kill(mode: Mode) -> anyhow::Result<()> {
    let _ = mode.launchctl(&["kill", "SIGTERM", &mode.target()?]);
    Ok(())
}

pub(super) fn kickstart(mode: Mode) -> anyhow::Result<()> {
    launchctl(mode, &["kickstart", &mode.target()?])
}

fn launchctl(mode: Mode, args: &[&str]) -> anyhow::Result<()> {
    let out = mode.launchctl(args)?;
    if !out.status.success() {
        bail!("launchctl {} failed: {}", args[0], String::from_utf8_lossy(&out.stderr).trim());
    }
    Ok(())
}
