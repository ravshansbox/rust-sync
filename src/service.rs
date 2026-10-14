//! Start the daemon automatically with launchd.
//!
//! By default this is a per-user agent, which starts at login. An agent runs as you,
//! so `~` paths work, but it is not exempt from macOS Local Network privacy, which
//! only matters when dialling LAN addresses (see README).
//!
//! With `--system` it is a system daemon in /Library/LaunchDaemons that runs as you
//! and starts at boot, without anyone logging in. Daemons are exempt from Local
//! Network privacy. Installing it needs `sudo`.

use crate::ctl::{self, Req};
use crate::paths;
use anyhow::{Context, bail};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

mod launchd;
mod mode;
mod plist;

use launchd::{bootstrap, launchctl, loaded, unload};
use mode::{Mode, uid};
use plist::{plist_xml, remove_plist, write_plist};

const LABEL: &str = "com.github.ravshansbox.rust-sync";

fn log_path() -> PathBuf {
    paths::home().join("Library/Logs/rust-sync.log")
}

fn installed_mode() -> Option<Mode> {
    [Mode::System, Mode::Agent].into_iter().find(|m| m.plist().exists())
}

/// When the daemon starts by itself ("login" or "boot"), or `None` if it is not installed.
pub fn starts_at() -> Option<&'static str> {
    installed_mode().map(Mode::when)
}

/// Ask for the password once, so later `sudo` calls do not prompt with their output hidden.
fn sudo_login() -> anyhow::Result<()> {
    if !Command::new("sudo").arg("-v").status().context("cannot run sudo")?.success() {
        bail!("sudo failed");
    }
    Ok(())
}

pub async fn install(system: bool) -> anyhow::Result<()> {
    let mode = if system { Mode::System } else { Mode::Agent };
    if mode == Mode::System && uid()? == 0 {
        bail!("run this as your own user, without sudo. It asks for your password when needed.");
    }
    // Not canonicalised on purpose: keep a symlink such as /opt/homebrew/bin/rust-sync,
    // which survives upgrades, rather than the versioned path it points to.
    let exe = std::env::current_exe()?;
    if mode == Mode::System || Mode::System.plist().exists() {
        sudo_login()?;
    }
    // Replace the old launcher, so a new binary path or setting takes effect.
    unload_installed(Some(mode))?;
    if running().await? {
        bail!("a daemon is already running. Stop it first (Ctrl-C where it runs), then run this again.");
    }
    std::fs::create_dir_all(log_path().parent().unwrap())?;
    write_plist(mode, &plist_xml(mode, &exe, std::env::var_os("RUST_SYNC_HOME").is_some())?)?;
    if bootstrap(mode).is_err() {
        // launchd sometimes refuses with "5: Input/output error", for example while a
        // job with the same label is still unloading. Clear it and try once more.
        unload(mode)?;
        std::thread::sleep(Duration::from_secs(1));
        bootstrap(mode)?;
    }
    print_installed(mode, &exe);
    Ok(())
}

fn print_installed(mode: Mode, exe: &Path) {
    outln!("Installed. rust-sync now starts at {}, and it is running now.", mode.when());
    outln!("  Launcher: {}", mode.plist().display());
    outln!("  Log:      {}", log_path().display());
    outln!("  Binary:   {}", exe.display());
    if exe.components().any(|c| c.as_os_str() == "target") {
        outln!(
            "\nNote: this binary is in a Cargo build folder, so `cargo clean` would break the launcher.\n\
             Consider copying it somewhere stable (for example ~/.local/bin) and running\n\
             `rust-sync service install` from there."
        );
    }
}

pub fn uninstall() -> anyhow::Result<()> {
    let Some(mode) = installed_mode() else {
        outln!("Not installed.");
        return Ok(());
    };
    if Mode::System.plist().exists() {
        sudo_login()?;
    }
    unload_installed(None)?;
    outln!(
        "Uninstalled. rust-sync no longer starts at {}. Your config and synced files are unchanged.",
        mode.when()
    );
    Ok(())
}

/// Unload every installed launcher and remove its plist, except the plist of `keep`.
fn unload_installed(keep: Option<Mode>) -> anyhow::Result<()> {
    for old in [Mode::Agent, Mode::System] {
        if old.plist().exists() {
            unload(old)?;
            if Some(old) != keep {
                remove_plist(old)?;
            }
        }
    }
    Ok(())
}

/// Stop the daemon now. It stays installed and starts again at the next login or boot.
pub async fn stop() -> anyhow::Result<()> {
    let mode = installed_or_bail()?;
    if !running().await? {
        outln!("Not running.");
        return Ok(());
    }
    stop_now(mode).await?;
    outln!("Stopped. It starts again at the next {}, or with `rust-sync service start`.", mode.when());
    Ok(())
}

/// Start the daemon now, if it is installed and not running.
pub async fn start() -> anyhow::Result<()> {
    let mode = installed_or_bail()?;
    if running().await? {
        outln!("Already running.");
        return Ok(());
    }
    start_now(mode).await?;
    outln!("Started.");
    Ok(())
}

/// Stop and start the daemon, for example to run an upgraded binary.
pub async fn restart() -> anyhow::Result<()> {
    let mode = installed_or_bail()?;
    if running().await? {
        stop_now(mode).await?;
    }
    start_now(mode).await?;
    outln!("Restarted.");
    Ok(())
}

fn installed_or_bail() -> anyhow::Result<Mode> {
    let Some(mode) = installed_mode() else { bail!("not installed. Run `rust-sync service install` first.") };
    if mode == Mode::System {
        sudo_login()?;
    }
    Ok(mode)
}

/// Ask the daemon to exit cleanly. launchd only restarts it after a crash
/// (`KeepAlive` / `SuccessfulExit` = false), so it stays stopped.
async fn stop_now(mode: Mode) -> anyhow::Result<()> {
    if loaded(mode)? {
        let _ = mode.launchctl(&["kill", "SIGTERM", &mode.target()?]);
    }
    if !wait_for(false).await? {
        bail!("still running. If you started it with `rust-sync daemon`, stop it there with Ctrl-C.");
    }
    Ok(())
}

async fn start_now(mode: Mode) -> anyhow::Result<()> {
    if loaded(mode)? {
        launchctl(mode, &["kickstart", &mode.target()?])?;
    } else {
        // Not loaded, for example after `launchctl bootout`. Loading it starts it (`RunAtLoad`).
        bootstrap(mode)?;
    }
    if !wait_for(true).await? {
        bail!("did not start. See the log: {}", log_path().display());
    }
    Ok(())
}

async fn running() -> anyhow::Result<bool> {
    Ok(ctl::call(&Req::Status).await?.is_some())
}

/// Wait up to 5 seconds for the daemon to be running (or stopped).
async fn wait_for(want: bool) -> anyhow::Result<bool> {
    for _ in 0..50 {
        if running().await? == want {
            return Ok(true);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Ok(false)
}
