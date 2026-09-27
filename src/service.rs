//! Start the daemon at login with a per-user launchd agent.
//!
//! An agent runs as you, so `~` paths work. Unlike a system daemon it is not exempt
//! from macOS Local Network privacy, which only matters when dialling LAN addresses
//! (see README).

use crate::ctl::{self, Req};
use crate::paths;
use anyhow::{Context, bail};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

const LABEL: &str = "com.github.ravshansbox.rust-sync";

fn plist_path() -> PathBuf {
    paths::home().join("Library/LaunchAgents").join(format!("{LABEL}.plist"))
}

fn log_path() -> PathBuf {
    paths::home().join("Library/Logs/rust-sync.log")
}

/// launchd domain for the logged-in user, e.g. `gui/501`.
fn domain() -> anyhow::Result<String> {
    Ok(format!("gui/{}", std::fs::metadata(paths::home())?.uid()))
}

pub fn installed() -> bool {
    plist_path().exists()
}

pub async fn install() -> anyhow::Result<()> {
    // Not canonicalised on purpose: keep a symlink such as /opt/homebrew/bin/rust-sync,
    // which survives upgrades, rather than the versioned path it points to.
    let exe = std::env::current_exe()?;
    if installed() {
        // Replace the old agent, so a new binary path or setting takes effect.
        unload()?;
    }
    if ctl::call(&Req::Status).await?.is_some() {
        bail!("a daemon is already running. Stop it first (Ctrl-C where it runs), then run this again.");
    }
    let plist = plist_path();
    std::fs::create_dir_all(plist.parent().unwrap())?;
    std::fs::create_dir_all(log_path().parent().unwrap())?;
    std::fs::write(&plist, plist_xml(&exe))?;
    let args = ["bootstrap", &domain()?, plist.to_str().context("path is not valid UTF-8")?];
    if launchctl(&args).is_err() {
        // launchd sometimes refuses with "5: Input/output error", for example while an
        // agent with the same label is still unloading. Clear it and try once more.
        unload()?;
        std::thread::sleep(Duration::from_secs(1));
        launchctl(&args)?;
    }

    outln!("Installed. rust-sync now starts at login, and it is running now.");
    outln!("  Launcher: {}", plist.display());
    outln!("  Log:      {}", log_path().display());
    outln!("  Binary:   {}", exe.display());
    if exe.components().any(|c| c.as_os_str() == "target") {
        outln!(
            "\nNote: this binary is in a Cargo build folder, so `cargo clean` would break the launcher.\n\
             Consider copying it somewhere stable (for example ~/.local/bin) and running\n\
             `rust-sync service install` from there."
        );
    }
    Ok(())
}

pub async fn uninstall() -> anyhow::Result<()> {
    if !installed() {
        outln!("Not installed.");
        return Ok(());
    }
    unload()?;
    std::fs::remove_file(plist_path())?;
    outln!("Uninstalled. rust-sync no longer starts at login. Your config and synced files are unchanged.");
    Ok(())
}

/// Stop the daemon now. It stays installed and starts again at the next login.
pub async fn stop() -> anyhow::Result<()> {
    if !installed() {
        bail!("not installed. Run `rust-sync service install` first.");
    }
    if !running().await? {
        outln!("Not running.");
        return Ok(());
    }
    stop_now().await?;
    outln!("Stopped. It starts again at the next login, or with `rust-sync service start`.");
    Ok(())
}

/// Start the daemon now, if it is installed and not running.
pub async fn start() -> anyhow::Result<()> {
    if !installed() {
        bail!("not installed. Run `rust-sync service install` first.");
    }
    if running().await? {
        outln!("Already running.");
        return Ok(());
    }
    start_now().await?;
    outln!("Started.");
    Ok(())
}

/// Stop and start the daemon, for example to run an upgraded binary.
pub async fn restart() -> anyhow::Result<()> {
    if !installed() {
        bail!("not installed. Run `rust-sync service install` first.");
    }
    if running().await? {
        stop_now().await?;
    }
    start_now().await?;
    outln!("Restarted.");
    Ok(())
}

/// Ask the daemon to exit cleanly. The agent only restarts it after a crash
/// (`KeepAlive` / `SuccessfulExit` = false), so it stays stopped.
async fn stop_now() -> anyhow::Result<()> {
    if loaded()? {
        let _ = launchctl(&["kill", "SIGTERM", &target()?]);
    }
    if !wait_for(false).await? {
        bail!("still running. If you started it with `rust-sync daemon`, stop it there with Ctrl-C.");
    }
    Ok(())
}

async fn start_now() -> anyhow::Result<()> {
    if loaded()? {
        launchctl(&["kickstart", &target()?])?;
    } else {
        // Not loaded, for example after `launchctl bootout`. Loading it starts it (`RunAtLoad`).
        let plist = plist_path();
        launchctl(&["bootstrap", &domain()?, plist.to_str().context("path is not valid UTF-8")?])?;
    }
    if !wait_for(true).await? {
        bail!("did not start. See the log: {}", log_path().display());
    }
    Ok(())
}

/// The agent's launchd name, e.g. `gui/501/com.github.ravshansbox.rust-sync`.
fn target() -> anyhow::Result<String> {
    Ok(format!("{}/{LABEL}", domain()?))
}

/// Whether launchd has the agent loaded (whether or not it is running).
fn loaded() -> anyhow::Result<bool> {
    let out = Command::new("launchctl").args(["print", &target()?]).output().context("cannot run launchctl")?;
    Ok(out.status.success())
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

/// Unload the agent and wait until the daemon has exited.
fn unload() -> anyhow::Result<()> {
    // Fails harmlessly if the agent is not loaded.
    let _ = Command::new("launchctl").args(["bootout", &format!("{}/{LABEL}", domain()?)]).output();
    for _ in 0..50 {
        if !ctl::socket_path()?.exists() {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    Ok(())
}

fn launchctl(args: &[&str]) -> anyhow::Result<()> {
    let out = Command::new("launchctl").args(args).output().context("cannot run launchctl")?;
    if !out.status.success() {
        bail!("launchctl {} failed: {}", args[0], String::from_utf8_lossy(&out.stderr).trim());
    }
    Ok(())
}

fn plist_xml(exe: &Path) -> String {
    let esc = |s: &str| s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;");
    let exe = esc(&exe.to_string_lossy());
    let log = esc(&log_path().to_string_lossy());
    // Keep a custom state folder, if one is in use.
    let env = match std::env::var("RUST_SYNC_HOME") {
        Ok(dir) => format!(
            "  <key>EnvironmentVariables</key>\n  <dict>\n    <key>RUST_SYNC_HOME</key>\n    <string>{}</string>\n  </dict>\n",
            esc(&dir)
        ),
        Err(_) => String::new(),
    };
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>{LABEL}</string>
  <key>ProgramArguments</key>
  <array>
    <string>{exe}</string>
    <string>daemon</string>
    <string>--log-file</string>
    <string>{log}</string>
  </array>
{env}  <key>RunAtLoad</key>
  <true/>
  <!-- Restart after a crash, but not after a clean stop. -->
  <key>KeepAlive</key>
  <dict>
    <key>SuccessfulExit</key>
    <false/>
  </dict>
</dict>
</plist>
"#
    )
}
