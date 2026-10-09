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
use std::io::Write;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

const LABEL: &str = "com.github.ravshansbox.rust-sync";

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Agent,
    System,
}

impl Mode {
    fn plist(self) -> PathBuf {
        match self {
            Mode::Agent => paths::home().join("Library/LaunchAgents").join(format!("{LABEL}.plist")),
            Mode::System => PathBuf::from(format!("/Library/LaunchDaemons/{LABEL}.plist")),
        }
    }

    /// launchd domain, e.g. `gui/501` or `system`.
    fn domain(self) -> anyhow::Result<String> {
        match self {
            Mode::Agent => Ok(format!("gui/{}", uid()?)),
            Mode::System => Ok("system".into()),
        }
    }

    /// The launchd name, e.g. `gui/501/com.github.ravshansbox.rust-sync`.
    fn target(self) -> anyhow::Result<String> {
        Ok(format!("{}/{LABEL}", self.domain()?))
    }

    /// When the daemon starts by itself: "login" or "boot".
    fn when(self) -> &'static str {
        match self {
            Mode::Agent => "login",
            Mode::System => "boot",
        }
    }

    /// Run `launchctl`, with `sudo` for the system domain.
    fn launchctl(self, args: &[&str]) -> anyhow::Result<std::process::Output> {
        let mut cmd = match self {
            Mode::Agent => Command::new("launchctl"),
            Mode::System => {
                let mut sudo = Command::new("sudo");
                sudo.arg("launchctl");
                sudo
            }
        };
        cmd.args(args).output().context("cannot run launchctl")
    }
}

fn log_path() -> PathBuf {
    paths::home().join("Library/Logs/rust-sync.log")
}

fn uid() -> anyhow::Result<u32> {
    Ok(std::fs::metadata(paths::home())?.uid())
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
    for old in [Mode::Agent, Mode::System] {
        if old.plist().exists() {
            // Replace the old launcher, so a new binary path or setting takes effect.
            unload(old)?;
            if old != mode {
                remove_plist(old)?;
            }
        }
    }
    if ctl::call(&Req::Status).await?.is_some() {
        bail!("a daemon is already running. Stop it first (Ctrl-C where it runs), then run this again.");
    }
    let plist = mode.plist();
    std::fs::create_dir_all(log_path().parent().unwrap())?;
    write_plist(mode, &plist_xml(mode, &exe)?)?;
    let plist_str = plist.to_str().context("path is not valid UTF-8")?;
    let domain = mode.domain()?;
    let args = ["bootstrap", &domain, plist_str];
    if launchctl(mode, &args).is_err() {
        // launchd sometimes refuses with "5: Input/output error", for example while a
        // job with the same label is still unloading. Clear it and try once more.
        unload(mode)?;
        std::thread::sleep(Duration::from_secs(1));
        launchctl(mode, &args)?;
    }

    outln!("Installed. rust-sync now starts at {}, and it is running now.", mode.when());
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
    let Some(mode) = installed_mode() else {
        outln!("Not installed.");
        return Ok(());
    };
    if Mode::System.plist().exists() {
        sudo_login()?;
    }
    for old in [Mode::Agent, Mode::System] {
        if old.plist().exists() {
            unload(old)?;
            remove_plist(old)?;
        }
    }
    outln!(
        "Uninstalled. rust-sync no longer starts at {}. Your config and synced files are unchanged.",
        mode.when()
    );
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
        let _ = launchctl(mode, &["kill", "SIGTERM", &mode.target()?]);
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
        let plist = mode.plist();
        launchctl(mode, &["bootstrap", &mode.domain()?, plist.to_str().context("path is not valid UTF-8")?])?;
    }
    if !wait_for(true).await? {
        bail!("did not start. See the log: {}", log_path().display());
    }
    Ok(())
}

/// Whether launchd has the job loaded (whether or not it is running).
fn loaded(mode: Mode) -> anyhow::Result<bool> {
    let out = Command::new("launchctl").args(["print", &mode.target()?]).output().context("cannot run launchctl")?;
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

/// Unload the job and wait until the daemon has exited.
fn unload(mode: Mode) -> anyhow::Result<()> {
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

fn launchctl(mode: Mode, args: &[&str]) -> anyhow::Result<()> {
    let out = mode.launchctl(args)?;
    if !out.status.success() {
        bail!("launchctl {} failed: {}", args[0], String::from_utf8_lossy(&out.stderr).trim());
    }
    Ok(())
}

fn write_plist(mode: Mode, xml: &str) -> anyhow::Result<()> {
    let plist = mode.plist();
    match mode {
        Mode::Agent => {
            std::fs::create_dir_all(plist.parent().unwrap())?;
            std::fs::write(&plist, xml)?;
        }
        Mode::System => {
            // `sudo tee` creates the file owned by root, as launchd requires for daemons.
            let mut child = Command::new("sudo")
                .arg("tee")
                .arg(&plist)
                .stdin(Stdio::piped())
                .stdout(Stdio::null())
                .spawn()
                .context("cannot run sudo")?;
            child.stdin.take().unwrap().write_all(xml.as_bytes())?;
            if !child.wait()?.success() {
                bail!("cannot write {}", plist.display());
            }
        }
    }
    Ok(())
}

fn remove_plist(mode: Mode) -> anyhow::Result<()> {
    let plist = mode.plist();
    match mode {
        Mode::Agent => std::fs::remove_file(&plist)?,
        Mode::System => {
            if !Command::new("sudo").arg("rm").arg("-f").arg(&plist).status().context("cannot run sudo")?.success() {
                bail!("cannot remove {}", plist.display());
            }
        }
    }
    Ok(())
}

fn user_name() -> anyhow::Result<String> {
    let out = Command::new("id").arg("-un").output().context("cannot run id")?;
    if !out.status.success() {
        bail!("cannot find your user name");
    }
    Ok(String::from_utf8(out.stdout)?.trim().to_string())
}

fn plist_xml(mode: Mode, exe: &Path) -> anyhow::Result<String> {
    let esc = |s: &str| s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;");
    let exe = esc(&exe.to_string_lossy());
    let log = esc(&log_path().to_string_lossy());
    let mut vars = Vec::new();
    let mut user = String::new();
    if mode == Mode::System {
        user = format!("  <key>UserName</key>\n  <string>{}</string>\n", esc(&user_name()?));
        vars.push(("HOME".to_string(), paths::home().to_string_lossy().into_owned()));
    }
    // Keep a custom state folder, if one is in use.
    if let Ok(dir) = std::env::var("RUST_SYNC_HOME") {
        vars.push(("RUST_SYNC_HOME".to_string(), dir));
    }
    let env = if vars.is_empty() {
        String::new()
    } else {
        let entries: String = vars
            .iter()
            .map(|(k, v)| format!("    <key>{k}</key>\n    <string>{}</string>\n", esc(v)))
            .collect();
        format!("  <key>EnvironmentVariables</key>\n  <dict>\n{entries}  </dict>\n")
    };
    Ok(format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>{LABEL}</string>
{user}  <key>ProgramArguments</key>
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
    ))
}
