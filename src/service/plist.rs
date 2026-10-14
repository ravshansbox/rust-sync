//! Writing, removing and building the launchd property list (plist) file.

use super::{LABEL, Mode, log_path};
use crate::paths;
use anyhow::{Context, bail};
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

/// Writes the plist file for the mode, using sudo for a system daemon.
pub(super) fn write_plist(mode: Mode, xml: &str) -> anyhow::Result<()> {
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

/// Removes the plist file for the mode, using sudo for a system daemon.
pub(super) fn remove_plist(mode: Mode) -> anyhow::Result<()> {
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

/// Returns the name of the current user, from `id -un`.
fn user_name() -> anyhow::Result<String> {
    let out = Command::new("id").arg("-un").output().context("cannot run id")?;
    if !out.status.success() {
        bail!("cannot find your user name");
    }
    Ok(String::from_utf8(out.stdout)?.trim().to_string())
}

/// Builds the plist XML that runs the daemon with its log file and any needed environment variables.
pub(super) fn plist_xml(mode: Mode, exe: &Path, custom_state: bool) -> anyhow::Result<String> {
    let esc = |s: &str| s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;");
    let exe = esc(&exe.to_string_lossy());
    let log = esc(&log_path().to_string_lossy());
    let mut vars = Vec::new();
    let user = if mode == Mode::System {
        vars.push(("HOME", paths::home().to_string_lossy().into_owned()));
        format!("  <key>UserName</key>\n  <string>{}</string>\n", esc(&user_name()?))
    } else {
        String::new()
    };
    // Keep a custom state folder, if one is in use.
    if custom_state {
        vars.push(("RUST_SYNC_HOME", paths::state_dir().to_string_lossy().into_owned()));
    }
    let env = if vars.is_empty() {
        String::new()
    } else {
        let entries: String =
            vars.iter().map(|(k, v)| format!("    <key>{k}</key>\n    <string>{}</string>\n", esc(v))).collect();
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

#[cfg(test)]
mod tests;
