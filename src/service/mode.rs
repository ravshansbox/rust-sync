use super::LABEL;
use crate::paths;
use anyhow::{Context, bail};
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use std::process::Command;

#[derive(Clone, Copy, PartialEq)]
pub(super) enum Mode {
    Agent,
    System,
}

impl Mode {
    pub(super) fn installed() -> Option<Self> {
        [Self::System, Self::Agent].into_iter().find(|m| m.plist().exists())
    }

    pub(super) fn plist(self) -> PathBuf {
        match self {
            Self::Agent => paths::home().join("Library/LaunchAgents").join(format!("{LABEL}.plist")),
            Self::System => PathBuf::from(format!("/Library/LaunchDaemons/{LABEL}.plist")),
        }
    }

    /// launchd domain, e.g. `gui/501` or `system`.
    pub(super) fn domain(self) -> anyhow::Result<String> {
        match self {
            Self::Agent => Ok(format!("gui/{}", uid()?)),
            Self::System => Ok("system".into()),
        }
    }

    /// The launchd name, e.g. `gui/501/com.github.ravshansbox.rust-sync`.
    pub(super) fn target(self) -> anyhow::Result<String> {
        Ok(format!("{}/{LABEL}", self.domain()?))
    }

    /// When the daemon starts by itself: "login" or "boot".
    pub(super) const fn when(self) -> &'static str {
        match self {
            Self::Agent => "login",
            Self::System => "boot",
        }
    }

    /// Run `launchctl`, with `sudo` for the system domain.
    pub(super) fn launchctl(self, args: &[&str]) -> anyhow::Result<std::process::Output> {
        let mut cmd = match self {
            Self::Agent => Command::new("launchctl"),
            Self::System => {
                let mut sudo = Command::new("sudo");
                sudo.arg("launchctl");
                sudo
            }
        };
        cmd.args(args).output().context("cannot run launchctl")
    }
}

/// Ask for the password once, so later `sudo` calls do not prompt with their output hidden.
pub(super) fn sudo_login() -> anyhow::Result<()> {
    if !Command::new("sudo").arg("-v").status().context("cannot run sudo")?.success() {
        bail!("sudo failed");
    }
    Ok(())
}

pub(super) fn uid() -> anyhow::Result<u32> {
    Ok(std::fs::metadata(paths::home())?.uid())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agent_target_is_in_the_gui_domain_of_the_current_user() {
        let uid = String::from_utf8(Command::new("id").arg("-u").output().unwrap().stdout).unwrap();
        assert_eq!(Mode::Agent.target().unwrap(), format!("gui/{}/{LABEL}", uid.trim()));
    }
}
