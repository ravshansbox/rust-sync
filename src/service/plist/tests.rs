use super::*;

#[test]
fn agent_plist() {
    let xml = plist_xml(Mode::Agent, Path::new("/opt/a&b/<rust-sync>"), false).unwrap();
    assert!(xml.starts_with("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"));
    assert!(xml.contains(&format!("<key>Label</key>\n  <string>{LABEL}</string>\n")));
    assert!(xml.contains("<string>/opt/a&amp;b/&lt;rust-sync&gt;</string>\n    <string>daemon</string>"));
    let log = paths::home().join("Library/Logs/rust-sync.log");
    assert!(xml.contains(&format!("<string>--log-file</string>\n    <string>{}</string>", log.display())));
    assert!(!xml.contains("UserName"));
    assert!(!xml.contains("<key>HOME</key>"));
    assert!(!xml.contains("<key>RUST_SYNC_HOME</key>"));
    assert!(xml.contains("<key>RunAtLoad</key>\n  <true/>"));
    assert!(xml.contains("<key>KeepAlive</key>\n  <dict>\n    <key>SuccessfulExit</key>\n    <false/>"));
    assert!(xml.ends_with("</plist>\n"));
}

#[test]
fn system_plist() {
    let xml = plist_xml(Mode::System, Path::new("/usr/local/bin/rust-sync"), false).unwrap();
    let user = String::from_utf8(Command::new("whoami").output().unwrap().stdout).unwrap();
    assert!(xml.contains(&format!("<key>UserName</key>\n  <string>{}</string>\n", user.trim())));
    assert!(!xml.contains("\n</string>"));
    assert!(xml.contains("<key>EnvironmentVariables</key>\n  <dict>\n"));
    assert!(xml.contains(&format!("<key>HOME</key>\n    <string>{}</string>", paths::home().display())));
}

#[test]
fn plist_keeps_a_custom_state_folder_as_an_absolute_path() {
    let xml = plist_xml(Mode::Agent, Path::new("/usr/local/bin/rust-sync"), true).unwrap();
    let dir = paths::state_dir();
    assert!(dir.is_absolute());
    assert!(xml.contains(&format!("<key>RUST_SYNC_HOME</key>\n    <string>{}</string>", dir.display())));
}
