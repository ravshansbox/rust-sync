use super::*;

#[test]
fn agent_target_is_in_the_gui_domain_of_the_current_user() {
    let uid = String::from_utf8(Command::new("id").arg("-u").output().unwrap().stdout).unwrap();
    assert_eq!(Mode::Agent.target().unwrap(), format!("gui/{}/{LABEL}", uid.trim()));
}
