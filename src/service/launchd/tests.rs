use super::*;

const RUNNING: &str = "system/com.github.ravshansbox.rust-sync = {\n\tactive count = 1\n\tstate = running\n\n\tprogram = /usr/local/bin/rust-sync\n\tpid = 11398\n\tendpoints = {\n\t\tpid = 1\n\t}\n}\n";
const STOPPED: &str = "system/com.github.ravshansbox.rust-sync = {\n\tactive count = 0\n\tstate = not running\n\n\tprogram = /usr/local/bin/rust-sync\n\tlast exit code = 0\n\tendpoints = {\n\t\tpid = 1\n\t}\n}\n";

#[test]
fn launchctl_print_of_a_running_job_shows_its_process() {
    assert!(shows_process(RUNNING));
}

#[test]
fn launchctl_print_of_a_stopped_job_shows_no_process_even_with_nested_pid_lines() {
    assert!(!shows_process(STOPPED));
}
