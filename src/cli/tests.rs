use super::*;
use clap::CommandFactory;

fn parse(args: &[&str]) -> Result<Cli, clap::Error> {
    Cli::try_parse_from(std::iter::once("rust-sync").chain(args.iter().copied()))
}

#[test]
fn cli_definition_is_valid() {
    Cli::command().debug_assert();
}

#[test]
fn list_and_remove_aliases_parse() {
    assert!(matches!(parse(&["path", "ls"]).unwrap().cmd, Cmd::Path { cmd: PathCmd::List }));
    assert!(matches!(parse(&["node", "ls"]).unwrap().cmd, Cmd::Node { cmd: NodeCmd::List }));
    let Cmd::Path { cmd: PathCmd::Remove { path } } = parse(&["path", "rm", "a"]).unwrap().cmd else { panic!() };
    assert_eq!(path, PathBuf::from("a"));
    let Cmd::Node { cmd: NodeCmd::Remove { id } } = parse(&["node", "rm", "x"]).unwrap().cmd else { panic!() };
    assert_eq!(id, "x");
}

#[test]
fn node_add_flags_parse() {
    let cli = parse(&["node", "add", "host:1", "--id", "ab", "-y"]).unwrap();
    let Cmd::Node { cmd: NodeCmd::Add(a) } = cli.cmd else { panic!() };
    assert_eq!((a.addr.as_str(), a.id.as_deref(), a.yes), ("host:1", Some("ab"), true));
    let cli = parse(&["node", "add", "host"]).unwrap();
    let Cmd::Node { cmd: NodeCmd::Add(a) } = cli.cmd else { panic!() };
    assert_eq!((a.id, a.yes), (None, false));
    assert!(parse(&["node", "add"]).is_err());
}

#[test]
fn daemon_and_service_install_flags_parse() {
    let cli = parse(&["daemon", "--port", "7000", "--log-file", "x.log"]).unwrap();
    let Cmd::Daemon { port, log_file } = cli.cmd else { panic!() };
    assert_eq!((port, log_file), (Some(7000), Some(PathBuf::from("x.log"))));
    let Cmd::Daemon { port, log_file } = parse(&["daemon"]).unwrap().cmd else { panic!() };
    assert_eq!((port, log_file), (None, None));
    assert!(parse(&["daemon", "--port", "70000"]).is_err());
    let cli = parse(&["service", "install", "--system"]).unwrap();
    assert!(matches!(cli.cmd, Cmd::Service { cmd: ServiceCmd::Install { system: true } }));
    let cli = parse(&["service", "install"]).unwrap();
    assert!(matches!(cli.cmd, Cmd::Service { cmd: ServiceCmd::Install { system: false } }));
}
