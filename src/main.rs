/// `print!` for CLI output. If stdout is closed early, as in `rust-sync node list | head -1`,
/// exit quietly instead of panicking with "Broken pipe".
macro_rules! out {
    ($($arg:tt)*) => { $crate::write_stdout(format_args!($($arg)*)) };
}

/// `println!` for CLI output, see `out!`.
macro_rules! outln {
    () => { out!("\n") };
    ($($arg:tt)*) => { out!("{}\n", format_args!($($arg)*)) };
}

#[cfg(not(target_os = "macos"))]
compile_error!("rust-sync only supports macOS");

mod cli;
mod commands;
mod config;
mod ctl;
mod daemon;
mod index;
mod log;
mod net;
mod paths;
mod service;

use clap::Parser;
use cli::Cli;
use std::io::Write;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    if let Err(e) = commands::run(Cli::parse().cmd).await {
        eprintln!("error: {e:#}");
        std::process::exit(1);
    }
}

fn write_stdout(args: std::fmt::Arguments) {
    let mut stdout = std::io::stdout().lock();
    if let Err(e) = stdout.write_fmt(args).and_then(|()| stdout.flush()) {
        if e.kind() == std::io::ErrorKind::BrokenPipe {
            std::process::exit(0);
        }
        eprintln!("error: cannot write output: {e}");
        std::process::exit(1);
    }
}
