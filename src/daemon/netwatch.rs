//! Telling the daemon when the Mac's network addresses or interfaces change, for example
//! when Tailscale or Wi-Fi goes up or down, so it can drop dead connections and redial.

use super::Event;
use crate::log::log;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::time::Duration;
use tokio::io::unix::AsyncFd;
use tokio::sync::mpsc;
use tokio::time::{Instant, timeout};

/// Wait for the routing messages to stop for this long before reporting a change.
const SETTLE: Duration = Duration::from_millis(500);
/// ...but never longer than this, if the network keeps changing.
const SETTLE_MAX: Duration = Duration::from_secs(3);

/// Send `NetworkChanged` after each burst of address or interface changes.
pub(super) async fn watch_network(tx: mpsc::UnboundedSender<Event>) {
    let sock = match route_socket() {
        Ok(s) => s,
        Err(e) => return log!("cannot watch for network changes: {e}"),
    };
    let mut buf = [0u8; 2048];
    loop {
        if !matches!(next_change(&sock, &mut buf).await, Ok(())) {
            return;
        }
        let deadline = Instant::now() + SETTLE_MAX;
        while Instant::now() < deadline && matches!(timeout(SETTLE, next_change(&sock, &mut buf)).await, Ok(Ok(()))) {}
        if tx.send(Event::NetworkChanged).is_err() {
            return;
        }
    }
}

/// A non-blocking routing socket, which receives a message for every network change.
fn route_socket() -> std::io::Result<AsyncFd<OwnedFd>> {
    // SAFETY: socket() has no memory-safety preconditions; the result is checked before use.
    let fd = unsafe { libc::socket(libc::PF_ROUTE, libc::SOCK_RAW, libc::AF_UNSPEC) };
    if fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: fd is a newly created descriptor that nothing else owns.
    let fd = unsafe { OwnedFd::from_raw_fd(fd) };
    // SAFETY: fd is a valid open descriptor.
    if unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFL, libc::O_NONBLOCK) } < 0 {
        return Err(std::io::Error::last_os_error());
    }
    AsyncFd::new(fd)
}

/// Read routing messages until one is an address or interface change.
async fn next_change(sock: &AsyncFd<OwnedFd>, buf: &mut [u8]) -> std::io::Result<()> {
    loop {
        let mut ready = sock.readable().await?;
        // SAFETY: buf is valid for writes of buf.len() bytes.
        let read = ready.try_io(|fd| match unsafe { libc::read(fd.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len()) } {
            n if n < 0 => Err(std::io::Error::last_os_error()),
            n => Ok(n.unsigned_abs()),
        });
        match read {
            Ok(Ok(0)) => return Err(std::io::ErrorKind::UnexpectedEof.into()),
            Ok(Ok(n)) if is_change(&buf[..n]) => return Ok(()),
            Ok(Ok(_)) | Err(_) => {}
            Ok(Err(e)) => return Err(e),
        }
    }
}

/// Whether a routing message reports an added or removed address, or an interface going up or down.
fn is_change(msg: &[u8]) -> bool {
    msg.get(3).is_some_and(|&t| [libc::RTM_NEWADDR, libc::RTM_DELADDR, libc::RTM_IFINFO].contains(&t.into()))
}

#[cfg(test)]
mod tests;
