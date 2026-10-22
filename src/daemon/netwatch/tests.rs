use super::*;

#[test]
fn address_and_interface_messages_are_network_changes() {
    let msg = |t: libc::c_int| [0, 0, libc::RTM_VERSION as u8, t as u8];
    for t in [libc::RTM_NEWADDR, libc::RTM_DELADDR, libc::RTM_IFINFO] {
        assert!(is_change(&msg(t)), "{t}");
    }
    for t in [libc::RTM_ADD, libc::RTM_DELETE, libc::RTM_RESOLVE, libc::RTM_MISS] {
        assert!(!is_change(&msg(t)), "{t}");
    }
    assert!(!is_change(&[]));
}

#[tokio::test]
async fn route_socket_opens() {
    route_socket().unwrap();
}
