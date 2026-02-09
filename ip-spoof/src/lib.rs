//! LD_PRELOAD library for Veilid devnet IP translation and BOOT response injection.
//!
//! Translates between fake "global" IPs (1.2.3.x) and localhost so multiple
//! Veilid nodes can run on a single machine.
//!
//! Mapping (port-based):
//!   Port 5160 (BASE+0) = 1.2.3.1 (bootstrap)
//!   Port 5161 (BASE+1) = 1.2.3.2
//!   ...
//!
//! Environment variables:
//!   VEILID_BASE_PORT      — Base port (default 5160)
//!   VEILID_PEER_INFO_FILE — Path to peer info JSON for BOOT injection

// Static muts are safe here: BASE_PORT/PEER_INFO_FILE are write-once behind Once,
// CACHED_PEER_INFO/PEER_INFO_MTIME are only touched from hooked libc calls (single-threaded).
#![allow(static_mut_refs)]

use std::fs;
use std::sync::Once;
use std::time::SystemTime;

use libc::{
    c_int, c_void, msghdr, sockaddr, sockaddr_in, sockaddr_in6, socklen_t, ssize_t, AF_INET,
    AF_INET6, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR, SO_TYPE,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Fake global IPv4 base: 1.2.3.0 in host byte order
const FAKE_GLOBAL_BASE: u32 = 0x0102_0300;
const FAKE_GLOBAL_MASK: u32 = 0xFFFF_FF00;

/// Loopback: 127.0.0.1 in host byte order
const LOOPBACK_ADDR: u32 = 0x7F00_0001;

/// Fake global IPv6 prefix (first 12 bytes): 2607:f8b0:4004::
const FAKE_GLOBAL_V6_PREFIX: [u8; 12] = [0x26, 0x07, 0xf8, 0xb0, 0x40, 0x04, 0, 0, 0, 0, 0, 0];

/// IPv6 loopback ::1
const LOOPBACK_V6: [u8; 16] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];

/// Maximum port offset from BASE_PORT (40 nodes max)
const MAX_PORT_OFFSET: u16 = 40;

// ---------------------------------------------------------------------------
// Global state (mirrors the C static variables)
// ---------------------------------------------------------------------------

static INIT: Once = Once::new();

static mut BASE_PORT: u16 = 5160;
static mut PEER_INFO_FILE: Option<String> = None;
static mut CACHED_PEER_INFO: Option<Vec<u8>> = None;
static mut PEER_INFO_MTIME: Option<SystemTime> = None;

/// One-time init: read env vars.
fn ensure_init() {
    INIT.call_once(|| unsafe {
        if let Ok(val) = std::env::var("VEILID_BASE_PORT") {
            if let Ok(port) = val.parse::<u16>() {
                if port > 0 {
                    BASE_PORT = port;
                }
            }
        }

        PEER_INFO_FILE = std::env::var("VEILID_PEER_INFO_FILE").ok();

        eprintln!(
            "[ip_spoof] LD_PRELOAD loaded — translating 1.2.3.x <-> 127.0.0.1 (base port: {})",
            BASE_PORT
        );
        if let Some(ref path) = PEER_INFO_FILE {
            eprintln!("[ip_spoof] BOOT response injection enabled from: {}", path);
        }
    });
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

#[inline]
fn base_port() -> u16 {
    unsafe { BASE_PORT }
}

#[inline]
fn is_fake_global(addr_host: u32) -> bool {
    (addr_host & FAKE_GLOBAL_MASK) == FAKE_GLOBAL_BASE
}

#[inline]
fn is_loopback(addr_host: u32) -> bool {
    (addr_host & 0xFF00_0000) == 0x7F00_0000
}

#[inline]
fn is_loopback_v6(addr: &[u8; 16]) -> bool {
    *addr == LOOPBACK_V6
}

#[inline]
fn is_fake_global_v6(addr: &[u8; 16]) -> bool {
    addr[..12] == FAKE_GLOBAL_V6_PREFIX
}

#[inline]
fn is_devnet_port(port: u16) -> bool {
    port >= base_port() && port < base_port() + MAX_PORT_OFFSET
}

#[inline]
fn is_excluded_port(port: u16) -> bool {
    port == 53 || port == 5353
}

/// Convert port to fake-global IP suffix. Returns 0 for non-devnet ports.
#[inline]
fn port_to_ip_suffix(port: u16) -> u8 {
    if is_devnet_port(port) {
        (port - base_port() + 1) as u8
    } else {
        0
    }
}

/// Read the local port of a socket via the *real* getsockname.
unsafe fn local_port_of(sockfd: c_int) -> u16 {
    let mut storage: libc::sockaddr_storage = std::mem::zeroed();
    let mut len: socklen_t = std::mem::size_of::<libc::sockaddr_storage>() as socklen_t;
    let ret =
        redhook::real!(getsockname)(sockfd, &mut storage as *mut _ as *mut sockaddr, &mut len);
    if ret != 0 {
        return 0;
    }
    match storage.ss_family as c_int {
        AF_INET => {
            let sin = &*(&storage as *const _ as *const sockaddr_in);
            u16::from_be(sin.sin_port)
        }
        AF_INET6 => {
            let sin6 = &*(&storage as *const _ as *const sockaddr_in6);
            u16::from_be(sin6.sin6_port)
        }
        _ => 0,
    }
}

// ---------------------------------------------------------------------------
// Address rewriting
// ---------------------------------------------------------------------------

/// Rewrite loopback → fake-global on RECEIVE (port-range gated).
unsafe fn rewrite_recv_addr(addr: *mut sockaddr) {
    if addr.is_null() {
        return;
    }
    match (*addr).sa_family as c_int {
        AF_INET => {
            let sin = &mut *(addr as *mut sockaddr_in);
            let addr_host = u32::from_be(sin.sin_addr.s_addr);
            if is_loopback(addr_host) {
                let port = u16::from_be(sin.sin_port);
                if is_excluded_port(port) {
                    return;
                }
                let suffix = port_to_ip_suffix(port);
                if suffix == 0 {
                    return;
                }
                sin.sin_addr.s_addr = (FAKE_GLOBAL_BASE | suffix as u32).to_be();
            }
        }
        AF_INET6 => {
            let sin6 = &mut *(addr as *mut sockaddr_in6);
            let octets: &mut [u8; 16] = &mut *(&mut sin6.sin6_addr as *mut _ as *mut [u8; 16]);
            if is_loopback_v6(octets) {
                let port = u16::from_be(sin6.sin6_port);
                if is_excluded_port(port) {
                    return;
                }
                let suffix = port_to_ip_suffix(port);
                if suffix == 0 {
                    return;
                }
                octets[..12].copy_from_slice(&FAKE_GLOBAL_V6_PREFIX);
                octets[12] = 0;
                octets[13] = 0;
                octets[14] = 0;
                octets[15] = suffix;
            }
        }
        _ => {}
    }
}

/// Force-rewrite loopback → fake-global, using suffix 99 for non-devnet ports.
unsafe fn rewrite_recv_addr_force(addr: *mut sockaddr) {
    if addr.is_null() {
        return;
    }
    match (*addr).sa_family as c_int {
        AF_INET => {
            let sin = &mut *(addr as *mut sockaddr_in);
            let addr_host = u32::from_be(sin.sin_addr.s_addr);
            if is_loopback(addr_host) {
                let port = u16::from_be(sin.sin_port);
                if is_excluded_port(port) {
                    return;
                }
                let mut suffix = port_to_ip_suffix(port);
                if suffix == 0 {
                    suffix = 99;
                }
                sin.sin_addr.s_addr = (FAKE_GLOBAL_BASE | suffix as u32).to_be();
            }
        }
        AF_INET6 => {
            let sin6 = &mut *(addr as *mut sockaddr_in6);
            let octets: &mut [u8; 16] = &mut *(&mut sin6.sin6_addr as *mut _ as *mut [u8; 16]);
            if is_loopback_v6(octets) {
                let port = u16::from_be(sin6.sin6_port);
                if is_excluded_port(port) {
                    return;
                }
                let mut suffix = port_to_ip_suffix(port);
                if suffix == 0 {
                    suffix = 99;
                }
                octets[..12].copy_from_slice(&FAKE_GLOBAL_V6_PREFIX);
                octets[12] = 0;
                octets[13] = 0;
                octets[14] = 0;
                octets[15] = suffix;
            }
        }
        _ => {}
    }
}

/// Rewrite fake-global → loopback on SEND.
unsafe fn rewrite_send_addr(addr: *mut sockaddr) {
    if addr.is_null() {
        return;
    }
    match (*addr).sa_family as c_int {
        AF_INET => {
            let sin = &mut *(addr as *mut sockaddr_in);
            let addr_host = u32::from_be(sin.sin_addr.s_addr);
            if is_fake_global(addr_host) {
                sin.sin_addr.s_addr = LOOPBACK_ADDR.to_be();
            }
        }
        AF_INET6 => {
            let sin6 = &mut *(addr as *mut sockaddr_in6);
            let octets: &mut [u8; 16] = &mut *(&mut sin6.sin6_addr as *mut _ as *mut [u8; 16]);
            if is_fake_global_v6(octets) {
                *octets = LOOPBACK_V6;
            }
        }
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// BOOT response injection
// ---------------------------------------------------------------------------

/// Reload peer info file if changed.
unsafe fn load_peer_info() {
    let path = match &PEER_INFO_FILE {
        Some(p) => p.clone(),
        None => return,
    };

    let meta = match fs::metadata(&path) {
        Ok(m) => m,
        Err(_) => return,
    };

    let mtime = meta.modified().ok();
    if mtime == PEER_INFO_MTIME && CACHED_PEER_INFO.is_some() {
        return;
    }

    if let Ok(data) = fs::read(&path) {
        if !data.is_empty() {
            eprintln!(
                "[ip_spoof] Loaded peer info ({} bytes) from {}",
                data.len(),
                path
            );
            PEER_INFO_MTIME = mtime;
            CACHED_PEER_INFO = Some(data);
        }
    }
}

/// Veilid assembly buffer V1 header length (version + reserved + seq + offset + total_len).
const AB_HEADER_LEN: usize = 8;
const AB_VERSION_1: u8 = 1;

/// Check whether `buf` contains an empty JSON array `[]` — either raw or wrapped
/// in a Veilid assembly-buffer V1 frame — and we have peer info to inject.
unsafe fn should_inject_boot_response(buf: *const c_void, len: usize) -> bool {
    if PEER_INFO_FILE.is_none() || buf.is_null() || len < 2 {
        return false;
    }

    let slice = std::slice::from_raw_parts(buf as *const u8, len);

    // Check for assembly-buffer-framed `[]`:
    //   Header: [VERSION_1, 0x00, seq(2), offset(2), total_len(2)] + payload
    //   For `[]`: total packet = 8 + 2 = 10 bytes, offset=0, total_len=2, payload="[]"
    if len == AB_HEADER_LEN + 2
        && slice[0] == AB_VERSION_1
        && slice[1] == 0
        && slice[4] == 0
        && slice[5] == 0 // offset = 0
        && slice[6] == 0
        && slice[7] == 2 // total_len = 2
        && slice[8] == b'['
        && slice[9] == b']'
    {
        load_peer_info();
        return CACHED_PEER_INFO.as_ref().is_some_and(|v| !v.is_empty());
    }

    // Fallback: raw (unframed) `[]`
    let trimmed = match std::str::from_utf8(slice) {
        Ok(s) => s.trim_start(),
        Err(_) => return false,
    };

    if trimmed.starts_with("[]") {
        load_peer_info();
        CACHED_PEER_INFO.as_ref().is_some_and(|v| !v.is_empty())
    } else {
        false
    }
}

// ---------------------------------------------------------------------------
// Hooks
// ---------------------------------------------------------------------------

redhook::hook! {
    unsafe fn recvfrom(
        sockfd: c_int,
        buf: *mut c_void,
        len: usize,
        flags: c_int,
        src_addr: *mut sockaddr,
        addrlen: *mut socklen_t
    ) -> ssize_t => my_recvfrom {
        ensure_init();

        let ret = redhook::real!(recvfrom)(sockfd, buf, len, flags, src_addr, addrlen);
        if ret >= 0 && !src_addr.is_null() {
            let local_port = local_port_of(sockfd);
            if is_devnet_port(local_port) {
                rewrite_recv_addr_force(src_addr);
            } else {
                rewrite_recv_addr(src_addr);
            }
        }
        ret
    }
}

redhook::hook! {
    unsafe fn sendto(
        sockfd: c_int,
        buf: *const c_void,
        len: usize,
        flags: c_int,
        dest_addr: *const sockaddr,
        addrlen: socklen_t
    ) -> ssize_t => my_sendto {
        ensure_init();

        let mut actual_buf = buf;
        let mut actual_len = len;
        // Holds the framed replacement; must outlive the sendto call.
        let mut _framed_buf: Option<Vec<u8>> = None;

        // BOOT response injection
        if should_inject_boot_response(buf, len) {
            if let Some(ref info) = CACHED_PEER_INFO {
                eprintln!("[ip_spoof] Injecting peer info into BOOT response ({} bytes)", info.len());
                let orig = std::slice::from_raw_parts(buf as *const u8, len);
                if len >= AB_HEADER_LEN && orig[0] == AB_VERSION_1 {
                    // Original was framed — build a framed replacement:
                    //   Copy version + reserved + seq from original header,
                    //   set offset=0, total_len=info.len(), then payload.
                    let info_len = info.len();
                    let mut framed = Vec::with_capacity(AB_HEADER_LEN + info_len);
                    framed.extend_from_slice(&orig[..4]); // version, reserved, seq
                    framed.push(0);
                    framed.push(0); // offset = 0
                    framed.push((info_len >> 8) as u8);
                    framed.push((info_len & 0xFF) as u8); // total_len
                    framed.extend_from_slice(info);
                    actual_len = framed.len();
                    _framed_buf = Some(framed);
                    actual_buf = _framed_buf.as_ref().unwrap().as_ptr() as *const c_void;
                } else {
                    // Unframed — inject raw peer info
                    actual_buf = info.as_ptr() as *const c_void;
                    actual_len = info.len();
                }
            }
        }

        if !dest_addr.is_null() {
            match (*dest_addr).sa_family as c_int {
                AF_INET => {
                    let sin = &*(dest_addr as *const sockaddr_in);
                    let addr_host = u32::from_be(sin.sin_addr.s_addr);
                    if is_fake_global(addr_host) {
                        let mut modified: sockaddr_in = *sin;
                        rewrite_send_addr(&mut modified as *mut _ as *mut sockaddr);
                        return redhook::real!(sendto)(
                            sockfd, actual_buf, actual_len, flags,
                            &modified as *const _ as *const sockaddr, addrlen,
                        );
                    }
                }
                AF_INET6 => {
                    let sin6 = &*(dest_addr as *const sockaddr_in6);
                    let octets: &[u8; 16] = &*(&sin6.sin6_addr as *const _ as *const [u8; 16]);
                    if is_fake_global_v6(octets) {
                        let mut modified: sockaddr_in6 = *sin6;
                        rewrite_send_addr(&mut modified as *mut _ as *mut sockaddr);
                        return redhook::real!(sendto)(
                            sockfd, actual_buf, actual_len, flags,
                            &modified as *const _ as *const sockaddr, addrlen,
                        );
                    }
                }
                _ => {}
            }
        }

        redhook::real!(sendto)(sockfd, actual_buf, actual_len, flags, dest_addr, addrlen)
    }
}

redhook::hook! {
    unsafe fn recvmsg(
        sockfd: c_int,
        msg: *mut msghdr,
        flags: c_int
    ) -> ssize_t => my_recvmsg {
        ensure_init();

        let ret = redhook::real!(recvmsg)(sockfd, msg, flags);
        if ret >= 0 && !msg.is_null() && !(*msg).msg_name.is_null() {
            let local_port = local_port_of(sockfd);
            if is_devnet_port(local_port) {
                rewrite_recv_addr_force((*msg).msg_name as *mut sockaddr);
            } else {
                rewrite_recv_addr((*msg).msg_name as *mut sockaddr);
            }
        }
        ret
    }
}

redhook::hook! {
    unsafe fn sendmsg(
        sockfd: c_int,
        msg: *const msghdr,
        flags: c_int
    ) -> ssize_t => my_sendmsg {
        ensure_init();

        if !msg.is_null() && !(*msg).msg_name.is_null() {
            let addr = (*msg).msg_name as *const sockaddr;

            match (*addr).sa_family as c_int {
                AF_INET if (*msg).msg_namelen as usize >= std::mem::size_of::<sockaddr_in>() => {
                    let sin = &*(addr as *const sockaddr_in);
                    let addr_host = u32::from_be(sin.sin_addr.s_addr);
                    if is_fake_global(addr_host) {
                        let mut modified_msg: msghdr = *msg;
                        let mut modified_addr: sockaddr_in = *sin;
                        rewrite_send_addr(&mut modified_addr as *mut _ as *mut sockaddr);
                        modified_msg.msg_name = &mut modified_addr as *mut _ as *mut c_void;
                        return redhook::real!(sendmsg)(sockfd, &modified_msg, flags);
                    }
                }
                AF_INET6 if (*msg).msg_namelen as usize >= std::mem::size_of::<sockaddr_in6>() => {
                    let sin6 = &*(addr as *const sockaddr_in6);
                    let octets: &[u8; 16] = &*(&sin6.sin6_addr as *const _ as *const [u8; 16]);
                    if is_fake_global_v6(octets) {
                        let mut modified_msg: msghdr = *msg;
                        let mut modified_addr: sockaddr_in6 = *sin6;
                        rewrite_send_addr(&mut modified_addr as *mut _ as *mut sockaddr);
                        modified_msg.msg_name = &mut modified_addr as *mut _ as *mut c_void;
                        return redhook::real!(sendmsg)(sockfd, &modified_msg, flags);
                    }
                }
                _ => {}
            }
        }

        redhook::real!(sendmsg)(sockfd, msg, flags)
    }
}

redhook::hook! {
    unsafe fn connect(
        sockfd: c_int,
        addr: *const sockaddr,
        addrlen: socklen_t
    ) -> c_int => my_connect {
        ensure_init();

        if !addr.is_null() {
            match (*addr).sa_family as c_int {
                AF_INET if addrlen as usize >= std::mem::size_of::<sockaddr_in>() => {
                    let sin = &*(addr as *const sockaddr_in);
                    let addr_host = u32::from_be(sin.sin_addr.s_addr);
                    let dest_port = u16::from_be(sin.sin_port);

                    if is_fake_global(addr_host) && is_devnet_port(dest_port) {
                        // TCP: try to bind outgoing socket to a devnet port
                        let mut optlen: socklen_t = std::mem::size_of::<c_int>() as socklen_t;
                        let mut sock_type: c_int = 0;
                        if libc::getsockopt(
                            sockfd, SOL_SOCKET, SO_TYPE,
                            &mut sock_type as *mut _ as *mut c_void, &mut optlen,
                        ) == 0 && sock_type == SOCK_STREAM {
                            let local_port = local_port_of(sockfd);
                            if !is_devnet_port(local_port) {
                                let reuse: c_int = 1;
                                libc::setsockopt(
                                    sockfd, SOL_SOCKET, SO_REUSEADDR,
                                    &reuse as *const _ as *const c_void,
                                    std::mem::size_of::<c_int>() as socklen_t,
                                );
                                for offset in 0..MAX_PORT_OFFSET {
                                    let try_port = base_port() + offset;
                                    let mut bind_addr: sockaddr_in = std::mem::zeroed();
                                    bind_addr.sin_family = AF_INET as libc::sa_family_t;
                                    bind_addr.sin_addr.s_addr = LOOPBACK_ADDR.to_be();
                                    bind_addr.sin_port = try_port.to_be();
                                    if libc::bind(
                                        sockfd,
                                        &bind_addr as *const _ as *const sockaddr,
                                        std::mem::size_of::<sockaddr_in>() as socklen_t,
                                    ) == 0 {
                                        eprintln!(
                                            "[ip_spoof] Bound outgoing connection to port {}",
                                            try_port
                                        );
                                        break;
                                    }
                                }
                            }
                        }

                        let mut modified: sockaddr_in = *sin;
                        modified.sin_addr.s_addr = LOOPBACK_ADDR.to_be();
                        return redhook::real!(connect)(
                            sockfd,
                            &modified as *const _ as *const sockaddr,
                            addrlen,
                        );
                    }
                }
                AF_INET6 if addrlen as usize >= std::mem::size_of::<sockaddr_in6>() => {
                    let sin6 = &*(addr as *const sockaddr_in6);
                    let octets: &[u8; 16] = &*(&sin6.sin6_addr as *const _ as *const [u8; 16]);
                    if is_fake_global_v6(octets) {
                        let mut modified: sockaddr_in6 = *sin6;
                        let out: &mut [u8; 16] =
                            &mut *(&mut modified.sin6_addr as *mut _ as *mut [u8; 16]);
                        *out = LOOPBACK_V6;
                        return redhook::real!(connect)(
                            sockfd,
                            &modified as *const _ as *const sockaddr,
                            addrlen,
                        );
                    }
                }
                _ => {}
            }
        }

        redhook::real!(connect)(sockfd, addr, addrlen)
    }
}

redhook::hook! {
    unsafe fn getpeername(
        sockfd: c_int,
        addr: *mut sockaddr,
        addrlen: *mut socklen_t
    ) -> c_int => my_getpeername {
        ensure_init();

        let ret = redhook::real!(getpeername)(sockfd, addr, addrlen);
        if ret == 0 && !addr.is_null() {
            let local_port = local_port_of(sockfd);
            if is_devnet_port(local_port) {
                rewrite_recv_addr_force(addr);
            }
        }
        ret
    }
}

redhook::hook! {
    unsafe fn getsockname(
        sockfd: c_int,
        addr: *mut sockaddr,
        addrlen: *mut socklen_t
    ) -> c_int => my_getsockname {
        ensure_init();

        let ret = redhook::real!(getsockname)(sockfd, addr, addrlen);
        if ret == 0 && !addr.is_null() {
            rewrite_recv_addr(addr);
        }
        ret
    }
}
