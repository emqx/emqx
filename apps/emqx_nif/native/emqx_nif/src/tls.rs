//! TLS server state machine on Rustls, with no socket I/O.
//!
//! The connection process reads raw bytes from its socket and gives them to `tls_feed`. The NIF
//! returns the bytes to send and the decrypted bytes. `tls_encrypt` returns the TLS records for
//! outgoing data. The NIF sends no messages and owns no socket.
//!
//! The Erlang module `emqx_niftls` wraps these functions. It converts the listener options,
//! selects the scheduler type (dirty CPU during the handshake), and splits large input into
//! chunks, so that one call on a normal scheduler handles at most one chunk.

use crate::atoms;
use rustler::sys::enif_consume_timeslice;
use rustler::{Atom, Binary, Encoder, Env, NifMap, OwnedBinary, Resource, ResourceArc, Term};
use rustls::crypto::{ring, CryptoProvider};
use rustls::pki_types::{
    CertificateDer, PrivateKeyDer, PrivatePkcs1KeyDer, PrivatePkcs8KeyDer, PrivateSec1KeyDer,
};
use rustls::server::{NoServerSessionStorage, WebPkiClientVerifier};
use rustls::sign::{CertifiedKey, SingleCertAndKey};
use rustls::{ProtocolVersion, RootCertStore, ServerConfig, ServerConnection};
use std::io::{Read, Write};
use std::os::raw::c_int;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

static LIVE_CONFS: AtomicI64 = AtomicI64::new(0);
static LIVE_CONNS: AtomicI64 = AtomicI64::new(0);

pub fn live_confs() -> i64 {
    LIVE_CONFS.load(Ordering::Relaxed)
}

pub fn live_conns() -> i64 {
    LIVE_CONNS.load(Ordering::Relaxed)
}

/// Estimate of the bytes that one percent of a timeslice (10 µs) processes.
const BYTES_PER_PCT: usize = 2048;

/// One listener configuration. All connections of the listener share it.
pub struct TlsConf {
    config: Arc<ServerConfig>,
}

#[rustler::resource_impl]
impl Resource for TlsConf {}

impl Drop for TlsConf {
    fn drop(&mut self) {
        LIVE_CONFS.fetch_sub(1, Ordering::Relaxed);
    }
}

/// One TLS session. The connection process that created it is the only caller.
pub struct Tls {
    conn: Mutex<ServerConnection>,
}

#[rustler::resource_impl]
impl Resource for Tls {}

impl Drop for Tls {
    fn drop(&mut self) {
        LIVE_CONNS.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Options after `emqx_niftls:config/1` converted and checked them. Every key is present.
#[derive(NifMap)]
struct ConfOpts<'a> {
    certs: Vec<Binary<'a>>,
    key: (Atom, Binary<'a>),
    cacerts: Vec<Binary<'a>>,
    verify_peer: bool,
    fail_if_no_peer_cert: bool,
    tls12: bool,
    tls13: bool,
    /// Rustls names, for example `TLS13_AES_256_GCM_SHA384`. Empty means the Rustls default.
    ciphers: Vec<String>,
    /// Rustls names, for example `X25519`. Empty means the Rustls default.
    groups: Vec<String>,
    alpn: Vec<Binary<'a>>,
    honor_cipher_order: bool,
}

fn make_bin<'a>(env: Env<'a>, data: &[u8]) -> Term<'a> {
    let mut bin = OwnedBinary::new(data.len()).expect("binary allocation");
    bin.as_mut_slice().copy_from_slice(data);
    bin.release(env).encode(env)
}

fn error_term<'a>(env: Env<'a>, reason: impl std::fmt::Display) -> Term<'a> {
    (atoms::error(), make_bin(env, reason.to_string().as_bytes())).encode(env)
}

fn consume_timeslice(env: Env, bytes: usize) {
    let pct = (bytes / BYTES_PER_PCT).clamp(1, 100) as c_int;
    unsafe { enif_consume_timeslice(env.as_c_arg(), pct) };
}

fn private_key(key_type: Atom, der: &[u8]) -> Result<PrivateKeyDer<'static>, String> {
    let der = der.to_vec();
    if key_type == atoms::rsa_private_key() {
        Ok(PrivatePkcs1KeyDer::from(der).into())
    } else if key_type == atoms::ec_private_key() {
        Ok(PrivateSec1KeyDer::from(der).into())
    } else if key_type == atoms::private_key_info() {
        Ok(PrivatePkcs8KeyDer::from(der).into())
    } else {
        Err(format!("unsupported private key type: {key_type:?}"))
    }
}

fn provider(opts: &ConfOpts) -> Result<CryptoProvider, String> {
    let base = ring::default_provider();
    let mut cipher_suites = Vec::new();
    for name in &opts.ciphers {
        let suite = base
            .cipher_suites
            .iter()
            .find(|s| s.suite().as_str() == Some(name.as_str()))
            .ok_or_else(|| format!("unsupported cipher suite: {name}"))?;
        cipher_suites.push(*suite);
    }
    let mut kx_groups = Vec::new();
    for name in &opts.groups {
        let group = base
            .kx_groups
            .iter()
            .find(|g| g.name().as_str() == Some(name.as_str()))
            .ok_or_else(|| format!("unsupported group: {name}"))?;
        kx_groups.push(*group);
    }
    Ok(CryptoProvider {
        cipher_suites: if cipher_suites.is_empty() {
            base.cipher_suites.clone()
        } else {
            cipher_suites
        },
        kx_groups: if kx_groups.is_empty() {
            base.kx_groups.clone()
        } else {
            kx_groups
        },
        ..base
    })
}

fn server_config(opts: &ConfOpts) -> Result<ServerConfig, String> {
    let provider = Arc::new(provider(opts)?);
    let key_provider = provider.key_provider;
    let mut versions = Vec::new();
    if opts.tls13 {
        versions.push(&rustls::version::TLS13);
    }
    if opts.tls12 {
        versions.push(&rustls::version::TLS12);
    }
    let builder = ServerConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&versions)
        .map_err(|e| e.to_string())?;
    let builder = if opts.verify_peer {
        let mut roots = RootCertStore::empty();
        for ca in &opts.cacerts {
            roots
                .add(CertificateDer::from(ca.as_slice().to_vec()))
                .map_err(|e| format!("bad CA certificate: {e}"))?;
        }
        let mut verifier =
            WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider.clone());
        if !opts.fail_if_no_peer_cert {
            verifier = verifier.allow_unauthenticated();
        }
        builder.with_client_cert_verifier(verifier.build().map_err(|e| e.to_string())?)
    } else {
        builder.with_no_client_auth()
    };
    let certs = opts
        .certs
        .iter()
        .map(|c| CertificateDer::from(c.as_slice().to_vec()))
        .collect();
    let key = private_key(opts.key.0, opts.key.1.as_slice())?;
    let signing_key = key_provider
        .load_private_key(key)
        .map_err(|e| e.to_string())?;
    // Rustls can compare the key with the certificate only byte by byte, on the encoded
    // SubjectPublicKeyInfo. That fails for a certificate that omits the NULL parameters of
    // rsaEncryption, which OTP ssl accepts. `emqx_niftls` compares the decoded public keys.
    let certified = CertifiedKey::new(certs, signing_key);
    let mut config = builder.with_cert_resolver(Arc::new(SingleCertAndKey::from(certified)));
    config.ignore_client_order = opts.honor_cipher_order;
    config.alpn_protocols = opts.alpn.iter().map(|p| p.as_slice().to_vec()).collect();
    // No session resumption and no tickets, the same as the OTP ssl server default.
    config.session_storage = Arc::new(NoServerSessionStorage {});
    config.send_tls13_tickets = 0;
    Ok(config)
}

#[rustler::nif(schedule = "DirtyCpu")]
fn tls_config<'a>(env: Env<'a>, opts: ConfOpts<'a>) -> Term<'a> {
    match server_config(&opts) {
        Ok(config) => {
            LIVE_CONFS.fetch_add(1, Ordering::Relaxed);
            let conf = ResourceArc::new(TlsConf {
                config: Arc::new(config),
            });
            (atoms::ok(), conf).encode(env)
        }
        Err(reason) => error_term(env, reason),
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
fn tls_new<'a>(env: Env<'a>, conf: ResourceArc<TlsConf>) -> Term<'a> {
    match ServerConnection::new(conf.config.clone()) {
        Ok(conn) => {
            LIVE_CONNS.fetch_add(1, Ordering::Relaxed);
            let tls = ResourceArc::new(Tls {
                conn: Mutex::new(conn),
            });
            (atoms::ok(), tls).encode(env)
        }
        Err(reason) => error_term(env, reason),
    }
}

/// Move all pending TLS records to `out`.
fn drain_tls(conn: &mut ServerConnection, out: &mut Vec<u8>) {
    while conn.wants_write() {
        // Writing to a Vec does not fail.
        if conn.write_tls(out).unwrap_or(0) == 0 {
            break;
        }
    }
}

fn handshake_info<'a>(env: Env<'a>, conn: &ServerConnection) -> Term<'a> {
    let protocol = match conn.protocol_version() {
        Some(ProtocolVersion::TLSv1_3) => atoms::tlsv1_3(),
        Some(ProtocolVersion::TLSv1_2) => atoms::tlsv1_2(),
        _ => atoms::undefined(),
    };
    let cipher = conn
        .negotiated_cipher_suite()
        .and_then(|s| s.suite().as_str())
        .map(|s| make_bin(env, s.as_bytes()))
        .unwrap_or_else(|| atoms::undefined().encode(env));
    let sni = conn
        .server_name()
        .map(|s| make_bin(env, s.as_bytes()))
        .unwrap_or_else(|| atoms::undefined().encode(env));
    let alpn = conn
        .alpn_protocol()
        .map(|p| make_bin(env, p))
        .unwrap_or_else(|| atoms::undefined().encode(env));
    let peer_certs: Vec<Term> = conn
        .peer_certificates()
        .unwrap_or(&[])
        .iter()
        .map(|c| make_bin(env, c.as_ref()))
        .collect();
    let pairs = [
        (atoms::protocol().encode(env), protocol.encode(env)),
        (atoms::cipher_suite().encode(env), cipher),
        (atoms::sni_hostname().encode(env), sni),
        (atoms::alpn().encode(env), alpn),
        (atoms::peer_certs().encode(env), peer_certs.encode(env)),
    ];
    Term::map_from_pairs(env, &pairs).expect("unique keys")
}

fn feed<'a>(env: Env<'a>, tls: &Tls, input: &[u8]) -> Term<'a> {
    let Ok(mut conn) = tls.conn.try_lock() else {
        return (atoms::error(), atoms::busy()).encode(env);
    };
    let was_handshaking = conn.is_handshaking();
    let mut rd = input;
    let mut app = Vec::new();
    let mut out = Vec::new();
    let mut failure: Option<String> = None;
    while !rd.is_empty() {
        match conn.read_tls(&mut rd) {
            // The peer sent close_notify. Rustls reads no more data.
            Ok(0) => break,
            Ok(_) => {}
            Err(e) => {
                failure = Some(e.to_string());
                break;
            }
        }
        match conn.process_new_packets() {
            Ok(state) => {
                let n = state.plaintext_bytes_to_read();
                if n > 0 {
                    let from = app.len();
                    app.resize(from + n, 0);
                    if let Err(e) = conn.reader().read_exact(&mut app[from..]) {
                        failure = Some(e.to_string());
                        break;
                    }
                }
                if state.peer_has_closed() {
                    break;
                }
            }
            Err(e) => {
                failure = Some(e.to_string());
                break;
            }
        }
    }
    // After an error, the pending records contain the alert for the peer.
    drain_tls(&mut conn, &mut out);
    if !was_handshaking {
        consume_timeslice(env, input.len());
    }
    let out_t = make_bin(env, &out);
    if let Some(reason) = failure {
        return (atoms::close(), out_t, make_bin(env, reason.as_bytes())).encode(env);
    }
    let app_t = make_bin(env, &app);
    if was_handshaking && !conn.is_handshaking() {
        let info = handshake_info(env, &conn);
        (atoms::handshake_done(), out_t, app_t, info).encode(env)
    } else {
        (atoms::ok(), out_t, app_t).encode(env)
    }
}

/// Feed bytes during the handshake. The handshake does public key operations.
#[rustler::nif(schedule = "DirtyCpu")]
fn tls_feed_handshake<'a>(env: Env<'a>, tls: ResourceArc<Tls>, input: Binary<'a>) -> Term<'a> {
    feed(env, &tls, input.as_slice())
}

/// Feed bytes after the handshake. The caller limits the input size.
#[rustler::nif]
fn tls_feed<'a>(env: Env<'a>, tls: ResourceArc<Tls>, input: Binary<'a>) -> Term<'a> {
    feed(env, &tls, input.as_slice())
}

/// Encrypt application data. The caller limits the input size.
#[rustler::nif]
fn tls_encrypt<'a>(env: Env<'a>, tls: ResourceArc<Tls>, data: Term<'a>) -> Term<'a> {
    let Ok(data) = Binary::from_iolist(data) else {
        return (atoms::error(), rustler::types::atom::badarg()).encode(env);
    };
    let Ok(mut conn) = tls.conn.try_lock() else {
        return (atoms::error(), atoms::busy()).encode(env);
    };
    let mut out = Vec::new();
    let mut rest = data.as_slice();
    while !rest.is_empty() {
        let n = match conn.writer().write(rest) {
            Ok(n) => n,
            Err(e) => return error_term(env, e),
        };
        if n == 0 && !conn.wants_write() {
            // Before the end of the handshake, Rustls buffers plaintext up to a limit.
            return error_term(env, "send buffer full");
        }
        rest = &rest[n..];
        drain_tls(&mut conn, &mut out);
    }
    consume_timeslice(env, data.len());
    (atoms::ok(), make_bin(env, &out)).encode(env)
}

/// Return the close_notify alert to send before the socket closes.
#[rustler::nif(schedule = "DirtyCpu")]
fn tls_close<'a>(env: Env<'a>, tls: ResourceArc<Tls>) -> Term<'a> {
    let Ok(mut conn) = tls.conn.try_lock() else {
        return make_bin(env, &[]);
    };
    conn.send_close_notify();
    let mut out = Vec::new();
    drain_tls(&mut conn, &mut out);
    make_bin(env, &out)
}
