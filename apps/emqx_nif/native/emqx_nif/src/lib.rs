//! Native library of EMQX. Each module adds one API. The Erlang module `emqx_nif` holds the
//! stubs of all functions. Callers use the wrapper module of each API, for example `emqx_niftls`.

mod tls;

use rustler::{Encoder, Env, NifResult, Term};

mod atoms {
    rustler::atoms! {
        ok,
        error,
        close,
        busy,
        handshake_done,
        undefined,
        tls_confs,
        tls_conns,
        protocol,
        cipher_suite,
        sni_hostname,
        alpn,
        peer_certs,
        tlsv1_2 = "tlsv1.2",
        tlsv1_3 = "tlsv1.3",
        rsa_private_key = "RSAPrivateKey",
        ec_private_key = "ECPrivateKey",
        private_key_info = "PrivateKeyInfo",
    }
}

#[rustler::nif]
fn info(env: Env) -> NifResult<Term> {
    let pairs = [
        (
            atoms::tls_confs().encode(env),
            tls::live_confs().encode(env),
        ),
        (
            atoms::tls_conns().encode(env),
            tls::live_conns().encode(env),
        ),
    ];
    Term::map_from_pairs(env, &pairs)
}

rustler::init!("emqx_nif");
