%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_default_cert).

-moduledoc """
Generates the node's default TLS certificate bundle at boot.

The bundle is a managed-certs bundle named `localhost`: a server
certificate for `CN=localhost` with subject alternative names
`DNS:localhost`, `IP:127.0.0.1` and `IP:::1`, signed by a one-off CA
whose private key is discarded at generation time.

It is written to the local node only, never through the clustered
managed-certs API. Each node's default certificate is its own
identity: two nodes are meant to have different ones. A node joining
an existing cluster receives that cluster's bundle on disk before
`emqx` boots (`emqx_conf` is force-booted first and its data sync
covers `data/certs2`), so it finds a complete bundle here and keeps
it rather than generating another.
""".

-include("emqx_managed_certs.hrl").
-include("emqx_config.hrl").
-include("logger.hrl").

-export([ensure_localhost_bundle/0]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-doc """
Creates the `localhost` managed-certs bundle unless this node already
has one. Idempotent.

Failure to generate or write the bundle is logged and otherwise
ignored: a node with no TLS server configured has no use for it, and
the actionable error surfaces when a listener actually references the
missing bundle.
""".
-spec ensure_localhost_bundle() -> ok.
ensure_localhost_bundle() ->
    case is_bundle_complete() of
        true ->
            ok;
        false ->
            generate_localhost_bundle()
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

is_bundle_complete() ->
    case emqx_managed_certs:list_managed_files(?global_ns, ?DEFAULT_CERT_BUNDLE_NAME) of
        {ok, #{?FILE_KIND_KEY := _, ?FILE_KIND_CHAIN := _}} ->
            true;
        _ ->
            false
    end.

generate_localhost_bundle() ->
    try
        #{ca := Ca, cert := Cert, key := Key} = emqx_utils_certs:self_signed_bundle(#{
            cn => "localhost",
            sans => [
                {dns, "localhost"},
                {ip, {127, 0, 0, 1}},
                {ip, {0, 0, 0, 0, 0, 0, 0, 1}}
            ]
        }),
        %% `emqx_utils_certs' speaks a plain ca/cert/key map and knows
        %% nothing about managed certs; translate to file kinds here.
        write_bundle(#{
            ?FILE_KIND_KEY => Key,
            ?FILE_KIND_CHAIN => Cert,
            ?FILE_KIND_CA => Ca
        })
    catch
        Class:Reason:Stacktrace ->
            ?SLOG(error, #{
                msg => "failed_to_generate_default_tls_certificate",
                bundle => ?DEFAULT_CERT_BUNDLE_NAME,
                exception => Class,
                reason => Reason,
                stacktrace => Stacktrace
            }),
            ok
    end.

write_bundle(Files) ->
    %% Strictly local, never the clustered `add_managed_files/3': see
    %% the module doc for why each node keeps its own bundle.
    case emqx_managed_certs:add_managed_files_v1(?global_ns, ?DEFAULT_CERT_BUNDLE_NAME, Files) of
        ok ->
            ?SLOG(info, #{
                msg => "default_tls_certificate_generated",
                bundle => ?DEFAULT_CERT_BUNDLE_NAME,
                dir => emqx_managed_certs:dir(?global_ns, ?DEFAULT_CERT_BUNDLE_NAME)
            }),
            ok;
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "failed_to_write_default_tls_certificate",
                bundle => ?DEFAULT_CERT_BUNDLE_NAME,
                reason => Reason
            }),
            ok
    end.
