%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_default_cert).

-moduledoc """
The node's default TLS certificate bundle.

`ensure_localhost_bundle/0' returns the managed-certs bundle named `localhost',
generating it if the node does not have one: a server certificate for
`CN=localhost' with subject alternative names `DNS:localhost', `IP:127.0.0.1'
and `IP:::1', signed by a one-off CA whose private key is discarded at
generation time.

The bundle holds a key and a chain, and deliberately no `ca' file: the chain
carries the leaf followed by the CA that signed it, and the `ca' slot is left
empty because it resolves to `cacertfile', the anchor used to verify a peer's
certificate — which is a separate decision for the operator to make.

It is generated on demand, when a TLS server finds itself without a configured
certificate, rather than at boot. Two consequences worth knowing:

* An operator who configures their own certificates and then deletes this
  bundle keeps it deleted, because nothing asks for it again. It comes back
  only if something needs a default certificate again.
* An operator may supply this bundle themselves. A complete bundle already
  stored under this name is used as it is and never overwritten, so seeding
  `localhost' is a supported way to choose the node's default certificate.

The bundle is stored on the local node only and never leaves it: it is written
through the local managed-certs API rather than the clustered one, and
`emqx_conf' excludes it from the `data/certs2' copy a joining node receives.
Each node's default certificate is its own identity, so no two nodes share a
private key.
""".

-include("emqx_managed_certs.hrl").
-include("emqx_config.hrl").
-include("logger.hrl").

-export([ensure_localhost_bundle/0]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-doc """
Returns the `localhost' bundle's files, generating the bundle first if this
node does not have a complete one.

Concurrent callers are safe: each generates its own bundle and the last one
into place wins, so the stored bundle is always one caller's complete and
self-consistent set of files.
""".
-spec ensure_localhost_bundle() ->
    {ok, #{emqx_managed_certs:file_kind() => #{path := file:filename_all()}}}
    | {error, term()}.
ensure_localhost_bundle() ->
    case list_bundle() of
        {ok, #{?FILE_KIND_KEY := _, ?FILE_KIND_CHAIN := _}} = Complete ->
            Complete;
        _ ->
            generate_localhost_bundle()
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

list_bundle() ->
    emqx_managed_certs:list_managed_files(?global_ns, ?NODE_DEFAULT_CERT_BUNDLE_NAME).

generate_localhost_bundle() ->
    %% Anything already stored under this name is incomplete (`ensure_localhost_bundle/0'
    %% checked), and would block the rename. Clear it first, locally: a bundle
    %% this node cannot use is not worth keeping, and other nodes keep theirs.
    _ = emqx_managed_certs:delete_bundle_v1(?global_ns, ?NODE_DEFAULT_CERT_BUNDLE_NAME),
    maybe
        {ok, Files} ?= generate(),
        ok ?= install(Files),
        list_bundle()
    else
        {error, Reason} = Error ->
            ?SLOG(error, #{
                msg => "failed_to_generate_default_tls_certificate",
                bundle => ?NODE_DEFAULT_CERT_BUNDLE_NAME,
                reason => Reason
            }),
            Error
    end.

install(Files) ->
    case emqx_managed_certs:create_bundle(?global_ns, ?NODE_DEFAULT_CERT_BUNDLE_NAME, Files) of
        ok ->
            ?SLOG(info, #{
                msg => "default_tls_certificate_generated",
                bundle => ?NODE_DEFAULT_CERT_BUNDLE_NAME,
                dir => emqx_managed_certs:dir(?global_ns, ?NODE_DEFAULT_CERT_BUNDLE_NAME)
            }),
            ok;
        {error, exists} ->
            %% Another caller generated one in the meantime. Theirs is as good
            %% as ours, so keep it and discard this generation.
            ok;
        {error, _} = Error ->
            Error
    end.

generate() ->
    try
        #{ca := Ca, cert := Cert, key := Key} = emqx_utils_certs:self_signed_bundle(#{
            cn => "localhost",
            sans => [
                {dns, "localhost"},
                {ip, {127, 0, 0, 1}},
                {ip, {0, 0, 0, 0, 0, 0, 0, 1}}
            ]
        }),
        %% `emqx_utils_certs' speaks a plain ca/cert/key map and knows nothing
        %% about managed certs; translate to file kinds here.
        %%
        %% The CA goes into the chain, after the leaf it signed, and no `ca'
        %% file is written. That slot resolves to `cacertfile', which is the
        %% trust anchor for verifying a *peer*: this node has no reason to
        %% trust certificates issued by its own one-off CA, and could not be
        %% presented one anyway, since that CA's key was discarded. A client
        %% that wants to trust this node takes the CA out of the chain.
        {ok, #{
            ?FILE_KIND_KEY => Key,
            ?FILE_KIND_CHAIN => <<Cert/binary, Ca/binary>>
        }}
    catch
        Class:Reason:Stacktrace ->
            {error, #{exception => Class, reason => Reason, stacktrace => Stacktrace}}
    end.
