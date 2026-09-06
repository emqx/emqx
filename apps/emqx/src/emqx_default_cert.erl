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

%% Internal export: spawned as the lock holder, see `ensure_localhost_bundle/0'.
-export([generator/0]).

%% Registering a name is the mutual exclusion: only one process holds it, and
%% it is released automatically if that process dies.
-define(LOCK, emqx_default_cert_generator).
-define(MAX_ATTEMPTS, 10).
-define(RETRY_INTERVAL, 5).
-define(GENERATE_TIMEOUT, 30_000).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-doc """
Returns the `localhost' bundle's files, generating the bundle first if this
node does not have a complete one.

Concurrent callers are safe. Generation runs in a short-lived process that
holds a registered name for the whole check-and-write sequence, so two callers
cannot each decide the bundle is missing and then overwrite one another's work.
Callers that lose the race wait for the holder and re-read what it produced.
""".
-spec ensure_localhost_bundle() ->
    {ok, #{emqx_managed_certs:file_kind() => #{path := file:filename_all()}}}
    | {error, term()}.
ensure_localhost_bundle() ->
    case complete_bundle() of
        {ok, _} = Complete ->
            Complete;
        _ ->
            %% Monitored rather than linked: a failed generation should come
            %% back as an error, not take the caller down with it.
            {Pid, MRef} = spawn_monitor(fun ?MODULE:generator/0),
            await_generator(Pid, MRef),
            complete_bundle()
    end.

-doc """
Generates the bundle, or waits for whichever process is already doing so.

Exported only to be spawned by `ensure_localhost_bundle/0'; not part of this
module's interface.
""".
-spec generator() -> ok.
generator() ->
    generator(?MAX_ATTEMPTS).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

await_generator(Pid, MRef) ->
    receive
        {'DOWN', MRef, process, _Pid, normal} ->
            ok;
        {'DOWN', MRef, process, _Pid, Reason} ->
            ?SLOG(error, #{
                msg => "default_tls_certificate_generator_crashed",
                bundle => ?NODE_DEFAULT_CERT_BUNDLE_NAME,
                reason => Reason
            }),
            ok
    after generate_timeout() ->
        %% Kill it rather than leave it behind: a generator wedged while
        %% holding the lock would keep every later caller waiting too. The
        %% bundle cannot be left half-written, since it only ever appears
        %% through one rename, but the temporary directory is not cleaned up
        %% here — an untrappable exit skips the cleanup in `create_bundle/3'.
        %% It sits outside `certs2', so it is never copied to another node.
        %% Frees the lock when this caller's own generator is the holder,
        %% which is the ordinary case. A holder orphaned by a caller that died
        %% while it was wedged is not covered; that would need the generator
        %% linked to its caller, which would let a failed generation take a
        %% starting listener down with it.
        exit(Pid, kill),
        _ = erlang:demonitor(MRef, [flush]),
        ?SLOG(error, #{
            msg => "default_tls_certificate_generation_timeout",
            bundle => ?NODE_DEFAULT_CERT_BUNDLE_NAME,
            timeout => generate_timeout()
        }),
        ok
    end.

%% Overridable so tests do not have to wait out the real timeout.
generate_timeout() ->
    application:get_env(emqx, default_cert_generate_timeout, ?GENERATE_TIMEOUT).

generator(0) ->
    %% Out of attempts. The caller re-reads the bundle and reports the failure.
    ok;
generator(Attempts) ->
    case complete_bundle() of
        {ok, _} ->
            %% Another process finished while this one was starting up.
            ok;
        _ ->
            try_generate(Attempts)
    end.

try_generate(Attempts) ->
    case acquire_lock() of
        ok ->
            hold_and_generate();
        busy ->
            %% Another process holds the name, or took it between the check
            %% above and here.
            wait_for_holder(Attempts)
    end.

acquire_lock() ->
    try register(?LOCK, self()) of
        true -> ok
    catch
        error:badarg -> busy
    end.

hold_and_generate() ->
    try
        _ = generate_localhost_bundle(),
        ok
    after
        release_lock()
    end.

%% The name is released when this process exits in any case; doing it here as
%% well keeps the holder's window as short as the work itself.
release_lock() ->
    try
        _ = unregister(?LOCK),
        ok
    catch
        error:badarg ->
            ok
    end.

wait_for_holder(Attempts) ->
    case whereis(?LOCK) of
        undefined ->
            %% The holder finished in the meantime; try to take it over.
            timer:sleep(?RETRY_INTERVAL),
            generator(Attempts - 1);
        Pid ->
            MRef = erlang:monitor(process, Pid),
            receive
                {'DOWN', MRef, process, _, Reason} when
                    Reason =:= normal; Reason =:= noproc
                ->
                    %% `noproc' means it had already finished when the monitor
                    %% was set up, which is as good as a clean exit.
                    generator(Attempts - 1);
                {'DOWN', MRef, process, _, Reason} ->
                    ?SLOG(error, #{
                        msg => "default_tls_certificate_generator_crashed",
                        bundle => ?NODE_DEFAULT_CERT_BUNDLE_NAME,
                        reason => Reason
                    }),
                    generator(Attempts - 1)
            end
    end.

list_bundle() ->
    emqx_managed_certs:list_managed_files(?global_ns, ?NODE_DEFAULT_CERT_BUNDLE_NAME).

complete_bundle() ->
    case list_bundle() of
        {ok, #{?FILE_KIND_KEY := _, ?FILE_KIND_CHAIN := _}} = Complete ->
            Complete;
        {ok, _Incomplete} ->
            {error, incomplete_bundle};
        {error, enoent} ->
            {error, no_bundle};
        {error, _} = Error ->
            Error
    end.

%% Runs while holding the lock, so nothing else is checking, deleting or
%% writing this bundle at the same time.
generate_localhost_bundle() ->
    maybe
        ok ?= clear_unusable_bundle(),
        {ok, Files} ?= generate(),
        install(Files)
    else
        {error, Reason} = Error ->
            ?SLOG(error, #{
                msg => "failed_to_generate_default_tls_certificate",
                bundle => ?NODE_DEFAULT_CERT_BUNDLE_NAME,
                reason => Reason
            }),
            Error
    end.

%% Whatever is stored is incomplete, or there is nothing at all. An incomplete
%% bundle would block the rename, and this node cannot use it either way.
clear_unusable_bundle() ->
    case emqx_managed_certs:delete_bundle_v1(?global_ns, ?NODE_DEFAULT_CERT_BUNDLE_NAME) of
        ok ->
            ok;
        {error, enoent} ->
            ok;
        {error, _} = Error ->
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
            %% Something appeared after the delete above. Accept it only if it
            %% is a bundle this node can actually use.
            case complete_bundle() of
                {ok, _} -> ok;
                {error, _} -> {error, unusable_bundle_in_place}
            end;
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
