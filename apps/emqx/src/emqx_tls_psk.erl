%%--------------------------------------------------------------------
%% Copyright (c) 2019-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_tls_psk).

-include("logger.hrl").

%% SSL PSK Callbacks
-export([lookup/3]).
-export_type([psk_identity/0]).

-type psk_identity() :: string().
-type psk_user_state() :: term().

-spec lookup(psk, psk_identity(), psk_user_state()) -> {ok, SharedSecret :: binary()} | error.
lookup(psk, PSKIdentity, _UserState) ->
    try emqx_hooks:run_fold('tls_handshake.psk_lookup', [PSKIdentity], normal) of
        {ok, SharedSecret} when is_binary(SharedSecret) ->
            {ok, SharedSecret};
        normal ->
            ?SLOG(info, #{
                msg => "psk_identity_not_found",
                psk_identity => PSKIdentity
            }),
            error;
        {error, Reason} ->
            ?SLOG(warning, #{
                msg => "psk_identity_not_found",
                psk_identity => PSKIdentity,
                reason => Reason
            }),
            error
    catch
        Class:Reason:Stacktrace ->
            ?SLOG(error, #{
                msg => "lookup_psk_failed",
                class => Class,
                reason => Reason,
                stacktrace => Stacktrace
            }),
            error
    end.
