%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authn_kerberos).

-include("emqx_auth_kerberos.hrl").
-include_lib("emqx_auth/include/emqx_authn.hrl").
-include_lib("typerefl/include/types.hrl").

-behaviour(emqx_authn_provider).

-export([
    create/2,
    update/2,
    destroy/1,
    authenticate/2
]).

create(
    AuthenticatorID,
    #{
        principal := Principal,
        keytab_file := KeyTabFile
    }
) ->
    KeyTabPath = emqx_schema:naive_env_interpolation(KeyTabFile),
    case sasl_auth:kinit(KeyTabPath, Principal) of
        ok ->
            {ok, #{
                id => AuthenticatorID,
                principal => Principal,
                keytab_file => KeyTabFile
            }};
        Error ->
            Error
    end.

update(Config, #{id := ID}) ->
    create(ID, Config).

destroy(_) ->
    ok.

authenticate(
    #{
        auth_method := <<"GSSAPI-KERBEROS">>,
        auth_data := AuthData,
        auth_cache := AuthCache
    },
    #{principal := Principal}
) when AuthData =/= undefined ->
    case AuthCache of
        #{sasl_conn := SaslConn} ->
            auth_continue(SaslConn, AuthData);
        _ ->
            case auth_new(Principal) of
                {ok, SaslConn} -> auth_begin(SaslConn, AuthData);
                Error -> Error
            end
    end;
authenticate(_Credential, _State) ->
    ignore.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

auth_new(Principal) ->
    case sasl_auth:server_new(<<"emqx">>, Principal) of
        {ok, SaslConn} ->
            {ok, SaslConn};
        Error ->
            ?TRACE_AUTHN_PROVIDER("sasl_kerberos_new_failed", #{
                reason => Error,
                sasl_function => "server_server_new"
            }),
            {error, not_authorized}
    end.

auth_begin(SaslConn, ClientToken) ->
    case sasl_auth:server_start(SaslConn, ClientToken) of
        {ok, {sasl_continue, ServerToken}} ->
            {continue, ServerToken, #{sasl_conn => SaslConn}};
        {ok, {sasl_ok, ServerToken}} ->
            sasl_auth:server_done(SaslConn),
            {ok, #{}, ServerToken};
        Reason ->
            ?TRACE_AUTHN_PROVIDER("sasl_kerberos_start_failed", #{
                reason => Reason,
                sasl_function => "server_server_start"
            }),
            sasl_auth:server_done(SaslConn),
            {error, not_authorized}
    end.

auth_continue(SaslConn, ClientToken) ->
    case sasl_auth:server_step(SaslConn, ClientToken) of
        {ok, {sasl_continue, ServerToken}} ->
            {continue, ServerToken, #{sasl_conn => SaslConn}};
        {ok, {sasl_ok, ServerToken}} ->
            sasl_auth:server_done(SaslConn),
            {ok, #{}, ServerToken};
        Reason ->
            ?TRACE_AUTHN_PROVIDER("sasl_kerberos_step_failed", #{
                reason => Reason,
                sasl_function => "server_server_step"
            }),
            sasl_auth:server_done(SaslConn),
            {error, not_authorized}
    end.
