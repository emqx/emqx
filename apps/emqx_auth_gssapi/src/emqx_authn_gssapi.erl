%%--------------------------------------------------------------------
%% Copyright (c) 2021-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_authn_gssapi).

-include("emqx_auth_gssapi.hrl").
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
    case sasl_auth:kinit(KeyTabFile, Principal) of
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
        auth_method := <<"GSSAPI">>,
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
            ?TRACE_AUTHN_PROVIDER("sasl_gssapi_new_failed", #{
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
            ?TRACE_AUTHN_PROVIDER("sasl_gssapi_start_failed", #{
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
            ?TRACE_AUTHN_PROVIDER("sasl_gssapi_step_failed", #{
                reason => Reason,
                sasl_function => "server_server_step"
            }),
            sasl_auth:server_done(SaslConn),
            {error, not_authorized}
    end.
