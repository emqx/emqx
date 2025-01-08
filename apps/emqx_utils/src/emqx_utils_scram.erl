%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_utils_scram).

-export([authenticate/7]).

%%------------------------------------------------------------------------------
%% Authentication
%%------------------------------------------------------------------------------
authenticate(AuthMethod, AuthData, AuthCache, Conf, RetrieveFun, OnErrFun, ResultKeys) ->
    case ensure_auth_method(AuthMethod, AuthData, Conf) of
        true ->
            case AuthCache of
                #{next_step := client_final} ->
                    check_client_final_message(AuthData, AuthCache, Conf, OnErrFun, ResultKeys);
                _ ->
                    check_client_first_message(AuthData, AuthCache, Conf, RetrieveFun, OnErrFun)
            end;
        false ->
            ignore
    end.

ensure_auth_method(_AuthMethod, undefined, _Conf) ->
    false;
ensure_auth_method(<<"SCRAM-SHA-256">>, _AuthData, #{algorithm := sha256}) ->
    true;
ensure_auth_method(<<"SCRAM-SHA-512">>, _AuthData, #{algorithm := sha512}) ->
    true;
ensure_auth_method(_AuthMethod, _AuthData, _Conf) ->
    false.

check_client_first_message(
    Bin, _Cache, #{iteration_count := IterationCount}, RetrieveFun, OnErrFun
) ->
    case
        sasl_auth_scram:check_client_first_message(
            Bin,
            #{
                iteration_count => IterationCount,
                retrieve => RetrieveFun
            }
        )
    of
        {continue, ServerFirstMessage, Cache} ->
            {continue, ServerFirstMessage, Cache};
        ignore ->
            ignore;
        {error, Reason} ->
            OnErrFun("check_client_first_message_error", Reason),
            {error, not_authorized}
    end.

check_client_final_message(Bin, Cache, #{algorithm := Alg}, OnErrFun, ResultKeys) ->
    case
        sasl_auth_scram:check_client_final_message(
            Bin,
            Cache#{algorithm => Alg}
        )
    of
        {ok, ServerFinalMessage} ->
            {ok, maps:with(ResultKeys, Cache), ServerFinalMessage};
        {error, Reason} ->
            OnErrFun("check_client_final_message_error", Reason),
            {error, not_authorized}
    end.
