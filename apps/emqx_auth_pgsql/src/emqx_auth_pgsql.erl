%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_auth_pgsql).

-include("emqx_auth_pgsql.hrl").

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").

-export([ check/3
        , description/0
        ]).

%%--------------------------------------------------------------------
%% Auth Module Callbacks
%%--------------------------------------------------------------------

check(ClientInfo = #{password := Password}, AuthResult,
      #{auth_query  := {AuthSql, AuthParams},
        super_query := SuperQuery,
        hash_type   := HashType,
        pool := Pool}) ->
    CheckPass = case emqx_auth_pgsql_cli:equery(Pool, AuthSql, AuthParams, ClientInfo) of
                    {ok, _, [Record]} ->
                        check_pass(erlang:append_element(Record, Password), HashType);
                    {ok, _, []} ->
                        {error, not_found};
                    {error, Reason} ->
                        ?LOG_SENSITIVE(error, "[Postgres] query '~p' failed: ~p", [AuthSql, Reason]),
                        {error, not_found}
                end,
    case CheckPass of
        ok ->
            ?LOG_SENSITIVE(debug, "[Postgres] Auth succeeded, Client: ~p", [ClientInfo]),
            {stop, AuthResult#{is_superuser => is_superuser(Pool, SuperQuery, ClientInfo),
                                anonymous => false,
                                auth_result => success}};
        {error, not_found} ->
            ?LOG_SENSITIVE(debug, "[Postgres] Auth ignored, Client: ~p", [ClientInfo]);
        {error, ResultCode} ->
            ?LOG_SENSITIVE(error, "[Postgres] Auth failed: ~p", [ResultCode]),
            {stop, AuthResult#{auth_result => ResultCode, anonymous => false}}
    end.

%%--------------------------------------------------------------------
%% Is Superuser?
%%--------------------------------------------------------------------

-spec(is_superuser(atom(),undefined | {string(), list()}, emqx_types:client()) -> boolean()).
is_superuser(_Pool, undefined, _Client) ->
    false;
is_superuser(Pool, {SuperSql, Params}, ClientInfo) ->
    case emqx_auth_pgsql_cli:equery(Pool, SuperSql, Params, ClientInfo) of
        {ok, [_Super], [{true}]} ->
            true;
        {ok, [_Super], [_False]} ->
            false;
        {ok, [_Super], []} ->
            false;
        {error, _Error} ->
            false
    end.

check_pass(Password, HashType) ->
    case emqx_passwd:check_pass(Password, HashType) of
        ok -> ok;
        {error, _Reason} -> {error, not_authorized}
    end.

description() -> "Authentication with PostgreSQL".
