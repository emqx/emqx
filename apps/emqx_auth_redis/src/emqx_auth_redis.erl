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

-module(emqx_auth_redis).

-include("emqx_auth_redis.hrl").

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").

-export([ check/3
        , description/0
        ]).

check(ClientInfo = #{password := Password}, AuthResult,
      #{auth_cmd  := AuthCmd,
        super_cmd := SuperCmd,
        hash_type := HashType,
        timeout   := Timeout,
        type      := Type,
        pool      := Pool}) ->
    CheckPass = case emqx_auth_redis_cli:q(Pool, Type, AuthCmd, ClientInfo, Timeout) of
                    {ok, PassHash} when is_binary(PassHash) ->
                        check_pass({PassHash, Password}, HashType);
                    {ok, [undefined|_]} ->
                        {error, not_found};
                    {ok, [PassHash]} ->
                        check_pass({PassHash, Password}, HashType);
                    {ok, [PassHash, Salt|_]} ->
                        check_pass({PassHash, Salt, Password}, HashType);
                    {error, Reason} ->
                        ?LOG_SENSITIVE(error, "[Redis] Command: ~p failed: ~p", [AuthCmd, Reason]),
                        {error, not_found}
                end,
    case CheckPass of
        ok ->
            ?LOG_SENSITIVE(debug, "[Redis] Auth succeeded, Client: ~p", [ClientInfo]),
            IsSuperuser = is_superuser(Pool, Type, SuperCmd, ClientInfo, Timeout),
            {stop, AuthResult#{is_superuser => IsSuperuser,
                               anonymous    => false,
                               auth_result  => success}};
        {error, not_found} ->
            ?LOG_SENSITIVE(debug, "[Redis] Auth ignored, Client: ~p", [ClientInfo]);
        {error, ResultCode} ->
            ?LOG_SENSITIVE(error, "[Redis] Auth failed: ~p", [ResultCode]),
            {stop, AuthResult#{auth_result => ResultCode, anonymous => false}}
    end.

description() -> "Authentication with Redis".

-spec(is_superuser(atom(), atom(), undefined|list(), emqx_types:client(), timeout()) -> boolean()).
is_superuser(_Pool, _Type, undefined, _ClientInfo, _Timeout) -> false;
is_superuser(Pool, Type, SuperCmd, ClientInfo, Timeout) ->
    case emqx_auth_redis_cli:q(Pool, Type, SuperCmd, ClientInfo, Timeout) of
        {ok, undefined} -> false;
        {ok, <<"1">>}   -> true;
        {ok, _Other}    -> false;
        {error, _Error} -> false
    end.

check_pass(Password, HashType) ->
    case emqx_passwd:check_pass(Password, HashType) of
        ok -> ok;
        {error, _Reason} -> {error, not_authorized}
    end.
