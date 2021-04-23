%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_auth_http).

-include("emqx_auth_http.hrl").

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/types.hrl").

-logger_header("[Auth http]").

-import(emqx_auth_http_cli,
        [ request/6
        , feedvar/2
        ]).

%% Callbacks
-export([ register_metrics/0
        , check/3
        , description/0
        ]).

-spec(register_metrics() -> ok).
register_metrics() ->
    lists:foreach(fun emqx_metrics:ensure/1, ?AUTH_METRICS).

check(ClientInfo, AuthResult, #{auth  := AuthParms = #{path := Path},
                                super := SuperParams}) ->
    case authenticate(AuthParms, ClientInfo) of
        {ok, 200, <<"ignore">>} ->
            emqx_metrics:inc(?AUTH_METRICS(ignore)), ok;
        {ok, 200, Body}  ->
            emqx_metrics:inc(?AUTH_METRICS(success)),
            IsSuperuser = is_superuser(SuperParams, ClientInfo),
            {stop, AuthResult#{is_superuser => IsSuperuser,
                                auth_result => success,
                                anonymous   => false,
                                mountpoint  => mountpoint(Body, ClientInfo)}};
        {ok, Code, _Body} ->
            ?LOG(error, "Deny connection from path: ~s, response http code: ~p",
                 [Path, Code]),
            emqx_metrics:inc(?AUTH_METRICS(failure)),
            {stop, AuthResult#{auth_result => http_to_connack_error(Code),
                               anonymous   => false}};
        {error, Error} ->
            ?LOG(error, "Request auth path: ~s, error: ~p", [Path, Error]),
            emqx_metrics:inc(?AUTH_METRICS(failure)),
            %%FIXME later: server_unavailable is not right.
            {stop, AuthResult#{auth_result => server_unavailable,
                               anonymous   => false}}
    end.

description() -> "Authentication by HTTP API".

%%--------------------------------------------------------------------
%% Requests
%%--------------------------------------------------------------------

authenticate(#{pool_name := PoolName,
               path := Path,
               method := Method,
               headers := Headers,
               params := Params,
               timeout := Timeout}, ClientInfo) ->
   request(PoolName, Method, Path, Headers, feedvar(Params, ClientInfo), Timeout).

-spec(is_superuser(maybe(map()), emqx_types:client()) -> boolean()).
is_superuser(undefined, _ClientInfo) ->
    false;
is_superuser(#{pool_name := PoolName,
               path := Path,
               method := Method,
               headers := Headers,
               params := Params,
               timeout := Timeout}, ClientInfo) ->
    case request(PoolName, Method, Path, Headers, feedvar(Params, ClientInfo), Timeout) of
        {ok, 200, _Body}   -> true;
        {ok, _Code, _Body} -> false;
        {error, Error}     -> ?LOG(error, "Request superuser path ~s, error: ~p", [Path, Error]),
                              false
    end.

mountpoint(Body, #{mountpoint := Mountpoint}) ->
    case emqx_json:safe_decode(Body, [return_maps]) of
        {error, _} -> Mountpoint;
        {ok, Json} when is_map(Json) ->
            maps:get(<<"mountpoint">>, Json, Mountpoint);
        {ok, _NotMap} -> Mountpoint
    end.

http_to_connack_error(400) -> bad_username_or_password;
http_to_connack_error(401) -> bad_username_or_password;
http_to_connack_error(403) -> not_authorized;
http_to_connack_error(429) -> banned;
http_to_connack_error(503) -> server_unavailable;
http_to_connack_error(504) -> server_busy;
http_to_connack_error(_) -> server_unavailable.
