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

-module(emqx_acl_http).

-include("emqx_auth_http.hrl").

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").

-logger_header("[ACL http]").

-import(emqx_auth_http_cli,
        [ request/7
        , feedvar/2
        ]).

%% ACL callbacks
-export([ check_acl/5
        , description/0
        ]).

%%--------------------------------------------------------------------
%% ACL callbacks
%%--------------------------------------------------------------------

check_acl(#{username := <<$$, _/binary>>}, _PubSub, _Topic, _AclResult, _Params) ->
    ok;
check_acl(ClientInfo, PubSub, Topic, _AclResult, #{acl := ACLParams = #{path := Path}}) ->
    ClientInfo1 = ClientInfo#{access => access(PubSub), topic => Topic},
    Username = maps:get(username, ClientInfo1, undefined),
    case check_acl_request(ACLParams, ClientInfo1) of
        {ok, 200, <<"ignore">>} ->
            ?LOG(debug, "ignored, ~s to topic ~ts, username: ~ts",
                [PubSub, Topic, Username]);
        {ok, 200, _Body} ->
            ?LOG(debug, "Allow ~s to topic ~ts, username: ~ts",
                 [PubSub, Topic, Username]),
            {stop, allow};
        {ok, Code, _Body} ->
            ?LOG(warning, "Deny ~s to topic ~ts, username: ~ts, http response code: ~p",
                 [PubSub, Topic, Username, Code]),
            {stop, deny};
        {error, Error} ->
            ?LOG(warning, "Deny ~s to topic ~ts, username: ~ts, due to request "
                          "http server failure, path: ~p, error: ~0p",
                          [PubSub, Topic, Username, Path, Error]),
            ok
    end.

description() -> "ACL with HTTP API".

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

check_acl_request(ACLParams =
                  #{pool_name := PoolName,
                    path := Path,
                    method := Method,
                    headers := Headers,
                    params := Params,
                    timeout := Timeout}, ClientInfo) ->
    Retry = maps:get(retry_times, ACLParams, ?DEFAULT_RETRY_TIMES),
    request(PoolName, Method, Path, Headers, feedvar(Params, ClientInfo), Timeout, Retry).

access(subscribe) -> 1;
access(publish)   -> 2.
