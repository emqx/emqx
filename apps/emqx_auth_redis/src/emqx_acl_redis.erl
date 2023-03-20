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

-module(emqx_acl_redis).

-include("emqx_auth_redis.hrl").

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").

-export([ check_acl/5
        , description/0
        ]).

check_acl(#{username := <<$$, _/binary>>}, _PubSub, _Topic, _AclResult, _Config) ->
    ok;
check_acl(ClientInfo, PubSub, Topic, _AclResult,
          #{acl_cmd := AclCmd, timeout := Timeout, type := Type, pool := Pool}) ->
    case emqx_auth_redis_cli:q(Pool, Type, AclCmd, ClientInfo, Timeout) of
        {ok, []} ->
            ?LOG_SENSITIVE(debug,
                "[Redis] ACL ignored, Topic: ~p, Action: ~p for Client: ~p",
                [Topic, PubSub, ClientInfo]);
        {ok, Rules} ->
            case match(ClientInfo, PubSub, Topic, Rules) of
                allow ->
                    ?LOG_SENSITIVE(debug,
                                   "[Redis] Allow Topic: ~p, Action: ~p for Client: ~p",
                                   [Topic, PubSub, ClientInfo]),
                    {stop, allow};
                nomatch ->
                    ?LOG_SENSITIVE(debug,
                                   "[Redis] Deny Topic: ~p, Action: ~p for Client: ~p",
                                   [Topic, PubSub, ClientInfo]),
                    {stop, deny}
            end;
        {error, Reason} ->
            ?LOG(error, "[Redis] do_check_acl error: ~p", [Reason]),
            ok
    end.

match(_ClientInfo, _PubSub, _Topic, []) ->
    nomatch;
match(ClientInfo, PubSub, Topic, [Filter, Access | Rules]) ->
    case {match_topic(Topic, feed_var(ClientInfo, Filter)),
          match_access(PubSub, b2i(Access))} of
        {true, true} -> allow;
        {_, _} -> match(ClientInfo, PubSub, Topic, Rules)
    end.

match_topic(Topic, Filter) ->
    emqx_topic:match(Topic, Filter).

match_access(subscribe, Access) ->
    (1 band Access) > 0;
match_access(publish, Access) ->
    (2 band Access) > 0.

feed_var(#{clientid := ClientId, username := Username}, Str) ->
    lists:foldl(fun({Var, Val}, Acc) ->
                feed_var(Acc, Var, Val)
        end, Str, [{"%u", Username}, {"%c", ClientId}]).

feed_var(Str, _Var, undefined) ->
    Str;
feed_var(Str, Var, Val) ->
    re:replace(Str, Var, Val, [global, {return, binary}]).

b2i(Bin) -> list_to_integer(binary_to_list(Bin)).

description() -> "Redis ACL Module".
