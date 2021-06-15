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

-module(emqx_authorization_mysql).

-include("emqx_authorization.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").

%% ACL Callbacks
-export([ check_authz/4
        , description/0
        ]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

check_authz(Client, PubSub, Topic,
            #{<<"resource_id">> := ResourceID,
              <<"sql">> := SQL
             }) ->
    case emqx_resource:query(ResourceID, {sql, SQL}) of
        {ok, _Columns, []} -> nomatch;
        {ok, _Columns, Rows} ->
            do_check_authz(Client, PubSub, Topic, Rows);
        {error, Reason} ->
            ?LOG(error, "[AuthZ] do_check_mysql error: ~p~n", [Reason]),
            nomatch
    end.

do_check_authz(_Client, _PubSub, _Topic, []) ->
    nomatch;
do_check_authz(Client, PubSub, Topic, [Row]) ->
    match(Client, PubSub, Topic, Row);
do_check_authz(Client, PubSub, Topic, [Row | Tail]) ->
    case match(Client, PubSub, Topic, Row) of
        {matched, Access} -> {matched, Access};
        nomatch -> do_check_authz(Client, PubSub, Topic, Tail)
    end.

match(Client, PubSub, Topic, [Access, IpAddr, Username, ClientId, Action, TopicFilter]) ->
    Rule = #{<<"principal">> => principal(IpAddr, Username, ClientId),
             <<"topics">> => [topic(TopicFilter)],
             <<"action">> => action(Action),
             <<"access">> =>  access(Access)
            },
    case emqx_authorization:match(Client, PubSub, Topic, emqx_authorization:compile(Rule)) of
        true -> {matched, access(Access)};
        false -> nomatch
    end.

principal(_, <<"$all">>, _) ->
    all;
principal(null, null, null) ->
    throw(undefined_who);
principal(CIDR, Username, ClientId) ->
    Cols = [{<<"ipaddress">>, CIDR}, {<<"username">>, Username}, {<<"clientid">>, ClientId}],
    case [#{C => V} || {C, V} <- Cols, not empty(V)] of
        [Who] -> Who;
        Conds -> #{<<"and">> => Conds}
    end.

access(1)  -> allow;
access(0)  -> deny;
access(<<"1">>)  -> allow;
access(<<"0">>)  -> deny.

action(1) -> sub;
action(2) -> pub;
action(3) -> pubsub;
action(<<"1">>) -> sub;
action(<<"2">>) -> pub;
action(<<"3">>) -> pubsub.

topic(<<"eq ", Topic/binary>>) ->
    #{<<"eq">> => Topic};
topic(Topic) ->
    Topic.

description() ->
    "ACL with Mysql".

empty(null) -> true;
empty("")   -> true;
empty(<<>>) -> true;
empty(_)    -> false.
