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

-module(emqx_authz_pgsql).

-include("emqx_authz.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").

%% ACL Callbacks
-export([ description/0
        , parse_query/1
        , check_authz/4
        ]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

description() ->
    "AuthZ with pgsql".

parse_query(undefined) ->
    undefined;
parse_query(Sql) ->
    case re:run(Sql, "'%[ucCad]'", [global, {capture, all, list}]) of
        {match, Variables} ->
            Params = [Var || [Var] <- Variables],
            Vars = ["$" ++ integer_to_list(I) || I <- lists:seq(1, length(Params))],
            NSql = lists:foldl(fun({Param, Var}, S) ->
                       re:replace(S, Param, Var, [{return, list}])
                   end, Sql, lists:zip(Params, Vars)),
            {NSql, Params};
        nomatch ->
            {Sql, []}
    end.

check_authz(Client, PubSub, Topic,
            #{<<"resource_id">> := ResourceID,
              <<"sql">> := {SQL, Params}
             }) ->
    case emqx_resource:query(ResourceID, {sql, SQL, replvar(Params, Client)}) of
        {ok, _Columns, []} -> nomatch;
        {ok, _Columns, Rows} ->
            do_check_authz(Client, PubSub, Topic, Rows);
        {error, Reason} ->
            ?LOG(error, "[AuthZ] do_check_pgsql error: ~p~n", [Reason]),
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

match(Client, PubSub, Topic, {Access, IpAddr, Username, ClientId, Action, TopicFilter}) ->
    Rule = #{<<"principal">> => principal(IpAddr, Username, ClientId),
             <<"topics">> => [topic(TopicFilter)],
             <<"action">> => action(Action),
             <<"access">> =>  access(Access)
            },
    case emqx_authz:match(Client, PubSub, Topic, emqx_authz:compile(Rule)) of
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

empty(null) -> true;
empty("")   -> true;
empty(<<>>) -> true;
empty(_)    -> false.

replvar(Params, ClientInfo) ->
    replvar(Params, ClientInfo, []).

replvar([], _ClientInfo, Acc) ->
    lists:reverse(Acc);

replvar(["'%u'" | Params], ClientInfo, Acc) ->
    replvar(Params, ClientInfo, [safe_get(username, ClientInfo) | Acc]);
replvar(["'%c'" | Params], ClientInfo = #{clientid := ClientId}, Acc) ->
    replvar(Params, ClientInfo, [ClientId | Acc]);
replvar(["'%a'" | Params], ClientInfo = #{peerhost := IpAddr}, Acc) ->
    replvar(Params, ClientInfo, [inet_parse:ntoa(IpAddr) | Acc]);
replvar(["'%C'" | Params], ClientInfo, Acc) ->
    replvar(Params, ClientInfo, [safe_get(cn, ClientInfo)| Acc]);
replvar(["'%d'" | Params], ClientInfo, Acc) ->
    replvar(Params, ClientInfo, [safe_get(dn, ClientInfo)| Acc]);
replvar([Param | Params], ClientInfo, Acc) ->
    replvar(Params, ClientInfo, [Param | Acc]).

safe_get(K, ClientInfo) ->
    bin(maps:get(K, ClientInfo, "undefined")).

bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
bin(B) when is_binary(B) -> B;
bin(L) when is_list(L) -> list_to_binary(L);
bin(X) -> X.

