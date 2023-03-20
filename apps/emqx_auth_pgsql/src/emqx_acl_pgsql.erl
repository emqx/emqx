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

-module(emqx_acl_pgsql).

-include("emqx_auth_pgsql.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").

%% ACL callbacks
-export([ check_acl/5
        , description/0
        ]).

check_acl(ClientInfo, PubSub, Topic, NoMatchAction, #{pool := Pool} = State) ->
    do_check_acl(Pool, ClientInfo, PubSub, Topic, NoMatchAction, State).

do_check_acl(_Pool, #{username := <<$$, _/binary>>}, _PubSub, _Topic, _NoMatchAction, _State) ->
    ok;
do_check_acl(Pool, ClientInfo, PubSub, Topic, _NoMatchAction, #{acl_query := {AclSql, AclParams}}) ->
    case emqx_auth_pgsql_cli:equery(Pool, AclSql, AclParams, ClientInfo) of
        {ok, _, []} ->
            ?LOG_SENSITIVE(debug,
                "[Postgres] ACL ignored, Topic: ~p, Action: ~p for Client: ~p",
                [Topic, PubSub, ClientInfo]);
        {ok, _, Rows} ->
            Rules = filter(PubSub, compile(Rows)),
            case match(ClientInfo, Topic, Rules) of
                {matched, allow} ->
                    ?LOG_SENSITIVE(debug,
                                   "[Postgres] Allow Topic: ~p, Action: ~p for Client: ~p",
                                   [Topic, PubSub, ClientInfo]),
                    {stop, allow};
                {matched, deny} ->
                    ?LOG_SENSITIVE(debug,
                                   "[Postgres] Deny Topic: ~p, Action: ~p for Client: ~p",
                                   [Topic, PubSub, ClientInfo]),
                    {stop, deny};
                nomatch -> ok
            end;
        {error, Reason} ->
            ?LOG(error, "[Postgres] do_check_acl error: ~p~n", [Reason]),
            ok
    end.

match(_ClientInfo, _Topic, []) ->
    nomatch;

match(ClientInfo, Topic, [Rule|Rules]) ->
    case emqx_access_rule:match(ClientInfo, Topic, Rule) of
        nomatch -> match(ClientInfo, Topic, Rules);
        {matched, AllowDeny} -> {matched, AllowDeny}
    end.

filter(PubSub, Rules) ->
    [Term || Term = {_, _, Access, _} <- Rules,
             Access =:= PubSub orelse Access =:= pubsub].

compile(Rows) ->
    compile(Rows, []).
compile([], Acc) ->
    Acc;
compile([{Allow, IpAddr, Username, ClientId, Access, Topic}|T], Acc) ->
    Who  = who(IpAddr, Username, ClientId),
    Term = {allow(Allow), Who, access(Access), [topic(Topic)]},
    compile(T, [emqx_access_rule:compile(Term) | Acc]).

who(_, <<"$all">>, _) ->
    all;
who(null, null, null) ->
    throw(undefined_who);
who(CIDR, Username, ClientId) ->
    Cols = [{ipaddr, b2l(CIDR)}, {user, Username}, {client, ClientId}],
    case [{C, V} || {C, V} <- Cols, not empty(V)] of
        [Who] -> Who;
        Conds -> {'and', Conds}
    end.

allow(1)        -> allow;
allow(0)        -> deny;
allow(<<"1">>)  -> allow;
allow(<<"0">>)  -> deny.

access(1)       -> subscribe;
access(2)       -> publish;
access(3)       -> pubsub;
access(<<"1">>) -> subscribe;
access(<<"2">>) -> publish;
access(<<"3">>) -> pubsub.

topic(<<"eq ", Topic/binary>>) ->
    {eq, Topic};
topic(Topic) ->
    Topic.

description() ->
    "ACL with Postgres".

b2l(null) -> null;
b2l(B)    -> binary_to_list(B).

empty(null) -> true;
empty("")   -> true;
empty(<<>>) -> true;
empty(_)    -> false.
