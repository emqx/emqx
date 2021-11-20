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

-module(emqx_authz_mysql).

-include("emqx_authz.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_placeholder.hrl").

%% AuthZ Callbacks
-export([ description/0
        , parse_query/1
        , authorize/4
        ]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

description() ->
    "AuthZ with Mysql".

parse_query(undefined) ->
    undefined;
parse_query(Sql) ->
    case re:run(Sql, "'%[ucCad]'", [global, {capture, all, list}]) of
        {match, Variables} ->
            Params = [Var || [Var] <- Variables],
            {re:replace(Sql, "'%[ucCad]'", "?", [global, {return, list}]), Params};
        nomatch ->
            {Sql, []}
    end.

authorize(Client, PubSub, Topic,
            #{annotations := #{id := ResourceID,
                               query := {Query, Params}
                              }
             }) ->
    case emqx_resource:query(ResourceID, {sql, Query, replvar(Params, Client)}) of
        {ok, _Columns, []} -> nomatch;
        {ok, Columns, Rows} ->
            do_authorize(Client, PubSub, Topic, Columns, Rows);
        {error, Reason} ->
            ?SLOG(error, #{ msg => "query_mysql_error"
                          , reason => Reason
                          , resource_id => ResourceID}),
            nomatch
    end.

do_authorize(_Client, _PubSub, _Topic, _Columns, []) ->
    nomatch;
do_authorize(Client, PubSub, Topic, Columns, [Row | Tail]) ->
    case emqx_authz_rule:match(Client, PubSub, Topic,
                               emqx_authz_rule:compile(format_result(Columns, Row))
                              ) of
        {matched, Permission} -> {matched, Permission};
        nomatch -> do_authorize(Client, PubSub, Topic, Columns, Tail)
    end.

format_result(Columns, Row) ->
    Permission = lists:nth(index(<<"permission">>, Columns), Row),
    Action = lists:nth(index(<<"action">>, Columns), Row),
    Topic = lists:nth(index(<<"topic">>, Columns), Row),
    {Permission, all, Action, [Topic]}.

index(Elem, List) ->
    index(Elem, List, 1).
index(_Elem, [], _Index) -> {error, not_found};
index(Elem, [ Elem | _List], Index) -> Index;
index(Elem, [ _ | List], Index) -> index(Elem, List, Index + 1).

replvar(Params, ClientInfo) ->
    replvar(Params, ClientInfo, []).

replvar([], _ClientInfo, Acc) ->
    lists:reverse(Acc);

replvar([?PH_S_USERNAME | Params], ClientInfo, Acc) ->
    replvar(Params, ClientInfo, [safe_get(username, ClientInfo) | Acc]);
replvar([?PH_S_CLIENTID | Params], ClientInfo = #{clientid := ClientId}, Acc) ->
    replvar(Params, ClientInfo, [ClientId | Acc]);
replvar([?PH_S_PEERHOST | Params], ClientInfo = #{peerhost := IpAddr}, Acc) ->
    replvar(Params, ClientInfo, [inet_parse:ntoa(IpAddr) | Acc]);
replvar([?PH_S_CERT_CN_NAME | Params], ClientInfo, Acc) ->
    replvar(Params, ClientInfo, [safe_get(cn, ClientInfo) | Acc]);
replvar([?PH_S_CERT_SUBJECT | Params], ClientInfo, Acc) ->
    replvar(Params, ClientInfo, [safe_get(dn, ClientInfo) | Acc]);
replvar([Param | Params], ClientInfo, Acc) ->
    replvar(Params, ClientInfo, [Param | Acc]).

safe_get(K, ClientInfo) ->
    bin(maps:get(K, ClientInfo, "undefined")).

bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
bin(B) when is_binary(B) -> B;
bin(L) when is_list(L) -> list_to_binary(L);
bin(X) -> X.
