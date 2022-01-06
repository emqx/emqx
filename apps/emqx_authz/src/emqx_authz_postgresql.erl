%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authz_postgresql).

-include("emqx_authz.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_placeholder.hrl").

-behaviour(emqx_authz).

%% AuthZ Callbacks
-export([ description/0
        , init/1
        , destroy/1
        , dry_run/1
        , authorize/4
        ]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

description() ->
    "AuthZ with Postgresql".

init(#{query := SQL} = Source) ->
    case emqx_authz_utils:create_resource(emqx_connector_pgsql, Source) of
        {error, Reason} -> error({load_config_error, Reason});
        {ok, Id} -> Source#{annotations =>
                            #{id => Id,
                              query => parse_query(SQL)}}
    end.

destroy(#{annotations := #{id := Id}}) ->
    ok = emqx_resource:remove_local(Id).

dry_run(Source) ->
    emqx_resource:create_dry_run_local(emqx_connector_pgsql, Source).

parse_query(Sql) ->
    case re:run(Sql, ?RE_PLACEHOLDER, [global, {capture, all, list}]) of
        {match, Capured} ->
            PlaceHolders = [PlaceHolder || [PlaceHolder] <- Capured],
            Replacements = ["$" ++ integer_to_list(I) || I <- lists:seq(1, length(PlaceHolders))],
            NSql = lists:foldl(
                     fun({PlaceHolder, Replacement}, S) ->
                             re:replace(
                               S, emqx_authz:ph_to_re(PlaceHolder), Replacement, [{return, list}])
                     end, Sql, lists:zip(PlaceHolders, Replacements)),
            {NSql, PlaceHolders};
        nomatch ->
            {Sql, []}
    end.

authorize(Client, PubSub, Topic,
            #{annotations := #{id := ResourceID,
                               query := {Query, Params}
                              }
             }) ->
    case emqx_resource:query(ResourceID, {prepared_query, ResourceID, Query, replvar(Params, Client)}) of
        {ok, _Columns, []} -> nomatch;
        {ok, Columns, Rows} ->
            do_authorize(Client, PubSub, Topic, Columns, Rows);
        {error, Reason} ->
            ?SLOG(error, #{ msg => "query_postgresql_error"
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
    Permission = lists:nth(index(<<"permission">>, 2, Columns), erlang:tuple_to_list(Row)),
    Action = lists:nth(index(<<"action">>, 2, Columns), erlang:tuple_to_list(Row)),
    Topic = lists:nth(index(<<"topic">>, 2, Columns), erlang:tuple_to_list(Row)),
    {Permission, all, Action, [Topic]}.

index(Key, N, TupleList) when is_integer(N) ->
    Tuple = lists:keyfind(Key, N, TupleList),
    index(Tuple, TupleList, 1);
index(_Tuple, [], _Index) -> {error, not_found};
index(Tuple, [Tuple | _TupleList], Index) -> Index;
index(Tuple, [_ | TupleList], Index) -> index(Tuple, TupleList, Index + 1).

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
