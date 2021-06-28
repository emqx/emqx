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

%% ACL Callbacks
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
            #{<<"resource_id">> := ResourceID,
              <<"sql">> := {SQL, Params}
             }) ->
    case emqx_resource:query(ResourceID, {sql, SQL, replvar(Params, Client)}) of
        {ok, _Columns, []} -> nomatch;
        {ok, Columns, Rows} ->
            do_authorize(Client, PubSub, Topic, Columns, Rows);
        {error, Reason} ->
            ?LOG(error, "[AuthZ] Query mysql error: ~p~n", [Reason]),
            nomatch
    end.

do_authorize(_Client, _PubSub, _Topic, _Columns, []) ->
    nomatch;
do_authorize(Client, PubSub, Topic, Columns, [Row | Tail]) ->
    case match(Client, PubSub, Topic, format_result(Columns, Row)) of
        {matched, Permission} -> {matched, Permission};
        nomatch -> do_authorize(Client, PubSub, Topic, Columns, Tail)
    end.

format_result(Columns, Row) ->
    L = [ begin
              K = lists:nth(I, Columns),
              V = lists:nth(I, Row),
              {K, V}
          end || I <- lists:seq(1, length(Columns)) ],
    maps:from_list(L).

match(Client, PubSub, Topic,
      #{<<"permission">> := Permission,
        <<"action">> := Action,
        <<"clientid">> := ClientId,
        <<"username">> := Username,
        <<"ipaddress">> := IpAddress,
        <<"topic">> := TopicFilter
       }) ->
    Rule = #{<<"principal">> => principal(IpAddress, Username, ClientId),
             <<"topics">> => [TopicFilter],
             <<"action">> => Action,
             <<"permission">> =>  Permission
            },
    #{<<"simple_rule">> :=
      #{<<"permission">> := NPermission} = NRule
     } = hocon_schema:check_plain(
            emqx_authz_schema,
            #{<<"simple_rule">> => Rule},
            #{},
            [simple_rule]),
    case emqx_authz:match(Client, PubSub, Topic, emqx_authz:compile(NRule)) of
        true -> {matched, NPermission};
        false -> nomatch
    end.

principal(CIDR, Username, ClientId) ->
    Cols = [{<<"ipaddress">>, CIDR}, {<<"username">>, Username}, {<<"clientid">>, ClientId}],
    case [#{C => V} || {C, V} <- Cols, not empty(V)] of
        [] -> throw(undefined_who);
        [Who] -> Who;
        Conds -> #{<<"and">> => Conds}
    end.

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

