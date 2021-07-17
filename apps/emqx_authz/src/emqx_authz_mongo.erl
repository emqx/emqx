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

-module(emqx_authz_mongo).

-include("emqx_authz.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").

%% AuthZ Callbacks
-export([ authorize/4
        , description/0
        ]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

description() ->
    "AuthZ with Mongo".

authorize(Client, PubSub, Topic,
            #{resource_id := ResourceID,
              collection := Collection,
              find := Find
             }) ->
    case emqx_resource:query(ResourceID, {find, Collection, replvar(Find, Client), #{}}) of
        {error, Reason} ->
            ?LOG(error, "[AuthZ] Query mongo error: ~p", [Reason]),
            nomatch;
        [] -> nomatch;
        Rows ->
            do_authorize(Client, PubSub, Topic, Rows)
    end.

do_authorize(_Client, _PubSub, _Topic, []) ->
    nomatch;
do_authorize(Client, PubSub, Topic, [Rule | Tail]) ->
    case match(Client, PubSub, Topic, Rule) of
        {matched, Permission} -> {matched, Permission};
        nomatch -> do_authorize(Client, PubSub, Topic, Tail)
    end.

match(Client, PubSub, Topic,
      #{<<"topics">> := Topics,
        <<"permission">> := Permission,
        <<"action">> := Action
       }) ->
    Rule = #{<<"permission">> => Permission,
             <<"topics">> => Topics,
             <<"action">> => Action
            },
    #{simple_rule :=
      #{permission := NPermission} = NRule
     } = hocon_schema:check_plain(
            emqx_authz_schema,
            #{<<"simple_rule">> => Rule},
            #{atom_key => true},
            [simple_rule]),
    case emqx_authz:match(Client, PubSub, Topic, emqx_authz:init_rule(NRule)) of
        true -> {matched, NPermission};
        false -> nomatch
    end.

replvar(Find, #{clientid := Clientid,
                username := Username,
                peerhost := IpAddress
               }) ->
    Fun = fun
              _Fun(K, V, AccIn) when is_map(V) -> maps:put(K, maps:fold(_Fun, AccIn, V), AccIn);
              _Fun(K, V, AccIn) when is_list(V) ->
                  maps:put(K, [ begin
                                    [{K1, V1}] = maps:to_list(M),
                                    _Fun(K1, V1, AccIn)
                                end || M <- V],
                           AccIn);
              _Fun(K, V, AccIn) when is_binary(V) ->
                  V1 = re:replace(V,  "%c", bin(Clientid), [global, {return, binary}]),
                  V2 = re:replace(V1, "%u", bin(Username), [global, {return, binary}]),
                  V3 = re:replace(V2, "%a", inet_parse:ntoa(IpAddress), [global, {return, binary}]),
                  maps:put(K, V3, AccIn);
              _Fun(K, V, AccIn) -> maps:put(K, V, AccIn)
          end,
    maps:fold(Fun, #{}, Find).

bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
bin(B) when is_binary(B) -> B;
bin(L) when is_list(L) -> list_to_binary(L);
bin(X) -> X.

