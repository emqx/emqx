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

-module(emqx_acl_mongo).

-include("emqx_auth_mongo.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").

%% ACL callbacks
-export([ check_acl/5
        , description/0
        ]).

check_acl(#{username := <<$$, _/binary>>}, _PubSub, _Topic, _AclResult, _State) ->
    ok;
check_acl(ClientInfo, PubSub, Topic, _AclResult, Env = #{aclquery := AclQuery}) ->
    #aclquery{collection = Coll, selector = SelectorList} = AclQuery,
    Pool = maps:get(pool, Env, ?APP),
    SelectorMapList =
        lists:map(fun(Selector) ->
            maps:from_list(emqx_auth_mongo:replvars(Selector, ClientInfo))
        end, SelectorList),
    case emqx_auth_mongo:query_multi(Pool, Coll, SelectorMapList) of
        [] -> ?LOG_SENSITIVE(debug,
                    "[MongoDB] ACL ignored, Topic: ~p, Action: ~p for Client: ~p",
                    [Topic, PubSub, ClientInfo]);
        Rows ->
            try match(ClientInfo, Topic, topics(PubSub, Rows)) of
                matched ->
                    ?LOG_SENSITIVE(debug,
                                   "[MongoDB] Allow Topic: ~p, Action: ~p for Client: ~p",
                                   [Topic, PubSub, ClientInfo]),
                    {stop, allow};
                nomatch ->
                    ?LOG_SENSITIVE(debug,
                                   "[MongoDB] Deny Topic: ~p, Action: ~p for Client: ~p",
                                   [Topic, PubSub, ClientInfo]),
                    {stop, deny}
            catch
                _Err:Reason->
                    ?LOG(error, "[MongoDB] ACL ignored, check mongo ~p ACL failed, got ACL config: ~p, error: ~p",
                                [PubSub, Rows, Reason]),
                    ignore
            end
    end.

match(_ClientInfo, _Topic, []) ->
    nomatch;
match(ClientInfo, Topic, [TopicFilter|More]) ->
    case emqx_topic:match(Topic, feedvar(ClientInfo, TopicFilter)) of
        true  -> matched;
        false -> match(ClientInfo, Topic, More)
    end.

topics(publish, Rows) ->
    lists:foldl(fun(Row, Acc) ->
        Topics = maps:get(<<"publish">>, Row, []) ++ maps:get(<<"pubsub">>, Row, []),
        lists:umerge(Acc, Topics)
    end, [], Rows);

topics(subscribe, Rows) ->
    lists:foldl(fun(Row, Acc) ->
        Topics = maps:get(<<"subscribe">>, Row, []) ++ maps:get(<<"pubsub">>, Row, []),
        lists:umerge(Acc, Topics)
    end, [], Rows).

feedvar(#{clientid := ClientId, username := Username}, Str) ->
    lists:foldl(fun({Var, Val}, Acc) ->
                    feedvar(Acc, Var, Val)
                end, Str, [{"%u", Username}, {"%c", ClientId}]).

feedvar(Str, _Var, undefined) ->
    Str;
feedvar(Str, Var, Val) ->
    re:replace(Str, Var, Val, [global, {return, binary}]).

description() -> "ACL with MongoDB".
