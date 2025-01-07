%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_mgmt_api_topics_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            {emqx, "durable_sessions.enable = true"},
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    Peer = emqx_common_test_helpers:start_peer(node1, []),
    [{apps, Apps}, {peer, Peer} | Config].

end_per_suite(Config) ->
    _ = emqx_common_test_helpers:stop_peer(?config(peer, Config)),
    ok = emqx_cth_suite:stop(?config(apps, Config)).

t_nodes_api(Config) ->
    Node = atom_to_binary(node(), utf8),
    Topic = <<"test_topic">>,
    Client = client(?FUNCTION_NAME),
    {ok, _, _} = emqtt:subscribe(Client, Topic),

    %% list all
    RoutesData = request_json(get, ["topics"]),
    Meta = maps:get(<<"meta">>, RoutesData),
    ?assertEqual(1, maps:get(<<"page">>, Meta)),
    ?assertEqual(emqx_mgmt:default_row_limit(), maps:get(<<"limit">>, Meta)),
    ?assertEqual(1, maps:get(<<"count">>, Meta)),
    [Route | _] = maps:get(<<"data">>, RoutesData),
    ?assertEqual(Topic, maps:get(<<"topic">>, Route)),
    ?assertEqual(Node, maps:get(<<"node">>, Route)),

    %% exact match
    Topic2 = <<"test_topic_2">>,
    {ok, _, _} = emqtt:subscribe(Client, Topic2),
    MatchData = request_json(get, ["topics"], [
        {"topic", Topic2},
        {"node", atom_to_list(node())}
    ]),
    ?assertMatch(
        #{
            <<"data">> := [#{<<"topic">> := Topic2, <<"node">> := Node}],
            <<"meta">> := #{<<"page">> := 1, <<"limit">> := 100, <<"count">> := 1}
        },
        MatchData
    ),

    %% get topics/:topic
    %% We add another route here to ensure that the response handles
    %% multiple routes for a single topic
    Peer = ?config(peer, Config),
    ok = emqx_router:add_route(Topic, Peer),
    RouteResponse = request_json(get, ["topics", Topic]),
    ok = emqx_router:delete_route(Topic, Peer),

    [
        #{<<"topic">> := Topic, <<"node">> := Node1},
        #{<<"topic">> := Topic, <<"node">> := Node2}
    ] = RouteResponse,

    ?assertEqual(lists:sort([Node, atom_to_binary(Peer)]), lists:sort([Node1, Node2])),

    ok = emqtt:stop(Client).

t_paging(_Config) ->
    Node = atom_to_list(node()),
    Client1 = client(c_paging_1),
    Client2 = client(c_paging_2),
    Topics1 = [
        <<"t/+">>,
        <<"test/client/#">>,
        <<"test/1">>,
        <<"test/2">>,
        <<"test/3">>
    ],
    Topics2 = [
        <<"t/+">>,
        <<"test/client/#">>,
        <<"test/4">>,
        <<"test/5">>,
        <<"test/6">>
    ],
    ok = lists:foreach(fun(T) -> {ok, _, _} = emqtt:subscribe(Client1, T) end, Topics1),
    ok = lists:foreach(fun(T) -> {ok, _, _} = emqtt:subscribe(Client2, T) end, Topics2),
    Matched = request_json(get, ["topics"]),
    ?assertEqual(
        Matched,
        request_json(get, ["topics"], [{"node", Node}])
    ),
    ?assertEqual(
        %% NOTE: No `count` in this case.
        #{<<"hasnext">> => true, <<"page">> => 1, <<"limit">> => 3},
        maps:get(<<"meta">>, request_json(get, ["topics"], [{"node", Node}, {"limit", "3"}]))
    ),
    R1 = #{<<"data">> := Data1} = request_json(get, ["topics"], [{"page", "1"}, {"limit", "5"}]),
    R2 = #{<<"data">> := Data2} = request_json(get, ["topics"], [{"page", "2"}, {"limit", "5"}]),
    ?assertMatch(
        #{
            <<"meta">> := #{<<"hasnext">> := true, <<"page">> := 1, <<"count">> := 8},
            <<"data">> := [_1, _2, _3, _4, _5]
        },
        R1
    ),
    ?assertMatch(
        #{
            <<"meta">> := #{<<"hasnext">> := false, <<"page">> := 2, <<"count">> := 8},
            <<"data">> := [_6, _7, _8]
        },
        R2
    ),
    ?assertEqual(
        lists:usort(Topics1 ++ Topics2),
        lists:sort([T || #{<<"topic">> := T} <- Data1 ++ Data2])
    ),

    ok = emqtt:stop(Client1),
    ok = emqtt:stop(Client2).

t_percent_topics(_Config) ->
    Node = atom_to_binary(node(), utf8),
    Topic = <<"test_%%1">>,
    Client = client(?FUNCTION_NAME),
    {ok, _, _} = emqtt:subscribe(Client, Topic),

    %% exact match with percent encoded topic
    MatchData = request_json(get, ["topics"], [
        {"topic", Topic},
        {"node", atom_to_list(node())}
    ]),
    ?assertMatch(
        #{
            <<"data">> := [#{<<"topic">> := Topic, <<"node">> := Node}],
            <<"meta">> := #{<<"page">> := 1, <<"limit">> := 100, <<"count">> := 1}
        },
        MatchData
    ),

    ok = emqtt:stop(Client).

t_shared_topics(_Configs) ->
    Node = atom_to_binary(node(), utf8),
    RealTopic = <<"t/+">>,
    Topic = <<"$share/g1/", RealTopic/binary>>,

    Client = client(?FUNCTION_NAME),
    {ok, _, _} = emqtt:subscribe(Client, Topic),
    {ok, _, _} = emqtt:subscribe(Client, RealTopic),

    %% exact match with shared topic
    MatchData = request_json(get, ["topics"], [
        {"topic", Topic},
        {"node", atom_to_list(node())}
    ]),
    ?assertMatch(
        #{
            <<"data">> := [#{<<"topic">> := Topic, <<"node">> := Node}],
            <<"meta">> := #{<<"page">> := 1, <<"limit">> := 100, <<"count">> := 1}
        },
        MatchData
    ),

    ok = emqtt:stop(Client).

t_queue_topics(_Configs) ->
    Node = atom_to_binary(node(), utf8),
    RealTopic = <<"t/+">>,
    Topic = <<"$queue/", RealTopic/binary>>,

    Client = client(?FUNCTION_NAME),
    {ok, _, _} = emqtt:subscribe(Client, Topic),
    {ok, _, _} = emqtt:subscribe(Client, RealTopic),

    %% exact match with shared topic
    MatchData = request_json(get, ["topics"], [
        {"topic", Topic},
        {"node", atom_to_list(node())}
    ]),
    ?assertMatch(
        #{
            <<"data">> := [#{<<"topic">> := Topic, <<"node">> := Node}],
            <<"meta">> := #{<<"page">> := 1, <<"limit">> := 100, <<"count">> := 1}
        },
        MatchData
    ),

    ok = emqtt:stop(Client).

t_shared_topics_invalid(_Config) ->
    %% no real topic
    InvalidShareTopicFilter = <<"$share/group">>,
    Path = emqx_mgmt_api_test_util:api_path(["topics"]),
    QS = uri_string:compose_query([
        {"topic", InvalidShareTopicFilter},
        {"node", atom_to_list(node())}
    ]),
    Headers = emqx_mgmt_api_test_util:auth_header_(),
    {error, {{_, 400, _}, _RespHeaders, Body}} = emqx_mgmt_api_test_util:request_api(
        get, Path, QS, Headers, [], #{return_all => true}
    ),
    ?assertMatch(
        #{<<"code">> := <<"INVALID_PARAMTER">>, <<"message">> := <<"topic_filter_invalid">>},
        emqx_utils_json:decode(Body, [return_maps])
    ).

t_persistent_topics(_Config) ->
    PersistentOpts = #{
        proto_ver => v5,
        properties => #{'Session-Expiry-Interval' => 300}
    },
    Client1 = client(t_persistent_topics_m1),
    Client2 = client(t_persistent_topics_m2),
    SessionId1 = <<"t_persistent_topics_p1">>,
    SessionId2 = <<"t_persistent_topics_p2">>,
    ClientPersistent1 = client(SessionId1, PersistentOpts),
    ClientPersistent2 = client(SessionId2, PersistentOpts),
    _ = [
        ?assertMatch({ok, _, _}, emqtt:subscribe(Client, Topic))
     || {Client, Topics} <- [
            {Client1, [<<"t/client/mem">>, <<"t/+">>]},
            {Client2, [<<"t/client/mem">>, <<"t/+">>]},
            {ClientPersistent1, [<<"t/persistent/#">>, <<"t/client/ps">>, <<"t/+">>]},
            {ClientPersistent2, [<<"t/persistent/#">>, <<"t/client/ps">>, <<"t/+">>]}
        ],
        Topic <- Topics
    ],
    Matched = request_json(get, ["topics"]),
    ?assertMatch(
        #{<<"page">> := 1, <<"limit">> := 100, <<"count">> := 8},
        maps:get(<<"meta">>, Matched)
    ),
    %% Get back both topics for both persistent and in-memory subscriptions.
    Expected = [
        #{<<"topic">> => <<"t/+">>, <<"node">> => atom_to_binary(node())},
        #{<<"topic">> => <<"t/+">>, <<"session">> => SessionId1},
        #{<<"topic">> => <<"t/+">>, <<"session">> => SessionId2},
        #{<<"topic">> => <<"t/client/mem">>, <<"node">> => atom_to_binary(node())},
        #{<<"topic">> => <<"t/client/ps">>, <<"session">> => SessionId1},
        #{<<"topic">> => <<"t/client/ps">>, <<"session">> => SessionId2},
        #{<<"topic">> => <<"t/persistent/#">>, <<"session">> => SessionId1},
        #{<<"topic">> => <<"t/persistent/#">>, <<"session">> => SessionId2}
    ],
    ?assertEqual(
        lists:sort(Expected),
        lists:sort(maps:get(<<"data">>, Matched))
    ),
    %% Are results the same when paginating?
    #{<<"data">> := Page1} = R1 = request_json(get, ["topics"], [{"page", "1"}, {"limit", "3"}]),
    #{<<"data">> := Page2} = request_json(get, ["topics"], [{"page", "2"}, {"limit", "3"}]),
    #{<<"data">> := Page3} = request_json(get, ["topics"], [{"page", "3"}, {"limit", "3"}]),
    ?assertEqual(
        lists:sort(Expected),
        lists:sort(Page1 ++ Page2 ++ Page3)
    ),
    %% Count respects persistent sessions.
    ?assertMatch(
        #{
            <<"meta">> := #{<<"page">> := 1, <<"limit">> := 3, <<"count">> := 8},
            <<"data">> := [_, _, _]
        },
        R1
    ),
    %% Filtering by node makes no sense for persistent sessions.
    ?assertMatch(
        #{
            <<"data">> := [
                #{<<"topic">> := <<"t/client/mem">>, <<"node">> := _},
                #{<<"topic">> := <<"t/+">>, <<"node">> := _}
            ],
            <<"meta">> := #{<<"page">> := 1, <<"limit">> := 100, <<"count">> := 2}
        },
        request_json(get, ["topics"], [{"node", atom_to_list(node())}])
    ).

%% Utilities

client(Name) ->
    client(Name, #{}).

client(Name, Overrides) ->
    {ok, Client} = emqtt:start_link(
        maps:merge(
            #{
                username => emqx_utils_conv:bin(Name),
                clientid => emqx_utils_conv:bin(Name)
            },
            Overrides
        )
    ),
    {ok, _} = emqtt:connect(Client),
    Client.

request_json(Method, Path) ->
    decode_response(request_api(Method, Path)).

request_json(Method, Path, QS) ->
    decode_response(request_api(Method, Path, QS)).

decode_response({ok, Response}) ->
    emqx_utils_json:decode(Response, [return_maps]);
decode_response({error, Reason}) ->
    error({request_api_error, Reason}).

request_api(Method, API) ->
    Path = emqx_mgmt_api_test_util:api_path(API),
    emqx_mgmt_api_test_util:request_api(Method, Path).

request_api(Method, API, QS) ->
    Path = emqx_mgmt_api_test_util:api_path(API),
    Auth = emqx_mgmt_api_test_util:auth_header_(),
    emqx_mgmt_api_test_util:request_api(Method, Path, uri_string:compose_query(QS), Auth).
