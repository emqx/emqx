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
-module(emqx_mgmt_api_subscription_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(CLIENTID, <<"api_clientid">>).
-define(USERNAME, <<"api_username">>).

%% notice: integer topic for sort response
-define(TOPIC1, <<"t/0000">>).
-define(TOPIC1RH, 1).
-define(TOPIC1RAP, false).
-define(TOPIC1NL, false).
-define(TOPIC1QOS, 1).
-define(TOPIC2, <<"$share/test_group/t/0001">>).
-define(TOPIC2_TOPIC_ONLY, <<"t/0001">>).

-define(TOPIC_SORT, #{?TOPIC1 => 1, ?TOPIC2 => 2}).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_mgmt_api_test_util:init_suite(),
    Config.

end_per_suite(_) ->
    emqx_mgmt_api_test_util:end_suite().

t_subscription_api(Config) ->
    Client = proplists:get_value(client, Config),
    {ok, _, _} = emqtt:subscribe(
        Client, [
            {?TOPIC1, [{rh, ?TOPIC1RH}, {rap, ?TOPIC1RAP}, {nl, ?TOPIC1NL}, {qos, ?TOPIC1QOS}]}
        ]
    ),
    {ok, _, _} = emqtt:subscribe(Client, ?TOPIC2),
    Path = emqx_mgmt_api_test_util:api_path(["subscriptions"]),
    {ok, Response} = emqx_mgmt_api_test_util:request_api(get, Path),
    Data = emqx_json:decode(Response, [return_maps]),
    Meta = maps:get(<<"meta">>, Data),
    ?assertEqual(1, maps:get(<<"page">>, Meta)),
    ?assertEqual(emqx_mgmt:default_row_limit(), maps:get(<<"limit">>, Meta)),
    ?assertEqual(2, maps:get(<<"count">>, Meta)),
    Subscriptions = maps:get(<<"data">>, Data),
    ?assertEqual(length(Subscriptions), 2),
    Sort =
        fun(#{<<"topic">> := T1}, #{<<"topic">> := T2}) ->
            maps:get(T1, ?TOPIC_SORT) =< maps:get(T2, ?TOPIC_SORT)
        end,
    [Subscriptions1, Subscriptions2] = lists:sort(Sort, Subscriptions),

    ?assertMatch(
        #{
            <<"topic">> := ?TOPIC1,
            <<"qos">> := ?TOPIC1QOS,
            <<"nl">> := _,
            <<"rap">> := _,
            <<"rh">> := ?TOPIC1RH,
            <<"clientid">> := ?CLIENTID,
            <<"node">> := _
        },
        Subscriptions1
    ),

    ?assertEqual(maps:get(<<"topic">>, Subscriptions2), ?TOPIC2),
    ?assertEqual(maps:get(<<"clientid">>, Subscriptions2), ?CLIENTID),

    QS = [
        {"clientid", ?CLIENTID},
        {"topic", ?TOPIC2_TOPIC_ONLY},
        {"node", atom_to_list(node())},
        {"qos", "0"},
        {"share_group", "test_group"},
        {"match_topic", "t/#"}
    ],
    Headers = emqx_mgmt_api_test_util:auth_header_(),

    DataTopic2 = #{<<"meta">> := Meta2} = request_json(get, QS, Headers),
    ?assertEqual(1, maps:get(<<"page">>, Meta2)),
    ?assertEqual(emqx_mgmt:default_row_limit(), maps:get(<<"limit">>, Meta2)),
    ?assertEqual(1, maps:get(<<"count">>, Meta2)),
    SubscriptionsList2 = maps:get(<<"data">>, DataTopic2),
    ?assertEqual(length(SubscriptionsList2), 1).

t_subscription_fuzzy_search(Config) ->
    Client = proplists:get_value(client, Config),
    Topics = [
        <<"t/foo">>,
        <<"t/foo/bar">>,
        <<"t/foo/baz">>,
        <<"topic/foo/bar">>,
        <<"topic/foo/baz">>
    ],
    _ = [{ok, _, _} = emqtt:subscribe(Client, T) || T <- Topics],

    Headers = emqx_mgmt_api_test_util:auth_header_(),
    MatchQs = [
        {"clientid", ?CLIENTID},
        {"node", atom_to_list(node())},
        {"match_topic", "t/#"}
    ],

    MatchData1 = #{<<"meta">> := MatchMeta1} = request_json(get, MatchQs, Headers),
    ?assertEqual(1, maps:get(<<"page">>, MatchMeta1)),
    ?assertEqual(emqx_mgmt:default_row_limit(), maps:get(<<"limit">>, MatchMeta1)),
    %% count is undefined in fuzzy searching
    ?assertNot(maps:is_key(<<"count">>, MatchMeta1)),
    ?assertMatch(3, length(maps:get(<<"data">>, MatchData1))),
    ?assertEqual(false, maps:get(<<"hasnext">>, MatchMeta1)),

    LimitMatchQuery = [
        {"clientid", ?CLIENTID},
        {"match_topic", "+/+/+"},
        {"limit", "3"}
    ],

    MatchData2 = #{<<"meta">> := MatchMeta2} = request_json(get, LimitMatchQuery, Headers),
    ?assertEqual(#{<<"page">> => 1, <<"limit">> => 3, <<"hasnext">> => true}, MatchMeta2),
    ?assertEqual(3, length(maps:get(<<"data">>, MatchData2))),

    MatchData2P2 =
        #{<<"meta">> := MatchMeta2P2} =
        request_json(get, [{"page", "2"} | LimitMatchQuery], Headers),
    ?assertEqual(#{<<"page">> => 2, <<"limit">> => 3, <<"hasnext">> => false}, MatchMeta2P2),
    ?assertEqual(1, length(maps:get(<<"data">>, MatchData2P2))).

%% checks that we can list when there are subscriptions made by
%% `emqx:subscribe'.
t_list_with_internal_subscription(_Config) ->
    emqx:subscribe(<<"some/topic">>),
    QS = [],
    Headers = emqx_mgmt_api_test_util:auth_header_(),
    ?assertMatch(
        #{<<"data">> := [#{<<"clientid">> := null}]},
        request_json(get, QS, Headers)
    ),
    ok.

request_json(Method, Query, Headers) when is_list(Query) ->
    Qs = uri_string:compose_query(Query),
    {ok, MatchRes} = emqx_mgmt_api_test_util:request_api(Method, path(), Qs, Headers),
    emqx_json:decode(MatchRes, [return_maps]).

path() ->
    emqx_mgmt_api_test_util:api_path(["subscriptions"]).

init_per_testcase(_TC, Config) ->
    {ok, Client} = emqtt:start_link(#{username => ?USERNAME, clientid => ?CLIENTID, proto_ver => v5}),
    {ok, _} = emqtt:connect(Client),
    [{client, Client} | Config].

end_per_testcase(_TC, Config) ->
    Client = proplists:get_value(client, Config),
    emqtt:disconnect(Client).
