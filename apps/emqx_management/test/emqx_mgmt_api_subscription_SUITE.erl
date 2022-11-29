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
-module(emqx_mgmt_api_subscription_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

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

t_subscription_api(_) ->
    {ok, Client} = emqtt:start_link(#{username => ?USERNAME, clientid => ?CLIENTID, proto_ver => v5}),
    {ok, _} = emqtt:connect(Client),
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
    ?assertEqual(emqx_mgmt:max_row_limit(), maps:get(<<"limit">>, Meta)),
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

    QS = uri_string:compose_query([
        {"clientid", ?CLIENTID},
        {"topic", ?TOPIC2_TOPIC_ONLY},
        {"node", atom_to_list(node())},
        {"qos", "0"},
        {"share_group", "test_group"},
        {"match_topic", "t/#"}
    ]),
    Headers = emqx_mgmt_api_test_util:auth_header_(),

    {ok, ResponseTopic2} = emqx_mgmt_api_test_util:request_api(get, Path, QS, Headers),
    DataTopic2 = emqx_json:decode(ResponseTopic2, [return_maps]),
    Meta2 = maps:get(<<"meta">>, DataTopic2),
    ?assertEqual(1, maps:get(<<"page">>, Meta2)),
    ?assertEqual(emqx_mgmt:max_row_limit(), maps:get(<<"limit">>, Meta2)),
    ?assertEqual(1, maps:get(<<"count">>, Meta2)),
    SubscriptionsList2 = maps:get(<<"data">>, DataTopic2),
    ?assertEqual(length(SubscriptionsList2), 1),

    MatchQs = uri_string:compose_query([
        {"clientid", ?CLIENTID},
        {"node", atom_to_list(node())},
        {"qos", "0"},
        {"match_topic", "t/#"}
    ]),

    {ok, MatchRes} = emqx_mgmt_api_test_util:request_api(get, Path, MatchQs, Headers),
    MatchData = emqx_json:decode(MatchRes, [return_maps]),
    MatchMeta = maps:get(<<"meta">>, MatchData),
    ?assertEqual(1, maps:get(<<"page">>, MatchMeta)),
    ?assertEqual(emqx_mgmt:max_row_limit(), maps:get(<<"limit">>, MatchMeta)),
    %% count equals 0 in fuzzy searching
    ?assertEqual(0, maps:get(<<"count">>, MatchMeta)),
    MatchSubs = maps:get(<<"data">>, MatchData),
    ?assertEqual(1, length(MatchSubs)),

    emqtt:disconnect(Client).
