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
-module(emqx_mgmt_api_subscription_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

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
    [
        {group, mem},
        {group, persistent}
    ].

groups() ->
    AllTCs = emqx_common_test_helpers:all(?MODULE),
    CommonTCs = AllTCs -- persistent_only_tcs(),
    [
        {mem, CommonTCs},
        %% Persistent shared subscriptions are an EE app.
        %% So they are tested outside emqx_management app which is CE.
        {persistent,
            (CommonTCs --
                [t_list_with_shared_sub, t_list_with_invalid_match_topic, t_subscription_api]) ++
                persistent_only_tcs()}
    ].

persistent_only_tcs() ->
    [
        t_mixed_persistent_sessions,
        t_many_persistent_sessions
    ].

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            {emqx,
                "durable_sessions {\n"
                "    enable = true\n"
                "    renew_streams_interval = 10ms\n"
                "}"},
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)).

init_per_group(persistent, Config) ->
    ClientConfig = #{
        username => ?USERNAME,
        clientid => ?CLIENTID,
        proto_ver => v5,
        clean_start => true,
        properties => #{'Session-Expiry-Interval' => 300}
    },
    [{client_config, ClientConfig}, {durable, true} | Config];
init_per_group(mem, Config) ->
    ClientConfig = #{
        username => ?USERNAME, clientid => ?CLIENTID, proto_ver => v5, clean_start => true
    },
    [{client_config, ClientConfig}, {durable, false} | Config].

end_per_group(_, Config) ->
    Config.

init_per_testcase(_TC, Config) ->
    case ?config(client_config, Config) of
        ClientConfig when is_map(ClientConfig) ->
            {ok, Client} = emqtt:start_link(ClientConfig),
            {ok, _} = emqtt:connect(Client),
            [{client, Client} | Config];
        _ ->
            Config
    end.

end_per_testcase(_TC, Config) ->
    Client = proplists:get_value(client, Config),
    emqtt:disconnect(Client).

t_subscription_api(Config) ->
    Client = proplists:get_value(client, Config),
    Durable = atom_to_list(?config(durable, Config)),
    {ok, _, _} = emqtt:subscribe(
        Client, [
            {?TOPIC1, [{rh, ?TOPIC1RH}, {rap, ?TOPIC1RAP}, {nl, ?TOPIC1NL}, {qos, ?TOPIC1QOS}]}
        ]
    ),
    {ok, _, _} = emqtt:subscribe(Client, ?TOPIC2),
    Path = emqx_mgmt_api_test_util:api_path(["subscriptions"]),
    timer:sleep(100),
    {ok, Response} = emqx_mgmt_api_test_util:request_api(get, Path),
    Data = emqx_utils_json:decode(Response, [return_maps]),
    Meta = maps:get(<<"meta">>, Data),
    ?assertEqual(1, maps:get(<<"page">>, Meta)),
    ?assertEqual(emqx_mgmt:default_row_limit(), maps:get(<<"limit">>, Meta)),
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
        {"match_topic", "t/#"},
        {"durable", Durable}
    ],
    Headers = emqx_mgmt_api_test_util:auth_header_(),

    DataTopic2 = #{<<"meta">> := Meta2} = request_json(get, QS, Headers),
    ?assertEqual(1, maps:get(<<"page">>, Meta2)),
    ?assertEqual(emqx_mgmt:default_row_limit(), maps:get(<<"limit">>, Meta2)),
    ?assertEqual(1, maps:get(<<"count">>, Meta2)),
    SubscriptionsList2 = maps:get(<<"data">>, DataTopic2),
    ?assertEqual(length(SubscriptionsList2), 1).

%% Checks a few edge cases where persistent and non-persistent client subscriptions exist.
t_mixed_persistent_sessions(Config) ->
    ClientConfig = ?config(client_config, Config),
    PersistentClient = ?config(client, Config),
    {ok, MemClient} = emqtt:start_link(ClientConfig#{clientid => <<"mem">>, properties => #{}}),
    {ok, _} = emqtt:connect(MemClient),

    {ok, _, [?RC_GRANTED_QOS_1]} = emqtt:subscribe(PersistentClient, <<"t/1">>, 1),
    {ok, _, [?RC_GRANTED_QOS_1]} = emqtt:subscribe(MemClient, <<"t/1">>, 1),

    %% First page with sufficient limit should have both mem and DS clients.
    ?assertMatch(
        {ok,
            {{_, 200, _}, _, #{
                <<"data">> := [_, _],
                <<"meta">> :=
                    #{<<"hasnext">> := false}
            }}},
        get_subs(#{page => "1"})
    ),

    ?assertMatch(
        {ok,
            {{_, 200, _}, _, #{
                <<"data">> := [_],
                <<"meta">> := #{<<"hasnext">> := true}
            }}},
        get_subs(#{page => "1", limit => "1"})
    ),
    ?assertMatch(
        {ok,
            {{_, 200, _}, _, #{
                <<"data">> := [_],
                <<"meta">> := #{<<"hasnext">> := false}
            }}},
        get_subs(#{page => "2", limit => "1"})
    ),

    emqtt:disconnect(MemClient),

    ok.

t_many_persistent_sessions(Config) ->
    ClientConfig = ?config(client_config, Config),
    Clients = lists:map(
        fun(I) ->
            IBin = integer_to_binary(I),
            {ok, Client} = emqtt:start_link(ClientConfig#{clientid => <<"sub_", IBin/binary>>}),
            {ok, _} = emqtt:connect(Client),
            ok = lists:foreach(
                fun(IT) ->
                    ITBin = integer_to_binary(IT),
                    {ok, _, [?RC_GRANTED_QOS_1]} = emqtt:subscribe(
                        Client, <<"t/", IBin/binary, "/", ITBin/binary>>, 1
                    )
                end,
                lists:seq(1, 30)
            ),
            Client
        end,
        lists:seq(1, 5)
    ),
    ct:sleep(100),
    ok = lists:foreach(
        fun(Client) -> emqtt:disconnect(Client) end,
        Clients
    ),
    ct:sleep(100),
    {ok,
        {{_, 200, _}, _, #{
            <<"data">> := Data
        }}} = get_subs(#{page => "1", limit => "1000"}),
    ?assertEqual(
        150,
        length(Data)
    ),
    ok.

t_subscription_fuzzy_search(Config) ->
    Client = proplists:get_value(client, Config),
    Durable = atom_to_list(?config(durable, Config)),
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
        {"match_topic", "t/#"},
        {"durable", Durable}
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
        {"limit", "3"},
        {"durable", Durable}
    ],

    MatchData2 = #{<<"meta">> := MatchMeta2} = request_json(get, LimitMatchQuery, Headers),
    ?assertEqual(#{<<"page">> => 1, <<"limit">> => 3, <<"hasnext">> => true}, MatchMeta2),
    ?assertEqual(3, length(maps:get(<<"data">>, MatchData2)), MatchData2),

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

t_list_with_shared_sub(_Config) ->
    Client = proplists:get_value(client, _Config),
    RealTopic = <<"t/+">>,
    Topic = <<"$share/g1/", RealTopic/binary>>,

    {ok, _, _} = emqtt:subscribe(Client, Topic),
    {ok, _, _} = emqtt:subscribe(Client, RealTopic),

    QS = [
        {"clientid", ?CLIENTID},
        {"match_topic", "t/#"}
    ],
    Headers = emqx_mgmt_api_test_util:auth_header_(),

    ?assertMatch(
        #{<<"data">> := [#{<<"clientid">> := ?CLIENTID}, #{<<"clientid">> := ?CLIENTID}]},
        request_json(get, QS, Headers)
    ),

    ok.

t_list_with_invalid_match_topic(Config) ->
    Client = proplists:get_value(client, Config),
    RealTopic = <<"t/+">>,
    Topic = <<"$share/g1/", RealTopic/binary>>,

    {ok, _, _} = emqtt:subscribe(Client, Topic),
    {ok, _, _} = emqtt:subscribe(Client, RealTopic),

    QS = [
        {"clientid", ?CLIENTID},
        {"match_topic", "$share/g1/t/1"}
    ],
    Headers = emqx_mgmt_api_test_util:auth_header_(),

    ?assertMatch(
        {error,
            {{_, 400, _}, _, #{
                <<"message">> := <<"match_topic_invalid">>,
                <<"code">> := <<"INVALID_PARAMETER">>
            }}},
        begin
            {error, {R, _H, Body}} = emqx_mgmt_api_test_util:request_api(
                get, path(), uri_string:compose_query(QS), Headers, [], #{return_all => true}
            ),
            {error, {R, _H, emqx_utils_json:decode(Body, [return_maps])}}
        end
    ),
    ok.

request_json(Method, Query, Headers) when is_list(Query) ->
    Qs = uri_string:compose_query(Query),
    {ok, MatchRes} = emqx_mgmt_api_test_util:request_api(Method, path(), Qs, Headers),
    emqx_utils_json:decode(MatchRes, [return_maps]).

path() ->
    emqx_mgmt_api_test_util:api_path(["subscriptions"]).

get_subs() ->
    get_subs(_QueryParams = #{}).

get_subs(QueryParams = #{}) ->
    QS = uri_string:compose_query(maps:to_list(emqx_utils_maps:binary_key_map(QueryParams))),
    request(get, path(), [], QS).

request(Method, Path, Params) ->
    request(Method, Path, Params, _QueryParams = "").

request(Method, Path, Params, QueryParams) ->
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    Opts = #{return_all => true},
    case emqx_mgmt_api_test_util:request_api(Method, Path, QueryParams, AuthHeader, Params, Opts) of
        {ok, {Status, Headers, Body0}} ->
            Body = maybe_json_decode(Body0),
            {ok, {Status, Headers, Body}};
        {error, {Status, Headers, Body0}} ->
            Body =
                case emqx_utils_json:safe_decode(Body0, [return_maps]) of
                    {ok, Decoded0 = #{<<"message">> := Msg0}} ->
                        Msg = maybe_json_decode(Msg0),
                        Decoded0#{<<"message">> := Msg};
                    {ok, Decoded0} ->
                        Decoded0;
                    {error, _} ->
                        Body0
                end,
            {error, {Status, Headers, Body}};
        Error ->
            Error
    end.

maybe_json_decode(X) ->
    case emqx_utils_json:safe_decode(X, [return_maps]) of
        {ok, Decoded} -> Decoded;
        {error, _} -> X
    end.
