%%--------------------------------------------------------------------
%% Copyright (c) 2020-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_dashboard_monitor_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-import(emqx_dashboard_SUITE, [auth_header_/0]).
-import(emqx_common_test_helpers, [on_exit/1]).

-include("emqx_dashboard.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-define(SERVER, "http://127.0.0.1:18083").
-define(BASE_PATH, "/api/v5").

-define(BASE_RETAINER_CONF, <<
    "retainer {\n"
    "    enable = true\n"
    "    msg_clear_interval = 0s\n"
    "    msg_expiry_interval = 0s\n"
    "    max_payload_size = 1MB\n"
    "    flow_control {\n"
    "        batch_read_number = 0\n"
    "        batch_deliver_number = 0\n"
    "     }\n"
    "   backend {\n"
    "        type = built_in_database\n"
    "        storage_type = ram\n"
    "        max_retained_messages = 0\n"
    "     }\n"
    "}"
>>).

%%--------------------------------------------------------------------
%% CT boilerplate
%%--------------------------------------------------------------------

all() ->
    [
        {group, common},
        {group, persistent_sessions}
    ].

groups() ->
    AllTCs = emqx_common_test_helpers:all(?MODULE),
    PSTCs = persistent_session_testcases(),
    [
        {common, [], AllTCs -- PSTCs},
        {persistent_sessions, [], PSTCs}
    ].

persistent_session_testcases() ->
    [
        t_persistent_session_stats
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(persistent_sessions = Group, Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            {emqx, "durable_sessions {enable = true}"},
            {emqx_retainer, ?BASE_RETAINER_CONF},
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard(
                "dashboard.listeners.http { enable = true, bind = 18083 }\n"
                "dashboard.sample_interval = 1s"
            )
        ],
        #{work_dir => emqx_cth_suite:work_dir(Group, Config)}
    ),
    {ok, _} = emqx_common_test_http:create_default_app(),
    [{apps, Apps} | Config];
init_per_group(common = Group, Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            {emqx_retainer, ?BASE_RETAINER_CONF},
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard(
                "dashboard.listeners.http { enable = true, bind = 18083 }\n"
                "dashboard.sample_interval = 1s"
            )
        ],
        #{work_dir => emqx_cth_suite:work_dir(Group, Config)}
    ),
    {ok, _} = emqx_common_test_http:create_default_app(),
    [{apps, Apps} | Config].

end_per_group(_Group, Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(_TestCase, Config) ->
    ok = snabbkaffe:start_trace(),
    ct:timetrap({seconds, 30}),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok = snabbkaffe:stop(),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%--------------------------------------------------------------------
%% Test Cases
%%--------------------------------------------------------------------

t_monitor_samplers_all(_Config) ->
    {ok, _} =
        snabbkaffe:block_until(
            ?match_n_events(2, #{?snk_kind := dashboard_monitor_flushed}),
            infinity
        ),
    Size = mnesia:table_info(emqx_dashboard_monitor, size),
    All = emqx_dashboard_monitor:samplers(all, infinity),
    All2 = emqx_dashboard_monitor:samplers(),
    ?assert(erlang:length(All) == Size),
    ?assert(erlang:length(All2) == Size),
    ok.

t_monitor_samplers_latest(_Config) ->
    {ok, _} =
        snabbkaffe:block_until(
            ?match_n_events(2, #{?snk_kind := dashboard_monitor_flushed}),
            infinity
        ),
    ?retry(1_000, 10, begin
        Samplers = emqx_dashboard_monitor:samplers(node(), 2),
        Latest = emqx_dashboard_monitor:samplers(node(), 1),
        ?assert(erlang:length(Samplers) == 2, #{samplers => Samplers}),
        ?assert(erlang:length(Latest) == 1, #{latest => Latest}),
        ?assert(hd(Latest) == lists:nth(2, Samplers))
    end),
    ok.

t_monitor_sampler_format(_Config) ->
    {ok, _} =
        snabbkaffe:block_until(
            ?match_event(#{?snk_kind := dashboard_monitor_flushed}),
            infinity
        ),
    Latest = hd(emqx_dashboard_monitor:samplers(node(), 1)),
    SamplerKeys = maps:keys(Latest),
    [?assert(lists:member(SamplerName, SamplerKeys)) || SamplerName <- ?SAMPLER_LIST],
    ok.

t_monitor_api(_) ->
    {ok, _} =
        snabbkaffe:block_until(
            ?match_n_events(2, #{?snk_kind := dashboard_monitor_flushed}),
            infinity
        ),
    {ok, Samplers} = request(["monitor"], "latest=200"),
    ?assert(erlang:length(Samplers) >= 2, #{samplers => Samplers}),
    Fun =
        fun(Sampler) ->
            Keys = [binary_to_atom(Key, utf8) || Key <- maps:keys(Sampler)],
            [?assert(lists:member(SamplerName, Keys)) || SamplerName <- ?SAMPLER_LIST]
        end,
    [Fun(Sampler) || Sampler <- Samplers],
    {ok, NodeSamplers} = request(["monitor", "nodes", node()]),
    [Fun(NodeSampler) || NodeSampler <- NodeSamplers],
    ok.

t_monitor_current_api(_) ->
    {ok, _} =
        snabbkaffe:block_until(
            ?match_n_events(2, #{?snk_kind := dashboard_monitor_flushed}),
            infinity
        ),
    {ok, Rate} = request(["monitor_current"]),
    [
        ?assert(maps:is_key(atom_to_binary(Key, utf8), Rate))
     || Key <- maps:values(?DELTA_SAMPLER_RATE_MAP) ++ ?GAUGE_SAMPLER_LIST
    ],
    {ok, NodeRate} = request(["monitor_current", "nodes", node()]),
    [
        ?assert(maps:is_key(atom_to_binary(Key, utf8), NodeRate))
     || Key <- maps:values(?DELTA_SAMPLER_RATE_MAP) ++ ?GAUGE_SAMPLER_LIST
    ],
    ok.

t_monitor_current_api_live_connections(_) ->
    process_flag(trap_exit, true),
    ClientId = <<"live_conn_tests">>,
    ClientId1 = <<"live_conn_tests1">>,
    {ok, C} = emqtt:start_link([{clean_start, false}, {clientid, ClientId}]),
    {ok, _} = emqtt:connect(C),
    ok = emqtt:disconnect(C),
    {ok, C1} = emqtt:start_link([{clean_start, true}, {clientid, ClientId1}]),
    {ok, _} = emqtt:connect(C1),
    ok = waiting_emqx_stats_and_monitor_update('live_connections.max'),
    ?retry(1_100, 5, begin
        {ok, Rate} = request(["monitor_current"]),
        ?assertEqual(1, maps:get(<<"live_connections">>, Rate)),
        ?assertEqual(2, maps:get(<<"connections">>, Rate))
    end),
    %% clears
    ok = emqtt:disconnect(C1),
    {ok, C2} = emqtt:start_link([{clean_start, true}, {clientid, ClientId}]),
    {ok, _} = emqtt:connect(C2),
    ok = emqtt:disconnect(C2).

t_monitor_current_retained_count(_) ->
    process_flag(trap_exit, true),
    ClientId = <<"live_conn_tests">>,
    {ok, C} = emqtt:start_link([{clean_start, false}, {clientid, ClientId}]),
    {ok, _} = emqtt:connect(C),
    _ = emqtt:publish(C, <<"t1">>, <<"qos1-retain">>, [{qos, 1}, {retain, true}]),

    ok = waiting_emqx_stats_and_monitor_update('retained.count'),
    {ok, Res} = request(["monitor_current"]),
    {ok, ResNode} = request(["monitor_current", "nodes", node()]),

    ?assertEqual(1, maps:get(<<"retained_msg_count">>, Res)),
    ?assertEqual(1, maps:get(<<"retained_msg_count">>, ResNode)),
    ok = emqtt:disconnect(C),
    ok.

t_monitor_current_shared_subscription(_) ->
    process_flag(trap_exit, true),
    ShareT = <<"$share/group1/t/1">>,
    AssertFun = fun(Num) ->
        {ok, Res} = request(["monitor_current"]),
        {ok, ResNode} = request(["monitor_current", "nodes", node()]),
        ?assertEqual(Num, maps:get(<<"shared_subscriptions">>, Res)),
        ?assertEqual(Num, maps:get(<<"shared_subscriptions">>, ResNode)),
        ok
    end,

    ok = AssertFun(0),

    ClientId1 = <<"live_conn_tests1">>,
    ClientId2 = <<"live_conn_tests2">>,
    {ok, C1} = emqtt:start_link([{clean_start, false}, {clientid, ClientId1}]),
    {ok, _} = emqtt:connect(C1),
    _ = emqtt:subscribe(C1, {ShareT, 1}),

    ok = ?retry(100, 10, AssertFun(1)),

    {ok, C2} = emqtt:start_link([{clean_start, true}, {clientid, ClientId2}]),
    {ok, _} = emqtt:connect(C2),
    _ = emqtt:subscribe(C2, {ShareT, 1}),
    ok = ?retry(100, 10, AssertFun(2)),

    _ = emqtt:unsubscribe(C2, ShareT),
    ok = ?retry(100, 10, AssertFun(1)),
    _ = emqtt:subscribe(C2, {ShareT, 1}),
    ok = ?retry(100, 10, AssertFun(2)),

    ok = emqtt:disconnect(C1),
    %% C1: clean_start = false, proto_ver = 3.1.1
    %% means disconnected but the session pid with a share-subscription is still alive
    ok = ?retry(100, 10, AssertFun(2)),

    _ = emqx_cm:kick_session(ClientId1),
    ok = ?retry(100, 10, AssertFun(1)),

    ok = emqtt:disconnect(C2),
    ok = ?retry(100, 10, AssertFun(0)),
    ok.

t_monitor_reset(_) ->
    restart_monitor(),
    {ok, Rate} = request(["monitor_current"]),
    [
        ?assert(maps:is_key(atom_to_binary(Key, utf8), Rate))
     || Key <- maps:values(?DELTA_SAMPLER_RATE_MAP) ++ ?GAUGE_SAMPLER_LIST
    ],
    {ok, _} =
        snabbkaffe:block_until(
            ?match_n_events(1, #{?snk_kind := dashboard_monitor_flushed}),
            infinity
        ),
    {ok, Samplers} = request(["monitor"], "latest=1"),
    ?assertEqual(1, erlang:length(Samplers)),
    ok.

t_monitor_api_error(_) ->
    {error, {404, #{<<"code">> := <<"NOT_FOUND">>}}} =
        request(["monitor", "nodes", 'emqx@127.0.0.2']),
    {error, {404, #{<<"code">> := <<"NOT_FOUND">>}}} =
        request(["monitor_current", "nodes", 'emqx@127.0.0.2']),
    {error, {400, #{<<"code">> := <<"BAD_REQUEST">>}}} =
        request(["monitor"], "latest=0"),
    {error, {400, #{<<"code">> := <<"BAD_REQUEST">>}}} =
        request(["monitor"], "latest=-1"),
    ok.

%% Verifies that subscriptions from persistent sessions are correctly accounted for.
t_persistent_session_stats(_Config) ->
    %% pre-condition
    true = emqx_persistent_message:is_persistence_enabled(),

    NonPSClient = start_and_connect(#{
        clientid => <<"non-ps">>,
        expiry_interval => 0
    }),
    PSClient = start_and_connect(#{
        clientid => <<"ps">>,
        expiry_interval => 30
    }),
    {ok, _, [?RC_GRANTED_QOS_2]} = emqtt:subscribe(NonPSClient, <<"non/ps/topic/+">>, 2),
    {ok, _, [?RC_GRANTED_QOS_2]} = emqtt:subscribe(NonPSClient, <<"non/ps/topic">>, 2),
    {ok, _, [?RC_GRANTED_QOS_2]} = emqtt:subscribe(NonPSClient, <<"common/topic/+">>, 2),
    {ok, _, [?RC_GRANTED_QOS_2]} = emqtt:subscribe(NonPSClient, <<"common/topic">>, 2),
    {ok, _, [?RC_GRANTED_QOS_2]} = emqtt:subscribe(PSClient, <<"ps/topic/+">>, 2),
    {ok, _, [?RC_GRANTED_QOS_2]} = emqtt:subscribe(PSClient, <<"ps/topic">>, 2),
    {ok, _, [?RC_GRANTED_QOS_2]} = emqtt:subscribe(PSClient, <<"common/topic/+">>, 2),
    {ok, _, [?RC_GRANTED_QOS_2]} = emqtt:subscribe(PSClient, <<"common/topic">>, 2),
    {ok, _} =
        snabbkaffe:block_until(
            ?match_n_events(2, #{?snk_kind := dashboard_monitor_flushed}),
            infinity
        ),
    ?retry(1_000, 10, begin
        ?assertMatch(
            {ok, #{
                %% N.B.: we currently don't perform any deduplication between persistent
                %% and non-persistent routes, so we count `commont/topic' twice and get 8
                %% instead of 6 here.
                <<"topics">> := 8,
                <<"durable_subscriptions">> := 4,
                <<"subscriptions">> := 4
            }},
            request(["monitor_current"])
        )
    end),
    %% Sanity checks
    PSRouteCount = emqx_persistent_session_ds_router:stats(n_routes),
    ?assert(PSRouteCount > 0, #{ps_route_count => PSRouteCount}),
    PSSubCount = emqx_persistent_session_bookkeeper:get_subscription_count(),
    ?assert(PSSubCount > 0, #{ps_sub_count => PSSubCount}),
    ok.

request(Path) ->
    request(Path, "").

request(Path, QS) ->
    Url = url(Path, QS),
    do_request_api(get, {Url, [auth_header_()]}).

url(Parts, QS) ->
    case QS of
        "" ->
            ?SERVER ++ filename:join([?BASE_PATH | Parts]);
        _ ->
            ?SERVER ++ filename:join([?BASE_PATH | Parts]) ++ "?" ++ QS
    end.

do_request_api(Method, Request) ->
    ct:pal("Req ~p ~p~n", [Method, Request]),
    case httpc:request(Method, Request, [], []) of
        {error, socket_closed_remotely} ->
            {error, socket_closed_remotely};
        {ok, {{"HTTP/1.1", Code, _}, _, Return}} when
            Code >= 200 andalso Code =< 299
        ->
            ct:pal("Resp ~p ~p~n", [Code, Return]),
            {ok, emqx_utils_json:decode(Return, [return_maps])};
        {ok, {{"HTTP/1.1", Code, _}, _, Return}} ->
            ct:pal("Resp ~p ~p~n", [Code, Return]),
            {error, {Code, emqx_utils_json:decode(Return, [return_maps])}};
        {error, Reason} ->
            {error, Reason}
    end.

restart_monitor() ->
    OldMonitor = erlang:whereis(emqx_dashboard_monitor),
    erlang:exit(OldMonitor, kill),
    ?assertEqual(ok, wait_new_monitor(OldMonitor, 10)).

wait_new_monitor(_OldMonitor, Count) when Count =< 0 -> timeout;
wait_new_monitor(OldMonitor, Count) ->
    NewMonitor = erlang:whereis(emqx_dashboard_monitor),
    case is_pid(NewMonitor) andalso NewMonitor =/= OldMonitor of
        true ->
            ok;
        false ->
            timer:sleep(100),
            wait_new_monitor(OldMonitor, Count - 1)
    end.

waiting_emqx_stats_and_monitor_update(WaitKey) ->
    Self = self(),
    meck:new(emqx_stats, [passthrough]),
    meck:expect(
        emqx_stats,
        setstat,
        fun(Stat, MaxStat, Val) ->
            (Stat =:= WaitKey orelse MaxStat =:= WaitKey) andalso (Self ! updated),
            meck:passthrough([Stat, MaxStat, Val])
        end
    ),
    receive
        updated -> ok
    after 5000 ->
        error(waiting_emqx_stats_update_timeout)
    end,
    meck:unload([emqx_stats]),
    %% manually call monitor update
    _ = emqx_dashboard_monitor:current_rate_cluster(),
    ok.

start_and_connect(Opts) ->
    Defaults = #{clean_start => false, expiry_interval => 30},
    #{
        clientid := ClientId,
        clean_start := CleanStart,
        expiry_interval := EI
    } = maps:merge(Defaults, Opts),
    {ok, Client} = emqtt:start_link([
        {clientid, ClientId},
        {clean_start, CleanStart},
        {proto_ver, v5},
        {properties, #{'Session-Expiry-Interval' => EI}}
    ]),
    on_exit(fun() ->
        catch emqtt:disconnect(Client, ?RC_NORMAL_DISCONNECTION, #{'Session-Expiry-Interval' => 0})
    end),
    {ok, _} = emqtt:connect(Client),
    Client.
