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

-include("emqx_dashboard.hrl").
-include_lib("eunit/include/eunit.hrl").

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
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    ok = emqx_mgmt_api_test_util:init_suite([emqx, emqx_conf, emqx_retainer]),
    Config.

end_per_suite(_Config) ->
    emqx_mgmt_api_test_util:end_suite([emqx_retainer]).

set_special_configs(emqx_retainer) ->
    emqx_retainer:update_config(?BASE_RETAINER_CONF),
    ok;
set_special_configs(_App) ->
    ok.

%%--------------------------------------------------------------------
%% Test Cases
%%--------------------------------------------------------------------

t_monitor_samplers_all(_Config) ->
    timer:sleep(?DEFAULT_SAMPLE_INTERVAL * 2 * 1000 + 20),
    Size = mnesia:table_info(emqx_dashboard_monitor, size),
    All = emqx_dashboard_monitor:samplers(all, infinity),
    All2 = emqx_dashboard_monitor:samplers(),
    ?assert(erlang:length(All) == Size),
    ?assert(erlang:length(All2) == Size),
    ok.

t_monitor_samplers_latest(_Config) ->
    timer:sleep(?DEFAULT_SAMPLE_INTERVAL * 2 * 1000 + 20),
    Samplers = emqx_dashboard_monitor:samplers(node(), 2),
    Latest = emqx_dashboard_monitor:samplers(node(), 1),
    ?assert(erlang:length(Samplers) == 2),
    ?assert(erlang:length(Latest) == 1),
    ?assert(hd(Latest) == lists:nth(2, Samplers)),
    ok.

t_monitor_sampler_format(_Config) ->
    timer:sleep(?DEFAULT_SAMPLE_INTERVAL * 2 * 1000 + 20),
    Latest = hd(emqx_dashboard_monitor:samplers(node(), 1)),
    SamplerKeys = maps:keys(Latest),
    [?assert(lists:member(SamplerName, SamplerKeys)) || SamplerName <- ?SAMPLER_LIST],
    ok.

t_monitor_api(_) ->
    timer:sleep(?DEFAULT_SAMPLE_INTERVAL * 2 * 1000 + 20),
    {ok, Samplers} = request(["monitor"], "latest=20"),
    ?assert(erlang:length(Samplers) >= 2),
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
    timer:sleep(?DEFAULT_SAMPLE_INTERVAL * 2 * 1000 + 20),
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
    {ok, Rate} = request(["monitor_current"]),
    ?assertEqual(1, maps:get(<<"live_connections">>, Rate)),
    ?assertEqual(2, maps:get(<<"connections">>, Rate)),
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

    ok = AssertFun(1),

    {ok, C2} = emqtt:start_link([{clean_start, true}, {clientid, ClientId2}]),
    {ok, _} = emqtt:connect(C2),
    _ = emqtt:subscribe(C2, {ShareT, 1}),
    ok = AssertFun(2),

    _ = emqtt:unsubscribe(C2, ShareT),
    ok = AssertFun(1),
    _ = emqtt:subscribe(C2, {ShareT, 1}),
    ok = AssertFun(2),

    ok = emqtt:disconnect(C1),
    %% C1: clean_start = false, proto_ver = 3.1.1
    %% means disconnected but the session pid with a share-subscription is still alive
    ok = AssertFun(2),

    _ = emqx_cm:kick_session(ClientId1),
    ok = AssertFun(1),

    ok = emqtt:disconnect(C2),
    ok = AssertFun(0),
    ok.

t_monitor_reset(_) ->
    restart_monitor(),
    {ok, Rate} = request(["monitor_current"]),
    [
        ?assert(maps:is_key(atom_to_binary(Key, utf8), Rate))
     || Key <- maps:values(?DELTA_SAMPLER_RATE_MAP) ++ ?GAUGE_SAMPLER_LIST
    ],
    timer:sleep(?DEFAULT_SAMPLE_INTERVAL * 2 * 1000 + 20),
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
