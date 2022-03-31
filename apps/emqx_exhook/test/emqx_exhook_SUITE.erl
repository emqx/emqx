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

-module(emqx_exhook_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-define(CLUSTER_RPC_SHARD, emqx_cluster_rpc_shard).

-define(CONF_DEFAULT, <<"
exhook {
  servers = [
    { name = default,
      url = \"http://127.0.0.1:9000\"
    },
    { name = enable,
      enable = false,
      url = \"http://127.0.0.1:9000\"
    },
    { name = error,
      url = \"http://127.0.0.1:9001\"
    },
    { name = not_reconnect,
      auto_reconnect = false,
      url = \"http://127.0.0.1:9001\"
    }
  ]
}
">>).

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Cfg) ->
    application:load(emqx_conf),
    ok = ekka:start(),
    ok = mria_rlog:wait_for_shards([?CLUSTER_RPC_SHARD], infinity),
    meck:new(emqx_alarm, [non_strict, passthrough, no_link]),
    meck:expect(emqx_alarm, activate, 3, ok),
    meck:expect(emqx_alarm, deactivate, 3, ok),

    _ = emqx_exhook_demo_svr:start(),
    load_cfg(?CONF_DEFAULT),
    emqx_common_test_helpers:start_apps([emqx_exhook]),
    Cfg.

end_per_suite(_Cfg) ->
    ekka:stop(),
    mria:stop(),
    mria_mnesia:delete_schema(),
    meck:unload(emqx_alarm),

    emqx_common_test_helpers:stop_apps([emqx_exhook]),
    emqx_exhook_demo_svr:stop().

init_per_testcase(_, Config) ->
    {ok, _} = emqx_cluster_rpc:start_link(),
    timer:sleep(200),
    Config.

end_per_testcase(_, Config) ->
    case erlang:whereis(node()) of
        undefined -> ok;
        P ->
            erlang:unlink(P),
            erlang:exit(P, kill)
    end,
    Config.

load_cfg(Cfg) ->
    ok = emqx_common_test_helpers:load_config(emqx_exhook_schema, Cfg).

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_access_failed_if_no_server_running(_) ->
    emqx_exhook_mgr:disable(<<"default">>),
    ClientInfo = #{clientid => <<"user-id-1">>,
                   username => <<"usera">>,
                   peerhost => {127,0,0,1},
                   sockport => 1883,
                   protocol => mqtt,
                   mountpoint => undefined
                  },
    ?assertMatch({stop, {error, not_authorized}},
                 emqx_exhook_handler:on_client_authenticate(ClientInfo, #{auth_result => success})),

    ?assertMatch({stop, deny},
                 emqx_exhook_handler:on_client_authorize(ClientInfo, publish, <<"t/1">>, allow)),

    Message = emqx_message:make(<<"t/1">>, <<"abc">>),
    ?assertMatch({stop, Message},
                 emqx_exhook_handler:on_message_publish(Message)),
    emqx_exhook_mgr:enable(<<"default">>).

t_lookup(_) ->
    Result = emqx_exhook_mgr:lookup(<<"default">>),
    ?assertMatch(#{name := <<"default">>, status := _}, Result),
    not_found = emqx_exhook_mgr:lookup(<<"not_found">>).

t_list(_) ->
    [H | _] = emqx_exhook_mgr:list(),
    ?assertMatch(#{name := _,
                   status := _,
                   hooks := _}, H).

t_unexpected(_) ->
    ok = gen_server:cast(emqx_exhook_mgr, unexpected),
    unexpected = erlang:send(erlang:whereis(emqx_exhook_mgr), unexpected),
    Result = gen_server:call(emqx_exhook_mgr, unexpected),
    ?assertEqual(Result, ok).

t_timer(_) ->
    Pid = erlang:whereis(emqx_exhook_mgr),
    refresh_tick = erlang:send(Pid, refresh_tick),
    _ = erlang:send(Pid, {timeout, undefined, {reload, <<"default">>}}),
    _ = erlang:send(Pid, {timeout, undefined, {reload, <<"not_found">>}}),
    _ = erlang:send(Pid, {timeout, undefined, {reload, <<"error">>}}),
    ok.

t_error_update_conf(_) ->
    Path = [exhook, servers],
    Name = <<"error_update">>,
    ErrorCfg = #{<<"name">> => Name},
    {error, _} = emqx_exhook_mgr:update_config(Path, {update, Name, ErrorCfg}),
    {error, _} = emqx_exhook_mgr:update_config(Path, {move, Name, top, <<>>}),
    {error, _} = emqx_exhook_mgr:update_config(Path, {enable, Name, true}),

    ErrorAnd = #{<<"name">> => Name, <<"url">> => <<"http://127.0.0.1:9001">>},
    {ok, _} = emqx_exhook_mgr:update_config(Path, {add, ErrorAnd}),

    DisableAnd = #{<<"name">> => Name,
                   <<"url">> => <<"http://127.0.0.1:9001">>,
                   <<"enable">> => false},
    {ok, _} = emqx_exhook_mgr:update_config(Path, {add, DisableAnd}),

    {ok, _} = emqx_exhook_mgr:update_config(Path, {delete, <<"error">>}),
    {ok, _} = emqx_exhook_mgr:update_config(Path, {delete, <<"delete_not_exists">>}),
    ok.

t_error_server_info(_) ->
    not_found = emqx_exhook_mgr:server_info(<<"not_exists">>),
    ok.

t_metrics(_) ->
    ok = emqx_exhook_metrics:succeed(<<"default">>, 'client.connect'),
    ok = emqx_exhook_metrics:failed(<<"default">>, 'client.connect'),
    true = emqx_exhook_metrics:update(1000),
    timer:sleep(100),
    SvrMetrics = emqx_exhook_metrics:server_metrics(<<"default">>),
    ?assertMatch(#{succeed := _, failed := _, rate := _, max_rate := _}, SvrMetrics),

    SvrsMetrics = emqx_exhook_metrics:servers_metrics(),
    ?assertMatch(#{<<"default">> := #{succeed := _}}, SvrsMetrics),

    HooksMetrics = emqx_exhook_metrics:hooks_metrics(<<"default">>),
    ?assertMatch(#{'client.connect' := #{succeed := _}}, HooksMetrics),
    ok.

t_handler(_) ->
    %% connect
    {ok, C} = emqtt:start_link([{host, "localhost"},
                                {port, 1883},
                                {username, <<"gooduser">>},
                                {clientid, <<"exhook_gooduser">>}]),
    {ok, _} = emqtt:connect(C),

    %% pub/sub
    {ok, _, _} = emqtt:subscribe(C, <<"/exhook">>, qos0),
    timer:sleep(100),
    ok = emqtt:publish(C, <<"/exhook">>, <<>>, qos0),
    ok = emqtt:publish(C, <<"/ignore">>, <<>>, qos0),
    timer:sleep(100),
    {ok, _, _} = emqtt:unsubscribe(C, <<"/exhook">>),

    %% sys pub/sub
    ok = emqtt:publish(C, <<"$SYS">>, <<>>, qos0),
    {ok, _, _} = emqtt:subscribe(C, <<"$SYS/systest">>, qos1),
    timer:sleep(100),
    {ok, _} = emqtt:publish(C, <<"$SYS/systest">>, <<>>, qos1),
    ok = emqtt:publish(C, <<"$SYS/ignore">>, <<>>, qos0),
    timer:sleep(100),
    {ok, _, _} = emqtt:unsubscribe(C, <<"$SYS/systest">>),

    %% ack
    {ok, _, _} = emqtt:subscribe(C, <<"/exhook1">>, qos1),
    timer:sleep(100),
    {ok, _} = emqtt:publish(C, <<"/exhook1">>, <<>>, qos1),
    timer:sleep(100),
    emqtt:stop(C),
    timer:sleep(100),
    ok.

t_simulated_handler(_) ->
    ClientInfo = #{clientid => <<"user-id-1">>,
                   username => <<"usera">>,
                   peerhost => {127,0,0,1},
                   sockport => 1883,
                   protocol => mqtt,
                   mountpoint => undefined
                  },
    %% resume/takeover
    ok = emqx_exhook_handler:on_session_resumed(ClientInfo, undefined),
    ok = emqx_exhook_handler:on_session_discarded(ClientInfo, undefined),
    ok = emqx_exhook_handler:on_session_takenover(ClientInfo, undefined),
    ok.

t_misc_test(_) ->
    "5.0.0" = emqx_exhook_proto_v1:introduced_in(),
    <<"test">> = emqx_exhook_server:name(#{name => <<"test">>}),
    _ = emqx_exhook_server:format(#{name => <<"test">>, hookspec => #{}}),
    ok.

%%--------------------------------------------------------------------
%% Utils
%%--------------------------------------------------------------------

meck_print() ->
    meck:new(emqx_ctl, [passthrough, no_history, no_link]),
    meck:expect(emqx_ctl, print, fun(_) -> ok end),
    meck:expect(emqx_ctl, print, fun(_, Args) -> Args end).

unmeck_print() ->
    meck:unload(emqx_ctl).

loaded_exhook_hookpoints() ->
    lists:filtermap(fun(E) ->
                            Name = element(2, E),
                            Callbacks = element(3, E),
                            case lists:any(fun is_exhook_callback/1, Callbacks) of
                                true -> {true, Name};
                                _ -> false
                            end
                    end, ets:tab2list(emqx_hooks)).

is_exhook_callback(Cb) ->
    Action = element(2, Cb),
    emqx_exhook_handler == element(1, Action).
