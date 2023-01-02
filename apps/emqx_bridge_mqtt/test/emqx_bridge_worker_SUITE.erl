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

-module(emqx_bridge_worker_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(wait(For, Timeout), emqx_ct_helpers:wait_for(?FUNCTION_NAME, ?LINE, fun() -> For end, Timeout)).

-define(SNK_WAIT(WHAT), ?assertMatch({ok, _}, ?block_until(#{?snk_kind := WHAT}, 5000, 5000))).

receive_messages(Count) ->
    receive_messages(Count, []).

receive_messages(0, Msgs) ->
    Msgs;
receive_messages(Count, Msgs) ->
    receive
        {publish, Msg} ->
            receive_messages(Count-1, [Msg|Msgs]);
        _Other ->
            receive_messages(Count, Msgs)
    after 1000 ->
        Msgs
    end.

all() ->
    lists:filtermap(
      fun({FunName, _Arity}) ->
              case atom_to_list(FunName) of
                  "t_" ++ _ -> {true, FunName};
                  _ -> false
              end
      end,
      ?MODULE:module_info(exports)).

init_per_suite(Config) ->
    case node() of
        nonode@nohost -> net_kernel:start(['emqx@127.0.0.1', longnames]);
        _ -> ok
    end,
    ok = application:set_env(gen_rpc, tcp_client_num, 1),
    emqx_ct_helpers:start_apps([emqx_modules, emqx_bridge_mqtt]),
    emqx_logger:set_log_level(error),
    [{log_level, error} | Config].

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([emqx_bridge_mqtt, emqx_modules]).

init_per_testcase(_TestCase, Config) ->
    ok = snabbkaffe:start_trace(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok = snabbkaffe:stop().

t_mngr(Config) when is_list(Config) ->
    Subs = [{<<"a">>, 1}, {<<"b">>, 2}],
    Cfg = #{address => node(),
            forwards => [<<"mngr">>],
            connect_module => emqx_bridge_rpc,
            mountpoint => <<"forwarded">>,
            subscriptions => Subs,
            start_type => auto},
    Name = ?FUNCTION_NAME,
    {ok, Pid} = emqx_bridge_worker:start_link(Name, Cfg),
    try
        ?assertEqual([<<"mngr">>], emqx_bridge_worker:get_forwards(Name)),
        ?assertEqual(ok, emqx_bridge_worker:ensure_forward_present(Name, "mngr")),
        ?assertEqual(ok, emqx_bridge_worker:ensure_forward_present(Name, "mngr2")),
        ?assertEqual([<<"mngr">>, <<"mngr2">>], emqx_bridge_worker:get_forwards(Pid)),
        ?assertEqual(ok, emqx_bridge_worker:ensure_forward_absent(Name, "mngr2")),
        ?assertEqual(ok, emqx_bridge_worker:ensure_forward_absent(Name, "mngr3")),
        ?assertEqual([<<"mngr">>], emqx_bridge_worker:get_forwards(Pid)),
        ?assertEqual({error, no_remote_subscription_support},
                     emqx_bridge_worker:ensure_subscription_present(Pid, <<"t">>, 0)),
        ?assertEqual({error, no_remote_subscription_support},
                     emqx_bridge_worker:ensure_subscription_absent(Pid, <<"t">>)),
        ?assertEqual(Subs, emqx_bridge_worker:get_subscriptions(Pid))
    after
        ok = emqx_bridge_worker:stop(Pid)
    end.

%% A loopback RPC to local node
t_rpc(Config) when is_list(Config) ->
    Cfg = #{address => node(),
            forwards => [<<"t_rpc/#">>],
            connect_module => emqx_bridge_rpc,
            forward_mountpoint => <<"forwarded">>,
            start_type => auto},
    {ok, Pid} = emqx_bridge_worker:start_link(?FUNCTION_NAME, Cfg),
    ClientId = <<"ClientId">>,
    try
        {ok, ConnPid} = emqtt:start_link([{clientid, ClientId}]),
        {ok, _} = emqtt:connect(ConnPid),
        {ok, _, [1]} = emqtt:subscribe(ConnPid, {<<"forwarded/t_rpc/one">>, ?QOS_1}),
        timer:sleep(100),
        {ok, _PacketId} = emqtt:publish(ConnPid, <<"t_rpc/one">>, <<"hello">>, ?QOS_1),
        timer:sleep(100),
        ?assertEqual(1, length(receive_messages(1))),
        emqtt:disconnect(ConnPid)
    after
        ok = emqx_bridge_worker:stop(Pid)
    end.

%% Full data loopback flow explained:
%% mqtt-client ----> local-broker ---(local-subscription)--->
%% bridge(export) --- (mqtt-connection)--> local-broker ---(remote-subscription) -->
%% bridge(import) --> mqtt-client
t_mqtt(Config) when is_list(Config) ->
    SendToTopic = <<"t_mqtt/one">>,
    SendToTopic2 = <<"t_mqtt/two">>,
    SendToTopic3 = <<"t_mqtt/three">>,
    Mountpoint = <<"forwarded/${node}/">>,
    Cfg = #{address => "127.0.0.1:1883",
            forwards => [SendToTopic],
            connect_module => emqx_bridge_mqtt,
            forward_mountpoint => Mountpoint,
            username => "user",
            clean_start => true,
            clientid => "bridge_aws",
            keepalive => 60000,
            password => "passwd",
            proto_ver => mqttv4,
            queue => #{replayq_dir => "data/t_mqtt/",
                       replayq_seg_bytes => 10000,
                       batch_bytes_limit => 1000,
                       batch_count_limit => 10
                      },
            reconnect_delay_ms => 1000,
            ssl => false,
            %% Consume back to forwarded message for verification
            %% NOTE: this is a indefenite loopback without mocking emqx_bridge_worker:import_batch/1
            subscriptions => [{SendToTopic2, ?QOS_1}],
            receive_mountpoint => <<"receive/aws/">>,
            start_type => auto},
    {ok, Pid} = emqx_bridge_worker:start_link(?FUNCTION_NAME, Cfg),
    ClientId = <<"client-1">>,
    try
        ?assertEqual([{SendToTopic2, 1}], emqx_bridge_worker:get_subscriptions(Pid)),
        ok = emqx_bridge_worker:ensure_subscription_present(Pid, SendToTopic3, ?QOS_1),
        ?assertEqual([{SendToTopic3, 1},{SendToTopic2, 1}],
                     emqx_bridge_worker:get_subscriptions(Pid)),
        {ok, ConnPid} = emqtt:start_link([{clientid, ClientId}]),
        {ok, _Props} = emqtt:connect(ConnPid),
        emqtt:subscribe(ConnPid, <<"forwarded/+/t_mqtt/one">>, 1),
        %% message from a different client, to avoid getting terminated by no-local
        Max = 10,
        Msgs = lists:seq(1, Max),
        lists:foreach(fun(I) ->
                          {ok, _PacketId} = emqtt:publish(ConnPid, SendToTopic, integer_to_binary(I), ?QOS_1)
                      end, Msgs),
        ?assertEqual(10, length(receive_messages(200))),

        emqtt:subscribe(ConnPid, <<"receive/aws/t_mqtt/two">>, 1),
        %% message from a different client, to avoid getting terminated by no-local
        Max = 10,
        Msgs = lists:seq(1, Max),
        lists:foreach(fun(I) ->
                          {ok, _PacketId} = emqtt:publish(ConnPid, SendToTopic2, integer_to_binary(I), ?QOS_1)
                      end, Msgs),
        ?assertEqual(10, length(receive_messages(200))),

        emqtt:disconnect(ConnPid)
    after
        ok = emqx_bridge_worker:stop(Pid)
    end.

t_stub_normal(Config) when is_list(Config) ->
    Cfg = #{forwards => [<<"t_stub_normal/#">>],
            connect_module => emqx_bridge_stub_conn,
            forward_mountpoint => <<"forwarded">>,
            start_type => auto,
            client_pid => self()
           },
    {ok, Pid} = emqx_bridge_worker:start_link(?FUNCTION_NAME, Cfg),
    receive
        {Pid, emqx_bridge_stub_conn, ready} -> ok
    after
        5000 ->
            error(timeout)
    end,
    ClientId = <<"ClientId">>,
    try
        {ok, ConnPid} = emqtt:start_link([{clientid, ClientId}]),
        {ok, _} = emqtt:connect(ConnPid),
        {ok, _PacketId} = emqtt:publish(ConnPid, <<"t_stub_normal/one">>, <<"hello">>, ?QOS_1),
        receive
            {stub_message, WorkerPid, BatchRef, _Batch} ->
                WorkerPid ! {batch_ack, BatchRef},
                ok
        after
            5000 ->
                error(timeout)
        end,
        ?SNK_WAIT(inflight_drained),
        ?SNK_WAIT(replayq_drained),
        emqtt:disconnect(ConnPid)
    after
        ok = emqx_bridge_worker:stop(Pid)
    end.

t_stub_overflow(Config) when is_list(Config) ->
    Topic = <<"t_stub_overflow/one">>,
    MaxInflight = 20,
    Cfg = #{forwards => [Topic],
            connect_module => emqx_bridge_stub_conn,
            forward_mountpoint => <<"forwarded">>,
            start_type => auto,
            client_pid => self(),
            max_inflight => MaxInflight
           },
    {ok, Worker} = emqx_bridge_worker:start_link(?FUNCTION_NAME, Cfg),
    ClientId = <<"ClientId">>,
    try
        {ok, ConnPid} = emqtt:start_link([{clientid, ClientId}]),
        {ok, _} = emqtt:connect(ConnPid),
        lists:foreach(
          fun(I) ->
                  Data = integer_to_binary(I),
                  _ = emqtt:publish(ConnPid, Topic, Data, ?QOS_1)
          end, lists:seq(1, MaxInflight * 2)),
        ?SNK_WAIT(inflight_full),
        Acks = stub_receive(MaxInflight),
        lists:foreach(fun({Pid, Ref}) -> Pid ! {batch_ack, Ref} end, Acks),
        Acks2 = stub_receive(MaxInflight),
        lists:foreach(fun({Pid, Ref}) -> Pid ! {batch_ack, Ref} end, Acks2),
        ?SNK_WAIT(inflight_drained),
        ?SNK_WAIT(replayq_drained),
        emqtt:disconnect(ConnPid)
    after
        ok = emqx_bridge_worker:stop(Worker)
    end.

t_stub_random_order(Config) when is_list(Config) ->
    Topic = <<"t_stub_random_order/a">>,
    MaxInflight = 10,
    Cfg = #{forwards => [Topic],
            connect_module => emqx_bridge_stub_conn,
            forward_mountpoint => <<"forwarded">>,
            start_type => auto,
            client_pid => self(),
            max_inflight => MaxInflight
           },
    {ok, Worker} = emqx_bridge_worker:start_link(?FUNCTION_NAME, Cfg),
    ClientId = <<"ClientId">>,
    try
        {ok, ConnPid} = emqtt:start_link([{clientid, ClientId}]),
        {ok, _} = emqtt:connect(ConnPid),
        lists:foreach(
          fun(I) ->
                  Data = integer_to_binary(I),
                  _ = emqtt:publish(ConnPid, Topic, Data, ?QOS_1)
          end, lists:seq(1, MaxInflight)),
        Acks = stub_receive(MaxInflight),
        lists:foreach(fun({Pid, Ref}) -> Pid ! {batch_ack, Ref} end,
                      lists:reverse(Acks)),
        ?SNK_WAIT(inflight_drained),
        ?SNK_WAIT(replayq_drained),
        emqtt:disconnect(ConnPid)
    after
        ok = emqx_bridge_worker:stop(Worker)
    end.

t_stub_retry_inflight(Config) when is_list(Config) ->
    Topic = <<"to_stub_retry_inflight/a">>,
    MaxInflight = 10,
    Cfg = #{forwards => [Topic],
            connect_module => emqx_bridge_stub_conn,
            forward_mountpoint => <<"forwarded">>,
            reconnect_delay_ms => 10,
            start_type => auto,
            client_pid => self(),
            max_inflight => MaxInflight
           },
    {ok, Worker} = emqx_bridge_worker:start_link(?FUNCTION_NAME, Cfg),
    ClientId = <<"ClientId2">>,
    try
        case ?block_until(#{?snk_kind := connected, inflight := 0}, 2000, 1000) of
            {ok, #{inflight := 0}} -> ok;
            Other -> ct:fail("~p", [Other])
        end,
        {ok, ConnPid} = emqtt:start_link([{clientid, ClientId}]),
        {ok, _} = emqtt:connect(ConnPid),
        lists:foreach(
          fun(I) ->
                  Data = integer_to_binary(I),
                  _ = emqtt:publish(ConnPid, Topic, Data, ?QOS_1)
          end, lists:seq(1, MaxInflight)),
        %% receive acks but do not ack
        Acks1 = stub_receive(MaxInflight),
        ?assertEqual(MaxInflight, length(Acks1)),
        %% simulate a disconnect
        Worker ! {disconnected, self(), test},
        ?SNK_WAIT(disconnected),
        case ?block_until(#{?snk_kind := connected, inflight := MaxInflight}, 2000, 20) of
            {ok, _} -> ok;
            Error -> ct:fail("~p", [Error])
        end,
        %% expect worker to retry inflight, so to receive acks again
        Acks2 = stub_receive(MaxInflight),
        ?assertEqual(MaxInflight, length(Acks2)),
        lists:foreach(fun({Pid, Ref}) -> Pid ! {batch_ack, Ref} end,
                      lists:reverse(Acks2)),
        ?SNK_WAIT(inflight_drained),
        ?SNK_WAIT(replayq_drained),
        emqtt:disconnect(ConnPid)
    after
        ok = emqx_bridge_worker:stop(Worker)
    end.

stub_receive(N) ->
    stub_receive(N, []).

stub_receive(0, Acc) -> lists:reverse(Acc);
stub_receive(N, Acc) ->
    receive
        {stub_message, WorkerPid, BatchRef, _Batch} ->
            stub_receive(N - 1, [{WorkerPid, BatchRef} | Acc])
    after
        5000 ->
            lists:reverse(Acc)
    end.
