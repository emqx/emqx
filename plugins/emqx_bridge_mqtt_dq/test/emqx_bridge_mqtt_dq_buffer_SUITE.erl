%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
%% @doc Buffer process tests with a fake connector.
%%
%% We start a fake conn_sup (a real supervisor registered via gproc)
%% whose child forwards {publish_batch,...} to the test process.
%% The buffer discovers the child via supervisor:which_children
%% just like production code.
%%--------------------------------------------------------------------
-module(emqx_bridge_mqtt_dq_buffer_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Port = emqx_common_test_helpers:select_free_port(tcp),
    Apps = emqx_cth_suite:start(
        [
            {emqx, #{
                config => #{
                    listeners => #{
                        tcp => #{default => #{bind => "127.0.0.1:" ++ integer_to_list(Port)}},
                        ssl => #{default => #{enable => false}},
                        ws => #{default => #{enable => false}},
                        wss => #{default => #{enable => false}}
                    }
                }
            }}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)),
    Config.

init_per_testcase(Case, Config) ->
    QueueDir = iolist_to_binary([
        <<"/tmp/emqx_bridge_mqtt_dq_buf_test/">>,
        atom_to_binary(Case)
    ]),
    [{queue_dir, QueueDir}, {pids, []} | Config].

end_per_testcase(_Case, _Config) ->
    cleanup_fake_conn_sups(),
    ok.

%%--------------------------------------------------------------------
%% Tests
%%--------------------------------------------------------------------

-doc "Enqueued items are flushed to connector as a publish_batch message.".
t_enqueue_and_flush(Config) ->
    BridgeName = <<"enq_flush">>,
    {Buf, _} = start_buffer_with_fake_conn(Config, BridgeName, #{}),
    Items = [make_item(I) || I <- lists:seq(1, 3)],
    lists:foreach(
        fun(Item) ->
            emqx_bridge_mqtt_dq_buffer:enqueue(Buf, Item, no_ack)
        end,
        Items
    ),
    {Ref, Received} = receive_publish_batch(2000),
    ?assertEqual(3, length(Received)),
    Buf ! {batch_ack, Ref, ok},
    ok.

-doc "Enqueue with alias sends ack back to caller (QoS > 0 path).".
t_enqueue_ack(Config) ->
    BridgeName = <<"enq_ack">>,
    {Buf, _} = start_buffer_with_fake_conn(Config, BridgeName, #{}),
    Alias = alias([reply]),
    Item = make_item(1),
    emqx_bridge_mqtt_dq_buffer:enqueue(Buf, Item, Alias),
    receive
        {Alias, ok} -> ok
    after 2000 ->
        ct:fail("enqueue ack not received")
    end,
    {Ref, _} = receive_publish_batch(2000),
    Buf ! {batch_ack, Ref, ok},
    ok.

-doc "No publish_batch sent when no connector is available.".
t_no_flush_without_connector(Config) ->
    QueueDir = ?config(queue_dir, Config),
    BridgeConfig = make_bridge_config(<<"no_conn">>, QueueDir, #{pool_size => 1}),
    {ok, Buf} = emqx_bridge_mqtt_dq_buffer:start_link(BridgeConfig, 0),
    unlink(Buf),
    emqx_bridge_mqtt_dq_buffer:enqueue(Buf, make_item(1), no_ack),
    receive
        {publish_batch, _, _, _} -> ct:fail("unexpected publish_batch without connector")
    after 500 ->
        ok
    end,
    exit(Buf, shutdown),
    ok.

-doc "Connector death salvages inflight batches; re-sent after reconnect.".
t_connector_down_salvages_inflight(Config) ->
    BridgeName = <<"salvage">>,
    {Buf, ConnSup} = start_buffer_with_fake_conn(Config, BridgeName, #{}),
    lists:foreach(
        fun(I) ->
            emqx_bridge_mqtt_dq_buffer:enqueue(Buf, make_item(I), no_ack)
        end,
        lists:seq(1, 3)
    ),
    %% Receive batch but don't ack — leave it in-flight
    {_Ref, _Items} = receive_publish_batch(2000),
    %% Kill the fake connector child directly so buffer gets DOWN
    [{_, ConnChild, _, _} | _] = supervisor:which_children(ConnSup),
    MRef = monitor(process, ConnChild),
    exit(ConnChild, kill),
    receive
        {'DOWN', MRef, process, ConnChild, _} -> ok
    after 2000 ->
        ct:fail("connector child did not stop")
    end,
    %% Buffer should re-send the salvaged batch
    {Ref2, Items2} = receive_publish_batch(3000),
    ?assertEqual(3, length(Items2)),
    Buf ! {batch_ack, Ref2, ok},
    ok.

-doc "Each batch pops at most max_inflight items from the queue.".
t_batch_size_matches_max_inflight(Config) ->
    MaxInflight = 2,
    BridgeName = <<"batch_sz">>,
    {Buf, _} = start_buffer_with_fake_conn(Config, BridgeName, #{max_inflight => MaxInflight}),
    lists:foreach(
        fun(I) ->
            emqx_bridge_mqtt_dq_buffer:enqueue(Buf, make_item(I), no_ack)
        end,
        lists:seq(1, 5)
    ),
    {Ref1, Items1} = receive_publish_batch(2000),
    ?assert(length(Items1) =< MaxInflight),
    Buf ! {batch_ack, Ref1, ok},
    {Ref2, Items2} = receive_publish_batch(2000),
    ?assert(length(Items2) =< MaxInflight),
    Buf ! {batch_ack, Ref2, ok},
    {Ref3, Items3} = receive_publish_batch(2000),
    ?assertEqual(1, length(Items3)),
    Buf ! {batch_ack, Ref3, ok},
    ok.

-doc "At most 2 batches in flight; 3rd blocked until one is acked.".
t_max_inflight_batches_cap(Config) ->
    MaxInflight = 2,
    BridgeName = <<"cap">>,
    {Buf, _} = start_buffer_with_fake_conn(Config, BridgeName, #{max_inflight => MaxInflight}),
    lists:foreach(
        fun(I) ->
            emqx_bridge_mqtt_dq_buffer:enqueue(Buf, make_item(I), no_ack)
        end,
        lists:seq(1, 10)
    ),
    {Ref1, _} = receive_publish_batch(2000),
    {Ref2, _} = receive_publish_batch(2000),
    %% Third batch should NOT arrive yet
    receive
        {publish_batch, _, _, _} -> ct:fail("3rd batch should be blocked")
    after 500 ->
        ok
    end,
    Buf ! {batch_ack, Ref1, ok},
    {Ref3, _} = receive_publish_batch(2000),
    Buf ! {batch_ack, Ref2, ok},
    Buf ! {batch_ack, Ref3, ok},
    ok.

-doc "Queued items survive buffer process restart (disk persistence).".
t_disk_persistence_across_restart(Config) ->
    QueueDir = ?config(queue_dir, Config),
    BridgeName = <<"persist">>,
    BridgeConfig = make_bridge_config(BridgeName, QueueDir, #{pool_size => 1}),

    %% Phase 1: start buffer without connector, enqueue items
    ok = filelib:ensure_path(binary_to_list(QueueDir) ++ "/0"),
    {ok, Buf1} = emqx_bridge_mqtt_dq_buffer:start_link(BridgeConfig, 0),
    unlink(Buf1),
    enqueue_items_and_wait(Buf1, [make_item(I) || I <- lists:seq(1, 5)]),
    stop_buffer(Buf1),

    %% Phase 2: start a fake connector, then restart buffer — should replay
    {_ConnSup, _ConnPid} = start_fake_conn_sup(BridgeName, 1),
    {ok, Buf2} = emqx_bridge_mqtt_dq_buffer:start_link(BridgeConfig, 0),
    unlink(Buf2),
    AllItems = drain_publish_batches(Buf2, 3000),
    ?assertEqual(5, length(AllItems)),
    exit(Buf2, shutdown),
    ok.

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

make_item(Seq) ->
    #{
        topic => <<"test/topic">>,
        payload => integer_to_binary(Seq),
        qos => 1,
        retain => false,
        timestamp => erlang:system_time(millisecond),
        properties => #{}
    }.

make_bridge_config(Name, QueueBaseDir, Overrides) ->
    Default = #{
        name => Name,
        queue_base_dir => QueueBaseDir,
        seg_bytes => 1048576,
        max_total_bytes => 10485760,
        pool_size => 1,
        max_inflight => 32
    },
    maps:merge(Default, Overrides).

start_buffer_with_fake_conn(Config, BridgeName, Overrides) ->
    QueueDir = ?config(queue_dir, Config),
    PoolSize = maps:get(pool_size, Overrides, 1),
    BridgeConfig = make_bridge_config(BridgeName, QueueDir, Overrides#{pool_size => PoolSize}),
    {ConnSup, _ConnPid} = start_fake_conn_sup(BridgeName, PoolSize),
    {ok, Buf} = emqx_bridge_mqtt_dq_buffer:start_link(BridgeConfig, 0),
    unlink(Buf),
    {Buf, ConnSup}.

%% Start a fake conn_sup registered via gproc (same as production).
%% Children forward publish_batch messages to the test process.
start_fake_conn_sup(BridgeName, PoolSize) ->
    TestPid = self(),
    {ok, Sup} = supervisor:start_link(
        {via, gproc, {n, l, {emqx_bridge_mqtt_dq_conn_sup, BridgeName}}},
        ?MODULE,
        {fake_conn_sup, PoolSize, TestPid, BridgeName}
    ),
    unlink(Sup),
    [{_, ConnPid, _, _} | _] = supervisor:which_children(Sup),
    {Sup, ConnPid}.

%% supervisor callback
init({fake_conn_sup, PoolSize, TestPid, BridgeName}) ->
    Children = [
        #{
            id => {conn, BridgeName, I},
            start => {?MODULE, start_fake_conn, [TestPid]},
            restart => permanent,
            shutdown => 5000,
            type => worker
        }
     || I <- lists:seq(0, PoolSize - 1)
    ],
    {ok, {#{strategy => one_for_one, intensity => 10, period => 60}, Children}}.

start_fake_conn(TestPid) ->
    Pid = spawn_link(fun() -> fake_conn_loop(TestPid) end),
    {ok, Pid}.

fake_conn_loop(TestPid) ->
    receive
        {publish_batch, _Items, _From, _Ref} = Msg ->
            TestPid ! Msg,
            fake_conn_loop(TestPid);
        _ ->
            fake_conn_loop(TestPid)
    end.

receive_publish_batch(Timeout) ->
    receive
        {publish_batch, Items, _From, Ref} ->
            {Ref, Items}
    after Timeout ->
        ct:fail("publish_batch not received within ~p ms", [Timeout])
    end.

drain_publish_batches(BufPid, Timeout) ->
    drain_publish_batches(BufPid, Timeout, []).

drain_publish_batches(BufPid, Timeout, Acc) ->
    receive
        {publish_batch, Items, _From, Ref} ->
            BufPid ! {batch_ack, Ref, ok},
            drain_publish_batches(BufPid, Timeout, Acc ++ Items)
    after Timeout ->
        Acc
    end.

enqueue_items_and_wait(Buf, Items) ->
    lists:foreach(
        fun(Item) ->
            Alias = alias([reply]),
            emqx_bridge_mqtt_dq_buffer:enqueue(Buf, Item, Alias),
            receive
                {Alias, ok} -> ok
            after 2000 ->
                ct:fail("enqueue ack not received")
            end
        end,
        Items
    ).

stop_buffer(Pid) ->
    MRef = monitor(process, Pid),
    exit(Pid, shutdown),
    receive
        {'DOWN', MRef, process, Pid, _} -> ok
    after 5000 ->
        ct:fail("buffer did not stop")
    end.

cleanup_fake_conn_sups() ->
    Names = [
        <<"enq_flush">>,
        <<"enq_ack">>,
        <<"no_conn">>,
        <<"salvage">>,
        <<"batch_sz">>,
        <<"cap">>,
        <<"persist">>
    ],
    lists:foreach(
        fun(Name) ->
            case emqx_bridge_mqtt_dq_conn_sup:sup_pid(Name) of
                undefined ->
                    ok;
                Pid ->
                    MRef = monitor(process, Pid),
                    exit(Pid, kill),
                    receive
                        {'DOWN', MRef, _, _, _} -> ok
                    after 2000 -> ok
                    end
            end
        end,
        Names
    ).
