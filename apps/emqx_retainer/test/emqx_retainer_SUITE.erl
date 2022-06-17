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

-module(emqx_retainer_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-define(CLUSTER_RPC_SHARD, emqx_cluster_rpc_shard).

-include("emqx_retainer.hrl").

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

all() ->
    [
        {group, mnesia_without_indices},
        {group, mnesia_with_indices},
        {group, mnesia_reindex}
    ].

groups() ->
    [
        {mnesia_without_indices, [sequence], common_tests()},
        {mnesia_with_indices, [sequence], common_tests()},
        {mnesia_reindex, [sequence], [t_reindex]}
    ].

common_tests() ->
    emqx_common_test_helpers:all(?MODULE) -- [t_reindex].

-define(BASE_CONF, <<
    ""
    "\n"
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
    ""
>>).

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

init_per_suite(Config) ->
    emqx_common_test_helpers:start_apps([emqx_conf]),
    load_conf(),
    emqx_limiter_sup:start_link(),
    timer:sleep(200),
    ok = application:ensure_started(?APP),
    Config.

end_per_suite(_Config) ->
    ekka:stop(),
    mria:stop(),
    mria_mnesia:delete_schema(),
    emqx_common_test_helpers:stop_apps([?APP, emqx_conf]).

init_per_group(mnesia_without_indices, Config) ->
    mnesia:clear_table(?TAB_INDEX_META),
    mnesia:clear_table(?TAB_INDEX),
    mnesia:clear_table(?TAB_MESSAGE),
    Config;
init_per_group(mnesia_reindex, Config) ->
    emqx_retainer_mnesia:populate_index_meta(),
    mnesia:clear_table(?TAB_INDEX),
    mnesia:clear_table(?TAB_MESSAGE),
    Config;
init_per_group(_, Config) ->
    emqx_retainer_mnesia:populate_index_meta(),
    mnesia:clear_table(?TAB_INDEX),
    mnesia:clear_table(?TAB_MESSAGE),
    Config.

end_per_group(_Group, Config) ->
    emqx_retainer_mnesia:populate_index_meta(),
    Config.

init_per_testcase(t_get_basic_usage_info, Config) ->
    mnesia:clear_table(?TAB_INDEX),
    mnesia:clear_table(?TAB_MESSAGE),
    emqx_retainer_mnesia:populate_index_meta(),
    Config;
init_per_testcase(_TestCase, Config) ->
    Config.

load_conf() ->
    ok = emqx_config:delete_override_conf_files(),
    emqx_ratelimiter_SUITE:init_config(),
    ok = emqx_config:init_load(emqx_retainer_schema, ?BASE_CONF).

%%--------------------------------------------------------------------
%% Test Cases
%%--------------------------------------------------------------------
t_store_and_clean(_) ->
    {ok, C1} = emqtt:start_link([{clean_start, true}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(C1),

    emqtt:publish(
        C1,
        <<"retained">>,
        <<"this is a retained message">>,
        [{qos, 0}, {retain, true}]
    ),
    timer:sleep(100),

    {ok, List} = emqx_retainer:page_read(<<"retained">>, 1, 10),
    ?assertEqual(1, length(List)),

    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained">>, [{qos, 0}, {rh, 0}]),
    ?assertEqual(1, length(receive_messages(1))),

    {ok, #{}, [0]} = emqtt:unsubscribe(C1, <<"retained">>),

    emqtt:publish(C1, <<"retained">>, <<"">>, [{qos, 0}, {retain, true}]),
    timer:sleep(100),
    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained">>, [{qos, 0}, {rh, 0}]),
    ?assertEqual(0, length(receive_messages(1))),

    ok = emqx_retainer:clean(),
    {ok, List2} = emqx_retainer:page_read(<<"retained">>, 1, 10),
    ?assertEqual(0, length(List2)),

    ok = emqtt:disconnect(C1).

t_retain_handling(_) ->
    {ok, C1} = emqtt:start_link([{clean_start, true}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(C1),

    ok = emqx_retainer:clean(),

    %% rh = 0, no wildcard, and with empty retained message
    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained">>, [{qos, 0}, {rh, 0}]),
    ?assertEqual(0, length(receive_messages(1))),
    {ok, #{}, [0]} = emqtt:unsubscribe(C1, <<"retained">>),

    %% rh = 0, has wildcard, and with empty retained message
    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained/#">>, [{qos, 0}, {rh, 0}]),
    ?assertEqual(0, length(receive_messages(1))),
    {ok, #{}, [0]} = emqtt:unsubscribe(C1, <<"retained/#">>),

    emqtt:publish(
        C1,
        <<"retained">>,
        <<"this is a retained message">>,
        [{qos, 0}, {retain, true}]
    ),

    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained">>, [{qos, 0}, {rh, 0}]),
    ?assertEqual(1, length(receive_messages(1))),

    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained">>, [{qos, 0}, {rh, 0}]),
    ?assertEqual(1, length(receive_messages(1))),

    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained/#">>, [{qos, 0}, {rh, 0}]),
    ?assertEqual(1, length(receive_messages(1))),

    {ok, #{}, [0]} = emqtt:unsubscribe(C1, <<"retained">>),

    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained">>, [{qos, 0}, {rh, 1}]),
    ?assertEqual(1, length(receive_messages(1))),

    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained">>, [{qos, 0}, {rh, 1}]),
    ?assertEqual(0, length(receive_messages(1))),

    {ok, #{}, [0]} = emqtt:unsubscribe(C1, <<"retained">>),

    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained">>, [{qos, 0}, {rh, 2}]),
    ?assertEqual(0, length(receive_messages(1))),

    emqtt:publish(C1, <<"retained">>, <<"">>, [{qos, 0}, {retain, true}]),
    ok = emqtt:disconnect(C1).

t_wildcard_subscription(_) ->
    {ok, C1} = emqtt:start_link([{clean_start, true}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(C1),
    emqtt:publish(
        C1,
        <<"retained/0">>,
        <<"this is a retained message 0">>,
        [{qos, 0}, {retain, true}]
    ),
    emqtt:publish(
        C1,
        <<"retained/1">>,
        <<"this is a retained message 1">>,
        [{qos, 0}, {retain, true}]
    ),
    emqtt:publish(
        C1,
        <<"retained/a/b/c">>,
        <<"this is a retained message 2">>,
        [{qos, 0}, {retain, true}]
    ),
    emqtt:publish(
        C1,
        <<"/x/y/z">>,
        <<"this is a retained message 3">>,
        [{qos, 0}, {retain, true}]
    ),

    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained/+">>, 0),
    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained/+/b/#">>, 0),
    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"/+/y/#">>, 0),
    ?assertEqual(4, length(receive_messages(4))),

    emqtt:publish(C1, <<"retained/0">>, <<"">>, [{qos, 0}, {retain, true}]),
    emqtt:publish(C1, <<"retained/1">>, <<"">>, [{qos, 0}, {retain, true}]),
    emqtt:publish(C1, <<"retained/a/b/c">>, <<"">>, [{qos, 0}, {retain, true}]),
    emqtt:publish(C1, <<"/x/y/z">>, <<"">>, [{qos, 0}, {retain, true}]),
    ok = emqtt:disconnect(C1).

t_message_expiry(_) ->
    {ok, C1} = emqtt:start_link([{clean_start, true}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(C1),

    emqtt:publish(
        C1,
        <<"retained/0">>,
        #{'Message-Expiry-Interval' => 0},
        <<"don't expire">>,
        [{qos, 0}, {retain, true}]
    ),
    emqtt:publish(
        C1,
        <<"retained/1">>,
        #{'Message-Expiry-Interval' => 2},
        <<"expire">>,
        [{qos, 0}, {retain, true}]
    ),
    emqtt:publish(
        C1,
        <<"retained/2">>,
        #{'Message-Expiry-Interval' => 5},
        <<"don't expire">>,
        [{qos, 0}, {retain, true}]
    ),
    emqtt:publish(
        C1,
        <<"retained/3">>,
        <<"don't expire">>,
        [{qos, 0}, {retain, true}]
    ),
    emqtt:publish(
        C1,
        <<"$SYS/retained/4">>,
        <<"don't expire">>,
        [{qos, 0}, {retain, true}]
    ),

    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained/+">>, 0),
    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"$SYS/retained/+">>, 0),
    ?assertEqual(5, length(receive_messages(5))),
    {ok, #{}, [0]} = emqtt:unsubscribe(C1, <<"retained/+">>),
    {ok, #{}, [0]} = emqtt:unsubscribe(C1, <<"$SYS/retained/+">>),

    timer:sleep(3000),
    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained/+">>, 0),
    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"$SYS/retained/+">>, 0),
    ?assertEqual(4, length(receive_messages(5))),

    emqtt:publish(C1, <<"retained/0">>, <<"">>, [{qos, 0}, {retain, true}]),
    emqtt:publish(C1, <<"retained/1">>, <<"">>, [{qos, 0}, {retain, true}]),
    emqtt:publish(C1, <<"retained/2">>, <<"">>, [{qos, 0}, {retain, true}]),
    emqtt:publish(C1, <<"retained/3">>, <<"">>, [{qos, 0}, {retain, true}]),
    emqtt:publish(C1, <<"$SYS/retained/4">>, <<"">>, [{qos, 0}, {retain, true}]),

    ok = emqtt:disconnect(C1).

t_message_expiry_2(_) ->
    ConfMod = fun(Conf) ->
        Conf#{<<"msg_expiry_interval">> := <<"2s">>}
    end,
    Case = fun() ->
        {ok, C1} = emqtt:start_link([{clean_start, true}, {proto_ver, v5}]),
        {ok, _} = emqtt:connect(C1),
        emqtt:publish(C1, <<"retained">>, <<"expire">>, [{qos, 0}, {retain, true}]),

        {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained">>, [{qos, 0}, {rh, 0}]),
        ?assertEqual(1, length(receive_messages(1))),
        timer:sleep(4000),
        {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained">>, [{qos, 0}, {rh, 0}]),
        ?assertEqual(0, length(receive_messages(1))),
        {ok, #{}, [0]} = emqtt:unsubscribe(C1, <<"retained">>),

        emqtt:publish(C1, <<"retained">>, <<"">>, [{qos, 0}, {retain, true}]),

        ok = emqtt:disconnect(C1)
    end,
    with_conf(ConfMod, Case).

t_clean(_) ->
    {ok, C1} = emqtt:start_link([{clean_start, true}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(C1),
    emqtt:publish(
        C1,
        <<"retained/0">>,
        <<"this is a retained message 0">>,
        [{qos, 0}, {retain, true}]
    ),
    emqtt:publish(
        C1,
        <<"retained/1">>,
        <<"this is a retained message 1">>,
        [{qos, 0}, {retain, true}]
    ),
    emqtt:publish(
        C1,
        <<"retained/test/0">>,
        <<"this is a retained message 2">>,
        [{qos, 0}, {retain, true}]
    ),
    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained/#">>, [{qos, 0}, {rh, 0}]),
    ?assertEqual(3, length(receive_messages(3))),

    ok = emqx_retainer:delete(<<"retained/test/0">>),
    ok = emqx_retainer:delete(<<"retained/+">>),
    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained/#">>, [{qos, 0}, {rh, 0}]),
    ?assertEqual(0, length(receive_messages(3))),

    ok = emqtt:disconnect(C1).

t_stop_publish_clear_msg(_) ->
    emqx_retainer:update_config(#{<<"stop_publish_clear_msg">> => true}),
    {ok, C1} = emqtt:start_link([{clean_start, true}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(C1),
    emqtt:publish(
        C1,
        <<"retained/0">>,
        <<"this is a retained message 0">>,
        [{qos, 0}, {retain, true}]
    ),

    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained/#">>, [{qos, 0}, {rh, 0}]),
    ?assertEqual(1, length(receive_messages(1))),

    emqtt:publish(C1, <<"retained/0">>, <<"">>, [{qos, 0}, {retain, true}]),
    ?assertEqual(0, length(receive_messages(1))),

    emqx_retainer:update_config(#{<<"stop_publish_clear_msg">> => false}),
    ok = emqtt:disconnect(C1).

t_flow_control(_) ->
    #{per_client := PerClient} = RetainerCfg = emqx_config:get([limiter, batch, bucket, retainer]),
    RetainerCfg2 = RetainerCfg#{
        per_client :=
            PerClient#{
                rate := emqx_ratelimiter_SUITE:to_rate("1/1s"),
                capacity := 1
            }
    },
    emqx_config:put([limiter, batch, bucket, retainer], RetainerCfg2),
    emqx_limiter_manager:restart_server(batch),
    timer:sleep(500),

    emqx_retainer_dispatcher:refresh_limiter(),
    timer:sleep(500),

    emqx_retainer:update_config(#{
        <<"flow_control">> =>
            #{
                <<"batch_read_number">> => 1,
                <<"batch_deliver_number">> => 1,
                <<"batch_deliver_limiter">> => retainer
            }
    }),
    {ok, C1} = emqtt:start_link([{clean_start, true}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(C1),
    emqtt:publish(
        C1,
        <<"retained/0">>,
        <<"this is a retained message 0">>,
        [{qos, 0}, {retain, true}]
    ),
    emqtt:publish(
        C1,
        <<"retained/1">>,
        <<"this is a retained message 1">>,
        [{qos, 0}, {retain, true}]
    ),
    emqtt:publish(
        C1,
        <<"retained/3">>,
        <<"this is a retained message 3">>,
        [{qos, 0}, {retain, true}]
    ),
    Begin = erlang:system_time(millisecond),
    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained/#">>, [{qos, 0}, {rh, 0}]),
    ?assertEqual(3, length(receive_messages(3))),
    End = erlang:system_time(millisecond),
    Diff = End - Begin,

    ?assert(
        Diff > timer:seconds(2.5) andalso Diff < timer:seconds(3.9),
        lists:flatten(io_lib:format("Diff is :~p~n", [Diff]))
    ),

    ok = emqtt:disconnect(C1),

    %% recover the limiter
    emqx_config:put([limiter, batch, bucket, retainer], RetainerCfg),
    emqx_limiter_manager:restart_server(batch),
    timer:sleep(500),

    emqx_retainer_dispatcher:refresh_limiter(),
    timer:sleep(500),
    ok.

t_clear_expired(_) ->
    ConfMod = fun(Conf) ->
        Conf#{
            <<"msg_clear_interval">> := <<"1s">>,
            <<"msg_expiry_interval">> := <<"3s">>
        }
    end,

    Case = fun() ->
        {ok, C1} = emqtt:start_link([{clean_start, true}, {proto_ver, v5}]),
        {ok, _} = emqtt:connect(C1),

        lists:foreach(
            fun(I) ->
                emqtt:publish(
                    C1,
                    <<"retained/", (I + 60):8/unsigned-integer>>,
                    #{'Message-Expiry-Interval' => 3},
                    <<"retained">>,
                    [{qos, 0}, {retain, true}]
                )
            end,
            lists:seq(1, 5)
        ),
        timer:sleep(1000),

        {ok, List} = emqx_retainer:page_read(<<"retained/+">>, 1, 10),
        ?assertEqual(5, erlang:length(List)),

        timer:sleep(4500),

        {ok, List2} = emqx_retainer:page_read(<<"retained/+">>, 1, 10),
        ?assertEqual(0, erlang:length(List2)),

        ok = emqtt:disconnect(C1)
    end,
    with_conf(ConfMod, Case).

t_max_payload_size(_) ->
    ConfMod = fun(Conf) -> Conf#{<<"max_payload_size">> := 6} end,
    Case = fun() ->
        emqx_retainer:clean(),
        timer:sleep(500),
        {ok, C1} = emqtt:start_link([{clean_start, true}, {proto_ver, v5}]),
        {ok, _} = emqtt:connect(C1),

        emqtt:publish(
            C1,
            <<"retained/1">>,
            #{},
            <<"1234">>,
            [{qos, 0}, {retain, true}]
        ),

        emqtt:publish(
            C1,
            <<"retained/2">>,
            #{},
            <<"1234567">>,
            [{qos, 0}, {retain, true}]
        ),

        timer:sleep(500),
        {ok, List} = emqx_retainer:page_read(<<"retained/+">>, 1, 10),
        ?assertEqual(1, erlang:length(List)),

        ok = emqtt:disconnect(C1)
    end,
    with_conf(ConfMod, Case).

t_page_read(_) ->
    {ok, C1} = emqtt:start_link([{clean_start, true}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(C1),
    ok = emqx_retainer:clean(),
    timer:sleep(500),

    Fun = fun(I) ->
        emqtt:publish(
            C1,
            <<"retained/", (I + 60)>>,
            <<"this is a retained message">>,
            [{qos, 0}, {retain, true}]
        )
    end,
    lists:foreach(Fun, lists:seq(1, 9)),
    timer:sleep(200),

    {ok, List} = emqx_retainer:page_read(<<"retained/+">>, 1, 5),
    ?assertEqual(5, length(List)),

    {ok, List2} = emqx_retainer:page_read(<<"retained/+">>, 2, 5),
    ?assertEqual(4, length(List2)),

    ok = emqtt:disconnect(C1).

t_only_for_coverage(_) ->
    ?assertEqual("retainer", emqx_retainer_schema:namespace()),
    ignored = gen_server:call(emqx_retainer, unexpected),
    ok = gen_server:cast(emqx_retainer, unexpected),
    unexpected = erlang:send(erlang:whereis(emqx_retainer), unexpected),

    Dispatcher = emqx_retainer_dispatcher:worker(),
    ignored = gen_server:call(Dispatcher, unexpected),
    ok = gen_server:cast(Dispatcher, unexpected),
    unexpected = erlang:send(Dispatcher, unexpected),
    true = erlang:exit(Dispatcher, normal),
    ok.

t_reindex(_) ->
    {ok, C} = emqtt:start_link([{clean_start, true}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(C),

    ok = emqx_retainer:clean(),
    ok = emqx_retainer_mnesia:reindex([[1, 3]], false, fun(_Done) -> ok end),

    %% Prepare retained messages for "retained/N1/N2" topics
    ?check_trace(
        ?wait_async_action(
            lists:foreach(
                fun(N1) ->
                    lists:foreach(
                        fun(N2) ->
                            emqtt:publish(
                                C,
                                erlang:iolist_to_binary([
                                    <<"retained/">>,
                                    io_lib:format("~5..0w", [N1]),
                                    <<"/">>,
                                    io_lib:format("~5..0w", [N2])
                                ]),
                                <<"this is a retained message">>,
                                [{qos, 0}, {retain, true}]
                            )
                        end,
                        lists:seq(1, 10)
                    )
                end,
                lists:seq(1, 1000)
            ),
            #{?snk_kind := message_retained, topic := <<"retained/01000/00010">>},
            5000
        ),
        []
    ),

    ?check_trace(
        ?wait_async_action(
            begin
                %% Spawn reindexing in the background
                spawn_link(
                    fun() ->
                        timer:sleep(1000),
                        emqx_retainer_mnesia:reindex(
                            [[1, 4]],
                            false,
                            fun(Done) ->
                                ?tp(
                                    info,
                                    reindexing_progress,
                                    #{done => Done}
                                )
                            end
                        )
                    end
                ),

                %% Subscribe to "retained/N/+" for some time, while reindexing is in progress
                T = erlang:monotonic_time(millisecond),
                ok = test_retain_while_reindexing(C, T + 3000)
            end,
            #{?snk_kind := reindexing_progress, done := 10000},
            10000
        ),
        fun(Trace) ->
            ?assertMatch(
                [_ | _],
                lists:filter(
                    fun
                        (#{done := 10000}) -> true;
                        (_) -> false
                    end,
                    ?of_kind(reindexing_progress, Trace)
                )
            )
        end
    ).

t_get_basic_usage_info(_Config) ->
    ?assertEqual(#{retained_messages => 0}, emqx_retainer:get_basic_usage_info()),
    Context = undefined,
    lists:foreach(
        fun(N) ->
            Num = integer_to_binary(N),
            Message = emqx_message:make(<<"retained/", Num/binary>>, <<"payload">>),
            ok = emqx_retainer:store_retained(Context, Message)
        end,
        lists:seq(1, 5)
    ),
    ?assertEqual(#{retained_messages => 5}, emqx_retainer:get_basic_usage_info()),
    ok.

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

test_retain_while_reindexing(C, Deadline) ->
    case erlang:monotonic_time(millisecond) > Deadline of
        true ->
            ok;
        false ->
            N = rand:uniform(1000),
            Topic = iolist_to_binary([
                <<"retained/">>,
                io_lib:format("~5..0w", [N]),
                <<"/+">>
            ]),
            {ok, #{}, [0]} = emqtt:subscribe(C, Topic, [{qos, 0}, {rh, 0}]),
            Messages = receive_messages(10),
            ?assertEqual(10, length(Messages)),
            {ok, #{}, [0]} = emqtt:unsubscribe(C, Topic),
            test_retain_while_reindexing(C, Deadline)
    end.

receive_messages(Count) ->
    receive_messages(Count, []).
receive_messages(0, Msgs) ->
    Msgs;
receive_messages(Count, Msgs) ->
    receive
        {publish, Msg} ->
            ct:log("Msg: ~p ~n", [Msg]),
            receive_messages(Count - 1, [Msg | Msgs]);
        Other ->
            ct:log("Other Msg: ~p~n", [Other]),
            receive_messages(Count, Msgs)
    after 2000 ->
        Msgs
    end.

with_conf(ConfMod, Case) ->
    Conf = emqx:get_raw_config([retainer]),
    NewConf = ConfMod(Conf),
    emqx_retainer:update_config(NewConf),
    try
        Case(),
        emqx_retainer:update_config(Conf)
    catch
        Type:Error:Strace ->
            emqx_retainer:update_config(Conf),
            erlang:raise(Type, Error, Strace)
    end.
