%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc This test suite verifies internals of the beamformer.
-module(emqx_ds_subscribe_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% @doc This testcase verifies that `subscribe' and `unsubscribe' APIs
%% work and produce expected side effects on the subscription table.
t_sub_unsub(Config) ->
    DB = ?FUNCTION_NAME,
    ?check_trace(
        #{timetrap => 30_000},
        begin
            Stream = make_stream(Config),
            {ok, It} = emqx_ds:make_iterator(DB, Stream, [<<"t">>], 0),
            {ok, Handle, _MRef} = emqx_ds:subscribe(DB, ?FUNCTION_NAME, It, #{max_unacked => 100}),
            %% Subscription is registered:
            ?assertMatch(
                #{},
                emqx_ds:subscription_info(DB, Handle),
                #{ref => Handle}
            ),
            %% Unsubscribe and check that subscription has been
            %% unregistered:
            ?assertMatch(true, emqx_ds:unsubscribe(DB, Handle)),
            ?assertMatch(
                undefined,
                emqx_ds:subscription_info(DB, Handle),
                #{handle => Handle}
            ),
            %% Try to unsubscribe with invalid handle:
            ?assertMatch(false, emqx_ds:unsubscribe(DB, Handle)),
            ?assertMatch([], collect_down_msgs())
        end,
        []
    ).

%% @doc Verify the scenario where the subscriber terminates without
%% unsubscribing. Here we create a subscription from a temporary
%% process that exits normally. Subscription must be automatically
%% removed.
t_dead_subscriber_cleanup(Config) ->
    DB = ?FUNCTION_NAME,
    ?check_trace(
        #{timetrap => 30_000},
        begin
            Stream = make_stream(Config),
            {ok, It} = emqx_ds:make_iterator(DB, Stream, [<<"t">>], 0),
            Parent = self(),
            Child = spawn_link(
                fun() ->
                    {ok, Handle, _MRef} = emqx_ds:subscribe(DB, ?FUNCTION_NAME, It, #{
                        max_unacked => 100
                    }),
                    Parent ! {ready, Handle},
                    receive
                        exit -> ok
                    end
                end
            ),
            receive
                {ready, Handle} -> ok
            end,
            %% Currently the process is running. Verify that the
            %% subscription is present:
            ?assertMatch(
                #{},
                emqx_ds:subscription_info(DB, Handle),
                #{handle => Handle}
            ),
            %% Shutdown the child process and verify that the
            %% subscription has been automatically removed:
            Child ! exit,
            timer:sleep(100),
            ?assertMatch(false, is_process_alive(Child)),
            ?assertMatch(
                undefined,
                emqx_ds:subscription_info(DB, Handle),
                #{handle => Handle}
            )
        end,
        []
    ).

%% @doc Verify that the client receives a `DOWN' message when the
%% server is down:
t_shard_down_notify(Config) ->
    DB = ?FUNCTION_NAME,
    ?check_trace(
        #{timetrap => 30_000},
        begin
            Stream = make_stream(Config),
            {ok, It} = emqx_ds:make_iterator(DB, Stream, [<<"t">>], 0),
            {ok, _Handle, MRef} = emqx_ds:subscribe(DB, ?FUNCTION_NAME, It, #{max_unacked => 100}),
            ?assertMatch(ok, emqx_ds:close_db(DB)),
            ?assertMatch([MRef], collect_down_msgs())
        end,
        []
    ).

%% @doc Verify behavior of a subscription that replayes old messages.
%% This testcase focuses on the correctness of `catchup' beamformer
%% workers.
t_catchup(Config) ->
    DB = ?FUNCTION_NAME,
    ?check_trace(
        #{timetrap => 30_000},
        begin
            Stream = make_stream(Config),
            %% Fill the storage and close the generation:
            ?assertMatch(ok, publish(DB, 1, 9)),
            emqx_ds:add_generation(DB),
            %% Subscribe:
            {ok, It} = emqx_ds:make_iterator(DB, Stream, [<<"t">>], 0),
            {ok, SRef, MRef} = emqx_ds:subscribe(DB, ?FUNCTION_NAME, It, #{max_unacked => 3}),
            %% We receive one batch and stop waiting for ack. Note:
            %% batch may contain more messages than `max_unacked',
            %% because batch size is independent.
            ?assertMatch(
                [
                    #poll_reply{
                        ref = MRef,
                        lagging = true,
                        seqno = 5,
                        size = 5,
                        payload =
                            {ok, _, [
                                {_, #message{payload = <<"0">>}},
                                {_, #message{payload = <<"1">>}},
                                {_, #message{payload = <<"2">>}},
                                {_, #message{payload = <<"3">>}},
                                {_, #message{payload = <<"4">>}}
                            ]}
                    }
                ],
                recv(?FUNCTION_NAME, MRef)
            ),
            %% Ack and receive the rest of the messages:
            ?assertMatch(ok, emqx_ds:suback(DB, SRef, 5)),
            ?assertMatch(
                [
                    #poll_reply{
                        ref = MRef,
                        lagging = true,
                        seqno = 10,
                        size = 5,
                        payload =
                            {ok, _, [
                                {_, #message{payload = <<"5">>}},
                                {_, #message{payload = <<"6">>}},
                                {_, #message{payload = <<"7">>}},
                                {_, #message{payload = <<"8">>}},
                                {_, #message{payload = <<"9">>}}
                            ]}
                    }
                ],
                recv(?FUNCTION_NAME, MRef)
            ),
            %% Ack and receive `end_of_stream':
            ?assertMatch(ok, emqx_ds:suback(DB, SRef, 10)),
            ?assertMatch(
                [
                    #poll_reply{
                        ref = MRef,
                        seqno = 11,
                        size = 1,
                        payload = {ok, end_of_stream}
                    }
                ],
                recv(?FUNCTION_NAME, MRef)
            )
        end,
        []
    ).

%% @doc Verify behavior of a subscription that always stays at the top
%% of the stream. This testcase focuses on the correctness of
%% `rt' beamformer workers.
t_realtime(Config) ->
    DB = ?FUNCTION_NAME,
    ?check_trace(
        #{timetrap => 30_000},
        begin
            Stream = make_stream(Config),
            %% Subscribe:
            {ok, It} = emqx_ds:make_iterator(
                DB, Stream, [<<"t">>], erlang:system_time(millisecond)
            ),
            {ok, SRef, MRef} = emqx_ds:subscribe(DB, ?FUNCTION_NAME, It, #{max_unacked => 100}),
            %% Publish/consume/ack loop:
            ?assertMatch(ok, publish(DB, 1, 2)),
            ?assertMatch(
                [
                    #poll_reply{
                        ref = MRef,
                        lagging = false,
                        stuck = false,
                        seqno = 2,
                        size = 2,
                        payload =
                            {ok, _, [
                                {_, #message{payload = <<"1">>}},
                                {_, #message{payload = <<"2">>}}
                            ]}
                    }
                ],
                recv(?FUNCTION_NAME, MRef)
            ),
            ?assertMatch(ok, publish(DB, 3, 4)),
            ?assertMatch(
                [
                    #poll_reply{
                        ref = MRef,
                        lagging = false,
                        stuck = false,
                        seqno = 4,
                        size = 2,
                        payload =
                            {ok, _, [
                                {_, #message{payload = <<"3">>}},
                                {_, #message{payload = <<"4">>}}
                            ]}
                    }
                ],
                recv(?FUNCTION_NAME, MRef)
            ),
            ?assertMatch(ok, emqx_ds:suback(DB, SRef, 4)),
            %% Close the generation. The subscriber should be promptly
            %% notified:
            ?assertMatch(ok, emqx_ds:add_generation(DB)),
            ?assertMatch(
                [#poll_reply{ref = MRef, seqno = 5, payload = {ok, end_of_stream}}],
                recv(?FUNCTION_NAME, MRef)
            )
        end,
        []
    ).

%% @doc This testcase emulates a slow subscriber.
t_slow_sub(Config) ->
    DB = ?FUNCTION_NAME,
    ?check_trace(
        #{timetrap => 30_000},
        begin
            Stream = make_stream(Config),
            %% Subscribe:
            {ok, It} = emqx_ds:make_iterator(DB, Stream, [<<"t">>], 0),
            {ok, SRef, MRef} = emqx_ds:subscribe(DB, ?FUNCTION_NAME, It, #{max_unacked => 3}),
            %% Check receiving of messages published at the beginning:
            ?assertMatch(
                [
                    #poll_reply{
                        ref = MRef,
                        lagging = true,
                        stuck = false,
                        seqno = 1,
                        size = 1,
                        payload =
                            {ok, _, [
                                {_, #message{payload = <<"0">>}}
                            ]}
                    }
                ],
                recv(?FUNCTION_NAME, MRef)
            ),
            %% Publish more data, it should result in an event:
            ?assertMatch(ok, publish(DB, 1, 2)),
            ?assertMatch(
                [
                    #poll_reply{
                        ref = MRef,
                        lagging = false,
                        stuck = true,
                        seqno = 3,
                        size = 2,
                        payload =
                            {ok, _, [
                                {_, #message{payload = <<"1">>}},
                                {_, #message{payload = <<"2">>}}
                            ]}
                    }
                ],
                recv(?FUNCTION_NAME, MRef)
            ),
            %% Fill more data:
            ?assertMatch(ok, publish(DB, 3, 4)),
            %% This data should NOT be delivered to the subscriber
            %% until it acks enough messages:
            ?assertMatch([], recv(?FUNCTION_NAME, MRef)),
            %% Ack sequence number:
            ?assertMatch(ok, emqx_ds:suback(DB, SRef, 3)),
            %% Now we get the messages:
            ?assertMatch(
                [
                    #poll_reply{
                        ref = MRef,
                        lagging = true,
                        stuck = false,
                        seqno = 5,
                        payload =
                            {ok, _, [
                                {_, #message{payload = <<"3">>}},
                                {_, #message{payload = <<"4">>}}
                            ]}
                    }
                ],
                recv(?FUNCTION_NAME, MRef)
            )
        end,
        []
    ).

%% @doc Remove generation during catchup. The client should receive an
%% unrecoverable error.
t_catchup_unrecoverable(Config) ->
    DB = ?FUNCTION_NAME,
    ?check_trace(
        #{timetrap => 30_000},
        begin
            Stream = make_stream(Config),
            %% Fill the storage and close the generation:
            ?assertMatch(ok, publish(DB, 1, 9)),
            emqx_ds:add_generation(DB),
            %% Subscribe:
            {ok, It} = emqx_ds:make_iterator(DB, Stream, [<<"t">>], 0),
            {ok, SRef, MRef} = emqx_ds:subscribe(DB, ?FUNCTION_NAME, It, #{max_unacked => 3}),
            %% Receive a batch and pause for the ack:
            ?assertMatch(
                [
                    #poll_reply{
                        ref = MRef,
                        seqno = 5,
                        size = 5,
                        payload =
                            {ok, _, [
                                {_, #message{payload = <<"0">>}},
                                {_, #message{payload = <<"1">>}},
                                {_, #message{payload = <<"2">>}},
                                {_, #message{payload = <<"3">>}},
                                {_, #message{payload = <<"4">>}}
                            ]}
                    }
                ],
                recv(?FUNCTION_NAME, MRef)
            ),
            %% Drop generation:
            ?assertMatch(
                #{{<<"0">>, 1} := _, {<<"0">>, 2} := _},
                emqx_ds:list_generations_with_lifetimes(DB)
            ),
            ?assertMatch(ok, emqx_ds:drop_generation(DB, {<<"0">>, 1})),
            %% Ack and receive unrecoverable error:
            emqx_ds:suback(DB, SRef, 5),
            ?assertMatch(
                [
                    #poll_reply{
                        ref = MRef,
                        size = 1,
                        seqno = 6,
                        payload = {error, unrecoverable, generation_not_found}
                    }
                ],
                recv(?FUNCTION_NAME, MRef)
            )
        end,
        []
    ).

%%

%% @doc Recieve poll replies with given UserData:
recv(UserData, MRef) ->
    recv(UserData, MRef, 1000).

recv(UserData, MRef, Timeout) ->
    receive
        #poll_reply{userdata = UserData} = Msg ->
            [Msg | recv(UserData, MRef, Timeout)];
        {'DOWN', MRef, _, _, Reason} ->
            error({unexpected_beamformer_termination, Reason})
    after Timeout ->
        []
    end.

collect_down_msgs() ->
    receive
        {'DOWN', MRef, _, _, _} ->
            [MRef | collect_down_msgs()]
    after 100 ->
        []
    end.

%% Fill topic "t" with some data and return the corresponding stream:
make_stream(Config) ->
    DB = proplists:get_value(tc, Config),
    ok = open_db(Config),
    ?assertMatch(ok, publish(DB, 0, 0)),
    timer:sleep(100),
    [{_, Stream}] = emqx_ds:get_streams(DB, [<<"t">>], 0),
    Stream.

publish(DB, Start, End) ->
    emqx_ds:store_batch(DB, [
        emqx_message:make(<<"pub">>, <<"t">>, integer_to_binary(I))
     || I <- lists:seq(Start, End)
    ]).

open_db(Config) ->
    emqx_ds:open_db(proplists:get_value(tc, Config), proplists:get_value(ds_conf, Config)).

all() ->
    [{group, Backend} || Backend <- backends()].

groups() ->
    TCs = emqx_common_test_helpers:all(?MODULE),
    [{Backend, TCs} || Backend <- backends()].

backends() ->
    application:load(emqx_ds_backends),
    {ok, L} = application:get_env(emqx_ds_backends, available_backends),
    L.

init_per_group(emqx_fdb_ds, _Config) ->
    {skip, not_implemented};
init_per_group(emqx_ds_builtin_raft, Config) ->
    [
        {ds_conf, emqx_ds_replication_layer:test_db_config(Config)}
        | Config
    ];
init_per_group(Backend, Config) ->
    [
        {ds_conf, Backend:test_db_config(Config)}
        | Config
    ].

end_per_group(_Group, Config) ->
    Config.

init_per_testcase(TCName, Config) ->
    WorkDir = emqx_cth_suite:work_dir(TCName, Config),
    Apps = emqx_cth_suite:start(
        [
            {emqx_durable_storage, #{override_env => [{poll_batch_size, 5}]}},
            {emqx_ds_backends, #{}}
        ],
        #{work_dir => WorkDir}
    ),
    [{apps, Apps}, {tc, TCName} | Config].

end_per_testcase(_TCName, Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)),
    snabbkaffe:stop().
