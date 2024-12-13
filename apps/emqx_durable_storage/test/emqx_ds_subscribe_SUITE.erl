%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include_lib("emqx_durable_storage/src/emqx_ds_beamformer.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%%

%% @doc This testcase verifies that `subscribe' and `unsubscribe' APIs
%% work and produce expected side effects on the subscription table.
%% Subscriptions are automatically removed when the subscriber
%% terminates, and the subscriber is notified when the shard is
%% restarted.
t_subscription_lifetime(Config) ->
    DB = ?FUNCTION_NAME,
    ?check_trace(
        #{timetrap => 30_000},
        begin
            ok = open_db(Config),
            %% Fill storage with some data to create streams and make
            %% an iterator:
            ?assertMatch(ok, publish(DB, 1, 1)),
            timer:sleep(100),
            [{_, Stream}] = emqx_ds:get_streams(DB, [<<"t">>], 0),
            {ok, It} = emqx_ds:make_iterator(DB, Stream, [<<"t">>], 0),
            %% 1. Test normal unsubscribe flow:
            {ok, Handle1, MRef1} = emqx_ds:subscribe(DB, ?FUNCTION_NAME, It, #{max_unacked => 100}),
            %% Subscription is registered:
            ?assertMatch(
                [_],
                emqx_ds_beamformer:lookup_sub(DB, Handle1),
                #{ref => Handle1}
            ),
            %% Unsubscribe and check that subscription has been
            %% unregistered:
            ?assertMatch(true, emqx_ds:unsubscribe(DB, Handle1)),
            ?assertMatch(
                [],
                emqx_ds_beamformer:lookup_sub(DB, Handle1),
                #{handle => Handle1}
            ),
            %% Try to unsubscribe with invalid handle:
            ?assertMatch(false, emqx_ds:unsubscribe(DB, Handle1)),
            demonitor(MRef1),
            %% 2. Verify the scenario where subscriber terminates:
            Parent = self(),
            Child = spawn_link(
                fun() ->
                    {ok, Handle2, _MRef2} = emqx_ds:subscribe(DB, ?FUNCTION_NAME, It, #{
                        max_unacked => 100
                    }),
                    Parent ! {ready, Handle2},
                    receive
                    after infinity -> ok
                    end
                end
            ),
            receive
                {ready, Handle2} -> ok
            end,
            ?assertMatch(
                [_],
                emqx_ds_beamformer:lookup_sub(DB, Handle2),
                #{handle => Handle2}
            ),
            %% Shutdown the child process and verify that the subscription has been automatically removed:
            unlink(Child),
            erlang:exit(Child, shutdown),
            timer:sleep(100),
            ?assertMatch(
                [],
                emqx_ds_beamformer:lookup_sub(DB, Handle2),
                #{handle => Handle2}
            ),
            %% 3. Stop the DB and make sure the subscriber receives
            %% `DOWN' message:
            {ok, _Handle3, MRef3} = emqx_ds:subscribe(DB, ?FUNCTION_NAME, It, #{max_unacked => 100}),
            ?assertMatch(ok, emqx_ds:close_db(DB)),
            ?assertMatch([MRef3], collect_down_msgs())
        end,
        []
    ).

%% @doc
t_subscription(Config) ->
    DB = ?FUNCTION_NAME,
    ?check_trace(
        #{timetrap => 30_000},
        begin
            ok = open_db(Config),
            %% Fill storage with some data to create streams and make
            %% an iterator:
            ?assertMatch(ok, publish(DB, 1, 1)),
            timer:sleep(100),
            [{_, Stream}] = emqx_ds:get_streams(DB, [<<"t">>], 0),
            {ok, It} = emqx_ds:make_iterator(
                DB, Stream, [<<"t">>], 0
            ),
            %% Subscribe and check state of the beamformer subscription table:
            {ok, SRef, MRef} = emqx_ds:subscribe(DB, ?FUNCTION_NAME, It, #{max_unacked => 3}),
            %% Check receiving of messages published at the beginning:
            ?assertMatch(
                [
                    #poll_reply{
                        ref = MRef,
                        seqno = 1,
                        payload =
                            {ok, _, [
                                {_, #message{payload = <<"1">>}}
                            ]}
                    }
                ],
                recv(?FUNCTION_NAME, MRef),
                #{sref => SRef, mref => MRef}
            ),
            %% Publish more data, it should result in an event:
            ?assertMatch(ok, publish(DB, 2, 3)),
            ?assertMatch(
                [
                    #poll_reply{
                        ref = MRef,
                        seqno = 3,
                        payload =
                            {ok, _, [
                                {_, #message{payload = <<"2">>}},
                                {_, #message{payload = <<"3">>}}
                            ]}
                    }
                ],
                recv(?FUNCTION_NAME, MRef),
                #{sref => SRef, mref => MRef}
            ),
            %% Fill more data:
            ?assertMatch(ok, publish(DB, 4, 5)),
            %% This data should not be delivered to the subscriber
            %% until it acks enough messages:
            ?assertMatch([], recv(?FUNCTION_NAME, MRef)),
            %% Ack sequence number:
            ?assertMatch(ok, emqx_ds:suback(DB, SRef, 3)),
            %% Now we get the messages:
            ?assertMatch(
                [
                    #poll_reply{
                        ref = MRef,
                        seqno = 5,
                        payload =
                            {ok, _, [
                                {_, #message{payload = <<"4">>}},
                                {_, #message{payload = <<"5">>}}
                            ]}
                    }
                ],
                recv(?FUNCTION_NAME, MRef),
                #{sref => SRef, mref => MRef}
            )
        end,
        []
    ).

%%

%% @doc Recieve poll replies with given UserData:
recv(UserData, MRef) ->
    recv(UserData, MRef, 10).

recv(_UserData, _Mref, 0) ->
    error(too_many_replies);
recv(UserData, MRef, N) ->
    receive
        #poll_reply{userdata = UserData} = Msg ->
            [Msg | recv(UserData, MRef, N - 1)];
        {'DOWN', MRef, _, _, Reason} ->
            error({unexpected_beamformer_termination, Reason})
    after 1000 ->
        []
    end.

collect_down_msgs() ->
    receive
        {'DOWN', MRef, _, _, _} ->
            [MRef | collect_down_msgs()]
    after 100 ->
        []
    end.

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
            {emqx_durable_storage, #{}},
            {emqx_ds_backends, #{}}
        ],
        #{work_dir => WorkDir}
    ),
    [{apps, Apps}, {tc, TCName} | Config].

end_per_testcase(_TCName, Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)),
    snabbkaffe:stop().
