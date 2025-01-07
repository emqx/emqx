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
-module(emqx_ds_new_streams_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%%

t_sub_unsub(_) ->
    ?check_trace(
        #{timetrap => 30_000},
        begin
            DB = ?FUNCTION_NAME,
            {ok, Pid} = emqx_ds_new_streams:start_link(DB),
            ?assertEqual(Pid, emqx_ds_new_streams:where(DB)),
            %% Try to create subscriptions with invalid topic filter,
            %% make sure the server handles that gracefully:
            {error, badarg} = emqx_ds_new_streams:watch(DB, garbage),
            %% Create a watch and make sure it is functional:
            {ok, Ref} = emqx_ds_new_streams:watch(DB, [<<"foo">>, <<"1">>]),
            ok = emqx_ds_new_streams:notify_new_stream(DB, [<<"foo">>, <<"1">>]),
            assertEvents([Ref]),
            %% Try to unsubscribe with wrong subid:
            false = emqx_ds_new_streams:unwatch(DB, wrong),
            %% Unsubscribe from the events and verify that notifications are
            %% no longer received:
            true = emqx_ds_new_streams:unwatch(DB, Ref),
            ok = emqx_ds_new_streams:notify_new_stream(DB, [<<"foo">>, <<"1">>]),
            assertEvents([])
        end,
        fun no_unexpected_events/1
    ).

%% Verify that subscriptions are automatically cleaned up when the
%% subscribing process dies:
t_clean_on_down(_) ->
    ?check_trace(
        #{timetrap => 10_000},
        begin
            DB = ?FUNCTION_NAME,
            {ok, _} = emqx_ds_new_streams:start_link(DB),
            %% Subscribe to topic updates from a temporary process:
            Pid = spawn_link(
                fun() ->
                    {ok, _} = emqx_ds_new_streams:watch(DB, [<<"1">>]),
                    {ok, _} = emqx_ds_new_streams:watch(DB, [<<"2">>]),
                    receive
                        done -> ok
                    end
                end
            ),
            %% Check if the subscriptions are present:
            timer:sleep(100),
            ?assertMatch([_, _], emqx_ds_new_streams:list_subscriptions(DB)),
            %% Stop the process and verify that subscriptions were
            %% automatically deleted:
            Pid ! done,
            timer:sleep(100),
            ?assertMatch([], emqx_ds_new_streams:list_subscriptions(DB))
        end,
        fun no_unexpected_events/1
    ).

%% Verify that SUT is capable of forwarding notifications about
%% changes in a group of topics (denoted by a topic filter) to a set
%% of subscribers that also use topic filter:
t_matching(_) ->
    ?check_trace(
        #{timetrap => 30_000},
        begin
            DB = ?FUNCTION_NAME,
            {ok, _} = emqx_ds_new_streams:start_link(DB),
            %% Create subscriptions:
            {ok, Ref1} = emqx_ds_new_streams:watch(DB, [<<"foo">>, <<"1">>]),
            {ok, Ref2} = emqx_ds_new_streams:watch(DB, [<<"foo">>, '+']),
            {ok, Ref3} = emqx_ds_new_streams:watch(DB, [<<"foo">>, '#']),
            {ok, Ref4} = emqx_ds_new_streams:watch(DB, ['']),
            %% Try patterns that aren't matched by any subscription:
            ok = emqx_ds_new_streams:notify_new_stream(DB, [<<"bar">>]),
            ok = emqx_ds_new_streams:notify_new_stream(DB, [<<"bar">>, '+']),
            assertEvents([]),
            %% These patterns should be matched by all non-empty topic
            %% subscriptions:
            ok = emqx_ds_new_streams:notify_new_stream(DB, [<<"foo">>, <<"1">>]),
            assertEvents([Ref1, Ref2, Ref3]),
            ok = emqx_ds_new_streams:notify_new_stream(DB, ['+', <<"1">>]),
            assertEvents([Ref1, Ref2, Ref3]),
            ok = emqx_ds_new_streams:notify_new_stream(DB, [<<"foo">>, '+']),
            assertEvents([Ref1, Ref2, Ref3]),
            %% This should include empty topic subscriptions as well:
            ok = emqx_ds_new_streams:notify_new_stream(DB, ['#']),
            assertEvents([Ref1, Ref2, Ref3, Ref4]),
            ok = emqx_ds_new_streams:notify_new_stream(DB, ['']),
            assertEvents([Ref4]),
            %% These patterns should exclude the first subscriber:
            ok = emqx_ds_new_streams:notify_new_stream(DB, [<<"foo">>, <<"2">>]),
            assertEvents([Ref2, Ref3]),
            ok = emqx_ds_new_streams:notify_new_stream(DB, ['+', <<"2">>]),
            assertEvents([Ref2, Ref3]),
            %% This pattern should exclude the second subscriber as well:
            ok = emqx_ds_new_streams:notify_new_stream(DB, [<<"foo">>, <<"1">>, '+']),
            assertEvents([Ref3]),
            ok = emqx_ds_new_streams:notify_new_stream(DB, ['+', '+', '+']),
            assertEvents([Ref3]),
            ok = emqx_ds_new_streams:notify_new_stream(DB, ['+', '+', '+', '#']),
            assertEvents([Ref3])
        end,
        fun no_unexpected_events/1
    ).

t_dirty(_) ->
    ?check_trace(
        #{timetrap => 30_000},
        begin
            DB = ?FUNCTION_NAME,
            {ok, _} = emqx_ds_new_streams:start_link(DB),
            %% Run dirty without subscribers:
            ok = emqx_ds_new_streams:set_dirty(DB),
            %% Create subscriptions:
            {ok, Ref1} = emqx_ds_new_streams:watch(DB, [<<"foo">>, <<"1">>]),
            {ok, Ref2} = emqx_ds_new_streams:watch(DB, [<<"foo">>, '+']),
            {ok, Ref3} = emqx_ds_new_streams:watch(DB, [<<"foo">>, '#']),
            {ok, Ref4} = emqx_ds_new_streams:watch(DB, [<<"1">>]),
            %% Setting database to dirty should notify all subscribers
            %% regardless of the topic:
            ok = emqx_ds_new_streams:set_dirty(DB),
            assertEvents([Ref1, Ref2, Ref3, Ref4])
        end,
        fun no_unexpected_events/1
    ).

no_unexpected_events(Trace) ->
    ?assertMatch([], ?of_kind(ds_new_streams_unexpected_event, Trace)).

%%

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_testcase(TCName, Config) ->
    WorkDir = emqx_cth_suite:work_dir(TCName, Config),
    Apps = emqx_cth_suite:start(
        [
            {emqx_durable_storage, #{
                override_env => [
                    {new_streams_dirty_pause, 1},
                    {new_streams_dirty_batch_size, 2}
                ]
            }}
        ],
        #{work_dir => WorkDir}
    ),
    [{apps, Apps} | Config].

end_per_testcase(_TCName, Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)),
    ok.

assertEvents(Expected) ->
    Got = (fun F() ->
        receive
            #new_stream_event{subref = Ref} -> [Ref | F()]
        after 100 ->
            []
        end
    end)(),
    ?assertEqual(lists:usort(Expected), lists:usort(Got)).
