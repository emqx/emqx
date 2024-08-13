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
-module(emqx_ds_builtin_mnesia_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("../../emqx_utils/include/emqx_message.hrl").
-include("../../emqx_durable_storage/include/emqx_ds.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include("../../emqx/include/asserts.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [emqx_ds_builtin_mnesia],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    ok = emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(_TC, Config) ->
    Config.

end_per_testcase(_TC, _Config) ->
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

open(DB, CTConfig) ->
    ok = emqx_ds:open_db(DB, opts(CTConfig)),
    on_exit(fun() -> emqx_ds:drop_db(DB) end),
    ok.

opts(_CTConfig) ->
    #{
        backend => builtin_mnesia
    }.

now_ms() ->
    erlang:system_time(millisecond).

message(ClientId, Topic, Payload) ->
    #message{
        from = ClientId,
        topic = iolist_to_binary(Topic),
        payload = Payload,
        timestamp = now_ms(),
        id = emqx_guid:gen()
    }.

topic_filter(Bin) ->
    emqx_topic:words(iolist_to_binary(Bin)).

delete_op(ClientId, Topic) ->
    {delete, #message_matcher{
        from = ClientId,
        topic = iolist_to_binary(Topic),
        timestamp = now_ms(),
        payload = '_'
    }}.

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

%% Tests that some simple sequence of operations has the intended effects.
%%   * This backend is "compact": there's at most one message per each topic;
%%   * Timestamp doesn't matter when reading/writing.
t_smoke_crud(Config) ->
    ?check_trace(
        begin
            DB = ?FUNCTION_NAME,
            open(DB, Config),
            ?assertEqual([], emqx_ds:get_streams(DB, topic_filter(<<"#">>), now_ms())),
            ClientId1 = <<"c1">>,
            ClientId2 = <<"c2">>,
            Msgs1 =
                [Msg1A, _, Msg1C] = [
                    message(ClientId1, [<<"subs/">>, ClientId1, <<"/1">>], <<"sub,1,1">>),
                    message(ClientId1, [<<"subs/">>, ClientId1, <<"/2">>], <<"sub,1,2">>),
                    message(ClientId1, [<<"seqnos/">>, ClientId1, <<"/1">>], <<"seqno,1,1">>)
                ],
            Msgs2 =
                [Msg2A, _, Msg2C, Msg2D] = [
                    message(ClientId2, [<<"ranks/">>, ClientId2, <<"/1">>], <<"rank,2,1">>),
                    message(ClientId2, [<<"subs/">>, ClientId2, <<"/1">>], <<"sub,2,1">>),
                    message(ClientId2, [<<"subs/">>, ClientId2, <<"/2">>], <<"sub,2,2">>),
                    message(ClientId2, [<<"seqnos/">>, ClientId2, <<"/1">>], <<"seqno,2,1">>)
                ],
            ?assertEqual(ok, emqx_ds:store_batch(DB, Msgs1 ++ Msgs2, #{sync => true})),

            Read1 = emqx_ds_test_helpers:consume(DB, topic_filter([<<"+/">>, ClientId1, <<"/+">>])),
            ?assertEqual(lists:sort(Msgs1), lists:sort(Read1)),
            Read2 = emqx_ds_test_helpers:consume(DB, topic_filter([<<"+/">>, ClientId2, <<"/+">>])),
            ?assertEqual(lists:sort(Msgs2), lists:sort(Read2)),

            %% Update some existing messages, create a new one
            Msgs3 =
                [Msg3A, Msg3B, Msg3C] = [
                    message(ClientId1, [<<"subs/">>, ClientId1, <<"/2">>], <<"sub,1,2,updated">>),
                    message(ClientId2, [<<"subs/">>, ClientId2, <<"/1">>], <<"sub,2,1,updated">>),
                    %% New message
                    message(ClientId2, [<<"meta/">>, ClientId2, <<"/1">>], <<"meta,2,1">>)
                ],
            ?assertEqual(ok, emqx_ds:store_batch(DB, Msgs3, #{sync => true})),

            Read3 = emqx_ds_test_helpers:consume(DB, topic_filter([<<"+/">>, ClientId1, <<"/+">>])),
            Expected3 = [Msg3A, Msg1A, Msg1C],
            ?assertEqual(lists:sort(Expected3), lists:sort(Read3)),
            Read4 = emqx_ds_test_helpers:consume(DB, topic_filter([<<"+/">>, ClientId2, <<"/+">>])),
            Expected4 = [Msg3B, Msg3C, Msg2A, Msg2C, Msg2D],
            ?assertEqual(lists:sort(Expected4), lists:sort(Read4)),

            %% Delete messages
            Msgs4 = [
                delete_op(ClientId1, [<<"subs/">>, ClientId1, <<"/2">>]),
                delete_op(ClientId2, [<<"ranks/">>, ClientId2, <<"/1">>])
            ],
            ?assertEqual(ok, emqx_ds:store_batch(DB, Msgs4, #{sync => true})),

            Read5 = emqx_ds_test_helpers:consume(DB, topic_filter([<<"+/">>, ClientId1, <<"/+">>])),
            Expected5 = [Msg1A, Msg1C],
            ?assertEqual(lists:sort(Expected5), lists:sort(Read5)),

            Read6 = emqx_ds_test_helpers:consume(DB, topic_filter([<<"+/">>, ClientId2, <<"/+">>])),
            Expected6 = [Msg3B, Msg3C, Msg2C, Msg2D],
            ?assertEqual(lists:sort(Expected6), lists:sort(Read6)),

            %% Testing `topkey/fixed level/+` read pattern
            Read7 = emqx_ds_test_helpers:consume(
                DB, topic_filter([<<"subs/">>, ClientId1, <<"/+">>])
            ),
            Expected7 = [Msg1A],
            ?assertEqual(lists:sort(Expected7), lists:sort(Read7)),

            %% Delete `+/fixed level/#`
            DelTFBin = emqx_topic:join(['+', ClientId1, '#']),
            NumDeleted = emqx_ds_test_helpers:delete(DB, topic_filter(DelTFBin)),
            ?assertEqual(2, NumDeleted),

            ?assertEqual(
                [],
                emqx_ds_test_helpers:consume(DB, topic_filter([<<"+/">>, ClientId1, <<"/+">>]))
            ),

            %% Nothing changed for this message set.
            Read8 = emqx_ds_test_helpers:consume(DB, topic_filter([<<"+/">>, ClientId2, <<"/+">>])),
            Expected8 = Expected6,
            ?assertEqual(lists:sort(Expected8), lists:sort(Read8)),

            ok
        end,
        []
    ),
    ok.
