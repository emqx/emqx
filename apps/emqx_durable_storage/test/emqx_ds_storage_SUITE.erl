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
-module(emqx_ds_storage_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("../../emqx/include/emqx.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

opts() ->
    #{storage => {emqx_ds_storage_reference, #{}}}.

%%

t_snapshot_take_restore(_Config) ->
    Shard = {?FUNCTION_NAME, _ShardId = <<"42">>},
    {ok, Pid} = emqx_ds_storage_layer:start_link(Shard, opts()),

    %% Push some messages to the shard.
    Msgs1 = [gen_message(N) || N <- lists:seq(1000, 2000)],
    ?assertEqual(ok, emqx_ds_storage_layer:store_batch(Shard, batch(Msgs1), #{})),

    %% Add new generation and push some more.
    ?assertEqual(ok, emqx_ds_storage_layer:add_generation(Shard, 3000)),
    Msgs2 = [gen_message(N) || N <- lists:seq(4000, 5000)],
    ?assertEqual(ok, emqx_ds_storage_layer:store_batch(Shard, batch(Msgs2), #{})),
    ?assertEqual(ok, emqx_ds_storage_layer:add_generation(Shard, 6000)),

    %% Take a snapshot of the shard.
    {ok, SnapReader} = emqx_ds_storage_layer:take_snapshot(Shard),

    %% Push even more messages to the shard AFTER taking the snapshot.
    Msgs3 = [gen_message(N) || N <- lists:seq(7000, 8000)],
    ?assertEqual(ok, emqx_ds_storage_layer:store_batch(Shard, batch(Msgs3), #{})),

    %% Destroy the shard.
    ok = stop_shard(Pid),
    ok = emqx_ds_storage_layer:drop_shard(Shard),

    %% Restore the shard from the snapshot.
    {ok, SnapWriter} = emqx_ds_storage_layer:accept_snapshot(Shard),
    ?assertEqual(ok, transfer_snapshot(SnapReader, SnapWriter)),

    %% Verify that the restored shard contains the messages up until the snapshot.
    {ok, _Pid} = emqx_ds_storage_layer:start_link(Shard, opts()),
    snabbkaffe_diff:assert_lists_eq(
        Msgs1 ++ Msgs2,
        lists:keysort(#message.timestamp, emqx_ds_test_helpers:storage_consume(Shard, ['#']))
    ).

transfer_snapshot(Reader, Writer) ->
    ChunkSize = rand:uniform(1024),
    ReadResult = emqx_ds_storage_snapshot:read_chunk(Reader, ChunkSize),
    ?assertMatch({RStatus, _, _} when RStatus == next; RStatus == last, ReadResult),
    {RStatus, Chunk, NReader} = ReadResult,
    Data = iolist_to_binary(Chunk),
    {WStatus, NWriter} = emqx_ds_storage_snapshot:write_chunk(Writer, Data),
    %% Verify idempotency.
    ?assertMatch(
        {WStatus, NWriter},
        emqx_ds_storage_snapshot:write_chunk(NWriter, Data)
    ),
    %% Verify convergence.
    ?assertEqual(
        RStatus,
        WStatus,
        #{reader => NReader, writer => NWriter}
    ),
    case WStatus of
        last ->
            ?assertEqual(ok, emqx_ds_storage_snapshot:release_reader(NReader)),
            ?assertEqual(ok, emqx_ds_storage_snapshot:release_writer(NWriter)),
            ok;
        next ->
            transfer_snapshot(NReader, NWriter)
    end.

%%

batch(Msgs) ->
    [{emqx_message:timestamp(Msg), Msg} || Msg <- Msgs].

gen_message(N) ->
    Topic = emqx_topic:join([<<"foo">>, <<"bar">>, integer_to_binary(N)]),
    message(Topic, crypto:strong_rand_bytes(16), N).

message(Topic, Payload, PublishedAt) ->
    #message{
        from = <<?MODULE_STRING>>,
        topic = Topic,
        payload = Payload,
        timestamp = PublishedAt,
        id = emqx_guid:gen()
    }.

stop_shard(Pid) ->
    _ = unlink(Pid),
    proc_lib:stop(Pid, shutdown, infinity).

%%

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_testcase(TCName, Config) ->
    WorkDir = emqx_cth_suite:work_dir(TCName, Config),
    Apps = emqx_cth_suite:start(
        [{emqx_durable_storage, #{override_env => [{db_data_dir, WorkDir}]}}],
        #{work_dir => WorkDir}
    ),
    [{apps, Apps} | Config].

end_per_testcase(_TCName, Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)),
    ok.
