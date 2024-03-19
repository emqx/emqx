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
-module(emqx_ds_storage_snapshot_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

opts() ->
    #{storage => {emqx_ds_storage_bitfield_lts, #{}}}.

%%

t_snapshot_take_restore(_Config) ->
    Shard = {?FUNCTION_NAME, _ShardId = <<"42">>},
    {ok, Pid} = emqx_ds_storage_layer:start_link(Shard, opts()),

    %% Push some messages to the shard.
    Msgs1 = [gen_message(N) || N <- lists:seq(1000, 2000)],
    ?assertEqual(ok, emqx_ds_storage_layer:store_batch(Shard, mk_batch(Msgs1), #{})),

    %% Add new generation and push some more.
    ?assertEqual(ok, emqx_ds_storage_layer:add_generation(Shard, 3000)),
    Msgs2 = [gen_message(N) || N <- lists:seq(4000, 5000)],
    ?assertEqual(ok, emqx_ds_storage_layer:store_batch(Shard, mk_batch(Msgs2), #{})),
    ?assertEqual(ok, emqx_ds_storage_layer:add_generation(Shard, 6000)),

    %% Take a snapshot of the shard.
    {ok, SnapReader} = emqx_ds_storage_layer:take_snapshot(Shard),

    %% Push even more messages to the shard AFTER taking the snapshot.
    Msgs3 = [gen_message(N) || N <- lists:seq(7000, 8000)],
    ?assertEqual(ok, emqx_ds_storage_layer:store_batch(Shard, mk_batch(Msgs3), #{})),

    %% Destroy the shard.
    _ = unlink(Pid),
    ok = proc_lib:stop(Pid, shutdown, infinity),
    ok = emqx_ds_storage_layer:drop_shard(Shard),

    %% Restore the shard from the snapshot.
    {ok, SnapWriter} = emqx_ds_storage_layer:accept_snapshot(Shard),
    ?assertEqual(ok, transfer_snapshot(SnapReader, SnapWriter)),

    %% Verify that the restored shard contains the messages up until the snapshot.
    {ok, _Pid} = emqx_ds_storage_layer:start_link(Shard, opts()),
    ?assertEqual(
        Msgs1 ++ Msgs2,
        lists:keysort(#message.timestamp, consume(Shard, ['#']))
    ).

mk_batch(Msgs) ->
    [{emqx_message:timestamp(Msg, microsecond), Msg} || Msg <- Msgs].

gen_message(N) ->
    Topic = emqx_topic:join([<<"foo">>, <<"bar">>, integer_to_binary(N)]),
    message(Topic, integer_to_binary(N), N * 100).

message(Topic, Payload, PublishedAt) ->
    #message{
        from = <<?MODULE_STRING>>,
        topic = Topic,
        payload = Payload,
        timestamp = PublishedAt,
        id = emqx_guid:gen()
    }.

transfer_snapshot(Reader, Writer) ->
    ChunkSize = rand:uniform(1024),
    case emqx_ds_storage_snapshot:read_chunk(Reader, ChunkSize) of
        {RStatus, Chunk, NReader} ->
            Data = iolist_to_binary(Chunk),
            {WStatus, NWriter} = emqx_ds_storage_snapshot:write_chunk(Writer, Data),
            %% Verify idempotency.
            ?assertEqual(
                {WStatus, NWriter},
                emqx_ds_storage_snapshot:write_chunk(Writer, Data)
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
            end;
        {error, Reason} ->
            {error, Reason, Reader}
    end.

consume(Shard, TopicFilter) ->
    consume(Shard, TopicFilter, 0).

consume(Shard, TopicFilter, StartTime) ->
    Streams = emqx_ds_storage_layer:get_streams(Shard, TopicFilter, StartTime),
    lists:flatmap(
        fun({_Rank, Stream}) ->
            {ok, It} = emqx_ds_storage_layer:make_iterator(Shard, Stream, TopicFilter, StartTime),
            consume_stream(Shard, It)
        end,
        Streams
    ).

consume_stream(Shard, It) ->
    case emqx_ds_storage_layer:next(Shard, It, 100) of
        {ok, _NIt, _Msgs = []} ->
            [];
        {ok, NIt, Batch} ->
            [Msg || {_DSKey, Msg} <- Batch] ++ consume_stream(Shard, NIt);
        {ok, end_of_stream} ->
            []
    end.

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
