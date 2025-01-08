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

%% @doc This script can be loaded to a running EMQX EE node. It will
%% create a number of DS databases with different options and fill
%% them with data of given size.
%%
%% Then it will measure size of the database directories and create
%% a "storage (in)efficiency" report.
-module(storage_efficiency).

-include_lib("emqx_utils/include/emqx_message.hrl").

%% API:
-export([run/0, run/1]).

%%================================================================================
%% API functions
%%================================================================================

run() ->
    run(#{}).

run(Custom) ->
    RunConf = maps:merge(
        #{
            %% Sleep between batches:
            sleep => 1_000,
            %% Don't run test, only plot data:
            dry_run => false,
            %% Payload size multiplier:
            size => 10,
            %% Number of batches:
            batches => 100,
            %% Add generation every N batches:
            add_generation => 10
        },
        Custom
    ),
    lists:foreach(
        fun(DBConf) ->
            run(DBConf, RunConf)
        end,
        configs()
    ).

%% erlfmt-ignore
gnuplot_script(Filename) ->
    "set terminal qt\n"
    %% "set logscale y 10\n"
    "set title \"" ++ filename:basename(Filename, ".dat") ++ "\"\n"
    "set key autotitle columnheader\n"
    "plot for [n=2:*] \"" ++ Filename ++ "\" using 1:n with linespoints".

%%================================================================================
%% Internal functions
%%================================================================================

configs() ->
    [
        {'benchmark-skipstream-asn1',
            db_conf({emqx_ds_storage_skipstream_lts, #{serialization_schema => asn1}})},
        {'benchmark-skipstream-v1',
            db_conf({emqx_ds_storage_skipstream_lts, #{serialization_schema => v1}})},
        {'benchmark-bitfield', db_conf({emqx_ds_storage_bitfield_lts, #{}})}
    ].

db_conf(Storage) ->
    #{
        backend => builtin_local,
        %% n_sites => 1,
        n_shards => 1,
        %% replication_factor => 1,
        %% replication_options => #{},
        storage => Storage
    }.

-record(s, {
    data_size = 0,
    payload_size = 0,
    n_messages = 0,
    datapoints = #{},
    x_axis = []
}).

run({DB, Config}, RunConf) ->
    #{
        batches := NBatches,
        size := PSMultiplier,
        add_generation := AddGeneration,
        sleep := Sleep,
        dry_run := DryRun
    } = RunConf,
    {ok, _} = application:ensure_all_started(emqx_ds_backends),
    Dir = dir(DB),
    Filename = atom_to_list(DB) ++ ".dat",
    DryRun orelse
        begin
            io:format(user, "Running benchmark for ~p in ~p~n", [DB, Dir]),
            %% Ensure safe directory:
            {match, _} = re:run(Dir, filename:join("data", DB)),
            %% Ensure clean state:
            ok = emqx_ds:open_db(DB, Config),
            ok = emqx_ds:drop_db(DB),
            ok = file:del_dir_r(Dir),
            %% Open a fresh DB:
            ok = emqx_ds:open_db(DB, Config),
            S = lists:foldl(
                fun(Batch, Acc0) ->
                    Size = PSMultiplier * Batch,
                    io:format(user, "Storing batch with payload size ~p~n", [Size]),
                    Acc1 = store_batch(DB, Size, Acc0),
                    %% Sleep so all data is hopefully flushed:
                    timer:sleep(Sleep),
                    (Batch div AddGeneration) =:= 0 andalso
                        emqx_ds:add_generation(DB),
                    collect_datapoint(DB, Acc1)
                end,
                collect_datapoint(DB, #s{}),
                lists:seq(1, NBatches)
            ),
            {ok, FD} = file:open(Filename, [write]),
            io:put_chars(FD, print(S)),
            file:close(FD)
        end,
    os:cmd("echo '" ++ gnuplot_script(Filename) ++ "' | gnuplot --persist -"),
    ok.

collect_datapoint(
    DB, S0 = #s{n_messages = N, data_size = DS, payload_size = PS, datapoints = DP0, x_axis = X}
) ->
    NewData = [{"$_n", N}, {"$data", DS}, {"$payloads", PS} | dirsize(DB)],
    DP = lists:foldl(
        fun({Key, Val}, Acc) ->
            maps:update_with(
                Key,
                fun(M) -> M#{N => Val} end,
                #{},
                Acc
            )
        end,
        DP0,
        NewData
    ),
    S0#s{
        datapoints = DP,
        x_axis = [N | X]
    }.

print(#s{x_axis = XX, datapoints = DP}) ->
    Cols = lists:sort(maps:keys(DP)),
    Lines = [
        %% Print header:
        Cols
        %% Scan through rows:
        | [
            %% Scan throgh columns:
            [integer_to_binary(maps:get(X, maps:get(Col, DP), 0)) || Col <- Cols]
         || X <- lists:reverse(XX)
        ]
    ],
    lists:join(
        "\n",
        [lists:join(" ", Line) || Line <- Lines]
    ).

dirsize(DB) ->
    RawOutput = os:cmd("cd " ++ dir(DB) ++ "; du -b --max-depth 1 ."),
    [
        begin
            [Sz, Dir] = string:lexemes(L, "\t"),
            {Dir, list_to_integer(Sz)}
        end
     || L <- string:lexemes(RawOutput, "\n")
    ].

dir(DB) ->
    filename:join(emqx_ds_storage_layer:base_dir(), DB).

store_batch(DB, PayloadSize, S0 = #s{n_messages = N, data_size = DS, payload_size = PS}) ->
    From = rand:bytes(16),
    BatchSize = 50,
    Batch = [
        #message{
            id = emqx_guid:gen(),
            timestamp = emqx_message:timestamp_now(),
            payload = rand:bytes(PayloadSize),
            from = From,
            topic = emqx_topic:join([
                <<"blah">>,
                <<"blah">>,
                '',
                <<"blah">>,
                From,
                <<"bazzzzzzzzzzzzzzzzzzzzzzz">>,
                integer_to_binary(I)
            ])
        }
     || I <- lists:seq(1, BatchSize)
    ],
    ok = emqx_ds:store_batch(DB, Batch, #{sync => true}),
    S0#s{
        n_messages = N + length(Batch),
        data_size = DS + lists:sum(lists:map(fun msg_size/1, Batch)),
        payload_size = PS + length(Batch) * PayloadSize
    }.

%% We consider MQTT wire encoding to be "close to the ideal".
msg_size(Msg = #message{}) ->
    iolist_size(emqx_frame:serialize(emqx_message:to_packet(undefined, Msg))).
