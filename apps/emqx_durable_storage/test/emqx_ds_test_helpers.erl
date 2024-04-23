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
-module(emqx_ds_test_helpers).

-compile(export_all).
-compile(nowarn_export_all).

%% RPC mocking

mock_rpc() ->
    ok = meck:new(erpc, [passthrough, no_history, unstick]),
    ok = meck:new(gen_rpc, [passthrough, no_history]).

unmock_rpc() ->
    catch meck:unload(erpc),
    catch meck:unload(gen_rpc).

mock_rpc_result(ExpectFun) ->
    mock_rpc_result(erpc, ExpectFun),
    mock_rpc_result(gen_rpc, ExpectFun).

mock_rpc_result(erpc, ExpectFun) ->
    ok = meck:expect(erpc, call, fun(Node, Mod, Function, Args) ->
        case ExpectFun(Node, Mod, Function, Args) of
            passthrough ->
                meck:passthrough([Node, Mod, Function, Args]);
            unavailable ->
                meck:exception(error, {erpc, noconnection});
            {timeout, Timeout} ->
                ok = timer:sleep(Timeout),
                meck:exception(error, {erpc, timeout})
        end
    end);
mock_rpc_result(gen_rpc, ExpectFun) ->
    ok = meck:expect(gen_rpc, call, fun(Dest = {Node, _}, Mod, Function, Args) ->
        case ExpectFun(Node, Mod, Function, Args) of
            passthrough ->
                meck:passthrough([Dest, Mod, Function, Args]);
            unavailable ->
                {badtcp, econnrefused};
            {timeout, Timeout} ->
                ok = timer:sleep(Timeout),
                {badrpc, timeout}
        end
    end).

%% Consuming streams and iterators

consume(DB, TopicFilter) ->
    consume(DB, TopicFilter, 0).

consume(DB, TopicFilter, StartTime) ->
    lists:flatmap(
        fun({_Stream, Msgs}) ->
            Msgs
        end,
        consume_per_stream(DB, TopicFilter, StartTime)
    ).

consume_per_stream(DB, TopicFilter, StartTime) ->
    Streams = emqx_ds:get_streams(DB, TopicFilter, StartTime),
    lists:map(
        fun({_Rank, Stream}) -> {Stream, consume_stream(DB, Stream, TopicFilter, StartTime)} end,
        Streams
    ).

consume_stream(DB, Stream, TopicFilter, StartTime) ->
    {ok, It0} = emqx_ds:make_iterator(DB, Stream, TopicFilter, StartTime),
    {ok, _It, Msgs} = consume_iter(DB, It0),
    Msgs.

consume_iter(DB, It) ->
    consume_iter(DB, It, #{}).

consume_iter(DB, It0, Opts) ->
    consume_iter_with(
        fun(It, BatchSize) ->
            emqx_ds:next(DB, It, BatchSize)
        end,
        It0,
        Opts
    ).

storage_consume(ShardId, TopicFilter) ->
    storage_consume(ShardId, TopicFilter, 0).

storage_consume(ShardId, TopicFilter, StartTime) ->
    Streams = emqx_ds_storage_layer:get_streams(ShardId, TopicFilter, StartTime),
    lists:flatmap(
        fun({_Rank, Stream}) ->
            storage_consume_stream(ShardId, Stream, TopicFilter, StartTime)
        end,
        Streams
    ).

storage_consume_stream(ShardId, Stream, TopicFilter, StartTime) ->
    {ok, It0} = emqx_ds_storage_layer:make_iterator(ShardId, Stream, TopicFilter, StartTime),
    {ok, _It, Msgs} = storage_consume_iter(ShardId, It0),
    Msgs.

storage_consume_iter(ShardId, It) ->
    storage_consume_iter(ShardId, It, #{}).

storage_consume_iter(ShardId, It0, Opts) ->
    consume_iter_with(
        fun(It, BatchSize) ->
            emqx_ds_storage_layer:next(ShardId, It, BatchSize, emqx_ds:timestamp_us())
        end,
        It0,
        Opts
    ).

consume_iter_with(NextFun, It0, Opts) ->
    BatchSize = maps:get(batch_size, Opts, 5),
    case NextFun(It0, BatchSize) of
        {ok, It, _Msgs = []} ->
            {ok, It, []};
        {ok, It1, Batch} ->
            {ok, It, Msgs} = consume_iter_with(NextFun, It1, Opts),
            {ok, It, [Msg || {_DSKey, Msg} <- Batch] ++ Msgs};
        {ok, Eos = end_of_stream} ->
            {ok, Eos, []};
        {error, Class, Reason} ->
            error({error, Class, Reason})
    end.
