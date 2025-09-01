%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_test_helpers).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx_utils/include/emqx_message.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("common_test/include/ct.hrl").
-include("emqx_ds.hrl").
-include_lib("stdlib/include/assert.hrl").

-spec on([node()] | node(), fun(() -> A)) -> A | [A].
on(Node, Fun) when is_atom(Node) ->
    [Ret] = on([Node], Fun),
    Ret;
on(Nodes, Fun) ->
    Results = erpc:multicall(Nodes, erlang, apply, [Fun, []]),
    lists:map(
        fun
            ({_Node, {ok, Result}}) ->
                Result;
            ({Node, Error}) ->
                ct:pal("Error on node ~p", [Node]),
                case Error of
                    {error, {exception, Reason, Stack}} ->
                        erlang:raise(error, Reason, Stack);
                    _ ->
                        error(Error)
                end
        end,
        lists:zip(Nodes, Results)
    ).

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

%% @doc Create an infinite list of messages from a given client:
interleaved_topic_messages(TestCase, NClients, NMsgs) ->
    %% List of fake client IDs:
    Clients = [integer_to_binary(I) || I <- lists:seq(1, NClients)],
    TopicStreams = [
        {ClientId, emqx_utils_stream:limit_length(NMsgs, topic_messages(TestCase, ClientId))}
     || ClientId <- Clients
    ],
    %% Interleaved stream of messages:
    Stream = emqx_utils_stream:interleave(
        [{2, Stream} || {_ClientId, Stream} <- TopicStreams], true
    ),
    {Stream, TopicStreams}.

topic_messages(TestCase, ClientId) ->
    topic_messages(TestCase, ClientId, 0).

topic_messages(TestCase, ClientId, N) ->
    fun() ->
        NBin = integer_to_binary(N),
        Msg = #message{
            id = <<N:128>>,
            from = ClientId,
            topic = client_topic(TestCase, ClientId),
            timestamp = N * 100,
            payload = <<NBin/binary, "                                                       ">>
        },
        [Msg | topic_messages(TestCase, ClientId, N + 1)]
    end.

client_topic(TestCase, ClientId) when is_atom(TestCase) ->
    client_topic(atom_to_binary(TestCase, utf8), ClientId);
client_topic(TestCase, ClientId) when is_binary(TestCase) ->
    <<TestCase/binary, "/", ClientId/binary>>.

%% Message comparison

%% Try to eliminate any ambiguity in the message representation.
message_canonical_form(Msg0 = #message{}) ->
    message_canonical_form(emqx_message:to_map(Msg0));
message_canonical_form(#{flags := Flags0, headers := _Headers0, payload := Payload0} = Msg) ->
    %% Remove flags that are false:
    Flags = maps:filter(
        fun(_Key, Val) -> Val end,
        Flags0
    ),
    %% Note: timestamp is assigned by DS, so it's impossible to
    %% compare it. ID may be dropped as well.
    maps:merge(
        maps:with([qos, from, headers, topic, extra], Msg),
        #{flags => Flags, payload => iolist_to_binary(Payload0)}
    ).

sublist(L) ->
    PrintMax = 20,
    case length(L) of
        0 ->
            [];
        N when N > PrintMax ->
            lists:sublist(L, 1, PrintMax) ++ ['...', N - PrintMax, 'more'];
        _ ->
            L
    end.

message_set(L) ->
    ordsets:from_list([message_canonical_form(I) || I <- L]).

message_set_subtract(A, B) ->
    ordsets:subtract(message_set(A), message_set(B)).

assert_same_set(Expected, Got) ->
    assert_same_set(Expected, Got, #{}).

assert_same_set(Expected, Got, Comment) ->
    SE = message_set(Expected),
    SG = message_set(Got),
    case {ordsets:subtract(SE, SG), ordsets:subtract(SG, SE)} of
        {[], []} ->
            ok;
        {Missing, Unexpected} ->
            error(Comment#{
                matching => sublist(ordsets:intersection(SE, SG)),
                missing => sublist(Missing),
                unexpected => sublist(Unexpected)
            })
    end.

message_eq(Fields, {_Key, Msg1 = #message{}}, Msg2) ->
    message_eq(Fields, Msg1, Msg2);
message_eq(Fields, Msg1, {_Key, Msg2 = #message{}}) ->
    message_eq(Fields, Msg1, Msg2);
message_eq(Fields, Msg1 = #message{}, Msg2 = #message{}) ->
    maps:with(Fields, message_canonical_form(Msg1)) =:=
        maps:with(Fields, message_canonical_form(Msg2)).

diff_messages(Expected, Got) ->
    Fields = [id, qos, from, flags, headers, topic, payload, extra],
    diff_messages(Fields, Expected, Got).

diff_messages(Fields, Expected, Got) ->
    snabbkaffe_diff:assert_lists_eq(Expected, Got, message_diff_options(Fields)).

message_diff_options(Fields) ->
    #{
        context => 20,
        window => 1000,
        compare_fun => fun(M1, M2) -> message_eq(Fields, M1, M2) end
    }.

%% @doc Group list of maps by value associated with a given key.
%% Groups appear in the order of the value of the grouping key, relative order
%% inside each group is preserved.
-spec group_maps_by(K, [#{K := V}]) -> #{K := [V]}.
group_maps_by(K, Maps) ->
    Values = lists:usort([maps:get(K, M) || M <- Maps]),
    [M || V <- Values, M <- Maps, map_get(K, M) =:= V].

%% Consume eagerly:

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
    Streams = emqx_ds_storage_layer:get_streams(ShardId, TopicFilter, StartTime, 0),
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
            emqx_ds_storage_layer:next(ShardId, It, BatchSize, emqx_ds:timestamp_us(), false)
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
            {ok, It, Batch ++ Msgs};
        {ok, Eos = end_of_stream} ->
            {ok, Eos, []};
        {error, Class, Reason} ->
            error({error, Class, Reason})
    end.

%% Sync wrapper over `dirty_append' API. If shard is not specified and
%% payload type is TTV, then data is added to the first shard. If
%% payload type is MQTT message, then shard is selected according to
%% the `from' field of the *first* message.
dirty_append(DB, MsgsOrTTVs) when is_atom(DB) ->
    dirty_append(#{db => DB}, MsgsOrTTVs);
dirty_append(Opts = #{db := DB}, MsgsOrTTVs) ->
    Retries = maps:get(retries, Opts, 0),
    case MsgsOrTTVs of
        [#message{from = ClientId} | _] ->
            Shard = emqx_ds:shard_of(DB, ClientId),
            TTVs = [emqx_ds_payload_transform:message_to_ttv(I) || I <- MsgsOrTTVs];
        [{_, _, _} | _] ->
            Shard = maps:get(shard, Opts, first_shard(DB)),
            TTVs = MsgsOrTTVs
    end,
    retry_dirty_append(Shard, Opts, TTVs, Retries).

retry_dirty_append(Shard, Opts, TTVs, Retries) ->
    Ref = emqx_ds:dirty_append(Opts#{reply => true, shard => Shard}, TTVs),
    ?assert(is_reference(Ref)),
    Result =
        receive
            ?ds_tx_commit_reply(Ref, Reply) ->
                case emqx_ds:dirty_append_outcome(Ref, Reply) of
                    {ok, Serial} when is_binary(Serial) ->
                        ok;
                    Other ->
                        Other
                end
        after 5_000 ->
            {error, recoverable, timeout}
        end,
    case Result of
        ok ->
            ok;
        _ when Retries > 0 ->
            retry_dirty_append(Shard, Opts, TTVs, Retries - 1);
        _ ->
            Result
    end.

first_shard(DB) ->
    hd(lists:sort(emqx_ds:list_shards(DB))).
