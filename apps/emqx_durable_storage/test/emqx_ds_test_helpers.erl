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
-module(emqx_ds_test_helpers).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(ON(NODE, BODY),
    emqx_ds_test_helpers:on(NODE, fun() -> BODY end)
).

skip_if_norepl() ->
    try emqx_release:edition() of
        ee ->
            false;
        _ ->
            {skip, no_ds_replication}
    catch
        error:undef ->
            {skip, standalone_not_supported}
    end.

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

%% Consume data from the DS storage on a given node as a stream:
-type ds_stream() :: emqx_utils_stream:stream({emqx_ds:message_key(), emqx_types:message()}).

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

ds_topic_generation_stream(DB, Node, Shard, Topic, Stream) ->
    {ok, Iterator} = ?ON(
        Node,
        emqx_ds_storage_layer:make_iterator(Shard, Stream, Topic, 0)
    ),
    do_ds_topic_generation_stream(DB, Node, Shard, Iterator).

do_ds_topic_generation_stream(DB, Node, Shard, It0) ->
    fun() ->
        case
            ?ON(
                Node,
                begin
                    emqx_ds_storage_layer:next(Shard, It0, 1, _Now = 1 bsl 63)
                end
            )
        of
            {ok, _It, []} ->
                [];
            {ok, end_of_stream} ->
                [];
            {ok, It, [KeyMsg]} ->
                [KeyMsg | do_ds_topic_generation_stream(DB, Node, Shard, It)]
        end
    end.

%% Payload generation:

apply_stream(DB, Nodes, Stream) ->
    apply_stream(
        DB,
        emqx_utils_stream:repeat(emqx_utils_stream:list(Nodes)),
        Stream,
        0
    ).

apply_stream(DB, NodeStream0, Stream0, N) ->
    case emqx_utils_stream:next(Stream0) of
        [] ->
            ?tp(all_done, #{});
        [Msg = #message{} | Stream] ->
            [Node | NodeStream] = emqx_utils_stream:next(NodeStream0),
            ?tp(
                test_push_message,
                maps:merge(
                    emqx_message:to_map(Msg),
                    #{n => N}
                )
            ),
            ?ON(Node, emqx_ds:store_batch(DB, [Msg], #{sync => true})),
            apply_stream(DB, NodeStream, Stream, N + 1);
        [add_generation | Stream] ->
            ?tp(notice, test_add_generation, #{}),
            [Node | NodeStream] = emqx_utils_stream:next(NodeStream0),
            ?ON(Node, emqx_ds:add_generation(DB)),
            apply_stream(DB, NodeStream, Stream, N);
        [{Node, Operation, Arg} | Stream] when
            Operation =:= join_db_site;
            Operation =:= leave_db_site;
            Operation =:= assign_db_sites
        ->
            ?tp(notice, test_apply_operation, #{node => Node, operation => Operation, arg => Arg}),
            %% Apply the transition.
            ?assertMatch(
                {ok, _},
                ?ON(
                    Node,
                    emqx_ds_replication_layer_meta:Operation(DB, Arg)
                )
            ),
            %% Give some time for at least one transition to complete.
            Transitions = transitions(Node, DB),
            ct:pal("Transitions after ~p: ~p", [Operation, Transitions]),
            case Transitions of
                [_ | _] ->
                    ?retry(200, 10, ?assertNotEqual(Transitions, transitions(Node, DB)));
                [] ->
                    ok
            end,
            apply_stream(DB, NodeStream0, Stream, N);
        [Fun | Stream] when is_function(Fun) ->
            Fun(),
            apply_stream(DB, NodeStream0, Stream, N)
    end.

transitions(Node, DB) ->
    ?ON(
        Node,
        begin
            Shards = emqx_ds_replication_layer_meta:shards(DB),
            [
                {S, T}
             || S <- Shards, T <- emqx_ds_replication_layer_meta:replica_set_transitions(DB, S)
            ]
        end
    ).

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
    Msg#{flags := Flags, payload := iolist_to_binary(Payload0)}.

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

%% Consuming streams and iterators

-spec verify_stream_effects(atom(), binary(), [node()], [{emqx_types:clientid(), ds_stream()}]) ->
    ok.
verify_stream_effects(DB, TestCase, Nodes0, L) ->
    Checked = lists:flatmap(
        fun({ClientId, Stream}) ->
            Nodes = nodes_of_clientid(DB, ClientId, Nodes0),
            ct:pal("Nodes allocated for client ~p: ~p", [ClientId, Nodes]),
            ?defer_assert(
                ?assertMatch([_ | _], Nodes, ["No nodes have been allocated for ", ClientId])
            ),
            [verify_stream_effects(DB, TestCase, Node, ClientId, Stream) || Node <- Nodes]
        end,
        L
    ),
    ?defer_assert(?assertMatch([_ | _], Checked, "Some messages have been verified")).

-spec verify_stream_effects(atom(), binary(), node(), emqx_types:clientid(), ds_stream()) -> ok.
verify_stream_effects(DB, TestCase, Node, ClientId, ExpectedStream) ->
    ct:pal("Checking consistency of effects for ~p on ~p", [ClientId, Node]),
    ?defer_assert(
        begin
            diff_messages(
                ExpectedStream,
                ds_topic_stream(DB, ClientId, client_topic(TestCase, ClientId), Node)
            ),
            ct:pal("Data for client ~p on ~p is consistent.", [ClientId, Node])
        end
    ).

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

%% Create a stream from the topic (wildcards are NOT supported for a
%% good reason: order of messages is implementation-dependent!).
%%
%% Note: stream produces messages with keys
-spec ds_topic_stream(atom(), binary(), binary(), node()) -> ds_stream().
ds_topic_stream(DB, ClientId, TopicBin, Node) ->
    Topic = emqx_topic:words(TopicBin),
    Shard = shard_of_clientid(DB, Node, ClientId),
    {ShardId, DSStreams} =
        ?ON(
            Node,
            begin
                DBShard = {DB, Shard},
                {DBShard, emqx_ds_storage_layer:get_streams(DBShard, Topic, 0)}
            end
        ),
    ct:pal("Streams for ~p, ~p @ ~p:~n    ~p", [ClientId, TopicBin, Node, DSStreams]),
    %% Sort streams by their rank Y, and chain them together:
    emqx_utils_stream:chain([
        ds_topic_generation_stream(DB, Node, ShardId, Topic, S)
     || {_RankY, S} <- lists:sort(DSStreams)
    ]).

%% Find which nodes from the list contain the shards for the given
%% client ID:
nodes_of_clientid(DB, ClientId, Nodes = [N0 | _]) ->
    Shard = shard_of_clientid(DB, N0, ClientId),
    SiteNodes = ?ON(
        N0,
        begin
            Sites = emqx_ds_replication_layer_meta:replica_set(DB, Shard),
            lists:map(fun emqx_ds_replication_layer_meta:node/1, Sites)
        end
    ),
    lists:filter(
        fun(N) ->
            lists:member(N, SiteNodes)
        end,
        Nodes
    ).

shard_of_clientid(DB, Node, ClientId) ->
    ?ON(
        Node,
        emqx_ds_buffer:shard_of_operation(DB, #message{from = ClientId}, clientid)
    ).

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

collect_poll_replies(Alias, Timeout) ->
    receive
        #poll_reply{payload = poll_timeout, ref = Alias} ->
            [];
        #poll_reply{userdata = ItRef, payload = Reply, ref = Alias} ->
            [{ItRef, Reply} | collect_poll_replies(Alias, Timeout)]
    after Timeout ->
        []
    end.
