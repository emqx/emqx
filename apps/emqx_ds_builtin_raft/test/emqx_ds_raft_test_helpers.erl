%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_raft_test_helpers).

-include_lib("emqx_utils/include/emqx_message.hrl").

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("stdlib/include/assert.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(ON(NODES, BODY),
    emqx_ds_test_helpers:on(NODES, fun() -> BODY end)
).

%%

-spec assert_db_open([node()], emqx_ds:db(), emqx_ds:create_db_opts()) -> _Ok.
assert_db_open(Nodes, DB, Opts) ->
    ?assertEqual(
        [{ok, ok} || _ <- Nodes],
        erpc:multicall(Nodes, emqx_ds, open_db, [DB, Opts])
    ),
    erpc:multicall(Nodes, emqx_ds, wait_db, [DB, all, infinity]),
    wait_db_bootstrapped(Nodes, DB).

wait_db_bootstrapped(Nodes, DB) ->
    wait_db_bootstrapped(Nodes, DB, infinity, infinity).

wait_db_bootstrapped(Nodes, DB, Timeout, BackInTime) ->
    SRefs = [
        snabbkaffe:subscribe(
            ?match_event(#{
                ?snk_kind := emqx_ds_replshard_bootstrapped,
                ?snk_meta := #{node := Node},
                db := DB,
                shard := Shard
            }),
            1,
            Timeout,
            BackInTime
        )
     || Node <- Nodes,
        Shard <- ?ON(Node, emqx_ds_builtin_raft_meta:my_shards(DB))
    ],
    lists:foreach(
        fun({ok, SRef}) ->
            ?assertMatch({ok, [_]}, snabbkaffe:receive_events(SRef))
        end,
        SRefs
    ).

-spec assert_db_stable([node()], emqx_ds:db()) -> _Ok.
assert_db_stable([Node | _], DB) ->
    Shards = ?ON(Node, emqx_ds_builtin_raft_meta:shards(DB)),
    ?assertMatch(
        _Leadership = [_ | _],
        ?ON(Node, db_leadership(DB, Shards))
    ).

db_leadership(DB, Shards) ->
    Leadership = [{S, shard_leadership(DB, S)} || S <- Shards],
    Inconsistent = [SL || SL = {_, Leaders} <- Leadership, map_size(Leaders) > 1],
    case Inconsistent of
        [] ->
            Leadership;
        [_ | _] ->
            {error, inconsistent, Inconsistent}
    end.

shard_leadership(DB, Shard) ->
    ReplicaSet = emqx_ds_builtin_raft_meta:replica_set(DB, Shard),
    Nodes = [emqx_ds_builtin_raft_meta:node(Site) || Site <- ReplicaSet],
    lists:foldl(
        fun({Site, SN}, Acc) -> Acc#{shard_leader(SN, DB, Shard, Site) => SN} end,
        #{},
        lists:zip(ReplicaSet, Nodes)
    ).

shard_leader(Node, DB, Shard, Site) ->
    shard_server_info(Node, DB, Shard, Site, leader).

shard_readiness(Node, DB, Shard, Site) ->
    shard_server_info(Node, DB, Shard, Site, readiness).

shard_server_info(Node, DB, Shard, Site, Info) ->
    ?ON(
        Node,
        emqx_ds_builtin_raft_shard:server_info(
            Info,
            emqx_ds_builtin_raft_shard:shard_server(DB, Shard, Site)
        )
    ).

-spec db_transitions(emqx_ds:db()) ->
    [{emqx_ds:shard(), emqx_ds_builtin_raft_meta:transition()}].
db_transitions(DB) ->
    Shards = emqx_ds_builtin_raft_meta:shards(DB),
    [
        {S, T}
     || S <- Shards, T <- emqx_ds_builtin_raft_meta:replica_set_transitions(DB, S)
    ].

wait_db_transitions_done(DB) ->
    ?retry(1000, 20, ?assertEqual([], db_transitions(DB))).

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
            ?ON(Node, emqx_ds_test_helpers:dirty_append(#{db => DB, retries => 10}, [Msg])),
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
                    emqx_ds_builtin_raft_meta:Operation(DB, Arg)
                )
            ),
            %% Give some time for at least one transition to complete.
            Transitions = ?ON(Node, db_transitions(DB)),
            ct:pal("Transitions after ~p: ~p", [Operation, Transitions]),
            case Transitions of
                [_ | _] ->
                    ?retry(200, 10, ?assertNotEqual(Transitions, ?ON(Node, db_transitions(DB))));
                [] ->
                    ok
            end,
            apply_stream(DB, NodeStream0, Stream, N);
        [Fun | Stream] when is_function(Fun) ->
            Fun(),
            apply_stream(DB, NodeStream0, Stream, N)
    end.

%% Consuming streams and iterators

%% Consume data from the DS storage on a given node as a stream:
-type ds_stream() :: emqx_utils_stream:stream({emqx_ds:message_key(), emqx_types:message()}).

%% Create a stream from the topic (wildcards are NOT supported for a
%% good reason: order of messages is implementation-dependent!).
%%
%% Note: stream produces messages with keys
-spec ds_topic_stream(atom(), binary(), binary(), node()) -> ds_stream().
ds_topic_stream(DB, ClientId, TopicBin, Node) ->
    Topic = emqx_topic:words(TopicBin),
    Shard = shard_of_clientid(DB, Node, ClientId),
    DSStreams =
        ?ON(
            Node,
            emqx_ds_builtin_raft:do_get_streams_v1(DB, Shard, Topic, 0, 0)
        ),
    ct:pal("Streams for ~p, ~p @ ~p:~n    ~p", [ClientId, TopicBin, Node, DSStreams]),
    %% Sort streams by their rank Y, and chain them together:
    emqx_utils_stream:chain([
        ds_topic_generation_stream(DB, Node, Shard, Topic, S)
     || S <- lists:sort(DSStreams)
    ]).

ds_topic_generation_stream(DB, Node, Shard, Topic, Stream) ->
    {ok, Iterator} = ?ON(
        Node,
        emqx_ds_builtin_raft:do_make_iterator_v1(DB, Shard, Stream, Topic, 0)
    ),
    do_ds_topic_generation_stream(DB, Node, Shard, Iterator).

do_ds_topic_generation_stream(DB, Node, Shard, It0) ->
    fun() ->
        case ?ON(Node, emqx_ds_builtin_raft:do_next_v1(DB, It0, 1)) of
            {ok, _It, []} ->
                [];
            {ok, end_of_stream} ->
                [];
            {ok, It, [KeyMsg]} ->
                [KeyMsg | do_ds_topic_generation_stream(DB, Node, Shard, It)]
        end
    end.

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
            Topic = emqx_ds_test_helpers:client_topic(TestCase, ClientId),
            emqx_ds_test_helpers:diff_messages(
                ExpectedStream,
                ds_topic_stream(DB, ClientId, Topic, Node)
            ),
            ct:pal("Data for client ~p on ~p is consistent.", [ClientId, Node])
        end
    ).

%% Find which nodes from the list contain the shards for the given
%% client ID:
nodes_of_clientid(DB, ClientId, Nodes = [N0 | _]) ->
    Shard = shard_of_clientid(DB, N0, ClientId),
    SiteNodes = ?ON(
        N0,
        begin
            Sites = emqx_ds_builtin_raft_meta:replica_set(DB, Shard),
            lists:map(fun emqx_ds_builtin_raft_meta:node/1, Sites)
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
        emqx_ds:shard_of(DB, ClientId)
    ).
