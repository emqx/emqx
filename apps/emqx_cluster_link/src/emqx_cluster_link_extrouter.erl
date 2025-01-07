%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cluster_link_extrouter).

-include_lib("snabbkaffe/include/trace.hrl").

-export([create_tables/0]).

%% Router API
-export([
    match_routes/1,
    lookup_routes/1,
    topics/0
]).

%% Actor API
-export([
    actor_init/4,
    actor_state/3,
    actor_apply_operation/2,
    actor_apply_operation/3,
    actor_gc/1,
    is_present_incarnation/1,
    list_actors/1
]).

%% Internal API
-export([
    mnesia_actor_init/4,
    mnesia_actor_heartbeat/3,
    mnesia_clean_incarnation/1,
    apply_actor_operation/5
]).

%% Internal export for bookkeeping
-export([count/1]).

%% Strictly monotonically increasing integer.
-type smint() :: integer().

%% Remote cluster name
-type cluster() :: binary().

%% Actor.
%% Identifies an independent route replication actor on the remote broker.
%% Usually something like `node()` or `{node(), _Shard}`.
-type actor() :: term().

%% Identifies incarnation of the actor.
%% In the event of actor restart, it's the actor's responsibility to keep track of
%% monotonicity of its incarnation number. Each time actor's incarnation increases,
%% we assume that all the state of the previous incarnations is lost.
-type incarnation() :: smint().

%% Operation.
%% RouteID should come in handy when two or more different routes on the actor side
%% are "intersected" to the same topic filter that needs to be replicated here.
-type op() :: {add | delete, {_TopicFilter :: binary(), _RouteID}} | heartbeat.

%% Basically a bit offset.
%% Each actor + incarnation pair occupies a separate lane in the multi-counter.
%% Example:
%% Actors | n1@ds n2@ds n3@ds
%%  Lanes | 0     1     2
%% ---------------------------
%%    Op1 | n3@ds add    client/42/# → MCounter += 1 bsl 2 = 4
%%    Op2 | n2@ds add    client/42/# → MCounter += 1 bsl 1 = 6
%%    Op3 | n3@ds delete client/42/# → MCounter -= 1 bsl 2 = 2
%%    Op4 | n2@ds delete client/42/# → MCounter -= 1 bsl 1 = 0 → route deleted
-type lane() :: non_neg_integer().

-include_lib("emqx/include/emqx.hrl").

-define(EXTROUTE_SHARD, ?MODULE).
-define(EXTROUTE_TAB, emqx_external_router_route).
-define(EXTROUTE_ACTOR_TAB, emqx_external_router_actor).

-define(ACTOR_ID(Cluster, Actor), {Cluster, Actor}).
-define(ROUTE_ID(Cluster, RouteID), {Cluster, RouteID}).

-record(extroute, {
    entry :: emqx_topic_index:key(_RouteID),
    mcounter = 0 :: non_neg_integer()
}).

-record(actor, {
    id :: {cluster(), actor()},
    incarnation :: incarnation(),
    lane :: lane(),
    until :: _Timestamp
}).

%%

create_tables() ->
    %% TODO: Table per link viable?
    mria_config:set_dirty_shard(?EXTROUTE_SHARD, true),
    ok = mria:create_table(?EXTROUTE_ACTOR_TAB, [
        {type, set},
        {rlog_shard, ?EXTROUTE_SHARD},
        {storage, ram_copies},
        {record_name, actor},
        {attributes, record_info(fields, actor)}
    ]),
    ok = mria:create_table(?EXTROUTE_TAB, [
        {type, ordered_set},
        {rlog_shard, ?EXTROUTE_SHARD},
        {storage, ram_copies},
        {record_name, extroute},
        {attributes, record_info(fields, extroute)},
        {storage_properties, [
            {ets, [
                {read_concurrency, true},
                {write_concurrency, true},
                {decentralized_counters, true}
            ]}
        ]}
    ]),
    [?EXTROUTE_ACTOR_TAB, ?EXTROUTE_TAB].

%%

-spec match_routes(emqx_types:topic()) -> [emqx_types:route()].
match_routes(Topic) ->
    Matches = emqx_topic_index:matches(Topic, ?EXTROUTE_TAB, [unique]),
    %% `unique` opt is not enough, since we keep the original Topic as a part of RouteID
    lists:ukeysort(#route.dest, [match_to_route(M) || M <- Matches]).

-spec lookup_routes(emqx_types:topic()) -> [emqx_types:route()].
lookup_routes(Topic) ->
    Pat = make_extroute_rec_pat(emqx_topic_index:make_key(Topic, '$1')),
    [match_to_route(R#extroute.entry) || Records <- ets:match(?EXTROUTE_TAB, Pat), R <- Records].

-spec topics() -> [emqx_types:topic()].
topics() ->
    Pat = make_extroute_rec_pat('$1'),
    [emqx_topic_index:get_topic(K) || [K] <- ets:match(?EXTROUTE_TAB, Pat)].

match_to_route(M) ->
    ?ROUTE_ID(Cluster, _) = emqx_topic_index:get_id(M),
    #route{topic = emqx_topic_index:get_topic(M), dest = Cluster}.

%% Make Dialyzer happy
make_extroute_rec_pat(Entry) ->
    erlang:make_tuple(
        record_info(size, extroute),
        '_',
        [{1, extroute}, {#extroute.entry, Entry}]
    ).

%% Internal exports for bookkeeping
count(ClusterName) ->
    TopicPat = '_',
    RouteIDPat = '_',
    Pat = make_extroute_rec_pat(
        emqx_trie_search:make_pat(TopicPat, ?ROUTE_ID(ClusterName, RouteIDPat))
    ),
    MS = [{Pat, [], [true]}],
    ets:select_count(?EXTROUTE_TAB, MS).

%%

-record(state, {
    cluster :: cluster(),
    actor :: actor(),
    incarnation :: incarnation(),
    lane :: lane() | undefined,
    extra = #{} :: map()
}).

-type state() :: #state{}.

-type env() :: #{timestamp => _Milliseconds}.

-spec actor_init(cluster(), actor(), incarnation(), env()) -> {ok, state()}.
actor_init(Cluster, Actor, Incarnation, Env = #{timestamp := Now}) ->
    %% TODO: Rolling upgrade safety?
    case transaction(fun ?MODULE:mnesia_actor_init/4, [Cluster, Actor, Incarnation, Now]) of
        {ok, State} ->
            {ok, State};
        {reincarnate, Rec} ->
            %% TODO: Do this asynchronously.
            ok = clean_incarnation(Rec),
            actor_init(Cluster, Actor, Incarnation, Env)
    end.

-spec is_present_incarnation(state()) -> boolean().
is_present_incarnation(#state{extra = #{is_present_incarnation := IsNew}}) ->
    IsNew;
is_present_incarnation(_State) ->
    false.

-spec list_actors(cluster()) -> [#{actor := actor(), incarnation := incarnation()}].
list_actors(Cluster) ->
    Pat = make_actor_rec_pat([{#actor.id, {Cluster, '$1'}}, {#actor.incarnation, '$2'}]),
    Matches = ets:match(emqx_external_router_actor, Pat),
    [#{actor => Actor, incarnation => Incr} || [Actor, Incr] <- Matches].

mnesia_actor_init(Cluster, Actor, Incarnation, TS) ->
    %% NOTE
    %% We perform this heavy-weight transaction only in the case of a new route
    %% replication connection. The implicit assumption is that each replication
    %% channel is uniquely identified by the ClientID (reflecting the Actor), and
    %% the broker will take care of ensuring that there's only one connection per
    %% ClientID. There's always a chance of having stray process severely lagging
    %% that applies some update out of the blue, but it seems impossible to prevent
    %% it completely w/o transactions.
    State = #state{cluster = Cluster, actor = Actor, incarnation = Incarnation},
    ActorID = ?ACTOR_ID(Cluster, Actor),
    case mnesia:read(?EXTROUTE_ACTOR_TAB, ActorID, write) of
        [#actor{incarnation = Incarnation, lane = Lane} = Rec] ->
            ok = mnesia:write(?EXTROUTE_ACTOR_TAB, Rec#actor{until = bump_actor_ttl(TS)}, write),
            {ok, State#state{lane = Lane, extra = #{is_present_incarnation => true}}};
        [] ->
            Lane = mnesia_assign_lane(Cluster),
            Rec = #actor{
                id = ActorID,
                incarnation = Incarnation,
                lane = Lane,
                until = bump_actor_ttl(TS)
            },
            ok = mnesia:write(?EXTROUTE_ACTOR_TAB, Rec, write),
            {ok, State#state{lane = Lane, extra = #{is_present_incarnation => false}}};
        [#actor{incarnation = Outdated} = Rec] when Incarnation > Outdated ->
            {reincarnate, Rec};
        [#actor{incarnation = Newer}] ->
            mnesia:abort({outdated_incarnation_actor, Actor, Incarnation, Newer})
    end.

-spec actor_state(cluster(), actor(), incarnation()) -> state().
actor_state(Cluster, Actor, Incarnation) ->
    ActorID = ?ACTOR_ID(Cluster, Actor),
    [#actor{lane = Lane}] = mnesia:dirty_read(?EXTROUTE_ACTOR_TAB, ActorID),
    #state{cluster = Cluster, actor = Actor, incarnation = Incarnation, lane = Lane}.

-spec actor_apply_operation(op(), state()) -> state().
actor_apply_operation(Op, State) ->
    actor_apply_operation(Op, State, #{}).

-spec actor_apply_operation(op(), state(), env()) -> state().
actor_apply_operation(
    {OpName, {TopicFilter, ID}},
    State = #state{cluster = Cluster, actor = Actor, incarnation = Incarnation, lane = Lane},
    _Env
) ->
    ActorID = ?ACTOR_ID(Cluster, Actor),
    Entry = emqx_topic_index:make_key(TopicFilter, ?ROUTE_ID(Cluster, ID)),
    case mria_config:whoami() of
        Role when Role /= replicant ->
            apply_actor_operation(ActorID, Incarnation, Entry, OpName, Lane);
        replicant ->
            mria:async_dirty(
                ?EXTROUTE_SHARD,
                fun ?MODULE:apply_actor_operation/5,
                [ActorID, Incarnation, Entry, OpName, Lane]
            )
    end,
    State;
actor_apply_operation(
    heartbeat,
    State = #state{cluster = Cluster, actor = Actor, incarnation = Incarnation},
    _Env = #{timestamp := Now}
) ->
    ActorID = ?ACTOR_ID(Cluster, Actor),
    ok = transaction(fun ?MODULE:mnesia_actor_heartbeat/3, [ActorID, Incarnation, Now]),
    State.

apply_actor_operation(ActorID, Incarnation, Entry, OpName, Lane) ->
    _ = assert_current_incarnation(ActorID, Incarnation),
    apply_operation(Entry, OpName, Lane).

apply_operation(Entry, OpName, Lane) ->
    %% NOTE
    %% This is safe sequence of operations only on core nodes. On replicants,
    %% `mria:dirty_update_counter/3` will be replicated asynchronously, which
    %% means this read can be stale.
    case mnesia:dirty_read(?EXTROUTE_TAB, Entry) of
        [#extroute{mcounter = MCounter}] ->
            apply_operation(Entry, MCounter, OpName, Lane);
        [] ->
            apply_operation(Entry, 0, OpName, Lane)
    end.

apply_operation(Entry, MCounter, OpName, Lane) ->
    %% NOTE
    %% We are relying on the fact that changes to each individual lane of this
    %% multi-counter are synchronized. Without this, such counter updates would
    %% be unsafe. Instead, we would have to use another, more complex approach,
    %% that runs `ets:lookup/2` + `ets:select_replace/2` in a loop until the
    %% counter is updated accordingly.
    Marker = 1 bsl Lane,
    case MCounter band Marker of
        0 when OpName =:= add ->
            Res = mria:dirty_update_counter(?EXTROUTE_TAB, Entry, Marker),
            ?tp("cluster_link_extrouter_route_added", #{}),
            Res;
        Marker when OpName =:= add ->
            %% Already added.
            MCounter;
        Marker when OpName =:= delete ->
            case mria:dirty_update_counter(?EXTROUTE_TAB, Entry, -Marker) of
                0 ->
                    Record = #extroute{entry = Entry, mcounter = 0},
                    ok = mria:dirty_delete_object(?EXTROUTE_TAB, Record),
                    ?tp("cluster_link_extrouter_route_deleted", #{}),
                    0;
                C ->
                    C
            end;
        0 when OpName =:= delete ->
            %% Already deleted.
            MCounter
    end.

-spec actor_gc(env()) -> _NumCleaned :: non_neg_integer().
actor_gc(#{timestamp := Now}) ->
    Pat = make_actor_rec_pat([{#actor.until, '$1'}]),
    MS = [{Pat, [{'<', '$1', Now}], ['$_']}],
    Dead = mnesia:dirty_select(?EXTROUTE_ACTOR_TAB, MS),
    try_clean_incarnation(Dead).

try_clean_incarnation([Rec | Rest]) ->
    %% NOTE: One at a time.
    case clean_incarnation(Rec) of
        ok ->
            1;
        stale ->
            try_clean_incarnation(Rest)
    end;
try_clean_incarnation([]) ->
    0.

mnesia_assign_lane(Cluster) ->
    Assignment = lists:foldl(
        fun(Lane, Acc) -> Acc bor (1 bsl Lane) end,
        0,
        select_cluster_lanes(Cluster)
    ),
    Lane = first_zero_bit(Assignment),
    Lane.

select_cluster_lanes(Cluster) ->
    Pat = make_actor_rec_pat([{#actor.id, {Cluster, '_'}}, {#actor.lane, '$1'}]),
    MS = [{Pat, [], ['$1']}],
    mnesia:select(?EXTROUTE_ACTOR_TAB, MS, write).

%% Make Dialyzer happy
make_actor_rec_pat(PosValues) ->
    erlang:make_tuple(
        record_info(size, actor),
        '_',
        [{1, actor} | PosValues]
    ).

mnesia_actor_heartbeat(ActorID, Incarnation, TS) ->
    case mnesia:read(?EXTROUTE_ACTOR_TAB, ActorID, write) of
        [#actor{incarnation = Incarnation} = Rec] ->
            ok = mnesia:write(?EXTROUTE_ACTOR_TAB, Rec#actor{until = bump_actor_ttl(TS)}, write);
        [#actor{incarnation = Outdated}] ->
            mnesia:abort({outdated_incarnation_actor, ActorID, Incarnation, Outdated});
        [] ->
            mnesia:abort({nonexistent_actor, ActorID})
    end.

clean_incarnation(Rec = #actor{id = {Cluster, Actor}}) ->
    case transaction(fun ?MODULE:mnesia_clean_incarnation/1, [Rec]) of
        ok ->
            ?tp(debug, clink_extrouter_actor_cleaned, #{
                cluster => Cluster,
                actor => Actor
            });
        Result ->
            Result
    end.

mnesia_clean_incarnation(#actor{id = Actor, incarnation = Incarnation, lane = Lane}) ->
    case mnesia:read(?EXTROUTE_ACTOR_TAB, Actor, write) of
        [#actor{incarnation = Incarnation}] ->
            _ = clean_lane(Lane),
            mnesia:delete(?EXTROUTE_ACTOR_TAB, Actor, write);
        _Renewed ->
            stale
    end.

clean_lane(Lane) ->
    ets:foldl(
        fun(#extroute{entry = Entry, mcounter = MCounter}, _) ->
            apply_operation(Entry, MCounter, delete, Lane)
        end,
        0,
        ?EXTROUTE_TAB
    ).

assert_current_incarnation(ActorID, Incarnation) ->
    %% NOTE
    %% Ugly, but should not really happen anyway. This is a safety net for the case
    %% when this process tries to apply some outdated operation for whatever reason
    %% (e.g. heavy CPU starvation). Still, w/o transactions, it's just a best-effort
    %% attempt.
    [#actor{incarnation = Incarnation}] = mnesia:dirty_read(?EXTROUTE_ACTOR_TAB, ActorID),
    ok.

%%

transaction(Fun, Args) ->
    case mria:transaction(?EXTROUTE_SHARD, Fun, Args) of
        {atomic, Result} ->
            Result;
        {aborted, Reason} ->
            error(Reason)
    end.

%%

first_zero_bit(N) ->
    first_zero_bit(N, 0).

first_zero_bit(N, I) ->
    case N band 1 of
        0 -> I;
        _ -> first_zero_bit(N bsr 1, I + 1)
    end.

%%

bump_actor_ttl(TS) ->
    TS + emqx_cluster_link_config:actor_ttl().
