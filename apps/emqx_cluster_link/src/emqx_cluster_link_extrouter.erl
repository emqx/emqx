%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cluster_link_extrouter).

-export([create_tables/0]).

%% Router API
-export([
    match_routes/1,
    lookup_routes/1,
    topics/0
]).

%% Actor API
-export([
    actor_init/3,
    actor_apply_operation/3,
    actor_gc/1
]).

%% Strictly monotonically increasing integer.
-type smint() :: integer().

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
-type op() :: {add | del, _TopicFilter :: binary(), _RouteID} | heartbeat.

%% Basically a bit offset.
%% Each actor + incarnation pair occupies a separate lane in the multi-counter.
%% Example:
%% Actors | n1@ds n2@ds n3@ds
%%  Lanes | 0     1     2
%%    Op1 | n3@ds add client/42/# → MCounter += 1 bsl 2 = 4
%%    Op2 | n2@ds add client/42/# → MCounter += 1 bsl 1 = 6
%%    Op3 | n3@ds del client/42/# → MCounter -= 1 bsl 2 = 2
%%    Op4 | n2@ds del client/42/# → MCounter -= 1 bsl 1 = 0 → route deleted
-type lane() :: non_neg_integer().

-define(DEFAULT_ACTOR_TTL_MS, 30_000).

-define(EXTROUTE_SHARD, ?MODULE).
-define(EXTROUTE_TAB, emqx_external_router_route).
-define(EXTROUTE_ACTOR_TAB, emqx_external_router_actor).

-record(extroute, {
    entry :: emqx_topic_index:key(_RouteID),
    mcounter = 0 :: non_neg_integer()
}).

-record(actor, {
    id :: actor(),
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
    [?EXTROUTE_TAB].

%%

match_routes(Topic) ->
    Matches = emqx_topic_index:matches(Topic, ?EXTROUTE_TAB, [unique]),
    [match_to_route(M) || M <- Matches].

lookup_routes(Topic) ->
    Pat = #extroute{entry = emqx_topic_index:make_key(Topic, '$1'), _ = '_'},
    [match_to_route(R#extroute.entry) || Records <- ets:match(?EXTROUTE_TAB, Pat), R <- Records].

topics() ->
    Pat = #extroute{entry = '$1', _ = '_'},
    [emqx_topic_index:get_topic(K) || [K] <- ets:match(?EXTROUTE_TAB, Pat)].

match_to_route(M) ->
    emqx_topic_index:get_topic(M).

%%

-record(state, {
    actor :: actor(),
    incarnation :: incarnation(),
    lane :: lane() | undefined
}).

-type state() :: #state{}.

-type env() :: #{timestamp := _Milliseconds}.

-spec actor_init(actor(), incarnation(), env()) -> {ok, state()}.
actor_init(Actor, Incarnation, Env = #{timestamp := Now}) ->
    %% FIXME: Sane transactions.
    case transaction(fun mnesia_actor_init/3, [Actor, Incarnation, Now]) of
        {ok, State} ->
            {ok, State};
        {reincarnate, Rec} ->
            %% TODO: Do this asynchronously.
            ok = clean_incarnation(Rec),
            actor_init(Actor, Incarnation, Env)
    end.

mnesia_actor_init(Actor, Incarnation, TS) ->
    %% NOTE
    %% We perform this heavy-weight transaction only in the case of a new route
    %% replication connection. The implicit assumption is that each replication
    %% channel is uniquely identified by the ClientID (reflecting the Actor), and
    %% the broker will take care of ensuring that there's only one connection per
    %% ClientID. There's always a chance of having stray process severely lagging
    %% that applies some update out of the blue, but it seems impossible to prevent
    %% it completely w/o transactions.
    State = #state{actor = Actor, incarnation = Incarnation},
    case mnesia:read(?EXTROUTE_ACTOR_TAB, Actor, write) of
        [#actor{incarnation = Incarnation, lane = Lane} = Rec] ->
            ok = mnesia:write(?EXTROUTE_ACTOR_TAB, Rec#actor{until = bump_actor_ttl(TS)}, write),
            {ok, State#state{lane = Lane}};
        [] ->
            Lane = mnesia_assign_lane(),
            Rec = #actor{
                id = Actor,
                incarnation = Incarnation,
                lane = Lane,
                until = bump_actor_ttl(TS)
            },
            ok = mnesia:write(?EXTROUTE_ACTOR_TAB, Rec, write),
            {ok, State#state{lane = Lane}};
        [#actor{incarnation = Outdated} = Rec] when Incarnation > Outdated ->
            {reincarnate, Rec};
        [#actor{incarnation = Newer}] ->
            mnesia:abort({outdated_incarnation_actor, Actor, Incarnation, Newer})
    end.

-spec actor_apply_operation(op(), state(), env()) -> state().
actor_apply_operation(
    {OpName, TopicFilter, ID},
    State = #state{actor = Actor, incarnation = Incarnation, lane = Lane},
    _Env
) ->
    _ = assert_current_incarnation(Actor, Incarnation),
    _ = apply_operation(emqx_topic_index:make_key(TopicFilter, ID), OpName, Lane),
    State;
actor_apply_operation(
    heartbeat,
    State = #state{actor = Actor, incarnation = Incarnation},
    _Env = #{timestamp := Now}
) ->
    ok = transaction(fun mnesia_actor_heartbeat/3, [Actor, Incarnation, Now]),
    State.

apply_operation(Entry, OpName, Lane) ->
    %% NOTE
    %% This is safe sequence of operations only on core nodes. On replicants,
    %% `mria:dirty_update_counter/3` will be replicated asynchronously, which
    %% means this read can be stale.
    % MCounter = ets:lookup_element(Tab, Entry, 2, 0),
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
            mria:dirty_update_counter(?EXTROUTE_TAB, Entry, Marker);
        Marker when OpName =:= add ->
            %% Already added.
            MCounter;
        Marker when OpName =:= del ->
            case mria:dirty_update_counter(?EXTROUTE_TAB, Entry, -Marker) of
                0 ->
                    Record = #extroute{entry = Entry, mcounter = 0},
                    ok = mria:dirty_delete_object(?EXTROUTE_TAB, Record),
                    0;
                C ->
                    C
            end;
        0 when OpName =:= del ->
            %% Already deleted.
            MCounter
    end.

-spec actor_gc(env()) -> ok.
actor_gc(#{timestamp := Now}) ->
    MS = [{#actor{until = '$1', _ = '_'}, [{'<', '$1', Now}], ['$_']}],
    case mnesia:dirty_select(?EXTROUTE_ACTOR_TAB, MS) of
        [Rec | _Rest] ->
            %% NOTE: One at a time.
            clean_incarnation(Rec);
        [] ->
            ok
    end.

mnesia_assign_lane() ->
    Assignment = mnesia:foldl(
        fun(#actor{lane = Lane}, Acc) ->
            Acc bor (1 bsl Lane)
        end,
        0,
        ?EXTROUTE_ACTOR_TAB,
        write
    ),
    Lane = first_zero_bit(Assignment),
    Lane.

mnesia_actor_heartbeat(Actor, Incarnation, TS) ->
    case mnesia:read(?EXTROUTE_ACTOR_TAB, Actor, write) of
        [#actor{incarnation = Incarnation} = Rec] ->
            ok = mnesia:write(?EXTROUTE_ACTOR_TAB, Rec#actor{until = bump_actor_ttl(TS)}, write);
        [#actor{incarnation = Outdated}] ->
            mnesia:abort({outdated_incarnation_actor, Actor, Incarnation, Outdated});
        [] ->
            mnesia:abort({nonexistent_actor, Actor})
    end.

clean_incarnation(Rec) ->
    transaction(fun mnesia_clean_incarnation/1, [Rec]).

mnesia_clean_incarnation(#actor{id = Actor, incarnation = Incarnation, lane = Lane}) ->
    case mnesia:read(?EXTROUTE_ACTOR_TAB, Actor, write) of
        [#actor{incarnation = Incarnation}] ->
            _ = clean_lane(Lane),
            mnesia:delete(?EXTROUTE_ACTOR_TAB, Actor, write);
        _Renewed ->
            ok
    end.

clean_lane(Lane) ->
    ets:foldl(
        fun(#extroute{entry = Entry, mcounter = MCounter}, _) ->
            apply_operation(Entry, MCounter, del, Lane)
        end,
        0,
        ?EXTROUTE_TAB
    ).

assert_current_incarnation(Actor, Incarnation) ->
    %% NOTE
    %% Ugly, but should not really happen anyway. This is a safety net for the case
    %% when this process tries to apply some outdated operation for whatever reason
    %% (e.g. heavy CPU starvation). Still, w/o transactions, it's just a best-effort
    %% attempt.
    [#actor{incarnation = Incarnation}] = mnesia:dirty_read(?EXTROUTE_ACTOR_TAB, Actor),
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
    TS + get_actor_ttl().

get_actor_ttl() ->
    ?DEFAULT_ACTOR_TTL_MS.
