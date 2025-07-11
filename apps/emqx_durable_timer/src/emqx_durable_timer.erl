%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_durable_timer).
-moduledoc """
Low-level API for the durable timers.

Business-level applications should not use this.

## Timer types

Nodes ignore any timers with the types that aren't locally registered.

Callback modules that implement timer types should be backward- and forward-compatible.
If data stored in the timer key or value changes, a new timer type should be created.

## State machine

```
                         (failed heartbeat)                       ,-----.
                             |    |                              |       |
                             |    v                              v       |
 dormant -(register_type)-> isolated -(successful heartbeat)-> normal --<
                               ^                                         |
                               |                                         |
                               `--(failed multple heartbeats in a row)--'
```

## Down detection

Naturally, "dead hand" functionality requires a method for determining when a remote node goes down.
Currently we use a very simple method that relies entirely on heartbeats.
Updating the heartbeat and closing the epoch involve writing data into a DS DB with `builtin_raft` backend,
so we piggyback on the Raft consensus to protect against network splits.
More specifically, a network-isolated node cannot update its own heartbeat or mark others' epochs as closed.

We pay for this simplicity by slow detection time, so this is something that can be improved.

### Local view

Every node periodically writes its local monotonic time to the DS topic associated with the current epoch.

If the node doesn't succeed in updating heartbeat for its current epoch at least once over `missed_heartbeats / 2` milliseconds,
then it considers itself isolated.

When node enters isolated state, it changes its current epoch and continues to try to update the heartbeat for the new epoch.
When this succeeds, it enters normal state.

While the node is in isolated state, all local operations with the timers hang until the node enters normal state.

### Remote view

All nodes monitor heartbeats for every known epoch, except their own.
When the remote fails to update the epoch for `missed_heartbeats` milliseconds,
node tries to close the epoch using `update_epoch`.

If this operation succeeds, the remote is considered down.
If it fails it may be because the local node itself is isolated.

## Topic structure

### Heartbeats topic:

```
"h"/Node/Epoch
```

Sharding by epoch.
Time is always = 0.
Value: <<TimeOfLastHeartbeatMS:64, IsUp:8>>

IsUp: 1 or 0.

### Regular timers

```
"s"/Type/Epoch/Key
```

Sharding by key.
Time: wall time when the timer is due to fire.
Value = arbitrary binary that is passed to the callback.

### Dead hand timers

```
"d"/Type/Epoch/Key
```

Sharding by key.
Time: offset from the last heartbeat for the epoch when the timer is due to fire.
Value = arbitrary binary that is passed to the callback.

### Epochs

```
"e"/Epoch
```

Sharding: copy in every shard.
Time: shard monotonic
Value: empty.
""".

-behavior(gen_statem).

%% API:
-export([start_link/0, register_type/1, dead_hand/4, apply_after/4, cancel/2]).

-ifdef(TEST).
-export([drop_tables/0]).
-endif.

%% Behavior callbacks:
-export([callback_mode/0, init/1, terminate/3, handle_event/4]).

%% internal exports:
-export([
    lts_threshold_cb/2,
    ls_epochs/0,
    ls_epochs/1,
    now_ms/0,
    epoch/0,
    generation/1,
    cfg_heartbeat_interval/0,
    cfg_missed_heartbeats/0,
    cfg_replay_retry_interval/0,
    cfg_transaction_timeout/0
]).

-export_type([
    type/0,
    key/0,
    value/0,
    epoch/0,
    delay/0
]).

-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include("internals.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(APP, emqx_durable_timer).
-define(epoch_optvar, {?MODULE, epoch}).
-define(epoch_tab, ?MODULE).

-record(ei, {
    k :: epoch(),
    node :: node(),
    up :: boolean(),
    last_heartbeat :: integer(),
    missed_heartbeats = 0 :: integer()
}).

-record(s, {
    regs = #{} :: #{type() => module()},
    this_epoch :: epoch() | undefined,
    my_last_heartbeat :: non_neg_integer() | undefined,
    epochs :: ets:tid() | undefined
}).

-doc "Unique ID of the timer event".
-type type() :: 0..?max_type.

-doc "Additional key of the timer".
-type key() :: binary().

-doc "Payload of the timer".
-type value() :: binary().

-type delay() :: non_neg_integer().

-type epoch() :: binary().

-callback durable_timer_type() -> type().

-callback timer_introduced_in() -> string().

-callback timer_deprected_since() -> string().

-callback handle_durable_timeout(key(), value()) -> ok.

-optional_callbacks([timer_deprected_since/0]).

%% States:
%%
%%  Unless at least one timer type is registered, this application
%%  lies dormant:
-define(s_dormant, dormant).
%%  There's no current epoch:
-define(s_isolated(NEW_EPOCH_ID), {isolated, NEW_EPOCH_ID}).
%%  Normal operation:
-define(s_normal, normal).

%% Timeouts:
-define(heartbeat, heartbeat).

-define(epoch_start, <<"s">>).
-define(epoch_end, <<"e">>).

%% Calls/casts:
-record(call_register, {module :: module()}).

-define(retry_fold(N, SLEEP, BODY), retry_fold(N, SLEEP, fun() -> BODY end)).

%%================================================================================
%% API functions
%%================================================================================

-define(SERVER, ?MODULE).

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_statem:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec register_type(module()) -> ok | {error, _}.
register_type(Module) ->
    gen_statem:call(?SERVER, #call_register{module = Module}, infinity).

-doc """
Create a timer that activates when the Erlang node that created it
dies.
""".
-spec dead_hand(type(), key(), value(), delay()) -> ok | emqx_ds:error(_).
dead_hand(Type, Key, Value, Delay) when ?is_valid_timer(Type, Key, Value, Delay) ->
    ?tp(debug, ?tp_new_dead_hand, #{type => Type, key => Key, val => Value, delay => Delay}),
    Epoch = epoch(),
    emqx_durable_timer_worker:dead_hand(Type, Epoch, Key, Value, Delay).

-doc """
Create a timer that activates immediately.
""".
-spec apply_after(type(), key(), value(), delay()) -> ok | emqx_ds:error(_).
apply_after(Type, Key, Value, Delay) when ?is_valid_timer(Type, Key, Value, Delay) ->
    ?tp(debug, ?tp_new_apply_after, #{type => Type, key => Key, val => Value, delay => Delay}),
    NotEarlierThan = now_ms() + Delay,
    Epoch = epoch(),
    %% This function may crash. Add a retry mechanism?
    emqx_durable_timer_worker:apply_after(Type, Epoch, Key, Value, NotEarlierThan).

-spec cancel(type(), key()) -> ok | emqx_ds:error(_).
cancel(Type, Key) when Type >= 0, Type =< ?max_type, is_binary(Key) ->
    ?tp(debug, ?tp_delete, #{type => Type, key => Key}),
    Epoch = epoch(),
    emqx_durable_timer_worker:cancel(Type, Epoch, Key).

%%================================================================================
%% behavior callbacks
%%================================================================================

callback_mode() ->
    [handle_event_function, state_enter].

init(_) ->
    {ok, ?s_dormant, undefined}.

%% Dormant:
handle_event(enter, PrevState, ?s_dormant, _) ->
    ?tp(debug, ?tp_state_change, #{from => PrevState, to => ?s_dormant}),
    keep_state_and_data;
handle_event(EventType, Event, ?s_dormant, D) ->
    ?tp(debug, ?tp_app_activation, #{type => EventType, event => Event}),
    {next_state, ?s_isolated(new_epoch_id()), D, postpone};
%% Isolated:
handle_event(enter, PrevState, ?s_isolated(NextEpoch), D0) ->
    %% Need lazy initialization?
    D =
        case PrevState of
            ?s_dormant ->
                ?tp(debug, ?tp_state_change, #{from => PrevState, to => ?s_isolated(NextEpoch)}),
                real_init();
            ?s_normal ->
                ?tp(error, ?tp_state_change, #{from => PrevState, to => ?s_isolated(NextEpoch)}),
                %% Shut down the workers.
                emqx_durable_timer_sup:stop_worker_sup(),
                %% Erase epoch optvar:
                optvar:unset(?epoch_optvar),
                ets:delete(?epoch_tab),
                D0
        end,
    {keep_state, D, {state_timeout, 0, ?heartbeat}};
%% Normal:
handle_event(enter, PrevState, ?s_normal, S = #s{this_epoch = Epoch}) ->
    ?tp(debug, ?tp_state_change, #{from => PrevState, to => ?s_normal, epoch => Epoch}),
    %% Close previuos epochs for this node:
    _ = [update_epoch(node(), E, LH, false) || {E, Up, LH} <- ls_epochs(node()), E =/= Epoch, Up],
    %% Create and init epoch table:
    ets:new(?epoch_tab, [protected, named_table, set, {keypos, #ei.k}]),
    %% Read peer info and start replayers of closed epochs:
    lists:foreach(
        fun(E) -> start_closed({epoch, E}, S) end,
        update_epochs(?s_normal)
    ),
    %% Restart workers:
    ok = emqx_durable_timer_sup:start_worker_sup(),
    start_active(S),
    %% Set epoch:
    optvar:set(?epoch_optvar, Epoch),
    %% Read epochs:
    {keep_state_and_data, {state_timeout, cfg_heartbeat_interval(), ?heartbeat}};
%% Common:
handle_event({call, From}, #call_register{module = Module}, State, Data) ->
    handle_register(State, Module, Data, From);
handle_event(state_timeout, ?heartbeat, State, Data) ->
    handle_heartbeat(State, Data);
handle_event(state_enter, From, To, _Data) ->
    ?tp(debug, ?tp_state_change, #{from => From, to => To}),
    keep_state_and_data;
handle_event(ET, Event, State, Data) ->
    ?tp(error, ?tp_unknown_event, #{m => ?MODULE, ET => Event, state => State, data => Data}),
    keep_state_and_data.

terminate(_Reason, ?s_normal, #s{this_epoch = Epoch}) ->
    _ = update_epoch(node(), Epoch, now_ms(), false),
    ok;
terminate(_Reason, _State, _D) ->
    ok.

%%================================================================================
%% Internal exports
%%================================================================================

lts_threshold_cb(0, _) ->
    infinity;
lts_threshold_cb(N, Root) when Root =:= ?top_deadhand; Root =:= ?top_started ->
    %% [<<"d">>, Type, Epoch, Key]
    if
        N =:= 1 -> infinity;
        true -> 0
    end;
lts_threshold_cb(_, _) ->
    0.

ls_epochs() ->
    L = emqx_ds:fold_topic(
        fun(_, _, {[?top_heartbeat, NodeBin, Epoch], _, <<Time:64, IUp:8>>}, Acc) ->
            [{binary_to_atom(NodeBin), Epoch, IUp =:= 1, Time} | Acc]
        end,
        [],
        heartbeat_topic('+', '+'),
        #{db => ?DB_GLOB, errors => ignore}
    ),
    lists:keysort(4, L).

ls_epochs(Node) ->
    L = emqx_ds:fold_topic(
        fun(_, _, {[?top_heartbeat, _, Epoch], _, <<Time:64, IUp:8>>}, Acc) ->
            [{Epoch, IUp =:= 1, Time} | Acc]
        end,
        [],
        heartbeat_topic(atom_to_binary(Node), '+'),
        #{db => ?DB_GLOB, errors => ignore}
    ),
    lists:keysort(3, L).

-spec now_ms() -> integer().
now_ms() ->
    erlang:monotonic_time(millisecond) + erlang:time_offset(millisecond).

-spec epoch() -> epoch().
epoch() ->
    optvar:read(?epoch_optvar).

%%================================================================================
%% Internal functions
%%================================================================================

real_init() ->
    ?tp_span(
        debug,
        ?tp_init,
        #{},
        begin
            ok = ensure_tables(),
            #s{
                my_last_heartbeat = now_ms()
            }
        end
    ).

handle_register(State, Module, Data0 = #s{regs = Regs}, From) ->
    try
        Type = Module:durable_timer_type(),
        case Regs of
            #{Type := Module} ->
                Reply = ok,
                Data = Data0;
            #{Type := OldMod} ->
                Reply = {error, {already_registered, OldMod}},
                Data = Data0;
            #{} ->
                Data = Data0#s{regs = Regs#{Type => Module}},
                ?tp(?tp_register, #{type => Type, cbm => Module}),
                case State of
                    ?s_normal ->
                        start_active(Type, Data),
                        start_closed({type, Type}, Data);
                    ?s_isolated(_) ->
                        ok
                end,
                Reply = ok
        end,
        {keep_state, Data, {reply, From, Reply}}
    catch
        EC:Err:Stack ->
            {keep_state_and_data, {reply, From, {error, {EC, Err, Stack}}}}
    end.

handle_heartbeat(State, S0 = #s{this_epoch = LastEpoch, my_last_heartbeat = MLH}) ->
    %% Should we try to close the previous epoch?
    Epoch =
        case State of
            ?s_isolated(NewEpochId) ->
                %% Try to close the previous one:
                _ =
                    is_binary(LastEpoch) andalso
                        update_epoch(node(), LastEpoch, MLH, false),
                NewEpochId;
            _ ->
                LastEpoch
        end,
    LEO = now_ms(),
    Result = update_epoch(node(), Epoch, LEO, true),
    update_epochs(State),
    Timeout = {state_timeout, cfg_heartbeat_interval(), ?heartbeat},
    DeadlineMissed = now_ms() >= (MLH + cfg_missed_heartbeats() / 2),
    case Result of
        ok ->
            ?tp(debug, ?tp_heartbeat, #{epoch => Epoch, state => State}),
            S = S0#s{my_last_heartbeat = LEO, this_epoch = Epoch},
            {next_state, ?s_normal, S, Timeout};
        {error, EC, Err} when DeadlineMissed ->
            ?tp(error, ?tp_missed_heartbeat, #{epoch => Epoch, EC => Err, state => State}),
            {next_state, ?s_isolated(new_epoch_id()), S0, Timeout};
        {error, EC, Err} ->
            ?tp(info, ?tp_missed_heartbeat, #{epoch => Epoch, EC => Err, state => State}),
            {keep_state_and_data, Timeout}
    end.

ensure_tables() ->
    %% FIXME: config
    NShards = 1,
    NSites = 1,
    RFactor = 5,
    Storage =
        {emqx_ds_storage_skipstream_lts_v2, #{
            lts_threshold_spec => {mf, ?MODULE, lts_threshold_cb},
            timestamp_bytes => 8
        }},
    Transaction = #{
        flush_interval => 1000, idle_flush_interval => 1, conflict_window => 5000
    },
    ok = emqx_ds:open_db(
        ?DB_GLOB,
        #{
            backend => builtin_raft,
            store_ttv => true,
            transaction => Transaction,
            storage => Storage,
            replication => #{},
            n_shards => NShards,
            n_sites => NSites,
            replication_factor => RFactor,
            atomic_batches => true,
            append_only => false,
            reads => leader_preferred
        }
    ).

update_epochs(?s_normal) ->
    {MissedEpochs, _Errors} =
        emqx_ds:fold_topic(
            fun traverse_epochs/4,
            [],
            heartbeat_topic('+', '+'),
            #{db => ?DB_GLOB, errors => report}
        ),
    MissedEpochs;
update_epochs(?s_isolated(_)) ->
    [].

traverse_epochs(_Slab, _Stream, {[_, NodeBin, Epoch], _, Val}, Acc) ->
    Node = binary_to_atom(NodeBin),
    <<Time:64, IUp:8>> = Val,
    IsUp = IUp =:= 1,
    case ets:lookup(?epoch_tab, Epoch) of
        [EI = #ei{last_heartbeat = LH, missed_heartbeats = MH}] ->
            %% Known epoch:
            case Time of
                LH when IsUp ->
                    %% Epoch was up and now it's missed a heartbeat:
                    case cfg_missed_heartbeats() of
                        MaxMissed when MH >= MaxMissed ->
                            %% Node missed enough heartbeats. Shut it down:
                            update_epoch(Node, Epoch, Time, false),
                            ets:insert(?epoch_tab, EI#ei{up = false}),
                            [Epoch | Acc];
                        _ ->
                            ets:insert(?epoch_tab, EI#ei{missed_heartbeats = MH + 1}),
                            Acc
                    end;
                _ when IsUp ->
                    %% Heartbeat success:
                    ets:insert(?epoch_tab, EI#ei{last_heartbeat = Time, missed_heartbeats = 0}),
                    Acc;
                _ ->
                    %% Epoch went down:
                    ets:insert(?epoch_tab, EI#ei{up = false}),
                    Acc
            end;
        [] ->
            %% New epoch:
            ets:insert(?epoch_tab, #ei{k = Epoch, node = Node, up = IsUp, last_heartbeat = Time}),
            case IsUp of
                true ->
                    Acc;
                false ->
                    [Epoch | Acc]
            end
    end.

update_epoch(Node, Epoch, LastHeartbeat, IsUp) ->
    NodeBin = atom_to_binary(Node),
    Result = epoch_trans(
        Epoch,
        fun() ->
            IUp =
                case IsUp of
                    true ->
                        1;
                    false ->
                        ?ds_tx_on_success(?tp(debug, ?tp_close_epoch, #{epoch => Epoch})),
                        0
                end,
            emqx_ds:tx_write({
                heartbeat_topic(NodeBin, Epoch),
                0,
                <<LastHeartbeat:64, IUp:8>>
            })
        end
    ),
    case Result of
        {atomic, _, _} ->
            ok;
        Err ->
            Err
    end.

generation(_DB) ->
    1.

heartbeat_topic(NodeBin, NodeEpochId) ->
    [?top_heartbeat, NodeBin, NodeEpochId].

cfg_heartbeat_interval() ->
    application:get_env(?APP, heartbeat_interval, 5_000).

cfg_missed_heartbeats() ->
    application:get_env(?APP, missed_heartbeats, 5).

cfg_replay_retry_interval() ->
    application:get_env(?APP, replay_retry_interval, 500).

cfg_transaction_timeout() ->
    application:get_env(?APP, transaction_timeout, 1000).

-doc """
A wrapper for transactions that operate on a particular epoch.
""".
epoch_trans(Epoch, Fun) ->
    %% FIXME: config
    emqx_ds:trans(
        #{
            db => ?DB_GLOB,
            shard => {auto, Epoch},
            generation => generation(?DB_GLOB),
            retries => 10,
            retry_interval => 100
        },
        Fun
    ).

start_active(S = #s{regs = Regs}) ->
    maps:foreach(
        fun(Type, _) ->
            start_active(Type, S)
        end,
        Regs
    ).

start_active(Type, #s{regs = Regs, this_epoch = Epoch}) ->
    #{Type := CBM} = Regs,
    lists:foreach(
        fun(Shard) ->
            emqx_durable_timer_sup:start_worker(Type, Epoch, Shard, CBM, active)
        end,
        shards()
    ).

start_closed({epoch, Epoch}, S = #s{regs = Regs}) ->
    maps:foreach(
        fun(Type, _) ->
            start_closed(Type, Epoch, S)
        end,
        Regs
    );
start_closed({type, Type}, S) ->
    lists:foreach(
        fun(Epoch) ->
            start_closed(Type, Epoch, S)
        end,
        closed_epochs()
    ).

start_closed(Type, Epoch, #s{regs = Regs}) ->
    #{Type := CBM} = Regs,
    lists:foreach(
        fun(Shard) ->
            ok = emqx_durable_timer_sup:start_worker(Type, Epoch, Shard, CBM, {closed, started}),
            ok = emqx_durable_timer_sup:start_worker(Type, Epoch, Shard, CBM, {closed, dead_hand})
        end,
        shards()
    ).

-dialyzer({nowarn_function, closed_epochs/0}).
closed_epochs() ->
    MS = {#ei{k = '$1', up = false, _ = '_'}, [], ['$1']},
    ets:select(?epoch_tab, [MS]).

shards() ->
    Gen = generation(?DB_GLOB),
    maps:fold(
        fun({Shard, G}, _Info, Acc) ->
            case G of
                Gen -> [Shard | Acc];
                _ -> Acc
            end
        end,
        [],
        emqx_ds:list_generations_with_lifetimes(?DB_GLOB)
    ).

new_epoch_id() ->
    crypto:strong_rand_bytes(8).

-ifdef(TEST).
drop_tables() ->
    emqx_ds:close_db(?DB_GLOB),
    emqx_ds:drop_db(?DB_GLOB).
-endif.
