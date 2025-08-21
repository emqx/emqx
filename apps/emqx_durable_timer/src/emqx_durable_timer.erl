%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_durable_timer).
-moduledoc """
Low-level API for the durable timers.

Business-level applications should not use this module directly.
They should implement a callback module with `emqx_durable_timer` behavior instead.

## Timer types

Nodes ignore any timers with the types that aren't locally registered.

Callback modules that implement timer types should be backward- and forward-compatible.
If data stored in the timer key or value changes, a new timer type should be created.

## State machine

```
   (failed heartbeat)                       ,-----.
       |    |                              |       |
       |    v                              v       |
()--> isolated -(successful heartbeat)-> normal --<
         ^                                         |
         |                                         |
         `--(failed multiple heartbeats in a row)--'
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

If the node doesn't succeed in updating heartbeat for its current epoch at least once over
`missed_heartbeats / 2` milliseconds, then it considers itself isolated.

When node enters isolated state, it changes its current epoch and continues to try to update the heartbeat
for the new epoch.
When this succeeds, it enters normal state.

While the node is in isolated state, all local operations with the timers hang until the node enters normal state.

### Remote view

All nodes monitor heartbeats for every known epoch, except their own.
When the remote fails to update the epoch `missed_heartbeats` times in a row,
node tries to close the epoch using `update_epoch`.

If this operation succeeds, the remote is considered down.
If it fails it may be because the local node itself is isolated.
""".

-behaviour(gen_statem).

%% API:
-export([register_type/1, dead_hand/4, apply_after/4, cancel/2]).

-ifdef(TEST).
-export([drop_tables/0]).
-endif.

%% Behavior callbacks:
-export([callback_mode/0, init/1, terminate/3, handle_event/4]).

%% internal exports:
-export([
    start_link/0,
    now_ms/0,
    epoch/0,
    get_cbm/1,
    list_types/0,
    handle_durable_timeout/3,

    cfg_heartbeat_interval/0,
    cfg_missed_heartbeats/0,
    cfg_replay_retry_interval/0,
    cfg_transaction_timeout/0,
    cfg_batch_size/0
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

-record(ei, {
    node :: node(),
    up :: boolean(),
    last_heartbeat :: integer(),
    missed_heartbeats = 0 :: integer()
}).

-record(s, {
    this_epoch :: epoch() | undefined,
    my_missed_heartbeats = 0 :: non_neg_integer(),
    my_last_heartbeat = 0 :: non_neg_integer(),
    peer_info = #{} :: #{epoch() => #ei{}}
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
%%  There's no current epoch:
-define(s_isolated(NEW_EPOCH_ID), {isolated, NEW_EPOCH_ID}).
%%  Normal operation:
-define(s_normal, normal).

%% Timeouts:
-define(heartbeat, heartbeat).

%% Calls/casts:
-record(call_register, {module :: module()}).
-record(cast_check_peers, {}).

%%================================================================================
%% API functions
%%================================================================================

-define(SERVER, ?MODULE).

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_statem:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec register_type(module()) -> ok | {error, _}.
register_type(Module) ->
    ok = emqx_durable_timer_sup:ensure_running(),
    gen_statem:call(?SERVER, #call_register{module = Module}, infinity).

-doc """
Create a timer that activates when the Erlang node that created it
dies.
""".
-spec dead_hand(type(), key(), value(), delay()) -> ok | emqx_ds:error(_).
dead_hand(Type, Key, Value, Delay) when ?is_valid_timer(Type, Key, Value, Delay) ->
    true = is_registered(Type),
    Epoch = epoch(),
    ?tp(debug, ?tp_new_dead_hand, #{
        type => Type, key => Key, val => Value, delay => Delay, epoch => Epoch
    }),
    emqx_durable_timer_dl:insert_dead_hand(Type, Epoch, Key, Value, Delay).

-doc """
Create a timer that activates immediately.
""".
-spec apply_after(type(), key(), value(), delay()) -> ok | emqx_ds:error(_).
apply_after(Type, Key, Value, Delay) when ?is_valid_timer(Type, Key, Value, Delay) ->
    true = is_registered(Type),
    ?tp(debug, ?tp_new_apply_after, #{type => Type, key => Key, val => Value, delay => Delay}),
    NotEarlierThan = now_ms() + Delay,
    Epoch = epoch(),
    retry(
        fun() -> emqx_durable_timer_worker:apply_after(Type, Epoch, Key, Value, NotEarlierThan) end,
        0
    ).

-spec cancel(type(), key()) -> ok | emqx_ds:error(_).
cancel(Type, Key) when Type >= 0, Type =< ?max_type, is_binary(Key) ->
    true = is_registered(Type),
    ?tp(debug, ?tp_delete, #{type => Type, key => Key}),
    _Epoch = epoch(),
    emqx_durable_timer_dl:cancel(Type, Key).

%%================================================================================
%% behavior callbacks
%%================================================================================

callback_mode() ->
    [handle_event_function, state_enter].

init(_) ->
    process_flag(trap_exit, true),
    self() ! ?heartbeat,
    {ok, ?s_isolated(new_epoch_id()), #s{}}.

%% Isolated:
handle_event(enter, PrevState, ?s_isolated(NextEpoch), D) ->
    enter_isolated(PrevState, NextEpoch, D);
handle_event(cast, #cast_check_peers{}, ?s_isolated(_), _D) ->
    keep_state_and_data;
%% Normal:
handle_event(enter, PrevState, ?s_normal, D) ->
    enter_normal(PrevState, D);
handle_event(cast, #cast_check_peers{}, ?s_normal, D = #s{peer_info = PeerInfo0}) ->
    {ClosedEpochs, PeerInfo} = check_peers(PeerInfo0),
    start_closed(list_types(), ClosedEpochs),
    {keep_state, D#s{peer_info = PeerInfo}};
%% Common:
handle_event({call, From}, #call_register{module = Module}, State, Data) ->
    Result = do_register_type(State, Module, Data),
    {keep_state_and_data, {reply, From, Result}};
handle_event(_, ?heartbeat, State, Data) ->
    handle_heartbeat(State, Data);
handle_event(state_enter, From, To, _Data) ->
    ?tp(debug, ?tp_state_change, #{from => From, to => To}),
    keep_state_and_data;
handle_event(info, {'EXIT', _, Reason}, _, _) ->
    case Reason of
        normal -> keep_state_and_data;
        _ -> {stop, shutdown}
    end;
handle_event(ET, Event, State, Data) ->
    ?tp(error, ?tp_unknown_event, #{m => ?MODULE, ET => Event, state => State, d => Data}),
    keep_state_and_data.

terminate(Reason, ?s_normal, #s{this_epoch = Epoch}) ->
    ?tp(?tp_terminate, #{m => ?MODULE, reason => Reason, s => ?s_normal, epoch => Epoch}),
    optvar:unset(?epoch_optvar),
    emqx_durable_timer_sup:stop_all_workers(),
    _ = emqx_durable_timer_dl:update_epoch(node(), Epoch, now_ms(), false),
    ok;
terminate(_Reason, _State, _D) ->
    ?tp(?tp_terminate, #{m => ?MODULE, reason => _Reason, s => _State}),
    ok.

%%================================================================================
%% Internal exports
%%================================================================================

-spec now_ms() -> integer().
now_ms() ->
    erlang:monotonic_time(millisecond) + erlang:time_offset(millisecond).

-spec epoch() -> epoch().
epoch() ->
    optvar:read(?epoch_optvar).

get_cbm(Type) ->
    ets:lookup_element(?regs_tab, Type, 2).

%%================================================================================
%% Internal functions
%%================================================================================

enter_isolated(PrevState, NextEpoch, D0) ->
    ok = ensure_tables(),
    case PrevState of
        ?s_isolated(_) ->
            ok;
        ?s_normal ->
            ?tp(error, ?tp_state_change, #{from => PrevState, to => ?s_isolated(NextEpoch)}),
            optvar:unset(?epoch_optvar),
            emqx_durable_timer_sup:stop_all_workers()
    end,
    D = D0#s{peer_info = #{}},
    {keep_state, D}.

enter_normal(PrevState, S = #s{this_epoch = Epoch}) ->
    ?tp(debug, ?tp_state_change, #{from => PrevState, to => ?s_normal, epoch => Epoch}),
    %% Close all previuos epochs for this node:
    _ = [
        emqx_durable_timer_dl:update_epoch(node(), E, LH, false)
     || {E, Up, LH} <- emqx_durable_timer_dl:dirty_ls_epochs(node()),
        E =/= Epoch,
        Up
    ],
    start_active(list_types(), S),
    optvar:set(?epoch_optvar, Epoch),
    keep_state_and_data.

do_register_type(State, Module, Data) ->
    try
        Type = Module:durable_timer_type(),
        case ets:lookup(?regs_tab, Type) of
            [{_, Module}] ->
                %% Same thing:
                ok;
            [{_, OldMod}] ->
                {error, {already_registered, OldMod}};
            [] ->
                ?tp(?tp_register, #{type => Type, cbm => Module}),
                ets:insert(?regs_tab, {Type, Module}),
                case State of
                    ?s_normal ->
                        start_active([Type], Data),
                        start_closed([Type], closed_epochs(Data)),
                        ok;
                    ?s_isolated(_) ->
                        ok
                end
        end
    catch
        EC:Err:Stack ->
            {error, {EC, Err, Stack}}
    end.

handle_heartbeat(
    State, S0 = #s{this_epoch = LastEpoch, my_missed_heartbeats = MMH}
) ->
    ?tp(debug, ?tp_heartbeat, #{state => State}),
    erlang:send_after(cfg_heartbeat_interval(), self(), ?heartbeat),
    LEO = now_ms(),
    Epoch =
        case State of
            ?s_isolated(NewEpoch) -> NewEpoch;
            ?s_normal -> LastEpoch
        end,
    case emqx_durable_timer_dl:update_epoch(node(), Epoch, LEO, true) of
        ok ->
            S = S0#s{
                my_missed_heartbeats = 0,
                my_last_heartbeat = LEO,
                this_epoch = Epoch
            },
            {next_state, ?s_normal, S, {next_event, cast, #cast_check_peers{}}};
        {error, EC, Err} ->
            MaxMissed = cfg_missed_heartbeats(),
            S = S0#s{my_missed_heartbeats = MMH + 1},
            case State of
                ?s_normal when MMH > MaxMissed div 2 ->
                    ?tp(error, ?tp_missed_heartbeat, #{epoch => Epoch, EC => Err, state => State}),
                    {next_state, ?s_isolated(new_epoch_id()), S};
                _ ->
                    ?tp(debug, ?tp_missed_heartbeat, #{epoch => Epoch, EC => Err, state => State}),
                    {keep_state, S}
            end
    end.

ensure_tables() ->
    Storage =
        {emqx_ds_storage_skipstream_lts_v2, #{
            lts_threshold_spec => {mf, emqx_durable_timer_dl, lts_threshold_cb},
            timestamp_bytes => 8
        }},
    DBConf = emqx_ds_schema:db_config_timers(),
    ok = emqx_ds:open_db(
        ?DB_GLOB,
        DBConf#{
            store_ttv => true,
            storage => Storage,
            atomic_batches => true,
            append_only => false,
            reads => leader_preferred
        }
    ),
    emqx_ds:wait_db(?DB_GLOB, all, infinity).

check_peers(PeerInfo0) ->
    %% Note: we ignore errors since data accumulates in the ets. So
    %% the check will be retried on every heartbeat:
    emqx_ds:fold_topic(
        fun(_Slab, _Stream, Payload, {AccClosed, AccPeerInfo}) ->
            ?hb(NodeBin, Epoch, LastHeartbeat, IUp) = Payload,
            Node = binary_to_atom(NodeBin),
            IsUp = IUp =:= 1,
            case traverse_epochs(Node, Epoch, LastHeartbeat, IsUp, AccPeerInfo) of
                {true, EI} ->
                    {[Epoch | AccClosed], AccPeerInfo#{Epoch => EI}};
                {false, EI} ->
                    {AccClosed, AccPeerInfo#{Epoch => EI}}
            end
        end,
        {[], PeerInfo0},
        ?hb_topic('+', '+'),
        #{db => ?DB_GLOB, errors => ignore}
    ).

traverse_epochs(Node, Epoch, LastHeartbeat, IsUp, PeerInfo) ->
    MaxMissed = cfg_missed_heartbeats(),
    case PeerInfo of
        #{Epoch := EI} ->
            %% Known epoch:
            check_remote_heartbeat(MaxMissed, Epoch, EI, Node, LastHeartbeat, IsUp);
        #{} ->
            %% New epoch:
            EI = #ei{node = Node, up = IsUp, last_heartbeat = LastHeartbeat},
            case IsUp of
                false ->
                    {true, EI};
                true ->
                    {false, EI}
            end
    end.

check_remote_heartbeat(
    MaxMissed,
    Epoch,
    EI = #ei{last_heartbeat = LastHeartbeat, missed_heartbeats = MH, up = WasUp},
    Node,
    Heartbeat,
    IsUp
) ->
    case IsUp of
        false when WasUp ->
            %% Epoch went down:
            {true, EI#ei{up = false}};
        false ->
            %% We already know it's down:
            {false, EI};
        true when Heartbeat > LastHeartbeat ->
            %% Epoch is healthy:
            {false, EI#ei{last_heartbeat = Heartbeat, missed_heartbeats = 0}};
        true when MH >= MaxMissed ->
            %% Epoch was up, but missed enough heartbeats. Try to shut it down:
            ?tp(info, ?tp_remote_missed_heartbeat, #{
                node => Node, last_heartbeat => Heartbeat, epoch => Epoch, missed => MH
            }),
            case emqx_durable_timer_dl:update_epoch(Node, Epoch, Heartbeat, false) of
                ok ->
                    {true, EI#ei{up = false}};
                {error, _, _} ->
                    %% Shard is unreachable?
                    {false, EI}
            end;
        true ->
            %% Epoch missed a heartbeat:
            {false, EI#ei{missed_heartbeats = MH + 1}}
    end.

cfg_heartbeat_interval() ->
    application:get_env(?APP, heartbeat_interval, 5_000).

cfg_missed_heartbeats() ->
    application:get_env(?APP, missed_heartbeats, 5).

cfg_replay_retry_interval() ->
    application:get_env(?APP, replay_retry_interval, 500).

cfg_transaction_timeout() ->
    application:get_env(?APP, transaction_timeout, 1000).

cfg_batch_size() ->
    application:get_env(?APP, batch_size, 1000).

start_active(Types, #s{this_epoch = Epoch}) ->
    Shards = emqx_durable_timer_dl:shards(),
    [
        emqx_durable_timer_sup:start_worker(active, Type, Epoch, Shard)
     || Shard <- Shards,
        Type <- Types
    ],
    ok.

start_closed(Types, Epochs) ->
    Shards = emqx_durable_timer_dl:shards(),
    [
        begin
            ok = emqx_durable_timer_sup:start_worker({closed, started}, Type, Epoch, Shard),
            ok = emqx_durable_timer_sup:start_worker({closed, dead_hand}, Type, Epoch, Shard)
        end
     || Epoch <- Epochs,
        Shard <- Shards,
        Type <- Types
    ],
    ok.

closed_epochs(#s{peer_info = PI}) ->
    maps:fold(
        fun
            (Epoch, #ei{up = false}, Acc) ->
                [Epoch | Acc];
            (_, _, Acc) ->
                Acc
        end,
        [],
        PI
    ).

new_epoch_id() ->
    crypto:strong_rand_bytes(8).

list_types() ->
    MS = {{'$1', '_'}, [], ['$1']},
    ets:select(?regs_tab, [MS]).

handle_durable_timeout(CBM, Key, Value) ->
    CBM:handle_durable_timeout(Key, Value).

is_registered(Type) ->
    case ets:lookup(?regs_tab, Type) of
        [_] ->
            true;
        [] ->
            false
    end.

retry(Fun, N) ->
    case Fun() of
        ?err_rec(_) when N < 5 ->
            timer:sleep(cfg_replay_retry_interval()),
            retry(Fun, N + 1);
        Other ->
            Other
    end.

-ifdef(TEST).
drop_tables() ->
    emqx_ds:close_db(?DB_GLOB),
    emqx_ds:drop_db(?DB_GLOB).
-endif.
