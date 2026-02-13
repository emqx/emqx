%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_builtin_raft_aux).
-moduledoc """
A set of functions that help Raft leader supervise optimistic
transaction leader process.
""".

%% API:
-export([init/1, handle_aux/5, state_enter/2]).
-export([set_cache/2, set_ts/2, set_otx_leader/2]).

%% behavior callbacks:
-export([]).

%% internal exports:
-export([]).

-export_type([]).

-include("emqx_ds_builtin_raft.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(stopped, stopped).
-record(starting, {starter :: pid()}).
-record(running, {pid :: pid()}).
-record(stopping, {pid :: pid(), stopper :: pid()}).

-type otx_state() :: ?stopped | #starting{} | #running{} | #stopping{}.

-record(cast_manage_otx, {delay = 0 :: non_neg_integer()}).
-record(cast_otx_started, {result}).

%% AUX server state:
-record(aux, {
    otx_leader = stopped :: otx_state(),
    postponed_event = undefined :: undefined | #cast_manage_otx{}
}).

-type aux() :: #aux{}.

-type otx_manage_event() ::
    #cast_manage_otx{}
    | #cast_otx_started{}
    | {down, pid(), _Reason}.

-define(is_otx_state_transitional(S), (is_record(S, starting) orelse is_record(S, stopping))).

%% Cooldown interval for restarts:
-define(restart_delay, 1_000).

%%================================================================================
%% API functions
%%================================================================================

init(_Name) ->
    #aux{}.

-spec handle_aux(ra_server:ra_state(), {call, ra:from()} | cast, _Command, aux(), IntState) ->
    {reply, _Reply, aux(), IntState}
    | {reply, _Reply, aux(), IntState, ra_machine:effects()}
    | {no_reply, aux(), IntState}
    | {no_reply, aux(), IntState, ra_machine:effects()}
when
    IntState :: ra_aux:internal_state().
handle_aux(ServerState, _, Event, Aux0 = #aux{otx_leader = Otx0}, IntState) ->
    #{db_shard := {DB, Shard}} = ra_aux:machine_state(IntState),
    case manage_otx(DB, Shard, ServerState, Event, Otx0) of
        {ok, Otx, Effects0} ->
            Aux = Aux0#aux{otx_leader = Otx},
            %% Fire queued event if out of transitional state:
            case Aux of
                #aux{} when ?is_otx_state_transitional(Otx) ->
                    {no_reply, Aux, IntState, Effects0};
                #aux{postponed_event = undefined} ->
                    {no_reply, Aux, IntState, Effects0};
                #aux{postponed_event = PostponedEvent} ->
                    Effects = [{aux, PostponedEvent} | Effects0],
                    {no_reply, Aux#aux{postponed_event = undefined}, IntState, Effects}
            end;
        {postpone, PostponeEvent} ->
            %% Keep the latest postponed event:
            Aux = Aux0#aux{postponed_event = PostponeEvent},
            {no_reply, Aux, IntState, []};
        ignore ->
            {no_reply, Aux0, IntState, []}
    end.

%% Called when the ra server changes state (e.g. leader -> follower).
-spec state_enter(ra_server:ra_state() | eol, RaState) -> ra_machine:effects() when
    RaState :: #{db_shard := {emqx_ds:db(), emqx_ds:shard()}, _ => _}.
state_enter(MemberState, State = #{db_shard := DBShard}) ->
    {DB, Shard} = DBShard,
    ?tp(
        notice,
        ds_ra_state_enter,
        State#{member_state => MemberState}
    ),
    emqx_ds_builtin_raft_metrics:rasrv_state_changed(DB, Shard, MemberState),
    set_cache(MemberState, State),
    [{aux, #cast_manage_otx{}}].

-doc """
Set PID of the optimistic transaction leader at the time of the last
Raft log entry applied locally. Since log replication may be delayed,
this pid may belong to a process long gone, and the pid can be even
reclaimed by other process if the node had restarted. Because of that,
DON'T SEND MESSAGES to this pid.

This pid is used ONLY to verify that the transaction context has been
created during the term of the current leader.
""".
set_otx_leader({DB, Shard}, Pid) ->
    ?tp(info, dsrepl_set_otx_leader, #{db => DB, shard => Shard, pid => Pid}),
    emqx_dsch:gvar_set(DB, Shard, ?gv_sc_replica, ?gv_otx_leader_pid, Pid).

set_ts({DB, Shard}, TS) ->
    emqx_dsch:gvar_set(DB, Shard, ?gv_sc_replica, ?gv_timestamp, TS).

%%================================================================================
%% Internal functions
%%================================================================================

set_cache(MemberState, State = #{db_shard := DBShard, latest := Latest}) when
    MemberState =:= leader; MemberState =:= follower
->
    set_ts(DBShard, Latest),
    case State of
        #{tx_serial := Serial} ->
            emqx_ds_storage_layer_ttv:set_read_tx_serial(DBShard, Serial);
        #{} ->
            ok
    end,
    case State of
        #{otx_leader_pid := Pid} ->
            set_otx_leader(DBShard, Pid);
        #{} ->
            ok
    end;
set_cache(_, _) ->
    ok.

-spec manage_otx(
    emqx_ds:db(),
    emqx_ds:shard(),
    ra_server:ra_state(),
    otx_manage_event(),
    otx_state()
) ->
    {ok, otx_state(), ra_machine:effects()} | {postpone, otx_manage_event()} | ignore.
manage_otx(DB, Shard, leader, #cast_manage_otx{delay = Delay}, ?stopped) ->
    %% Start OTX leader process:
    Server = emqx_ds_builtin_raft_shard:local_server(DB, Shard),
    AsyncStarter = spawn_link(
        fun() ->
            timer:sleep(Delay),
            Result = ?tp_span(
                debug,
                dsrepl_start_otx_leader,
                #{db => DB, shard => Shard},
                emqx_ds_builtin_raft_db_sup:start_otx_leader(DB, Shard)
            ),
            ok = ra:cast_aux_command(Server, #cast_otx_started{result = Result})
        end
    ),
    {ok, #starting{starter = AsyncStarter}, []};
manage_otx(_DB, _Shard, leader, #cast_manage_otx{}, S = #running{}) ->
    %% Unreachable, except for when entering `eol` "virtual" state, as `ra` calls it.
    {ok, S, []};
manage_otx(_DB, _Shard, _State, #cast_otx_started{result = {ok, Pid}}, #starting{}) ->
    %% Sucessfully started, monitor the server:
    Effects = [{monitor, process, aux, Pid}],
    {ok, #running{pid = Pid}, Effects};
manage_otx(DB, Shard, leader, #cast_otx_started{result = Err}, #starting{}) ->
    %% OTX server failed to start, but we're still the leader:
    ?tp(
        warning,
        dsrepl_optimistic_leader_start_fail,
        #{
            db => DB,
            shard => Shard,
            result => Err
        }
    ),
    %% Retry:
    manage_otx(DB, Shard, leader, #cast_manage_otx{delay = ?restart_delay}, ?stopped);
manage_otx(_DB, _Shard, _State, #cast_otx_started{}, #starting{}) ->
    %% Failed to start, but we are not the leader. Ignore.
    {ok, ?stopped, []};
manage_otx(DB, Shard, State, #cast_manage_otx{}, #running{pid = Pid}) when
    State =/= leader
->
    ?tp(dsrepl_shut_down_otx, #{db => DB, shard => Shard, state => State}),
    AsyncStopper =
        spawn_link(
            fun() ->
                %% Parent is notfied via monitor:
                emqx_ds_builtin_raft_db_sup:stop_otx_leader(DB, Shard)
            end
        ),
    {ok, #stopping{pid = Pid, stopper = AsyncStopper}, []};
manage_otx(_DB, _Shard, State, #cast_manage_otx{}, S = ?stopped) when
    State =/= leader
->
    {ok, S, []};
manage_otx(DB, Shard, _State, #cast_manage_otx{}, ?stopped) ->
    emqx_ds_builtin_raft:leader_shard_cleanup(DB, Shard),
    {ok, ?stopped, []};
manage_otx(DB, Shard, leader, {down, Pid, Reason}, #running{pid = Pid}) ->
    %% OTX server is down and we're still the leader. Restart it:
    LogLevel =
        case Reason of
            shutdown -> debug;
            _ -> warning
        end,
    ?tp(
        LogLevel,
        dsrepl_optimistic_leader_fail,
        #{
            db => DB,
            shard => Shard,
            reason => Reason
        }
    ),
    emqx_ds_builtin_raft:leader_shard_cleanup(DB, Shard),
    manage_otx(DB, Shard, leader, #cast_manage_otx{delay = ?restart_delay}, ?stopped);
manage_otx(DB, Shard, _State, {down, Pid, _Reason}, #stopping{pid = Pid}) ->
    emqx_ds_builtin_raft:leader_shard_cleanup(DB, Shard),
    {ok, ?stopped, []};
manage_otx(_DB, _Shard, _State, Event = #cast_manage_otx{}, Otx) when
    ?is_otx_state_transitional(Otx)
->
    %% Already in transitional state, postpone the request.
    %% The latest one will be processed once transition is complete.
    %% For example, if OTX is `#stopping{}` and by the time it stops
    %% there's a postponed event, OTX needs to be quickly restarted.
    {postpone, Event};
manage_otx(DB, Shard, State, Event, Otx) when
    is_record(Event, cast_manage_otx);
    is_record(Event, cast_otx_started)
->
    exit(
        {unexpected_otx_event, #{
            db => DB,
            shard => Shard,
            server => emqx_ds_builtin_raft_shard:local_server(DB, Shard),
            server_state => State,
            event => Event,
            optimistic_leader => Otx
        }}
    );
manage_otx(_DB, _Shard, _State, _Event, _Otx) ->
    ignore.
