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

%% AUX server state:
-record(aux, {
    otx_leader = stopped :: otx_state()
}).

-type aux() :: #aux{}.

-record(cast_start_otx, {delay = 0 :: non_neg_integer()}).
-record(cast_otx_started, {result}).
-record(cast_stop_otx, {}).

-type otx_manage_event() ::
    #cast_start_otx{}
    | #cast_otx_started{}
    | #cast_stop_otx{}
    | {down, pid(), _Reason}.

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
    Server = emqx_ds_builtin_raft_shard:local_server(DB, Shard),
    case manage_otx(DB, Shard, Server, ServerState, Event, Otx0) of
        {ok, Otx, Effects} ->
            Aux = #aux{otx_leader = Otx},
            {no_reply, Aux, IntState, Effects};
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
    case MemberState of
        leader ->
            [{aux, #cast_start_otx{}}];
        _ ->
            [{aux, #cast_stop_otx{}}]
    end.

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
    ra:server_id(),
    ra_server:ra_state(),
    otx_manage_event(),
    otx_state()
) ->
    {ok, otx_state(), ra_machine:effects()} | ignore.
manage_otx(DB, Shard, Server, leader, #cast_start_otx{delay = Delay}, ?stopped) ->
    %% Start OTX leader process:
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
manage_otx(_DB, _Shard, _Server, _State, #cast_start_otx{}, S) ->
    %% Either this replica is not the leader or OTX is already
    %% starting/stopping, keep state:
    {ok, S, []};
manage_otx(_DB, _Shard, Server, State, #cast_otx_started{result = {ok, Pid}}, #starting{}) ->
    %% Sucessfully started, monitor the server:
    Effects = [{monitor, process, aux, Pid}],
    %% Check if the server state changed in the meantime:
    case State of
        leader -> ok;
        _ -> ra:cast_aux_command(Server, #cast_stop_otx{})
    end,
    {ok, #running{pid = Pid}, Effects};
manage_otx(DB, Shard, Server, leader, #cast_otx_started{result = Err}, #starting{}) ->
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
    manage_otx(DB, Shard, Server, leader, #cast_start_otx{delay = ?restart_delay}, ?stopped);
manage_otx(_DB, _Shard, _Server, _State, #cast_otx_started{}, #starting{}) ->
    %% Failed to start, but we are not the leader. Ignore.
    {ok, ?stopped, []};
manage_otx(DB, Shard, _Server, State, #cast_stop_otx{}, #running{pid = Pid}) when
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
manage_otx(DB, Shard, _Server, _State, #cast_stop_otx{}, ?stopped) ->
    emqx_dsch:gvar_unset_all(DB, Shard, ?gv_sc_leader),
    {ok, ?stopped, []};
manage_otx(_DB, _Shard, _Server, _State, #cast_stop_otx{}, S) ->
    %% Already in transitional state, ignore the request. If server is
    %% starting, then `#cast_otx_started' clause will issue a command
    %% to tear it down.
    {ok, S, []};
manage_otx(DB, Shard, Server, leader, {down, Pid, Reason}, #running{pid = Pid}) ->
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
    manage_otx(DB, Shard, Server, leader, #cast_start_otx{delay = ?restart_delay}, ?stopped);
manage_otx(DB, Shard, _Server, _State, {down, Pid, _Reason}, #stopping{pid = Pid}) ->
    emqx_ds_builtin_raft:leader_shard_cleanup(DB, Shard),
    {ok, ?stopped, []};
manage_otx(DB, Shard, Server, State, Event, Otx) when
    is_record(Event, cast_start_otx);
    is_record(Event, cast_otx_started);
    is_record(Event, cast_stop_otx)
->
    exit(
        {unexpected_otx_event, #{
            db => DB,
            shard => Shard,
            server => Server,
            server_state => State,
            event => Event,
            optimistic_leader => Otx
        }}
    );
manage_otx(_DB, _Shard, _Server, _State, _Event, _Otx) ->
    ignore.
