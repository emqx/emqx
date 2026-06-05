%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cluster_config_sync).

-behaviour(gen_server).

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% API
-export([
    start_link/0,
    child_spec/0,
    current_config/0
]).

%% Plugin callbacks
-export([
    on_config_changed/2,
    on_health_check/0
]).

%% Testable helpers
-export([
    enabled/1,
    interval_ms/1
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-define(SERVER, ?MODULE).
-define(TIMEOUT, 15000).
-define(SYNC, sync).
-define(CANCEL_TIMEOUT, cancel_timeout).
-define(SYNC_LOCK_RESOURCE, {?MODULE, sync}).
-define(DEFAULT_INTERVAL_MS, 300000).
-ifdef(TEST).
-define(CANCEL_TIMEOUT_MS, 100).
-else.
-define(CANCEL_TIMEOUT_MS, ?TIMEOUT).
-endif.

-record(worker, {
    pid :: pid(),
    mref :: reference(),
    cancel_ref :: reference(),
    status = running :: running | cancelling
}).

-record(state, {
    config = #{} :: map(),
    timer = undefined :: undefined | reference(),
    cancel_timer = undefined :: undefined | reference(),
    worker = undefined :: undefined | #worker{},
    last_status = ok :: ok | {error, term()}
}).

-record(on_config_changed, {
    old_conf :: map(),
    new_conf :: map()
}).

-record(on_health_check, {}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec child_spec() -> supervisor:child_spec().
child_spec() ->
    #{
        id => ?SERVER,
        start => {?MODULE, start_link, []},
        type => worker,
        modules => [?MODULE],
        restart => permanent,
        shutdown => infinity
    }.

%%------------------------------------------------------------------------------
%% EMQX Plugin callbacks
%%------------------------------------------------------------------------------

on_config_changed(OldConf, NewConf) ->
    try
        gen_server:call(
            ?SERVER, #on_config_changed{old_conf = OldConf, new_conf = NewConf}, ?TIMEOUT
        )
    catch
        exit:{noproc, _} ->
            ok
    end.

on_health_check() ->
    try
        gen_server:call(?SERVER, #on_health_check{}, ?TIMEOUT)
    catch
        exit:{noproc, _} ->
            {error, <<"Plugin is not running">>}
    end.

%%------------------------------------------------------------------------------
%% Testable helpers
%%------------------------------------------------------------------------------

-spec enabled(map()) -> boolean().
enabled(Conf0) ->
    emqx_cluster_config_sync_client:validate_config(Conf0) =:= ok.

-spec interval_ms(map()) -> pos_integer().
interval_ms(Conf0) ->
    Conf = emqx_cluster_config_sync_client:normalize_config(Conf0),
    Sync = maps:get(<<"sync">>, Conf),
    Duration = maps:get(<<"interval">>, Sync),
    case emqx_schema:to_duration_ms(Duration) of
        {ok, Ms} when is_integer(Ms), Ms > 0 -> Ms;
        _ -> ?DEFAULT_INTERVAL_MS
    end.

%%------------------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------------------

init([]) ->
    erlang:process_flag(trap_exit, true),
    Conf = current_config(),
    {ok, apply_config(Conf, #state{})}.

handle_call(#on_config_changed{new_conf = NewConf}, _From, State) ->
    NewState = apply_config(NewConf, State#state{last_status = ok}),
    {reply, ok, NewState};
handle_call(#on_health_check{}, _From, State) ->
    {reply, health_check(State), State};
handle_call(Request, From, State) ->
    ?SLOG(error, #{
        msg => "cluster_config_sync_unexpected_call", request => Request, from => From
    }),
    {reply, {error, unexpected_call}, State}.

handle_cast(Request, State) ->
    ?SLOG(error, #{
        msg => "cluster_config_sync_unexpected_cast", request => Request
    }),
    {noreply, State}.

handle_info(?SYNC, State0) ->
    State1 = State0#state{timer = undefined},
    {noreply, maybe_start_sync(State1)};
handle_info(
    {sync_result, Pid, Result},
    State0 = #state{worker = #worker{pid = Pid, mref = MRef, status = running}}
) ->
    erlang:demonitor(MRef, [flush]),
    State1 = handle_sync_result(Result, clear_worker(State0)),
    {noreply, schedule_sync(State1)};
handle_info(
    {sync_result, Pid, Result},
    State0 = #state{worker = #worker{pid = Pid, mref = MRef, status = cancelling}}
) ->
    erlang:demonitor(MRef, [flush]),
    State1 = handle_sync_result(Result, clear_worker(State0)),
    {noreply, schedule_sync(State1)};
handle_info({sync_result, _Pid, _Result}, State) ->
    {noreply, State};
handle_info(
    {'DOWN', MRef, process, Pid, Reason},
    State0 = #state{worker = #worker{pid = Pid, mref = MRef, status = running}}
) ->
    State1 = (clear_worker(State0))#state{last_status = {error, {worker_down, Reason}}},
    ?SLOG(error, #{msg => "cluster_config_sync_worker_down", reason => Reason}),
    {noreply, schedule_sync(State1)};
handle_info(
    {'DOWN', MRef, process, Pid, Reason},
    State0 = #state{worker = #worker{pid = Pid, mref = MRef, status = cancelling}}
) ->
    ?SLOG(info, #{msg => "cluster_config_sync_cancelled_worker_down", reason => Reason}),
    State1 = clear_worker(State0),
    {noreply, schedule_sync(State1#state{last_status = ok})};
handle_info({'DOWN', _MRef, process, _Pid, _Reason}, State) ->
    {noreply, State};
handle_info(
    {timeout, Timer, ?CANCEL_TIMEOUT},
    State0 = #state{
        cancel_timer = Timer,
        worker = #worker{pid = Pid, status = cancelling}
    }
) ->
    State1 = State0#state{
        cancel_timer = undefined,
        last_status = {error, {worker_cancel_timeout, Pid}}
    },
    ?SLOG(error, #{msg => "cluster_config_sync_worker_cancel_timeout", worker => Pid}),
    {noreply, State1};
handle_info({timeout, _Timer, ?CANCEL_TIMEOUT}, State) ->
    {noreply, State};
handle_info(Info, State) ->
    ?SLOG(error, #{
        msg => "cluster_config_sync_unexpected_info", info => Info
    }),
    {noreply, State}.

terminate(_Reason, State) ->
    _ = cancel_timer(State#state.timer),
    _ = cancel_timer(State#state.cancel_timer),
    _ = cancel_worker_and_wait(State#state.worker),
    ok.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

apply_config(Conf0, State0) ->
    Conf = emqx_cluster_config_sync_client:normalize_config(Conf0),
    State1 = State0#state{config = Conf},
    State2 = cancel_sync(State1),
    State3 = cancel_running_worker(State2),
    schedule_sync_if_idle(State3).

schedule_sync(State = #state{config = Conf}) ->
    case should_schedule(Conf) of
        true ->
            Interval = interval_ms(Conf),
            State#state{timer = erlang:send_after(Interval, self(), ?SYNC)};
        false ->
            State
    end.

cancel_sync(State = #state{timer = undefined}) ->
    State;
cancel_sync(State = #state{timer = Timer}) ->
    _ = cancel_timer(Timer),
    State#state{timer = undefined}.

cancel_timer(undefined) ->
    ok;
cancel_timer(Timer) ->
    _ = erlang:cancel_timer(Timer),
    ok.

cancel_worker(undefined) ->
    ok;
cancel_worker(#worker{pid = Pid, cancel_ref = CancelRef}) ->
    Pid ! {cancel_sync, CancelRef},
    ok.

cancel_worker_and_wait(undefined) ->
    ok;
cancel_worker_and_wait(Worker = #worker{pid = Pid, mref = MRef}) ->
    _ = cancel_worker(Worker),
    receive
        {'DOWN', MRef, process, Pid, _Reason} ->
            ok
    end.

cancel_running_worker(State = #state{worker = undefined}) ->
    State;
cancel_running_worker(
    State = #state{
        worker = #worker{pid = Pid, status = cancelling},
        cancel_timer = undefined
    }
) ->
    State#state{last_status = {error, {worker_cancel_timeout, Pid}}};
cancel_running_worker(State = #state{worker = #worker{status = cancelling}}) ->
    State;
cancel_running_worker(State = #state{worker = Worker}) ->
    _ = cancel_worker(Worker),
    schedule_cancel_timeout(State#state{worker = Worker#worker{status = cancelling}}).

schedule_cancel_timeout(State = #state{cancel_timer = undefined}) ->
    State#state{cancel_timer = erlang:start_timer(?CANCEL_TIMEOUT_MS, self(), ?CANCEL_TIMEOUT)};
schedule_cancel_timeout(State) ->
    State.

clear_worker(State) ->
    _ = cancel_timer(State#state.cancel_timer),
    State#state{worker = undefined, cancel_timer = undefined}.

schedule_sync_if_idle(State = #state{worker = undefined}) ->
    schedule_sync(State);
schedule_sync_if_idle(State = #state{worker = #worker{}}) ->
    State.

should_schedule(Conf) ->
    enabled(Conf) andalso is_core_node().

should_sync(Conf) ->
    enabled(Conf) andalso can_run_on_this_node().

can_run_on_this_node() ->
    is_core_node() andalso selected_core_node() =:= node().

is_core_node() ->
    try mria_rlog:role() of
        core -> true;
        _ -> false
    catch
        _:_ -> true
    end.

selected_core_node() ->
    selected_core_node(running_core_nodes()).

selected_core_node(CoreNodes) ->
    case lists:sort(CoreNodes) of
        [] -> undefined;
        [Node | _] -> Node
    end.

maybe_start_sync(State = #state{config = Conf, worker = undefined}) ->
    case should_sync(Conf) of
        true ->
            start_sync_worker(State);
        false ->
            schedule_sync(State#state{last_status = ok})
    end;
maybe_start_sync(State = #state{worker = #worker{}}) ->
    State.

start_sync_worker(State = #state{config = Conf}) ->
    Server = self(),
    CancelRef = make_ref(),
    {Pid, MRef} = erlang:spawn_monitor(fun() ->
        Result = safe_sync_once(Conf, CancelRef),
        Server ! {sync_result, self(), Result}
    end),
    State#state{worker = #worker{pid = Pid, mref = MRef, cancel_ref = CancelRef}}.

safe_sync_once(Conf, CancelRef) ->
    try sync_once_with_lock(Conf, CancelRef) of
        Result ->
            Result
    catch
        Class:Reason:Stacktrace ->
            {error, {Class, Reason, Stacktrace}}
    end.

sync_once_with_lock(Conf, CancelRef) ->
    CoreNodes = running_core_nodes(),
    case {worker_cancelled(CancelRef), selected_core_node(CoreNodes)} of
        {true, _} ->
            {error, cancelled};
        {false, undefined} ->
            {ok, skipped_no_core_nodes};
        {false, SelectedCore} when SelectedCore =/= node() ->
            {ok, skipped_not_selected_core};
        {false, _SelectedCore} ->
            case
                global:trans(
                    {?SYNC_LOCK_RESOURCE, self()},
                    fun() ->
                        emqx_cluster_config_sync_client:sync_once(Conf, #{
                            cancelled_fun => fun() -> worker_cancelled(CancelRef) end
                        })
                    end,
                    CoreNodes,
                    _Retries = 0
                )
            of
                aborted ->
                    {ok, skipped_by_lock};
                Result ->
                    Result
            end
    end.

running_core_nodes() ->
    try mria_membership:running_core_nodelist() of
        Nodes when is_list(Nodes) -> Nodes
    catch
        _:_ -> []
    end.

worker_cancelled(CancelRef) ->
    case erlang:get({sync_cancelled, CancelRef}) of
        true ->
            true;
        _ ->
            receive
                {cancel_sync, CancelRef} ->
                    erlang:put({sync_cancelled, CancelRef}, true),
                    true
            after 0 ->
                false
            end
    end.

handle_sync_result(SyncResult, State) ->
    Metadata = sync_metadata(State#state.config),
    case SyncResult of
        {ok, skipped_by_lock} ->
            log_sync_result(info, Metadata#{
                msg => "cluster_config_sync_skipped_by_lock",
                result => skipped_by_lock,
                stage => lock
            }),
            State#state{last_status = ok};
        {ok, skipped_no_core_nodes} ->
            log_sync_result(info, Metadata#{
                msg => "cluster_config_sync_skipped_no_core_nodes",
                result => skipped_no_core_nodes,
                stage => node_selection
            }),
            State#state{last_status = ok};
        {ok, skipped_not_selected_core} ->
            log_sync_result(info, Metadata#{
                msg => "cluster_config_sync_skipped_not_selected_core",
                result => skipped_not_selected_core,
                stage => node_selection
            }),
            State#state{last_status = ok};
        {ok, Result} ->
            log_sync_result(info, Metadata#{
                msg => "cluster_config_sync_finished",
                result => Result,
                stage => finished,
                filename => maps:get(filename, Result, undefined),
                cleanup => maps:get(cleanup, Result, undefined)
            }),
            State#state{last_status = ok};
        {error, Reason} ->
            log_sync_result(error, Metadata#{
                msg => "cluster_config_sync_failed",
                result => failed,
                stage => sync_stage(Reason),
                reason => Reason
            }),
            State#state{last_status = {error, Reason}}
    end.

sync_metadata(Conf0) ->
    Conf = emqx_cluster_config_sync_client:normalize_config(Conf0),
    Sync = maps:get(<<"sync">>, Conf),
    #{
        root_keys => maps:get(<<"root_keys">>, Sync),
        table_sets => maps:get(<<"table_sets">>, Sync),
        node => node(),
        selected_core_node => selected_core_node()
    }.

log_sync_result(Level, Fields) ->
    ?SLOG(Level, Fields),
    ?tp(cluster_config_sync_result, Fields).

sync_stage({http_error, post, _, _, _}) -> export;
sync_stage({http_error, post, _, _}) -> export;
sync_stage({bad_export_response, _}) -> export;
sync_stage(missing_filename) -> export;
sync_stage({http_error, get, _, _, _}) -> download;
sync_stage({http_error, get, _, _}) -> download;
sync_stage({upload_failed, _}) -> upload;
sync_stage({import_failed, _}) -> import;
sync_stage({cleanup_failed, _}) -> cleanup;
sync_stage(cancelled) -> cancel;
sync_stage({worker_cancel_timeout, _}) -> cancel;
sync_stage(_) -> unknown.

health_check(#state{config = Conf, last_status = LastStatus}) ->
    case emqx_cluster_config_sync_client:validate_config(Conf) of
        ok ->
            case LastStatus of
                ok -> ok;
                {error, Reason} -> {error, format_reason(Reason)}
            end;
        {error, Reason} ->
            {error, format_reason(Reason)}
    end.

format_reason(Reason) ->
    iolist_to_binary(io_lib:format("~0p", [Reason])).

current_config() ->
    emqx_plugins:get_config(name_vsn(), #{}).

name_vsn() ->
    {ok, Vsn} = application:get_key(emqx_cluster_config_sync, vsn),
    iolist_to_binary([<<"emqx_cluster_config_sync-">>, Vsn]).
