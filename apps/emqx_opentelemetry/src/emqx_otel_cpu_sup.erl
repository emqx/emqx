%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_otel_cpu_sup).

-behaviour(gen_server).

-include_lib("emqx/include/logger.hrl").

%% gen_server APIs
-export([start_link/1]).

-export([
    start_otel_cpu_sup/1,
    stop_otel_cpu_sup/0,
    stats/1
]).

%% gen_server callbacks
-export([
    init/1,
    handle_continue/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(REFRESH, refresh).
-define(OTEL_CPU_USAGE_WORKER, ?MODULE).
-define(SUPERVISOR, emqx_otel_sup).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_otel_cpu_sup(Conf) ->
    Spec = emqx_otel_sup:worker_spec(?MODULE, Conf),
    assert_started(supervisor:start_child(?SUPERVISOR, Spec)).

stop_otel_cpu_sup() ->
    case erlang:whereis(?SUPERVISOR) of
        undefined ->
            ok;
        Pid ->
            case supervisor:terminate_child(Pid, ?MODULE) of
                ok -> supervisor:delete_child(Pid, ?MODULE);
                {error, not_found} -> ok;
                Error -> Error
            end
    end.

stats(Name) ->
    gen_server:call(?OTEL_CPU_USAGE_WORKER, {?FUNCTION_NAME, Name}, infinity).

%%--------------------------------------------------------------------
%% gen_server callbacks
%% simply handle cpu_sup:util/0,1 called in one process
%%--------------------------------------------------------------------

start_link(Conf) ->
    gen_server:start_link({local, ?OTEL_CPU_USAGE_WORKER}, ?MODULE, Conf, []).

init(Conf) ->
    {ok, _InitState = #{}, {continue, {setup, Conf}}}.

%% Interval in milliseconds
handle_continue({setup, #{metrics := #{enable := true, interval := Interval}}}, State) ->
    %% start os_mon temporarily
    {ok, _} = application:ensure_all_started(os_mon),
    %% The returned value of the first call to cpu_sup:util/0 or cpu_sup:util/1 by a
    %% process will on most systems be the CPU utilization since system boot,
    %% but this is not guaranteed and the value should therefore be regarded as garbage.
    %% This also applies to the first call after a restart of cpu_sup.
    _Val = cpu_sup:util(),
    TRef = start_refresh_timer(Interval),
    {noreply, State#{interval => Interval, refresh_time_ref => TRef}}.

handle_call({stats, Name}, _From, State) ->
    {reply, get_stats(Name, State), State};
handle_call(stop, _From, State) ->
    cancel_outdated_timer(State),
    {stop, normal, State};
handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", call => Req}),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast => Msg}),
    {noreply, State}.

handle_info({timeout, _Timer, ?REFRESH}, State) ->
    {noreply, refresh(State)}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

refresh(#{interval := Interval} = State) ->
    NState =
        case cpu_sup:util([]) of
            {all, Use, Idle, _} ->
                U = floor(Use * 100) / 100,
                I = ceil(Idle * 100) / 100,
                State#{'cpu.use' => U, 'cpu.idle' => I};
            _ ->
                State#{'cpu.use' => 0, 'cpu.idle' => 0}
        end,
    TRef = start_refresh_timer(Interval),
    NState#{refresh_time_ref => TRef}.

get_stats(Name, State) ->
    maps:get(Name, State, 0).

cancel_outdated_timer(#{refresh_time_ref := TRef}) ->
    emqx_utils:cancel_timer(TRef),
    ok.

start_refresh_timer(Interval) ->
    start_timer(Interval, ?REFRESH).

start_timer(Interval, Msg) ->
    emqx_utils:start_timer(Interval, Msg).

assert_started({ok, _Pid}) -> ok;
assert_started({ok, _Pid, _Info}) -> ok;
assert_started({error, {already_started, _Pid}}) -> ok;
assert_started({error, Reason}) -> {error, Reason}.
