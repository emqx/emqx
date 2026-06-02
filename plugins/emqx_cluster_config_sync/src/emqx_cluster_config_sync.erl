%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cluster_config_sync).

-behaviour(gen_server).

-include_lib("emqx/include/logger.hrl").

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
    enabled_secondary/1,
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
-define(DEFAULT_INTERVAL_MS, 300000).

-record(state, {
    config = #{} :: map(),
    timer = undefined :: undefined | reference(),
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
        shutdown => ?TIMEOUT
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

-spec enabled_secondary(map()) -> boolean().
enabled_secondary(Conf0) ->
    Conf = emqx_cluster_config_sync_client:normalize_config(Conf0),
    maps:get(<<"enable">>, Conf) =:= true andalso maps:get(<<"role">>, Conf) =:= <<"secondary">>.

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

handle_info(?SYNC, State0 = #state{config = Conf}) ->
    State1 = State0#state{timer = undefined},
    State2 =
        case should_sync(Conf) of
            true ->
                run_sync(State1);
            false ->
                State1#state{last_status = ok}
        end,
    {noreply, schedule_sync(State2)};
handle_info(Info, State) ->
    ?SLOG(error, #{
        msg => "cluster_config_sync_unexpected_info", info => Info
    }),
    {noreply, State}.

terminate(_Reason, State) ->
    _ = cancel_timer(State#state.timer),
    ok.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

apply_config(Conf0, State0) ->
    Conf = emqx_cluster_config_sync_client:normalize_config(Conf0),
    State1 = State0#state{config = Conf},
    State2 = cancel_sync(State1),
    schedule_sync(State2).

schedule_sync(State = #state{config = Conf}) ->
    case should_sync(Conf) of
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

should_sync(Conf) ->
    enabled_secondary(Conf) andalso can_run_on_this_node().

can_run_on_this_node() ->
    try mria_rlog:role() of
        core -> true;
        _ -> false
    catch
        _:_ -> true
    end.

run_sync(State = #state{config = Conf}) ->
    case emqx_cluster_config_sync_client:sync_once(Conf) of
        {ok, Result} ->
            ?SLOG(info, #{msg => "cluster_config_sync_finished", result => Result}),
            State#state{last_status = ok};
        {error, Reason} ->
            ?SLOG(error, #{msg => "cluster_config_sync_failed", reason => Reason}),
            State#state{last_status = {error, Reason}}
    end.

health_check(#state{config = Conf, last_status = LastStatus}) ->
    case enabled_secondary(Conf) of
        false ->
            ok;
        true ->
            case LastStatus of
                ok -> ok;
                {error, Reason} -> {error, format_reason(Reason)}
            end
    end.

format_reason(Reason) ->
    iolist_to_binary(io_lib:format("~0p", [Reason])).

current_config() ->
    emqx_plugins:get_config(name_vsn(), #{}).

name_vsn() ->
    {ok, Vsn} = application:get_key(emqx_cluster_config_sync, vsn),
    iolist_to_binary([<<"emqx_cluster_config_sync-">>, Vsn]).
