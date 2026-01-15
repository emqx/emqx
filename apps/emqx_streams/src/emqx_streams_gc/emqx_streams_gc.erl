%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_gc).

-moduledoc """
The module is responsible for scheduling garbage collection of Stream data.
""".

%% TODO
%% Move to a separate "cron" app, as well as the same workers for the MQ GC.

-behaviour(gen_server).

-include("../emqx_streams_internal.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/logger.hrl").

-export([
    start_link/0,
    child_spec/0,
    gc/0,
    reschedule/1
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    handle_continue/2
]).

-record(st, {
    timer :: undefined | reference()
}).

%%--------------------------------------------------------------------
%% Messages
%%--------------------------------------------------------------------

-record(gc, {}).
-record(reschedule, {interval_ms :: pos_integer()}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec child_spec() -> supervisor:child_spec().
child_spec() ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [?MODULE]
    }.

-spec start_link() -> gen_server:start_ret().
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec gc() -> ok.
gc() ->
    gen_server:cast(?MODULE, #gc{}).

-spec reschedule(pos_integer()) -> ok.
reschedule(IntervalMs) ->
    gen_server:cast(?MODULE, #reschedule{interval_ms = IntervalMs}).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    Interval = rand:uniform(emqx_streams_config:gc_interval()),
    TRef = erlang:start_timer(Interval, self(), #gc{}),
    {ok, #st{timer = TRef}}.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(#reschedule{interval_ms = IntervalMs}, State = #st{timer = TRefOld}) ->
    ok = emqx_utils:cancel_timer(TRefOld),
    TRef = erlang:start_timer(IntervalMs, self(), #gc{}),
    {noreply, State#st{timer = TRef}};
%% Manual GC start
handle_cast(#gc{}, State) ->
    {noreply, State, {continue, start_gc}};
handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({timeout, TRef, #gc{}}, State = #st{timer = TRef}) ->
    {noreply, State, {continue, start_gc}};
handle_info(Info, State) ->
    ?SLOG(warning, #{msg => "unexpected_info", info => Info}),
    {noreply, State}.

handle_continue(start_gc, #st{timer = TRefOld} = State) ->
    ok = start_gc(),
    ok = emqx_utils:cancel_timer(TRefOld),
    TRef = erlang:start_timer(emqx_streams_config:gc_interval(), self(), #gc{}),
    {noreply, State#st{timer = TRef}}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

start_gc() ->
    case is_responsible() of
        true ->
            ?tp_debug(streams_gc_starting, #{}),
            ok = emqx_streams_sup:start_gc();
        false ->
            ok
    end.

is_responsible() ->
    case lists:sort(mria_membership:running_core_nodelist()) of
        [Node | _] when Node == node() ->
            true;
        _ ->
            false
    end.
