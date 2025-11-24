%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-export([start_link/0, child_spec/0, gc/0]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

%%--------------------------------------------------------------------
%% Messages
%%--------------------------------------------------------------------

-record(gc, {}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

child_spec() ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [?MODULE]
    }.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

gc() ->
    erlang:send(?MODULE, #gc{}).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    Interval = rand:uniform(gc_interval()),
    erlang:send_after(Interval, self(), #gc{}),
    {ok, #{}}.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(#gc{}, State) ->
    ok = start_gc(),
    erlang:send_after(gc_interval(), self(), #gc{}),
    {noreply, State}.

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

gc_interval() ->
    emqx_config:get([streams, gc_interval]).

is_responsible() ->
    case lists:sort(mria_membership:running_core_nodelist()) of
        [Node | _] when Node == node() ->
            true;
        _ ->
            false
    end.
