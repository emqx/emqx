%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_gc_worker).

-moduledoc """
The module is responsible for garbage collection of Message Queue data.
""".

-behaviour(gen_server).

-include("../emqx_mq_internal.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([start_link/1, child_spec/1]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    handle_continue/2
]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link(GCType) ->
    gen_server:start_link(?MODULE, [GCType], []).

child_spec(GCType) ->
    #{
        id => GCType,
        start => {?MODULE, start_link, [GCType]},
        restart => transient,
        shutdown => 5000,
        type => worker,
        modules => [?MODULE]
    }.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([GCType]) ->
    {ok, #{gc_type => GCType}, {continue, start_gc}}.

handle_continue(start_gc, State) ->
    ok = gc(State),
    {stop, normal, State}.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

gc(_State) ->
    ok.
