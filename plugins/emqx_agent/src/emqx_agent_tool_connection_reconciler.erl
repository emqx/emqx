%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_tool_connection_reconciler).

-behaviour(gen_server).

%% API
-export([start_link/0, retry/0]).

%% gen_server callbacks
-export([init/1, handle_cast/2, handle_info/2, handle_call/3]).

-define(RETRY_INTERVAL, 100).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec retry() -> ok.
retry() ->
    gen_server:cast(?MODULE, retry).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    {ok, #{timer_ref => undefined}}.

handle_cast(retry, #{timer_ref := undefined} = State) ->
    TRef = erlang:send_after(?RETRY_INTERVAL, self(), reconcile),
    {noreply, State#{timer_ref => TRef}};
handle_cast(retry, State) ->
    {noreply, State}.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown}, State}.

handle_info(reconcile, #{timer_ref := _TRef} = State) ->
    ok = emqx_agent_tool_connections:reconcile(),
    {noreply, State#{timer_ref => undefined}}.
