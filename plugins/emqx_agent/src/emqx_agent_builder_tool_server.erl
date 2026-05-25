%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_builder_tool_server).

-moduledoc """
This gen server just serializes metatool invocations (skill/pipeline creation, etc.).
The serialization is needed because `emqx_plugin` does not handle concurrent config updates well.

This serialization is not ideal, since it serializes only updates on a single node.

We just assume that no one edits the same pipline and related skills simultaneously.
This is the same level assumption as `emqx_plugin`'s assumption about config updates
through API or dashboard.

A complete solution should be done on `emqx_plugin` level.
""".

-behaviour(gen_server).

-export([start_link/0, call/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

%%--------------------------------------------------------------------
%% API
%%---------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

call(Fun) when is_function(Fun, 0) ->
    gen_server:call(?MODULE, {call, Fun}, infinity).

%%--------------------------------------------------------------------
%% Callback Functions
%%--------------------------------------------------------------------

init([]) ->
    {ok, #{}}.

handle_call({call, Fun}, _From, State) ->
    {reply, Fun(), State};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.
