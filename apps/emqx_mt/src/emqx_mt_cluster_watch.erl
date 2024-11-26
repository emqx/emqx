%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mt_cluster_watch).

-behaviour(gen_server).

%% API
-export([
    start_link/0
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-include_lib("emqx/include/logger.hrl").

-define(SERVER, ?MODULE).

%% @doc Starts the watcher.
-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    process_flag(trap_exit, true),
    ok = ekka:monitor(membership),
    {ok, #{}}.

handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", server => ?MODULE, call => Req}),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", server => ?MODULE, cast => Msg}),
    {noreply, State}.

handle_info({nodedown, Node}, State) ->
    case mria_rlog:role() of
        core ->
            emqx_mt_state:clear_for_node(Node);
        replicant ->
            ok
    end,
    {noreply, State};
handle_info({membership, {mnesia, down, Node}}, State) ->
    handle_info({nodedown, Node}, State);
handle_info({membership, {node, down, Node}}, State) ->
    handle_info({nodedown, Node}, State);
handle_info({membership, _Event}, State) ->
    {noreply, State};
handle_info(Info, State) ->
    ?SLOG(error, #{msg => "unexpected_info", server => ?MODULE, info => Info}),
    {noreply, State}.

terminate(_Reason, _State) ->
    ekka:unmonitor(membership).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
