%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mt_cluster_watch).

-behaviour(gen_server).

%% API
-export([
    start_link/0,
    immediate_node_clear/1
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

-include("emqx_mt.hrl").

-define(SERVER, ?MODULE).
%% Delay in seconds before clearing the records of a node which is down.
%% If the node is restarting, it expected to clear for itself.
%% If the node is down for more than this duration,
%% one of the core nodes should delete the records on behalf of the node
-ifdef(TEST).
-define(CLEAR_NODE_DELAY_SECONDS, 0).
-else.
-define(CLEAR_NODE_DELAY_SECONDS, 600).
-endif.

%% @doc Starts the watcher.
-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Clear the records of a node immediately.
-spec immediate_node_clear(node()) -> async.
immediate_node_clear(Node) ->
    _ = erlang:send(?SERVER, {clear_for_node, Node}),
    async.

init([]) ->
    process_flag(trap_exit, true),
    ok = ekka:monitor(membership),
    %% clear the records of self-node after restart
    %% This must be done before init/1 returns
    %% TODO: spawn a process to do it and listeners should wait
    %% for the process to finish before accepting connections
    emqx_mt_state:clear_self_node(),
    {ok, #{}}.

handle_call(Req, _From, State) ->
    ?LOG(error, #{msg => "unexpected_call", server => ?MODULE, call => Req}),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?LOG(error, #{msg => "unexpected_cast", server => ?MODULE, cast => Msg}),
    {noreply, State}.

handle_info({clear_for_node, Node}, State) ->
    case lists:member(Node, emqx:running_nodes()) of
        true ->
            ?LOG(info, #{msg => "skip_clear_multi_tenancy_records_for_node", node => Node});
        false ->
            ?LOG(warning, #{msg => "clear_multi_tenancy_records_for_down_node_begin", node => Node}),
            T1 = erlang:system_time(millisecond),
            Res = emqx_mt_state:clear_for_node(Node),
            T2 = erlang:system_time(millisecond),
            ?LOG(warning, #{
                msg => "clear_multi_tenancy_records_for_down_node_done",
                node => Node,
                elapsed => T2 - T1,
                result => Res
            })
    end,
    {noreply, State};
handle_info({nodedown, Node}, State) ->
    case mria_rlog:role() of
        core ->
            _ = erlang:send_after(
                timer:seconds(?CLEAR_NODE_DELAY_SECONDS), self(), {clear_for_node, Node}
            ),
            ok;
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
    ?LOG(error, #{msg => "unexpected_info", server => ?MODULE, info => Info}),
    {noreply, State}.

terminate(_Reason, _State) ->
    ekka:unmonitor(membership).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
