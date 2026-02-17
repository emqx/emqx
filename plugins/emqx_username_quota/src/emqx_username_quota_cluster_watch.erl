%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_username_quota_cluster_watch).

-behaviour(gen_server).

-export([
    start_link/0,
    immediate_node_clear/1
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(SERVER, ?MODULE).

-ifdef(TEST).
-define(CLEAR_NODE_DELAY_SECONDS, 0).
-define(CLEAR_NODE_JITTER_MAX_SECONDS, 0).
-else.
-define(CLEAR_NODE_DELAY_SECONDS, 600).
-define(CLEAR_NODE_JITTER_MAX_SECONDS, 30).
-endif.

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

immediate_node_clear(Node) ->
    _ = erlang:send(?SERVER, {clear_for_node, Node}),
    async.

init([]) ->
    process_flag(trap_exit, true),
    ok = ekka:monitor(membership),
    ok = emqx_username_quota_state:clear_self_node(),
    {ok, #{}}.

handle_call(_Req, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({clear_for_node, Node}, State) ->
    case lists:member(Node, emqx:running_nodes()) of
        true ->
            ok;
        false ->
            _ = emqx_username_quota_state:clear_for_node(Node),
            ok
    end,
    {noreply, State};
handle_info({nodedown, Node}, State) ->
    case mria_rlog:role() of
        core ->
            _ = erlang:send_after(
                clear_node_delay_ms(Node), self(), {clear_for_node, Node}
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
handle_info({membership, _}, State) ->
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ekka:unmonitor(membership).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

clear_node_delay_ms(Node) ->
    BaseS = ?CLEAR_NODE_DELAY_SECONDS,
    MaxJitterS = ?CLEAR_NODE_JITTER_MAX_SECONDS,
    JitterS =
        case MaxJitterS > 0 of
            true -> erlang:phash2({node(), Node}, MaxJitterS + 1);
            false -> 0
        end,
    timer:seconds(BaseS + JitterS).
