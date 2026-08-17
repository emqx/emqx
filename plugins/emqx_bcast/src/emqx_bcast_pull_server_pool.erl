%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bcast_pull_server_pool).

-behaviour(gen_server).

-export([
    start_link/0,
    want_next/2,
    ack_batch/1,
    qos0_broadcast/3,
    qos1_trigger/3
]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-include("emqx_bcast.hrl").

-define(ENSURE_COPIES_MS, 30000).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% Local call used both by local pull_pool workers and by remote pull_pool
%% workers through emqx_rpc:call (F7: pull batches pick a random core).
want_next(Node, Entries) ->
    gen_server:call(?MODULE, {want_next, Node, Entries}, 15000).

ack_batch(Acks) ->
    gen_server:cast(?MODULE, {ack_batch, Acks}).

%% QoS0 / PubBroadcast: one-shot delivery. Core broadcasts full deliver data
%% to all nodes; each local pull_pool checks online + subscription and drops
%% silently when either check fails.
qos0_broadcast(ProductKey, TopicTemplate, Payload) ->
    broadcast_to_pull_pools({qos0_deliver_local, [ProductKey, TopicTemplate, Payload]}).

%% QoS1 BatchPub: pure trigger signal (no payload). Each pull_pool checks
%% online + subscription and turns the trigger into a want_next batch entry.
qos1_trigger(ProductKey, DeviceNames, TopicTemplate) ->
    broadcast_to_pull_pools({qos1_core_trigger_local, [ProductKey, DeviceNames, TopicTemplate]}).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    _ = ensure_core_copies(),
    _ = erlang:send_after(?ENSURE_COPIES_MS, self(), ensure_core_copies),
    {ok, #{}}.

handle_call({want_next, _Node, Entries}, From, State) ->
    submit_worker(fun() ->
        Results = emqx_bcast_storage:claim_want_next_batch(Entries),
        gen_server:reply(From, Results)
    end),
    {noreply, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({ack_batch, Acks}, State) ->
    submit_worker(fun() ->
        _ = emqx_bcast_storage:process_ack_batch(Acks)
    end),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(ensure_core_copies, State) ->
    _ = ensure_core_copies(),
    _ = erlang:send_after(?ENSURE_COPIES_MS, self(), ensure_core_copies),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal
%%--------------------------------------------------------------------

broadcast_to_pull_pools({Fun, Args}) ->
    Nodes0 = emqx:running_nodes(),
    Nodes =
        case lists:member(node(), Nodes0) of
            true -> Nodes0;
            false -> [node() | Nodes0]
        end,
    lists:foreach(
        fun(Node) ->
            case Node =:= node() of
                true ->
                    apply(emqx_bcast_pull_pool, local_cast_fun(Fun), Args);
                false ->
                    emqx_rpc:cast(Node, emqx_bcast_pull_pool, local_cast_fun(Fun), Args)
            end
        end,
        Nodes
    ).

local_cast_fun(qos0_deliver_local) -> qos0_deliver_local;
local_cast_fun(qos1_core_trigger_local) -> qos1_core_trigger_local.

ensure_core_copies() ->
    try
        emqx_bcast:ensure_core_copies()
    catch
        _:_ -> ok
    end.

submit_worker(Fun) ->
    try
        emqx_pool:async_submit_to_pool(emqx_bcast_pull_server_worker_pool, Fun)
    catch
        _:_ -> Fun()
    end.
