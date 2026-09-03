%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bcast_ack_pool).

-behaviour(gen_server).

-export([start_link/0, ack/3, client_down/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-include("emqx_bcast.hrl").

-record(state, {
    acks = [],
    timer = undefined
}).

-spec start_link() -> gen_server:start_ret().
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec ack(binary(), binary(), binary()) -> ok.
ack(ClientId, DeliveryId, ProductKey) ->
    gen_server:cast(?MODULE, {ack, ClientId, DeliveryId, ProductKey}).

-spec client_down(binary()) -> ok.
client_down(ClientId) ->
    gen_server:cast(?MODULE, {client_down, ClientId}).

init([]) ->
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({ack, ClientId, DeliveryId, ProductKey}, State) ->
    %% pull_pool is the ack entry point: it matches the local buffer first
    %% (setting the ack-in-flight marker before this ack can be applied at
    %% core). Here we only accumulate for batched core accounting.
    Acks = [{ProductKey, ClientId, DeliveryId} | State#state.acks],
    State1 = State#state{acks = Acks},
    State2 = maybe_flush(State1),
    {noreply, State2};
handle_cast({client_down, ClientId}, State) ->
    %% A client went down; flush only its already-acked-but-unreported acks
    %% (spec 4.6). The remaining clients' acks stay batched and flush on the
    %% timer.
    {Mine, Rest} = lists:partition(
        fun({_ProductKey, C, _DeliveryId}) -> C =:= ClientId end,
        State#state.acks
    ),
    ok = send_acks(Mine),
    {noreply, State#state{acks = Rest}};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(flush_acks, State) ->
    {noreply, flush(State#state{timer = undefined})};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

maybe_flush(State = #state{acks = Acks}) ->
    Timer = emqx_bcast_utils:maybe_batch_flush(length(Acks), State#state.timer, flush_acks),
    State#state{timer = Timer}.

flush(State = #state{acks = []}) ->
    State;
flush(State = #state{acks = Acks}) ->
    ok = emqx_bcast_utils:cancel_timer(State#state.timer),
    ok = send_acks(Acks),
    State#state{acks = [], timer = undefined}.

send_acks([]) ->
    ok;
send_acks(Acks) ->
    case is_core() of
        true ->
            emqx_bcast_pull_server_pool:ack_batch(Acks, node());
        false ->
            Core = emqx_bcast:random_core(),
            emqx_rpc:cast(Core, emqx_bcast_pull_server_pool, ack_batch, [Acks, node()])
    end,
    ok.

is_core() ->
    emqx_bcast:is_core().
