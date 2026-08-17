%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bcast_ack_pool).

-behaviour(gen_server).

-export([start_link/0, ack/3, client_down/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-include("emqx_bcast.hrl").

-define(FLUSH_MS, 50).
-define(FLUSH_COUNT, 100).

-record(state, {
    acks = [],
    timer = undefined
}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

ack(ClientId, DeliveryId, ProductKey) ->
    gen_server:cast(?MODULE, {ack, ClientId, DeliveryId, ProductKey}).

client_down(ClientId) ->
    gen_server:cast(?MODULE, {client_down, ClientId}).

init([]) ->
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({ack, ClientId, DeliveryId, ProductKey}, State) ->
    %% Ack received on the client's own node: count it here so the metric is
    %% node-local (aggregated across nodes by the observer).
    emqx_bcast_metrics:qos1_acked(),
    %% 1. Atomically delete the in-progress pending entry and trigger the next
    %%    want_next in pull_pool.
    gen_server:cast(emqx_bcast_pull_pool, {ack, ClientId, DeliveryId, ProductKey}),
    %% 2. Accumulate for batched core accounting.
    Acks = [{ProductKey, ClientId, DeliveryId} | State#state.acks],
    State1 = State#state{acks = Acks},
    State2 = maybe_flush(State1),
    {noreply, State2};
handle_cast({client_down, _ClientId}, State) ->
    %% A client went down; do not drop already-acked-but-unreported acks.
    %% Flushing the whole batch is safe because ack accounting is idempotent
    %% on core (duplicate ACKs do not count twice).
    {noreply, flush(State)};
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
    case length(Acks) >= ?FLUSH_COUNT of
        true ->
            ok = cancel_timer(State#state.timer),
            self() ! flush_acks,
            State#state{timer = undefined};
        false ->
            ensure_timer(State)
    end.

ensure_timer(State = #state{timer = undefined}) ->
    TRef = erlang:send_after(?FLUSH_MS, self(), flush_acks),
    State#state{timer = TRef};
ensure_timer(State) ->
    State.

flush(State = #state{acks = []}) ->
    State;
flush(State = #state{acks = Acks}) ->
    ok = cancel_timer(State#state.timer),
    case is_core() of
        true ->
            emqx_bcast_pull_server_pool:ack_batch(Acks);
        false ->
            Core = emqx_bcast:random_core(),
            emqx_rpc:cast(Core, emqx_bcast_pull_server_pool, ack_batch, [Acks])
    end,
    State#state{acks = [], timer = undefined}.

cancel_timer(undefined) ->
    ok;
cancel_timer(TRef) ->
    _ = erlang:cancel_timer(TRef),
    ok.

is_core() ->
    emqx_bcast:is_core().
