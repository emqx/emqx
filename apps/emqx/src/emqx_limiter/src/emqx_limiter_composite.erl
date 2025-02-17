%%--------------------------------------------------------------------
%% Copyright (c) 2019-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_limiter_composite).

-behaviour(emqx_limiter_client).

-export([
    new/1,
    try_consume/2,
    put_back/2
]).

-type t() :: [emqx_limiter_client:t()].

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec new(list(emqx_limiter_client:t())) -> t().
new(Clients) ->
    Clients.

%%--------------------------------------------------------------------
%% emqx_limiter_client
%%--------------------------------------------------------------------

-spec try_consume(t(), non_neg_integer()) -> {boolean(), t()}.
try_consume(Clients, Amount) ->
    consume_from_clients(Clients, Amount, []).

-spec put_back(t(), non_neg_integer()) -> t().
put_back(Clients, Amount) ->
    put_back_to_clients([], Amount, lists:reverse(Clients)).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

consume_from_clients([], _Amount, ClientsConsumed) ->
    {true, lists:reverse(ClientsConsumed)};
consume_from_clients([Client | Rest], Amount, ClientsConsumed) ->
    case emqx_limiter_client:try_consume(Client, Amount) of
        {true, NewClient} ->
            consume_from_clients(Rest, Amount, [NewClient | ClientsConsumed]);
        {false, NewClient} ->
            put_back_to_clients([NewClient | Rest], Amount, ClientsConsumed)
    end.

put_back_to_clients(Clients, _Amount, []) ->
    Clients;
put_back_to_clients(Clients, Amount, [ClientConsumed | Rest]) ->
    Client = emqx_limiter_client:put_back(ClientConsumed, Amount),
    put_back_to_clients([Client | Clients], Amount, Rest).
