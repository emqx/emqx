%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_limiter_composite).

-behaviour(emqx_limiter_client).

-include("logger.hrl").

-export([
    new/1,
    try_consume/2,
    put_back/2
]).

-type t() :: emqx_limiter_client:t().
-type state() :: [emqx_limiter_client:t()].

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec new(list(emqx_limiter_client:t())) -> t().
new(Clients) ->
    emqx_limiter_client:new(?MODULE, Clients).

%%--------------------------------------------------------------------
%% emqx_limiter_client
%%--------------------------------------------------------------------

-spec try_consume(state(), non_neg_integer()) -> {boolean(), state()}.
try_consume(Clients, Amount) ->
    consume_from_clients(Clients, Amount, []).

-spec put_back(state(), non_neg_integer()) -> state().
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
            {false, put_back_to_clients([NewClient | Rest], Amount, ClientsConsumed)}
    end.

put_back_to_clients(Clients, _Amount, []) ->
    Clients;
put_back_to_clients(Clients, Amount, [ClientConsumed | Rest]) ->
    Client = emqx_limiter_client:put_back(ClientConsumed, Amount),
    put_back_to_clients([Client | Clients], Amount, Rest).
