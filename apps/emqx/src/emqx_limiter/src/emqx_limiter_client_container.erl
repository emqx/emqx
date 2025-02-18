%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc
%% A convenience module for managing a collection of limiters identified by names.
%% It allows to consume from several limiters with a single call.
%%
-module(emqx_limiter_client_container).

-export([
    new/1,
    try_consume/2
]).

-type t() :: #{emqx_limiter:limiter_name() => emqx_limiter_client:t()}.

-export_type([t/0]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec new(list({emqx_limiter:limiter_name(), emqx_limiter_client:t()})) -> t().
new(Clients) ->
    maps:from_list(Clients).

-spec try_consume(t(), [{emqx_limiter:limiter_name(), non_neg_integer()}]) -> {boolean(), t()}.
try_consume(Container, Needs) ->
    try_consume_from_clients(Container, Needs, []).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

try_consume_from_clients(Container, [], _Consumed) ->
    {true, Container};
try_consume_from_clients(Container, [{Name, Amount} | Rest], Consumed) ->
    case Container of
        #{Name := Client} ->
            case emqx_limiter_client:try_consume(Client, Amount) of
                {true, NewClient} ->
                    try_consume_from_clients(Container#{Name => NewClient}, Rest, [
                        {Name, Amount} | Consumed
                    ]);
                {false, NewClient} ->
                    {false, put_back_to_clients(Container#{Name => NewClient}, Consumed)}
            end;
        _ ->
            %% TODO
            %% error?
            try_consume_from_clients(Container, Rest, Consumed)
    end.

put_back_to_clients(Container, []) ->
    Container;
put_back_to_clients(Container, [{Name, Amount} | Rest]) ->
    #{Name := Client0} = Container,
    Client = emqx_limiter_client:put_back(Client0, Amount),
    put_back_to_clients(Container#{Name => Client}, Rest).
