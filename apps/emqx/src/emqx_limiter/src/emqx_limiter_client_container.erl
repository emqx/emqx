%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-type t() :: #{emqx_limiter:name() => emqx_limiter_client:t()}.
-type reason() :: emqx_limiter_client:reason().

-export_type([t/0, reason/0]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec new(list({emqx_limiter:name(), emqx_limiter_client:t()})) -> t().
new(Clients) ->
    maps:from_list(Clients).

-spec try_consume(t(), [{emqx_limiter:name(), non_neg_integer()}]) ->
    {true, t()} | {false, t(), reason()}.
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
                {false, NewClient, Reason} ->
                    {false, put_back_to_clients(Container#{Name => NewClient}, Consumed), Reason}
            end;
        _ ->
            error({limiter_not_found_in_container, Name})
    end.

put_back_to_clients(Container, []) ->
    Container;
put_back_to_clients(Container, [{Name, Amount} | Rest]) ->
    #{Name := Client0} = Container,
    Client = emqx_limiter_client:put_back(Client0, Amount),
    put_back_to_clients(Container#{Name => Client}, Rest).
