%%--------------------------------------------------------------------
%% Copyright (c) 2021-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc
%% A convenience module for managing a collection of limiters identified by names.
%% It allows to consume from several limiters with a single call.
%%
%% An entry may be a connected client or a lazy `{lazy, LimiterIds}` spec.
%% A lazy entry stays a compact id list while every one of its limiters is
%% unlimited, and connects into a real client on the first consume after a
%% finite limit is configured.
%%
-module(emqx_limiter_client_container).

-export([
    new/1,
    try_consume/2
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-type entry() :: emqx_limiter_client:t() | {lazy, [emqx_limiter:id()]}.
-type t() :: #{emqx_limiter:name() => entry()}.
-type reason() :: emqx_limiter_client:reason().

-export_type([t/0, entry/0, reason/0]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec new(list({emqx_limiter:name(), entry()})) -> t().
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
        #{Name := {lazy, LimiterIds}} ->
            case materialize(LimiterIds) of
                unlimited ->
                    try_consume_from_clients(Container, Rest, Consumed);
                {ok, Client} ->
                    try_consume_from_client(Container, Client, Name, Amount, Rest, Consumed)
            end;
        #{Name := Client} ->
            try_consume_from_client(Container, Client, Name, Amount, Rest, Consumed);
        _ ->
            error({limiter_not_found_in_container, Name})
    end.

try_consume_from_client(Container, Client, Name, Amount, Rest, Consumed) ->
    case emqx_limiter_client:try_consume(Client, Amount) of
        {true, NewClient} ->
            try_consume_from_clients(Container#{Name => NewClient}, Rest, [
                {Name, Amount} | Consumed
            ]);
        {false, NewClient, Reason} ->
            {false, put_back_to_clients(Container#{Name => NewClient}, Consumed), Reason}
    end.

materialize(LimiterIds) ->
    case lists:all(fun is_unlimited/1, LimiterIds) of
        true ->
            unlimited;
        false ->
            {ok, connect_clients(LimiterIds)}
    end.

%% A limiter whose group or name is gone is treated as unlimited (fail open),
%% consistent with how connected clients handle a vanished limiter.
is_unlimited({Group, Name}) ->
    case emqx_limiter_registry:find_group(Group) of
        undefined ->
            true;
        {_Module, LimiterOptions} ->
            case lists:keyfind(Name, 1, LimiterOptions) of
                {_, #{capacity := infinity}} -> true;
                {_, _} -> false;
                false -> true
            end
    end.

connect_clients([LimiterId]) ->
    emqx_limiter:connect(LimiterId);
connect_clients(LimiterIds) ->
    emqx_limiter_composite:new([emqx_limiter:connect(Id) || Id <- LimiterIds]).

put_back_to_clients(Container, []) ->
    Container;
put_back_to_clients(Container, [{Name, Amount} | Rest]) ->
    #{Name := Client0} = Container,
    Client = emqx_limiter_client:put_back(Client0, Amount),
    put_back_to_clients(Container#{Name => Client}, Rest).
