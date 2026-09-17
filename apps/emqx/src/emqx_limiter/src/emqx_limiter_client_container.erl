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

%% A lazy spec names a limiter to connect on first use, optionally with
%% client options (`not_found_mode => close` denies instead of failing
%% open when the limiter's group is gone).
-type lazy_spec() :: emqx_limiter:id() | {emqx_limiter:id(), emqx_limiter:client_options()}.
-type entry() :: emqx_limiter_client:t() | {lazy, [lazy_spec()]}.
-type t() :: #{emqx_limiter:name() => entry()}.
-type reason() :: emqx_limiter_client:reason().

-export_type([t/0, entry/0, lazy_spec/0, reason/0]).

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
        #{Name := {lazy, Specs}} ->
            case materialize(Specs) of
                unlimited ->
                    try_consume_from_clients(Container, Rest, Consumed);
                {ok, Client} ->
                    try_consume_from_client(Container, Client, Name, Amount, Rest, Consumed);
                {error, Reason} ->
                    {false, put_back_to_clients(Container, Consumed), Reason}
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

%% Decide what a lazy entry becomes on consume. While every limiter of the
%% entry is unlimited it stays lazy. A limiter whose group or name is gone
%% is treated as unlimited (fail open), consistent with how connected
%% clients handle a vanished limiter, unless its spec asks for
%% `not_found_mode => close`: then the consume is denied.
materialize(Specs) ->
    case classify(Specs, unlimited) of
        unlimited ->
            unlimited;
        limited ->
            {ok, connect_clients(Specs)};
        {error, _} = Error ->
            Error
    end.

classify([], Acc) ->
    Acc;
classify([Spec | Rest], Acc) ->
    {LimiterId, ClientOpts} = spec(Spec),
    case limiter_state(LimiterId) of
        unlimited ->
            classify(Rest, Acc);
        limited ->
            classify(Rest, limited);
        missing ->
            case maps:get(not_found_mode, ClientOpts, open) of
                close -> {error, {limiter_not_found, LimiterId}};
                open -> classify(Rest, Acc)
            end
    end.

limiter_state({Group, Name}) ->
    case emqx_limiter_registry:find_group(Group) of
        undefined ->
            missing;
        {_Module, LimiterOptions} ->
            case lists:keyfind(Name, 1, LimiterOptions) of
                {_, #{capacity := infinity}} -> unlimited;
                {_, _} -> limited;
                false -> missing
            end
    end.

%% Vanished open-mode limiters are skipped: they count as unlimited.
connect_clients(Specs) ->
    Clients = [
        emqx_limiter:connect(LimiterId, ClientOpts)
     || Spec <- Specs,
        {LimiterId, ClientOpts} <- [spec(Spec)],
        limiter_state(LimiterId) =/= missing
    ],
    case Clients of
        [Client] -> Client;
        _ -> emqx_limiter_composite:new(Clients)
    end.

spec({LimiterId, ClientOpts}) when is_map(ClientOpts) ->
    {LimiterId, ClientOpts};
spec(LimiterId) ->
    {LimiterId, #{}}.

put_back_to_clients(Container, []) ->
    Container;
put_back_to_clients(Container, [{Name, Amount} | Rest]) ->
    #{Name := Client0} = Container,
    Client = emqx_limiter_client:put_back(Client0, Amount),
    put_back_to_clients(Container#{Name => Client}, Rest).
