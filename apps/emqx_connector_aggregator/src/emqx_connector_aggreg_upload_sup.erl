%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_connector_aggreg_upload_sup).

-export([
    start_link/3,
    start_link_delivery_sup/2
]).

-export([
    start_delivery/2,
    start_delivery_proc/3
]).

-behaviour(supervisor).
-export([init/1]).

-define(SUPREF(NAME), {via, gproc, {n, l, {?MODULE, NAME}}}).

%%

start_link(Name, AggregOpts, DeliveryOpts) ->
    supervisor:start_link(?MODULE, {root, Name, AggregOpts, DeliveryOpts}).

start_link_delivery_sup(Name, DeliveryOpts) ->
    supervisor:start_link(?SUPREF(Name), ?MODULE, {delivery, Name, DeliveryOpts}).

%%

start_delivery(Name, Buffer) ->
    supervisor:start_child(?SUPREF(Name), [Buffer]).

start_delivery_proc(Name, DeliveryOpts, Buffer) ->
    emqx_connector_aggreg_delivery:start_link(Name, Buffer, DeliveryOpts).

%%

init({root, Name, AggregOpts, DeliveryOpts}) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 5
    },
    AggregatorChildSpec = #{
        id => aggregator,
        start => {emqx_connector_aggregator, start_link, [Name, AggregOpts]},
        type => worker,
        restart => permanent
    },
    DeliverySupChildSpec = #{
        id => delivery_sup,
        start => {?MODULE, start_link_delivery_sup, [Name, DeliveryOpts]},
        type => supervisor,
        restart => permanent
    },
    {ok, {SupFlags, [DeliverySupChildSpec, AggregatorChildSpec]}};
init({delivery, Name, DeliveryOpts}) ->
    SupFlags = #{
        strategy => simple_one_for_one,
        intensity => 100,
        period => 5
    },
    ChildSpec = #{
        id => delivery,
        start => {?MODULE, start_delivery_proc, [Name, DeliveryOpts]},
        type => worker,
        restart => temporary,
        shutdown => 1000
    },
    {ok, {SupFlags, [ChildSpec]}}.
