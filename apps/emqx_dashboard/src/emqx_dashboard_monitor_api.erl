%%--------------------------------------------------------------------
%% Copyright (c) 2019-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_monitor_api).

-include("emqx_dashboard.hrl").
-include_lib("typerefl/include/types.hrl").

-behaviour(minirest_api).

-export([ api_spec/0]).

-export([ paths/0
        , schema/1
        , fields/1
        ]).

-export([ monitor/2
        , monitor_current/2
        ]).

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true, translate_body => true}).

paths() ->
    [ "/monitor"
    , "/monitor/nodes/:node"
    , "/monitor/current"
    ].

schema("/monitor") ->
    #{
        'operationId' => monitor,
        get => #{
            description => <<"List monitor data.">>,
            parameters => [
                {latest, hoconsc:mk(integer(), #{in => query, nullable => true, example => 1000})}
            ],
            responses => #{
                200 => hoconsc:mk(hoconsc:array(hoconsc:ref(sampler)), #{}),
                400 => emqx_dashboard_swagger:error_codes(['BAD_RPC'], <<"Bad RPC">>)
            }
        }
    };

schema("/monitor/nodes/:node") ->
    #{
        'operationId' => monitor,
        get => #{
            description => <<"List the monitor data on the node.">>,
            parameters => [
                {node, hoconsc:mk(binary(), #{in => path, nullable => false, example => node()})},
                {latest, hoconsc:mk(integer(), #{in => query, nullable => true, example => 1000})}
            ],
            responses => #{
                200 => hoconsc:mk(hoconsc:array(hoconsc:ref(sampler)), #{}),
                400 => emqx_dashboard_swagger:error_codes(['BAD_RPC'], <<"Bad RPC">>)
            }
        }
    };

schema("/monitor/current") ->
    #{
        'operationId' => monitor_current,
        get => #{
            description => <<"Current monitor data. Gauge and rate">>,
            responses => #{
                200 => hoconsc:mk(hoconsc:ref(sampler_current), #{})
            }
        }
    }.

fields(sampler) ->
    Samplers =
        [{SamplerName, hoconsc:mk(integer(), #{desc => sampler_desc(SamplerName)})}
        || SamplerName <- ?SAMPLER_LIST],
    [{time_stamp, hoconsc:mk(integer(), #{desc => <<"Timestamp">>})} | Samplers];

fields(sampler_current) ->
    [{SamplerName, hoconsc:mk(integer(), #{desc => sampler_desc(SamplerName)})}
    || SamplerName <- maps:values(?DELTA_SAMPLER_RATE_MAP) ++ ?GAUGE_SAMPLER_LIST].

%% -------------------------------------------------------------------------------------------------
%% API

monitor(get, #{query_string := QS, bindings := Bindings}) ->
    Latest = maps:get(<<"latest">>, QS, 1000),
    Node = binary_to_atom(maps:get(node, Bindings, <<"all">>)),
    case emqx_dashboard_monitor:samplers(Node, Latest) of
        {badrpc, {Node, Reason}} ->
            Message = list_to_binary(io_lib:format("Bad node ~p, rpc failed ~p", [Node, Reason])),
            {400, 'BAD_RPC', Message};
        Samplers ->
            {200, Samplers}
    end.

monitor_current(get, #{query_string := QS}) ->
    NodeOrCluster = binary_to_atom(maps:get(<<"node">>, QS, <<"all">>), utf8),
    case emqx_dashboard_monitor:current_rate(NodeOrCluster) of
        {ok, CurrentRate} ->
            {200, CurrentRate};
        {badrpc, {Node, Reason}} ->
            Message = list_to_binary(io_lib:format("Bad node ~p, rpc failed ~p", [Node, Reason])),
            {400, 'BAD_RPC', Message}
    end.

%% -------------------------------------------------------------------------------------------------
%% Internal

sampler_desc(received)       -> sampler_desc_format("Received messages ");
sampler_desc(received_bytes) -> sampler_desc_format("Received bytes ");
sampler_desc(sent)           -> sampler_desc_format("Sent messages ");
sampler_desc(sent_bytes)     -> sampler_desc_format("Sent bytes ");
sampler_desc(dropped)        -> sampler_desc_format("Dropped messages ");
sampler_desc(subscriptions) ->
    <<"Subscriptions at the time of sampling."
    " Can only represent the approximate state">>;
sampler_desc(routes) ->
    <<"Routes at the time of sampling."
    " Can only represent the approximate state">>;
sampler_desc(connections) ->
    <<"Connections at the time of sampling."
    " Can only represent the approximate state">>;

sampler_desc(received_rate)       -> sampler_desc_format("Dropped messages ", per);
sampler_desc(received_bytes_rate) -> sampler_desc_format("Received bytes ", per);
sampler_desc(sent_rate)           -> sampler_desc_format("Sent messages ", per);
sampler_desc(sent_bytes_rate)     -> sampler_desc_format("Sent bytes ", per);
sampler_desc(dropped_rate)        -> sampler_desc_format("Dropped messages ", per).

sampler_desc_format(Format) ->
    sampler_desc_format(Format, last).

sampler_desc_format(Format, Type) ->
    Interval = emqx_conf:get([dashboard, monitor, interval], ?DEFAULT_SAMPLE_INTERVAL),
    list_to_binary(io_lib:format(Format ++ "~p ~p seconds", [Type, Interval])).
