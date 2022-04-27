%%--------------------------------------------------------------------
%% Copyright (c) 2019-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_monitor_api).

-include("emqx_dashboard.hrl").
-include_lib("typerefl/include/types.hrl").

-behaviour(minirest_api).

-export([api_spec/0]).

-export([
    paths/0,
    schema/1,
    fields/1
]).

-export([
    monitor/2,
    monitor_current/2
]).

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true, translate_body => true}).

paths() ->
    [
        "/monitor",
        "/monitor/nodes/:node",
        "/monitor_current",
        "/monitor_current/nodes/:node"
    ].

schema("/monitor") ->
    #{
        'operationId' => monitor,
        get => #{
            tags => [dashboard],
            desc => <<"List monitor data.">>,
            parameters => [parameter_latest()],
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
            tags => [dashboard],
            desc => <<"List the monitor data on the node.">>,
            parameters => [parameter_node(), parameter_latest()],
            responses => #{
                200 => hoconsc:mk(hoconsc:array(hoconsc:ref(sampler)), #{}),
                400 => emqx_dashboard_swagger:error_codes(['BAD_RPC'], <<"Bad RPC">>)
            }
        }
    };
schema("/monitor_current") ->
    #{
        'operationId' => monitor_current,
        get => #{
            tags => [dashboard],
            desc => <<"Current status. Gauge and rate.">>,
            responses => #{
                200 => hoconsc:mk(hoconsc:ref(sampler_current), #{})
            }
        }
    };
schema("/monitor_current/nodes/:node") ->
    #{
        'operationId' => monitor_current,
        get => #{
            tags => [dashboard],
            desc => <<"Node current status. Gauge and rate.">>,
            parameters => [parameter_node()],
            responses => #{
                200 => hoconsc:mk(hoconsc:ref(sampler_current), #{}),
                400 => emqx_dashboard_swagger:error_codes(['BAD_RPC'], <<"Bad RPC">>)
            }
        }
    }.

parameter_latest() ->
    Info = #{
        in => query,
        required => false,
        example => 5 * 60,
        desc => <<"The latest N seconds data. Like 300 for 5 min.">>
    },
    {latest, hoconsc:mk(range(1, inf), Info)}.

parameter_node() ->
    Info = #{
        in => path,
        required => true,
        example => node(),
        desc => <<"EMQX node name.">>
    },
    {node, hoconsc:mk(binary(), Info)}.

fields(sampler) ->
    Samplers =
        [
            {SamplerName, hoconsc:mk(integer(), #{desc => swagger_desc(SamplerName)})}
         || SamplerName <- ?SAMPLER_LIST
        ],
    [{time_stamp, hoconsc:mk(non_neg_integer(), #{desc => <<"Timestamp">>})} | Samplers];
fields(sampler_current) ->
    Names = maps:values(?DELTA_SAMPLER_RATE_MAP) ++ ?GAUGE_SAMPLER_LIST,
    [
        {SamplerName, hoconsc:mk(integer(), #{desc => swagger_desc(SamplerName)})}
     || SamplerName <- Names
    ].

%% -------------------------------------------------------------------------------------------------
%% API

monitor(get, #{query_string := QS, bindings := Bindings}) ->
    Latest = maps:get(<<"latest">>, QS, infinity),
    Node = binary_to_atom(maps:get(node, Bindings, <<"all">>)),
    case emqx_dashboard_monitor:samplers(Node, Latest) of
        {badrpc, {Node, Reason}} ->
            Message = list_to_binary(io_lib:format("Bad node ~p, rpc failed ~p", [Node, Reason])),
            {400, 'BAD_RPC', Message};
        Samplers ->
            {200, Samplers}
    end.

monitor_current(get, #{bindings := Bindings}) ->
    NodeOrCluster = binary_to_atom(maps:get(node, Bindings, <<"all">>), utf8),
    case emqx_dashboard_monitor:current_rate(NodeOrCluster) of
        {ok, CurrentRate} ->
            {200, CurrentRate};
        {badrpc, {Node, Reason}} ->
            Message = list_to_binary(io_lib:format("Bad node ~p, rpc failed ~p", [Node, Reason])),
            {400, 'BAD_RPC', Message}
    end.

%% -------------------------------------------------------------------------------------------------
%% Internal

swagger_desc(received) ->
    swagger_desc_format("Received messages ");
swagger_desc(received_bytes) ->
    swagger_desc_format("Received bytes ");
swagger_desc(sent) ->
    swagger_desc_format("Sent messages ");
swagger_desc(sent_bytes) ->
    swagger_desc_format("Sent bytes ");
swagger_desc(dropped) ->
    swagger_desc_format("Dropped messages ");
swagger_desc(subscriptions) ->
    <<
        "Subscriptions at the time of sampling."
        " Can only represent the approximate state"
    >>;
swagger_desc(topics) ->
    <<
        "Count topics at the time of sampling."
        " Can only represent the approximate state"
    >>;
swagger_desc(connections) ->
    <<
        "Connections at the time of sampling."
        " Can only represent the approximate state"
    >>;
swagger_desc(received_msg_rate) ->
    swagger_desc_format("Dropped messages ", per);
%swagger_desc(received_bytes_rate) -> swagger_desc_format("Received bytes ", per);
swagger_desc(sent_msg_rate) ->
    swagger_desc_format("Sent messages ", per);
%swagger_desc(sent_bytes_rate)     -> swagger_desc_format("Sent bytes ", per);
swagger_desc(dropped_msg_rate) ->
    swagger_desc_format("Dropped messages ", per).

swagger_desc_format(Format) ->
    swagger_desc_format(Format, last).

swagger_desc_format(Format, Type) ->
    Interval = emqx_conf:get([dashboard, monitor, interval], ?DEFAULT_SAMPLE_INTERVAL),
    list_to_binary(io_lib:format(Format ++ "~p ~p seconds", [Type, Interval])).
