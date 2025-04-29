%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_monitor_api).

-include("emqx_dashboard.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hocon_types.hrl").
-include_lib("emqx_utils/include/emqx_utils_api.hrl").
-include_lib("emqx/include/logger.hrl").

-behaviour(minirest_api).

-export([api_spec/0]).

-export([
    paths/0,
    schema/1,
    fields/1,
    namespace/0
]).

-export([
    monitor/2,
    monitor_current/2
]).

namespace() -> undefined.

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
            tags => [<<"Metrics">>],
            description => ?DESC(list_monitor),
            parameters => [parameter_latest()],
            responses => #{
                200 => hoconsc:mk(hoconsc:array(hoconsc:ref(sampler)), #{}),
                400 => emqx_dashboard_swagger:error_codes(['BAD_RPC'], <<"Bad RPC">>)
            }
        },
        delete => #{
            tags => [<<"Metrics">>],
            description => ?DESC(clear_monitor),
            responses => #{
                204 => <<"Metrics deleted">>
            }
        }
    };
schema("/monitor/nodes/:node") ->
    #{
        'operationId' => monitor,
        get => #{
            tags => [<<"Metrics">>],
            description => ?DESC(list_monitor_node),
            parameters => [parameter_node(), parameter_latest()],
            responses => #{
                200 => hoconsc:mk(hoconsc:array(hoconsc:ref(sampler)), #{}),
                404 => emqx_dashboard_swagger:error_codes(['NOT_FOUND'], <<"Node not found">>)
            }
        }
    };
schema("/monitor_current") ->
    #{
        'operationId' => monitor_current,
        get => #{
            tags => [<<"Metrics">>],
            description => ?DESC(current_stats),
            responses => #{
                200 => hoconsc:mk(hoconsc:ref(sampler_current), #{})
            }
        }
    };
schema("/monitor_current/nodes/:node") ->
    #{
        'operationId' => monitor_current,
        get => #{
            tags => [<<"Metrics">>],
            description => ?DESC(current_stats_node),
            parameters => [parameter_node()],
            responses => #{
                200 => hoconsc:mk(hoconsc:ref(sampler_current_node), #{}),
                404 => emqx_dashboard_swagger:error_codes(['NOT_FOUND'], <<"Node not found">>)
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
fields(sampler_current_node) ->
    fields_current(sample_names(sampler_current_node));
fields(sampler_current) ->
    fields_current(sample_names(sampler_current));
fields(session_hist_hwmark) ->
    [
        {peak_time, hoconsc:mk(non_neg_integer(), #{desc => ?DESC(hwmark_kpeak_time)})},
        {peak_value, hoconsc:mk(non_neg_integer(), #{desc => ?DESC(hwmark_peak_value)})},
        {current_value, hoconsc:mk(non_neg_integer(), #{desc => ?DESC(hwmark_current_value)})}
    ].

sample_names(sampler_current_node) ->
    maps:values(?DELTA_SAMPLER_RATE_MAP) ++ ?GAUGE_SAMPLER_LIST ++ ?CURRENT_SAMPLE_NON_RATE;
sample_names(sampler_current) ->
    sample_names(sampler_current_node) -- [node_uptime].

fields_current(Names) ->
    IntegerSamplers = [
        {SamplerName, hoconsc:mk(integer(), #{desc => swagger_desc(SamplerName)})}
     || SamplerName <- Names
    ],
    HwmarkSamplers = [
        {sessions_hist_hwmark,
            hoconsc:mk(hoconsc:ref(session_hist_hwmark), #{desc => ?DESC(sessions_hist_hwmark)})}
    ],
    IntegerSamplers ++ HwmarkSamplers.

%% -------------------------------------------------------------------------------------------------
%% API

monitor(get, #{query_string := QS, bindings := Bindings}) ->
    Latest = maps:get(<<"latest">>, QS, infinity),
    RawNode = maps:get(node, Bindings, <<"all">>),
    emqx_utils_api:with_node_or_cluster(RawNode, dashboard_samplers_fun(Latest));
monitor(delete, _) ->
    Nodes = emqx:running_nodes(),
    Results = emqx_dashboard_proto_v2:clear_table(Nodes),
    NodeResults = lists:zip(Nodes, Results),
    NodeErrors = [Result || Result = {_Node, NOk} <- NodeResults, NOk =/= {atomic, ok}],
    NodeErrors == [] orelse
        ?SLOG(warning, #{
            msg => "clear_monitor_metrics_rpc_errors",
            errors => NodeErrors
        }),
    ?NO_CONTENT.

dashboard_samplers_fun(Latest) ->
    fun(NodeOrCluster) ->
        case emqx_dashboard_monitor:samplers(NodeOrCluster, Latest) of
            {badrpc, _} = Error -> {error, Error};
            Samplers -> {ok, Samplers}
        end
    end.

monitor_current(get, #{bindings := Bindings}) ->
    RawNode = maps:get(node, Bindings, <<"all">>),
    case emqx_utils_api:with_node_or_cluster(RawNode, fun current_rate/1) of
        ?OK(Rates) ->
            ?OK(maybe_reject_cluster_only_metrics(RawNode, Rates));
        Error ->
            Error
    end.

-spec current_rate(atom()) ->
    {error, term()}
    | {ok, Result :: map()}.
current_rate(Node) ->
    %% Node :: 'all' or `NodeName`
    case emqx_dashboard_monitor:current_rate(Node) of
        {badrpc, _} = BadRpc ->
            {error, BadRpc};
        {ok, _} = OkResult ->
            OkResult
    end.

%% -------------------------------------------------------------------------------------------------
%% Internal

-define(APPROXIMATE_DESC, " Can only represent an approximate state.").

swagger_desc(received) ->
    swagger_desc_format("Received messages");
swagger_desc(received_bytes) ->
    swagger_desc_format("Received bytes");
swagger_desc(sent) ->
    swagger_desc_format("Sent messages");
swagger_desc(sent_bytes) ->
    swagger_desc_format("Sent bytes");
swagger_desc(dropped) ->
    swagger_desc_format("Dropped messages");
swagger_desc(validation_succeeded) ->
    swagger_desc_format("Schema validations succeeded");
swagger_desc(validation_failed) ->
    swagger_desc_format("Schema validations failed");
swagger_desc(transformation_succeeded) ->
    swagger_desc_format("Message transformations succeeded");
swagger_desc(transformation_failed) ->
    swagger_desc_format("Message transformations failed");
swagger_desc(persisted) ->
    swagger_desc_format("Messages saved to the durable storage");
swagger_desc(disconnected_durable_sessions) ->
    <<"Disconnected durable sessions at the time of sampling.", ?APPROXIMATE_DESC>>;
swagger_desc(subscriptions_durable) ->
    <<"Subscriptions from durable sessions at the time of sampling.", ?APPROXIMATE_DESC>>;
swagger_desc(subscriptions) ->
    <<"Subscriptions at the time of sampling.", ?APPROXIMATE_DESC>>;
swagger_desc(topics) ->
    <<"Count topics at the time of sampling.", ?APPROXIMATE_DESC>>;
swagger_desc(connections) ->
    <<"Sessions at the time of sampling.", ?APPROXIMATE_DESC>>;
swagger_desc(live_connections) ->
    <<"Connections at the time of sampling.", ?APPROXIMATE_DESC>>;
swagger_desc(cluster_sessions) ->
    <<
        "Total number of sessions in the cluster at the time of sampling. "
        "It includes expired sessions when `broker.session_history_retain` is set to a duration greater than `0s`."
        ?APPROXIMATE_DESC
    >>;
swagger_desc(received_msg_rate) ->
    <<"Dropped messages per second">>;
swagger_desc(sent_msg_rate) ->
    <<"Sent messages per second">>;
swagger_desc(dropped_msg_rate) ->
    <<"Dropped messages per second">>;
swagger_desc(validation_succeeded_rate) ->
    <<"Schema validations succeeded per second">>;
swagger_desc(validation_failed_rate) ->
    <<"Schema validations failed per second">>;
swagger_desc(transformation_succeeded_rate) ->
    <<"Message transformations succeeded per second">>;
swagger_desc(transformation_failed_rate) ->
    <<"Message transformations failed per second">>;
swagger_desc(persisted_rate) ->
    <<"Messages saved to the durable storage per second">>;
swagger_desc(retained_msg_count) ->
    <<"Retained messages count at the time of sampling.", ?APPROXIMATE_DESC>>;
swagger_desc(shared_subscriptions) ->
    <<"Shared subscriptions count at the time of sampling.", ?APPROXIMATE_DESC>>;
swagger_desc(node_uptime) ->
    <<"Node up time in seconds. Only presented in endpoint: `/monitor_current/nodes/:node`.">>;
swagger_desc(license_quota) ->
    <<"License quota. AKA: limited max_connections for cluster">>;
swagger_desc(Name) ->
    ?DESC(Name).

swagger_desc_format(Format) ->
    Interval = emqx_conf:get([dashboard, monitor, interval], ?DEFAULT_SAMPLE_INTERVAL),
    list_to_binary(io_lib:format(Format ++ " last ~p seconds", [Interval])).

maybe_reject_cluster_only_metrics(<<"all">>, Rates) ->
    Rates;
maybe_reject_cluster_only_metrics(_Node, Rates) ->
    maps:without(?CLUSTERONLY_SAMPLER_LIST, Rates).
