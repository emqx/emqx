%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_monitor_api).

-include("emqx_dashboard.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hocon_types.hrl").
-include_lib("emqx_utils/include/emqx_http_api.hrl").
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
        desc => ?DESC("parameter_latest")
    },
    {latest, hoconsc:mk(range(1, inf), Info)}.

parameter_node() ->
    Info = #{
        in => path,
        required => true,
        example => node(),
        desc => ?DESC("parameter_node")
    },
    {node, hoconsc:mk(binary(), Info)}.

fields(sampler) ->
    Samplers =
        [
            {SamplerName, hoconsc:mk(integer(), #{desc => swagger_desc(SamplerName)})}
         || SamplerName <- ?SAMPLER_LIST
        ],
    [{time_stamp, hoconsc:mk(non_neg_integer(), #{desc => ?DESC("time_stamp")})} | Samplers];
fields(sampler_current_node) ->
    fields_current(sample_names(sampler_current_node));
fields(sampler_current) ->
    fields_current(sample_names(sampler_current));
fields(sessions_hist_hwmark) ->
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
            hoconsc:mk(hoconsc:ref(sessions_hist_hwmark), #{desc => ?DESC(sessions_hist_hwmark)})}
    ],
    IntegerSamplers ++ HwmarkSamplers.

%% -------------------------------------------------------------------------------------------------
%% API

monitor(get, #{query_string := QS, bindings := Bindings}) ->
    Latest = maps:get(<<"latest">>, QS, infinity),
    RawNode = maps:get(node, Bindings, <<"all">>),
    emqx_mgmt_api_lib:with_node_or_cluster(RawNode, dashboard_samplers_fun(Latest));
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
    case emqx_mgmt_api_lib:with_node_or_cluster(RawNode, fun current_rate/1) of
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

swagger_desc(received) -> ?DESC("received");
swagger_desc(sent) -> ?DESC("sent");
swagger_desc(dropped) -> ?DESC("dropped");
swagger_desc(validation_succeeded) -> ?DESC("validation_succeeded");
swagger_desc(validation_failed) -> ?DESC("validation_failed");
swagger_desc(transformation_succeeded) -> ?DESC("transformation_succeeded");
swagger_desc(transformation_failed) -> ?DESC("transformation_failed");
swagger_desc(persisted) -> ?DESC("persisted");
swagger_desc(disconnected_durable_sessions) -> ?DESC("disconnected_durable_sessions");
swagger_desc(subscriptions_durable) -> ?DESC("subscriptions_durable");
swagger_desc(subscriptions) -> ?DESC("subscriptions");
swagger_desc(topics) -> ?DESC("topics");
swagger_desc(connections) -> ?DESC("connections");
swagger_desc(live_connections) -> ?DESC("live_connections");
swagger_desc(cluster_sessions) -> ?DESC("cluster_sessions");
swagger_desc(received_msg_rate) -> ?DESC("received_msg_rate");
swagger_desc(sent_msg_rate) -> ?DESC("sent_msg_rate");
swagger_desc(dropped_msg_rate) -> ?DESC("dropped_msg_rate");
swagger_desc(validation_succeeded_rate) -> ?DESC("validation_succeeded_rate");
swagger_desc(validation_failed_rate) -> ?DESC("validation_failed_rate");
swagger_desc(transformation_succeeded_rate) -> ?DESC("transformation_succeeded_rate");
swagger_desc(transformation_failed_rate) -> ?DESC("transformation_failed_rate");
swagger_desc(persisted_rate) -> ?DESC("persisted_rate");
swagger_desc(retained_msg_count) -> ?DESC("retained_msg_count");
swagger_desc(shared_subscriptions) -> ?DESC("shared_subscriptions");
swagger_desc(node_uptime) -> ?DESC("node_uptime");
swagger_desc(license_quota) -> ?DESC("license_quota").

maybe_reject_cluster_only_metrics(<<"all">>, Rates) ->
    Rates;
maybe_reject_cluster_only_metrics(_Node, Rates) ->
    maps:without(?CLUSTERONLY_SAMPLER_LIST, Rates).
