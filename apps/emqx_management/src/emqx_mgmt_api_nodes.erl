%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_mgmt_api_nodes).

-behaviour(minirest_api).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-import(hoconsc, [mk/2, ref/1, ref/2, enum/1, array/1]).

-define(NODE_METRICS_MODULE, emqx_mgmt_api_metrics).
-define(NODE_STATS_MODULE, emqx_mgmt_api_stats).

%% Swagger specs from hocon schema
-export([
    api_spec/0,
    schema/1,
    paths/0,
    fields/1,
    namespace/0
]).

%% API callbacks
-export([
    nodes/2,
    node/2,
    node_metrics/2,
    node_stats/2
]).

%%--------------------------------------------------------------------
%% API spec funcs
%%--------------------------------------------------------------------

namespace() -> undefined.

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    [
        "/nodes",
        "/nodes/:node",
        "/nodes/:node/metrics",
        "/nodes/:node/stats"
    ].

schema("/nodes") ->
    #{
        'operationId' => nodes,
        get =>
            #{
                description => ?DESC(list_nodes),
                tags => [<<"Nodes">>],
                responses =>
                    #{
                        200 => mk(
                            array(ref(node_info)),
                            #{desc => ?DESC("list_nodes")}
                        )
                    }
            }
    };
schema("/nodes/:node") ->
    #{
        'operationId' => node,
        get =>
            #{
                description => ?DESC(get_node_info),
                tags => [<<"Nodes">>],
                parameters => [ref(node_name)],
                responses =>
                    #{
                        200 => mk(
                            ref(node_info),
                            #{desc => ?DESC("get_node_info_success")}
                        ),
                        404 => not_found()
                    }
            }
    };
schema("/nodes/:node/metrics") ->
    #{
        'operationId' => node_metrics,
        get =>
            #{
                description => ?DESC(get_node_metrics),
                tags => [<<"Nodes">>],
                parameters => [ref(node_name)],
                responses =>
                    #{
                        200 => mk(
                            ref(?NODE_METRICS_MODULE, node_metrics),
                            #{desc => ?DESC("get_node_metrics_success")}
                        ),
                        404 => not_found()
                    }
            }
    };
schema("/nodes/:node/stats") ->
    #{
        'operationId' => node_stats,
        get =>
            #{
                description => ?DESC(get_node_stats),
                tags => [<<"Nodes">>],
                parameters => [ref(node_name)],
                responses =>
                    #{
                        200 => mk(
                            ref(?NODE_STATS_MODULE, aggregated_data),
                            #{desc => ?DESC("get_node_stats_success")}
                        ),
                        404 => not_found()
                    }
            }
    }.

%%--------------------------------------------------------------------
%% Fields

fields(node_name) ->
    [
        {node,
            mk(
                binary(),
                #{
                    in => path,
                    description => ?DESC("node_name"),
                    required => true,
                    example => <<"emqx@127.0.0.1">>
                }
            )}
    ];
fields(node_info) ->
    [
        {node,
            mk(
                atom(),
                #{desc => ?DESC("node_name"), example => <<"emqx@127.0.0.1">>}
            )},
        {connections,
            mk(
                non_neg_integer(),
                #{desc => ?DESC("clients_session_count"), example => 0}
            )},
        {live_connections,
            mk(
                non_neg_integer(),
                #{desc => ?DESC("live_connections_count"), example => 0}
            )},
        {cluster_sessions,
            mk(
                non_neg_integer(),
                #{
                    desc => ?DESC("cluster_sessions_desc"),
                    example => 0
                }
            )},
        {load1,
            mk(
                float(),
                #{desc => ?DESC("cpu_load_1min"), example => 2.66}
            )},
        {load5,
            mk(
                float(),
                #{desc => ?DESC("cpu_load_5min"), example => 2.66}
            )},
        {load15,
            mk(
                float(),
                #{desc => ?DESC("cpu_load_15min"), example => 2.66}
            )},
        {max_fds,
            mk(
                non_neg_integer(),
                #{desc => ?DESC("file_descriptors_limit"), example => 1024}
            )},
        {memory_total,
            mk(
                emqx_schema:bytesize(),
                #{desc => ?DESC("allocated_memory"), example => "512.00M"}
            )},
        {memory_used,
            mk(
                emqx_schema:bytesize(),
                #{desc => ?DESC("used_memory"), example => "256.00M"}
            )},
        {node_status,
            mk(
                enum(['running', 'stopped']),
                #{desc => ?DESC("node_status"), example => "running"}
            )},
        {otp_release,
            mk(
                string(),
                #{desc => ?DESC("erlang_otp_version"), example => "24.2/12.2"}
            )},
        {process_available,
            mk(
                non_neg_integer(),
                #{desc => ?DESC("erlang_processes_limit"), example => 2097152}
            )},
        {process_used,
            mk(
                non_neg_integer(),
                #{desc => ?DESC("running_erlang_processes"), example => 1024}
            )},
        {uptime,
            mk(
                non_neg_integer(),
                #{desc => ?DESC("system_uptime"), example => 5120000}
            )},
        {version,
            mk(
                string(),
                #{desc => ?DESC("release_version"), example => "5.0.0"}
            )},
        {edition,
            mk(
                enum(['Enterprise']),
                #{desc => ?DESC("release_edition"), example => "Enterprise"}
            )},
        {sys_path,
            mk(
                string(),
                #{desc => ?DESC("system_files_path"), example => "path/to/emqx"}
            )},
        {log_path,
            mk(
                string(),
                #{
                    desc => ?DESC("log_files_path"),
                    example => "path/to/log | The log path is not yet set"
                }
            )},
        {role,
            mk(
                enum([core, replicant]),
                #{desc => ?DESC("node_role"), example => "core"}
            )}
    ].

%%--------------------------------------------------------------------
%% API Handler funcs
%%--------------------------------------------------------------------

nodes(get, _Params) ->
    list_nodes(#{}).

node(get, #{bindings := #{node := NodeName}}) ->
    with_node(NodeName, to_ok_result_fun(fun get_node/1)).

node_metrics(get, #{bindings := #{node := NodeName}}) ->
    with_node(NodeName, to_ok_result_fun(fun emqx_mgmt:get_metrics/1)).

node_stats(get, #{bindings := #{node := NodeName}}) ->
    with_node(NodeName, to_ok_result_fun(fun emqx_mgmt:get_stats/1)).

%%--------------------------------------------------------------------
%% api apply

list_nodes(#{}) ->
    NodesInfo = [format(NodeInfo) || {_Node, NodeInfo} <- emqx_mgmt:list_nodes()],
    {200, NodesInfo}.

get_node(Node) ->
    format(emqx_mgmt:lookup_node(Node)).

%%--------------------------------------------------------------------
%% internal function

format(Info = #{memory_total := Total, memory_used := Used}) ->
    Info#{
        memory_total := emqx_mgmt_util:kmg(Total),
        memory_used := emqx_mgmt_util:kmg(Used)
    };
format(Info) when is_map(Info) ->
    Info.

to_ok_result({error, _} = Error) ->
    Error;
to_ok_result({ok, _} = Ok) ->
    Ok;
to_ok_result(Result) ->
    {ok, Result}.

to_ok_result_fun(Fun) when is_function(Fun) ->
    fun(Arg) ->
        to_ok_result(Fun(Arg))
    end.

not_found() ->
    emqx_dashboard_swagger:error_codes(['NOT_FOUND'], ?DESC("node_not_found")).

with_node(Name, Fn) ->
    emqx_mgmt_api_lib:with_node(Name, Fn).
