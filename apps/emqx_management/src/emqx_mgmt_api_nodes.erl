%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_mgmt_api_nodes).

-behaviour(minirest_api).

-include_lib("emqx/include/emqx.hrl").
-include_lib("typerefl/include/types.hrl").

-import(hoconsc, [mk/2, ref/1, ref/2, enum/1, array/1]).

-define(NODE_METRICS_MODULE, emqx_mgmt_api_metrics).
-define(NODE_STATS_MODULE, emqx_mgmt_api_stats).

-define(SOURCE_ERROR, 'SOURCE_ERROR').

%% Swagger specs from hocon schema
-export([
    api_spec/0,
    schema/1,
    paths/0,
    fields/1
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
                description => <<"List EMQX nodes">>,
                responses =>
                    #{
                        200 => mk(
                            array(ref(node_info)),
                            #{desc => <<"List all EMQX nodes">>}
                        )
                    }
            }
    };
schema("/nodes/:node") ->
    #{
        'operationId' => node,
        get =>
            #{
                description => <<"Get node info">>,
                parameters => [ref(node_name)],
                responses =>
                    #{
                        200 => mk(
                            ref(node_info),
                            #{desc => <<"Get node info successfully">>}
                        ),
                        400 => node_error()
                    }
            }
    };
schema("/nodes/:node/metrics") ->
    #{
        'operationId' => node_metrics,
        get =>
            #{
                description => <<"Get node metrics">>,
                parameters => [ref(node_name)],
                responses =>
                    #{
                        200 => mk(
                            ref(?NODE_METRICS_MODULE, node_metrics),
                            #{desc => <<"Get node metrics successfully">>}
                        ),
                        400 => node_error()
                    }
            }
    };
schema("/nodes/:node/stats") ->
    #{
        'operationId' => node_stats,
        get =>
            #{
                description => <<"Get node stats">>,
                parameters => [ref(node_name)],
                responses =>
                    #{
                        200 => mk(
                            ref(?NODE_STATS_MODULE, node_stats_data),
                            #{desc => <<"Get node stats successfully">>}
                        ),
                        400 => node_error()
                    }
            }
    }.

%%--------------------------------------------------------------------
%% Fields

fields(node_name) ->
    [
        {node,
            mk(
                atom(),
                #{
                    in => path,
                    description => <<"Node name">>,
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
                #{desc => <<"Node name">>, example => <<"emqx@127.0.0.1">>}
            )},
        {connections,
            mk(
                non_neg_integer(),
                #{desc => <<"Number of clients currently connected to this node">>, example => 0}
            )},
        {load1,
            mk(
                string(),
                #{desc => <<"CPU average load in 1 minute">>, example => "2.66"}
            )},
        {load5,
            mk(
                string(),
                #{desc => <<"CPU average load in 5 minute">>, example => "2.66"}
            )},
        {load15,
            mk(
                string(),
                #{desc => <<"CPU average load in 15 minute">>, example => "2.66"}
            )},
        {max_fds,
            mk(
                non_neg_integer(),
                #{desc => <<"File descriptors limit">>, example => 1024}
            )},
        {memory_total,
            mk(
                emqx_schema:bytesize(),
                #{desc => <<"Allocated memory">>, example => "512.00M"}
            )},
        {memory_used,
            mk(
                emqx_schema:bytesize(),
                #{desc => <<"Used memory">>, example => "256.00M"}
            )},
        {node_status,
            mk(
                enum(['Running', 'Stopped']),
                #{desc => <<"Node status">>, example => "Running"}
            )},
        {otp_release,
            mk(
                string(),
                #{desc => <<"Erlang/OTP version">>, example => "24.2/12.2"}
            )},
        {process_available,
            mk(
                non_neg_integer(),
                #{desc => <<"Erlang processes limit">>, example => 2097152}
            )},
        {process_used,
            mk(
                non_neg_integer(),
                #{desc => <<"Running Erlang processes">>, example => 1024}
            )},
        {uptime,
            mk(
                non_neg_integer(),
                #{desc => <<"System uptime, milliseconds">>, example => 5120000}
            )},
        {version,
            mk(
                string(),
                #{desc => <<"Release version">>, example => "5.0.0-beat.3-00000000"}
            )},
        {sys_path,
            mk(
                string(),
                #{desc => <<"Path to system files">>, example => "path/to/emqx"}
            )},
        {log_path,
            mk(
                string(),
                #{
                    desc => <<"Path to log files">>,
                    example => "path/to/log | The log path is not yet set"
                }
            )},
        {role,
            mk(
                enum([core, replicant]),
                #{desc => <<"Node role">>, example => "core"}
            )}
    ].

%%--------------------------------------------------------------------
%% API Handler funcs
%%--------------------------------------------------------------------

nodes(get, _Params) ->
    list_nodes(#{}).

node(get, #{bindings := #{node := NodeName}}) ->
    get_node(NodeName).

node_metrics(get, #{bindings := #{node := NodeName}}) ->
    get_metrics(NodeName).

node_stats(get, #{bindings := #{node := NodeName}}) ->
    get_stats(NodeName).

%%--------------------------------------------------------------------
%% api apply

list_nodes(#{}) ->
    NodesInfo = [format(Node, NodeInfo) || {Node, NodeInfo} <- emqx_mgmt:list_nodes()],
    {200, NodesInfo}.

get_node(Node) ->
    case emqx_mgmt:lookup_node(Node) of
        {error, _} ->
            {400, #{code => 'SOURCE_ERROR', message => <<"rpc_failed">>}};
        NodeInfo ->
            {200, format(Node, NodeInfo)}
    end.

get_metrics(Node) ->
    case emqx_mgmt:get_metrics(Node) of
        {error, _} ->
            {400, #{code => 'SOURCE_ERROR', message => <<"rpc_failed">>}};
        Metrics ->
            {200, Metrics}
    end.

get_stats(Node) ->
    case emqx_mgmt:get_stats(Node) of
        {error, _} ->
            {400, #{code => 'SOURCE_ERROR', message => <<"rpc_failed">>}};
        Stats ->
            {200, Stats}
    end.

%%--------------------------------------------------------------------
%% internal function

format(_Node, Info = #{memory_total := Total, memory_used := Used}) ->
    {ok, SysPathBinary} = file:get_cwd(),
    SysPath = list_to_binary(SysPathBinary),
    LogPath =
        case log_path() of
            undefined ->
                <<"log.file_handler.default.enable is false,only log to console">>;
            Path ->
                filename:join(SysPath, Path)
        end,
    Info#{
        memory_total := emqx_mgmt_util:kmg(Total),
        memory_used := emqx_mgmt_util:kmg(Used),
        sys_path => SysPath,
        log_path => LogPath
    }.

log_path() ->
    Configs = logger:get_handler_config(),
    get_log_path(Configs).

get_log_path([#{config := #{file := Path}} | _LoggerConfigs]) ->
    filename:dirname(Path);
get_log_path([_LoggerConfig | LoggerConfigs]) ->
    get_log_path(LoggerConfigs);
get_log_path([]) ->
    undefined.

node_error() ->
    emqx_dashboard_swagger:error_codes([?SOURCE_ERROR], <<"Node error">>).
