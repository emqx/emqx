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

-import(emqx_mgmt_util, [ schema/2
                        , object_schema/2
                        , object_array_schema/2
                        , error_schema/2
                        , properties/1
                        ]).

-export([api_spec/0]).

-export([ nodes/2
        , node/2
        , node_metrics/2
        , node_stats/2]).

-include_lib("emqx/include/emqx.hrl").

api_spec() ->
    {apis(), []}.

apis() ->
    [ nodes_api()
    , node_api()
    , node_metrics_api()
    , node_stats_api()].

properties() ->
    properties([
        {node, string, <<"Node name">>},
        {connections, integer, <<"Number of clients currently connected to this node">>},
        {load1, string, <<"CPU average load in 1 minute">>},
        {load5, string, <<"CPU average load in 5 minute">>},
        {load15, string, <<"CPU average load in 15 minute">>},
        {max_fds, integer, <<"Maximum file descriptor limit for the operating system">>},
        {memory_total, string, <<"VM allocated system memory">>},
        {memory_used, string, <<"VM occupied system memory">>},
        {node_status, string, <<"Node status">>},
        {otp_release, string, <<"Erlang/OTP version used by EMQX Broker">>},
        {process_available, integer, <<"Number of available processes">>},
        {process_used, integer, <<"Number of used processes">>},
        {uptime, integer, <<"EMQX Broker runtime, millisecond">>},
        {version, string, <<"EMQX Broker version">>},
        {sys_path, string, <<"EMQX system file location">>},
        {log_path, string, <<"EMQX log file location">>},
        {config_path, string, <<"EMQX config file location">>},
        {role, string, <<"Node role">>}
    ]).

parameters() ->
    [#{
        name => node_name,
        in => path,
        description => <<"node name">>,
        schema => #{type => string},
        required => true,
        example => node()
    }].
nodes_api() ->
    Metadata = #{
        get => #{
            description => <<"List EMQX nodes">>,
            responses => #{
                <<"200">> => object_array_schema(properties(), <<"List EMQX Nodes">>)
            }
        }
    },
    {"/nodes", Metadata, nodes}.

node_api() ->
    Metadata = #{
        get => #{
            description => <<"Get node info">>,
            parameters => parameters(),
            responses => #{
                <<"400">> => error_schema(<<"Node error">>, ['SOURCE_ERROR']),
                <<"200">> => object_schema(properties(), <<"Get EMQX Nodes info by name">>)}}},
    {"/nodes/:node_name", Metadata, node}.

node_metrics_api() ->
    Metadata = #{
        get => #{
            description => <<"Get node metrics">>,
            parameters => parameters(),
            responses => #{
                <<"400">> => error_schema(<<"Node error">>, ['SOURCE_ERROR']),
                %% TODO: Node Metrics Schema
                <<"200">> => schema(metrics, <<"Get EMQX Node Metrics">>)}}},
    {"/nodes/:node_name/metrics", Metadata, node_metrics}.

node_stats_api() ->
    Metadata = #{
        get => #{
            description => <<"Get node stats">>,
            parameters => parameters(),
            responses => #{
                <<"400">> => error_schema(<<"Node error">>, ['SOURCE_ERROR']),
                %% TODO: Node Stats Schema
                <<"200">> => schema(stat, <<"Get EMQX Node Stats">>)}}},
    {"/nodes/:node_name/stats", Metadata, node_stats}.

%%%==============================================================================================
%% parameters trans
nodes(get, _Params) ->
    list(#{}).

node(get, #{bindings := #{node_name := NodeName}}) ->
    get_node(binary_to_atom(NodeName, utf8)).

node_metrics(get, #{bindings := #{node_name := NodeName}}) ->
    get_metrics(binary_to_atom(NodeName, utf8)).

node_stats(get, #{bindings := #{node_name := NodeName}}) ->
    get_stats(binary_to_atom(NodeName, utf8)).

%%%==============================================================================================
%% api apply
list(#{}) ->
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

%%============================================================================================================
%% internal function

format(_Node, Info = #{memory_total := Total, memory_used := Used}) ->
    {ok, SysPathBinary} = file:get_cwd(),
    SysPath = list_to_binary(SysPathBinary),
    ConfigPath = <<SysPath/binary, "/etc/emqx.conf">>,
    LogPath = case log_path() of
                  undefined ->
                      <<"not found">>;
                  Path0 ->
                      Path = list_to_binary(Path0),
                      <<SysPath/binary, Path/binary>>
              end,
    Info#{ memory_total := emqx_mgmt_util:kmg(Total)
         , memory_used := emqx_mgmt_util:kmg(Used)
         , sys_path => SysPath
         , config_path => ConfigPath
         , log_path => LogPath}.

log_path() ->
    Configs = logger:get_handler_config(),
    get_log_path(Configs).

get_log_path([#{id := file} = LoggerConfig | _LoggerConfigs]) ->
    Config = maps:get(config, LoggerConfig),
    maps:get(file, Config);
get_log_path([_LoggerConfig | LoggerConfigs]) ->
    get_log_path(LoggerConfigs);
get_log_path([]) ->
    undefined.
