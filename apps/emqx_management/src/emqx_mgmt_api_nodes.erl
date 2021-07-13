%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-behavior(minirest_api).

-export([api_spec/0]).

-export([ nodes/2
        , node/2
        , node_metrics/2
        , node_stats/2]).

-include_lib("emqx/include/emqx.hrl").

api_spec() ->
    {apis(), schemas()}.

apis() ->
    [ nodes_api()
    , node_api()
    , node_metrics_api()
    , node_stats_api()].

schemas() ->
    %% notice: node api used schema metrics and stats
    %% see these schema in emqx_mgmt_api_metrics emqx_mgmt_api_status
    [node_schema()].

node_schema() ->
    DefinitionName = <<"node">>,
    DefinitionProperties = #{
        <<"node">> => #{
            type => <<"string">>,
            description => <<"Node name">>},
        <<"connections">> => #{
            type => <<"integer">>,
            description => <<"Number of clients currently connected to this node">>},
        <<"load1">> => #{
            type => <<"string">>,
            description => <<"CPU average load in 1 minute">>},
        <<"load5">> => #{
            type => <<"string">>,
            description => <<"CPU average load in 5 minute">>},
        <<"load15">> => #{
            type => <<"string">>,
            description => <<"CPU average load in 15 minute">>},
        <<"max_fds">> => #{
            type => <<"integer">>,
            description => <<"Maximum file descriptor limit for the operating system">>},
        <<"memory_total">> => #{
            type => <<"string">>,
            description => <<"VM allocated system memory">>},
        <<"memory_used">> => #{
            type => <<"string">>,
            description => <<"VM occupied system memory">>},
        <<"node_status">> => #{
            type => <<"string">>,
            description => <<"Node status">>},
        <<"otp_release">> => #{
            type => <<"string">>,
            description => <<"Erlang/OTP version used by EMQ X Broker">>},
        <<"process_available">> => #{
            type => <<"integer">>,
            description => <<"Number of available processes">>},
        <<"process_used">> => #{
            type => <<"integer">>,
            description => <<"Number of used processes">>},
        <<"uptime">> => #{
            type => <<"string">>,
            description => <<"EMQ X Broker runtime">>},
        <<"version">> => #{
            type => <<"string">>,
            description => <<"EMQ X Broker version">>},
        <<"sys_path">> => #{
            type => <<"string">>,
            description => <<"EMQ X system file location">>},
        <<"log_path">> => #{
            type => <<"string">>,
            description => <<"EMQ X log file location">>},
        <<"config_path">> => #{
            type => <<"string">>,
            description => <<"EMQ X config file location">>}
    },
    {DefinitionName, DefinitionProperties}.

nodes_api() ->
    Metadata = #{
        get => #{
            description => "List EMQ X nodes",
            responses => #{
                <<"200">> => #{description => <<"List EMQ X Nodes">>,
                    schema => #{
                        type => array,
                        items => cowboy_swagger:schema(<<"node">>)}}}}},
    {"/nodes", Metadata, nodes}.

node_api() ->
    Metadata = #{
        get => #{
            description => "Get node info",
            parameters => [#{
                name => node_name,
                in => path,
                description => "node name",
                type => string,
                required => true,
                default => node()}],
            responses => #{
                <<"400">> =>
                emqx_mgmt_util:not_found_schema(<<"Node error">>, [<<"SOURCE_ERROR">>]),
                <<"200">> => #{
                    description => <<"Get EMQ X Nodes info by name">>,
                    schema => cowboy_swagger:schema(<<"node">>)}}}},
    {"/nodes/:node_name", Metadata, node}.

node_metrics_api() ->
    Metadata = #{
        get => #{
            description => "Get node metrics",
            parameters => [#{
                name => node_name,
                in => path,
                description => "node name",
                type => string,
                required => true,
                default => node()}],
            responses => #{
                <<"400">> =>
                emqx_mgmt_util:not_found_schema(<<"Node error">>, [<<"SOURCE_ERROR">>]),
                <<"200">> => #{
                    description => <<"Get EMQ X Node Metrics">>,
                    schema => cowboy_swagger:schema(<<"metrics">>)}}}},
    {"/nodes/:node_name/metrics", Metadata, node_metrics}.

node_stats_api() ->
    Metadata = #{
        get => #{
            description => "Get node stats",
            parameters => [#{
                name => node_name,
                in => path,
                description => "node name",
                type => string,
                required => true,
                default => node()}],
            responses => #{
                <<"400">> =>
                emqx_mgmt_util:not_found_schema(<<"Node error">>, [<<"SOURCE_ERROR">>]),
                <<"200">> => #{
                    description => <<"Get EMQ X Node Stats">>,
                    schema => cowboy_swagger:schema(<<"stats">>)}}}},
    {"/nodes/:node_name/stats", Metadata, node_metrics}.

%%%==============================================================================================
%% parameters trans
nodes(get, _Request) ->
    list(#{}).

node(get, Request) ->
    Params = node_name_path_parameter(Request),
    get_node(Params).

node_metrics(get, Request) ->
    Params = node_name_path_parameter(Request),
    get_metrics(Params).

node_stats(get, Request) ->
    Params = node_name_path_parameter(Request),
    get_stats(Params).

%%%==============================================================================================
%% api apply
list(#{}) ->
    NodesInfo = [format(Node, NodeInfo) || {Node, NodeInfo} <- emqx_mgmt:list_nodes()],
    Response = emqx_json:encode(NodesInfo),
    {200, Response}.

get_node(#{node := Node}) ->
    case emqx_mgmt:lookup_node(Node) of
        #{node_status := 'ERROR'} ->
            {400, emqx_json:encode(#{code => 'SOURCE_ERROR', reason => <<"rpc_failed">>})};
        NodeInfo ->
            Response = emqx_json:encode(format(Node, NodeInfo)),
            {200, Response}
    end.

get_metrics(#{node := Node}) ->
    case emqx_mgmt:get_metrics(Node) of
        {error, _} ->
            {400, emqx_json:encode(#{code => 'SOURCE_ERROR', reason => <<"rpc_failed">>})};
        Metrics ->
            {200, emqx_json:encode(Metrics)}
    end.

get_stats(#{node := Node}) ->
    case emqx_mgmt:get_stats(Node) of
        {error, _} ->
            {400, emqx_json:encode(#{code => 'SOURCE_ERROR', reason => <<"rpc_failed">>})};
        Stats ->
            {200, emqx_json:encode(Stats)}
    end.

%%============================================================================================================
%% internal function
node_name_path_parameter(Request) ->
    NodeName = cowboy_req:binding(node_name, Request),
    Node = binary_to_atom(NodeName, utf8),
    #{node => Node}.

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
