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
-module(emqx_mgmt_api_nodes).

%% API
-export([ rest_schema/0
        , rest_api/0]).

-export([ handle_list/1
        , handle_get/1]).

% for rpc
-export([node_info/1]).

-include_lib("emqx/include/emqx.hrl").

rest_schema() ->
    [node_schema()].

rest_api() ->
    [ nodes_api()
    , node_api()].

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
            description => <<"EMQ X Broker version">>}},
    {DefinitionName, DefinitionProperties}.

nodes_api() ->
    Metadata = #{
        get => #{
            tags => ["system"],
            description => "List EMQ X nodes",
            operationId => handle_list,
            responses => #{
                <<"200">> => #{description => <<"List EMQ X Nodes">>,
                    schema => #{
                        type => array,
                        items => cowboy_swagger:schema(<<"node">>)}}},
            security => [#{application => []}]}},
    {"/nodes", Metadata}.

node_api() ->
    Metadata = #{
        get => #{
            tags => ["system"],
            description => "Get node info",
            operationId => handle_get,
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
                    schema => cowboy_swagger:schema(<<"node">>)}},
            security => [#{application => []}]}},
    {"/nodes/:node_name", Metadata}.

%%%==============================================================================================
%% parameters trans
handle_list(_Request) ->
    list(#{}).

handle_get(Request) ->
    NodeName = cowboy_req:binding(node_name, Request),
    Node = binary_to_atom(NodeName, utf8),
    get_node(#{node => Node}).

%%%==============================================================================================
%% api apply
list(#{}) ->
    Response = emqx_json:encode(list_nodes()),
    {200, Response}.

get_node(#{node := Node}) ->
    case lookup_node(Node) of
        #{node_status := 'ERROR'} ->
            {400, emqx_json:encode(#{code => 'SOURCE_ERROR', reason => <<"rpc_failed">>})};
        NodeInfo ->
            Response = emqx_json:encode(NodeInfo),
            {200, Response}
    end.

%%============================================================================================================
%% internal function
list_nodes() ->
    Running = mnesia:system_info(running_db_nodes),
    Stopped = mnesia:system_info(db_nodes) -- Running,
    DownNodes = lists:map(fun stopped_node_info/1, Stopped),
    [node_info(Node) || Node <- Running] ++ DownNodes.

lookup_node(Node) -> node_info(Node).

%% format
node_info(Node) when Node =:= node() ->
    Memory  = emqx_vm:get_memory(),
    Info = maps:from_list([{K, list_to_binary(V)} || {K, V} <- emqx_vm:loads()]),
    BrokerInfo = emqx_sys:info(),
    Info#{
        node              => node(),
        otp_release       => iolist_to_binary(lists:concat([emqx_vm:get_otp_version(), "/", erlang:system_info(version)])),
        memory_total      => proplists:get_value(allocated, Memory),
        memory_used       => proplists:get_value(used, Memory),
        process_available => erlang:system_info(process_limit),
        process_used      => erlang:system_info(process_count),
        max_fds           => proplists:get_value(max_fds, lists:usort(lists:flatten(erlang:system_info(check_io)))),
        connections       => ets:info(emqx_channel, size),
        node_status       => 'Running',
        uptime            => iolist_to_binary(proplists:get_value(uptime, BrokerInfo)),
        version           => iolist_to_binary(proplists:get_value(version, BrokerInfo))
    };
node_info(Node) ->
    case rpc:call(Node, ?MODULE, ?FUNCTION_NAME, [Node]) of
        {badrpc, _Reason} ->
            #{node => Node, node_status => 'ERROR'};
        Res ->
            Res
    end.

stopped_node_info(Node) ->
    #{node => Node, node_status => 'Stopped'}.
