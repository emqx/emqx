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

-rest_api(#{name   => list_nodes,
            method => 'GET',
            path   => "/nodes/",
            func   => list,
            descr  => "A list of nodes in the cluster"}).

-rest_api(#{name   => get_node,
            method => 'GET',
            path   => "/nodes/:atom:node",
            func   => get,
            descr  => "Lookup a node in the cluster"}).

-export([ list/2
        , get/2
        ]).

list(_Bindings, _Params) ->
    emqx_mgmt:return({ok, [format(Node, Info) || {Node, Info} <- emqx_mgmt:list_nodes()]}).

get(#{node := Node}, _Params) ->
    emqx_mgmt:return({ok, emqx_mgmt:lookup_node(Node)}).

format(Node, {error, Reason}) -> #{node => Node, error => Reason};

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
