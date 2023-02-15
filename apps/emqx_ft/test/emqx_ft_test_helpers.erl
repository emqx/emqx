%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_ft_test_helpers).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").

start_additional_node(Config, Node) ->
    SelfNode = node(),
    emqx_common_test_helpers:start_slave(
        Node,
        [
            {apps, [emqx_ft]},
            {join_to, SelfNode},
            {configure_gen_rpc, false},
            {env_handler, fun
                (emqx_ft) ->
                    ok = emqx_config:put([file_transfer, storage], #{
                        type => local, root => ft_root(Config, node())
                    });
                (_) ->
                    ok
            end}
        ]
    ).

stop_additional_node(Config) ->
    Node = ?config(additional_node, Config),
    ok = rpc:call(Node, ekka, leave, []),
    ok = rpc:call(Node, emqx_common_test_helpers, stop_apps, [[emqx_ft]]),
    {ok, _} = emqx_common_test_helpers:stop_slave(Node),
    ok.

tcp_port(Node) ->
    {_, Port} = rpc:call(Node, emqx_config, get, [[listeners, tcp, default, bind]]),
    Port.

ft_root(Config, Node) ->
    filename:join([
        ?config(priv_dir, Config), <<"file_transfer">>, atom_to_binary(Node)
    ]).

upload_file(ClientId, FileId, Data, Node) ->
    Port = tcp_port(Node),

    {ok, C1} = emqtt:start_link([{proto_ver, v5}, {clientid, ClientId}, {port, Port}]),
    {ok, _} = emqtt:connect(C1),
    Meta = #{
        name => FileId,
        expire_at => erlang:system_time(_Unit = second) + 3600,
        size => byte_size(Data)
    },
    MetaPayload = emqx_json:encode(Meta),

    MetaTopic = <<"$file/", FileId/binary, "/init">>,
    {ok, _} = emqtt:publish(C1, MetaTopic, MetaPayload, 1),
    {ok, _} = emqtt:publish(C1, <<"$file/", FileId/binary, "/0">>, Data, 1),

    FinTopic = <<"$file/", FileId/binary, "/fin">>,
    {ok, _} = emqtt:publish(C1, FinTopic, <<>>, 1),
    ok = emqtt:stop(C1).
