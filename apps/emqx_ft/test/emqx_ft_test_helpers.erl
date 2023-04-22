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

start_additional_node(Config, Name) ->
    emqx_common_test_helpers:start_slave(
        Name,
        [
            {apps, [emqx_ft]},
            {join_to, node()},
            {configure_gen_rpc, true},
            {env_handler, env_handler(Config)}
        ]
    ).

stop_additional_node(Node) ->
    ok = rpc:call(Node, ekka, leave, []),
    ok = rpc:call(Node, emqx_common_test_helpers, stop_apps, [[emqx_ft]]),
    ok = emqx_common_test_helpers:stop_slave(Node),
    ok.

env_handler(Config) ->
    fun
        (emqx_ft) ->
            load_config(#{storage => local_storage(Config)});
        (_) ->
            ok
    end.

local_storage(Config) ->
    #{
        type => local,
        segments => #{
            root => root(Config, node(), [segments])
        },
        exporter => #{
            type => local,
            root => root(Config, node(), [exports])
        }
    }.

load_config(Config) ->
    emqx_common_test_helpers:load_config(emqx_ft_schema, #{file_transfer => Config}).

tcp_port(Node) ->
    {_, Port} = rpc:call(Node, emqx_config, get, [[listeners, tcp, default, bind]]),
    Port.

root(Config, Node, Tail) ->
    filename:join([?config(priv_dir, Config), "file_transfer", Node | Tail]).

upload_file(ClientId, FileId, Name, Data) ->
    upload_file(ClientId, FileId, Name, Data, node()).

upload_file(ClientId, FileId, Name, Data, Node) ->
    Port = tcp_port(Node),
    Size = byte_size(Data),

    {ok, C1} = emqtt:start_link([{proto_ver, v5}, {clientid, ClientId}, {port, Port}]),
    {ok, _} = emqtt:connect(C1),
    Meta = #{
        name => Name,
        expire_at => erlang:system_time(_Unit = second) + 3600,
        size => Size
    },
    MetaPayload = emqx_utils_json:encode(emqx_ft:encode_filemeta(Meta)),

    ct:pal("MetaPayload = ~ts", [MetaPayload]),

    MetaTopic = <<"$file/", FileId/binary, "/init">>,
    {ok, _} = emqtt:publish(C1, MetaTopic, MetaPayload, 1),
    {ok, _} = emqtt:publish(C1, <<"$file/", FileId/binary, "/0">>, Data, 1),

    FinTopic = <<"$file/", FileId/binary, "/fin/", (integer_to_binary(Size))/binary>>,
    {ok, _} = emqtt:publish(C1, FinTopic, <<>>, 1),
    ok = emqtt:stop(C1).
