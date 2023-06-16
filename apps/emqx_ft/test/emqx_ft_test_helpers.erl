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

-define(S3_HOST, <<"minio">>).
-define(S3_PORT, 9000).

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
    _ = rpc:call(Node, ekka, leave, []),
    ok = rpc:call(Node, emqx_common_test_helpers, stop_apps, [[emqx_ft]]),
    ok = emqx_common_test_helpers:stop_slave(Node),
    ok.

env_handler(Config) ->
    fun
        (emqx_ft) ->
            load_config(#{<<"enable">> => true, <<"storage">> => local_storage(Config)});
        (_) ->
            ok
    end.

local_storage(Config) ->
    local_storage(Config, #{exporter => local}).

local_storage(Config, Opts) ->
    #{
        <<"local">> => #{
            <<"enable">> => true,
            <<"segments">> => #{<<"root">> => root(Config, node(), [segments])},
            <<"exporter">> => exporter(Config, Opts)
        }
    }.

exporter(Config, #{exporter := local}) ->
    #{
        <<"local">> => #{
            <<"enable">> => true,
            <<"root">> => root(Config, node(), [exports])
        }
    };
exporter(_Config, #{exporter := s3, bucket_name := BucketName}) ->
    BaseConfig = emqx_s3_test_helpers:base_raw_config(tcp),
    #{
        <<"s3">> => BaseConfig#{
            <<"enable">> => true,
            <<"bucket">> => list_to_binary(BucketName),
            <<"host">> => ?S3_HOST,
            <<"port">> => ?S3_PORT
        }
    }.

load_config(Config) ->
    emqx_common_test_helpers:load_config(emqx_ft_schema, #{<<"file_transfer">> => Config}).

tcp_port(Node) ->
    {_, Port} = rpc:call(Node, emqx_config, get, [[listeners, tcp, default, bind]]),
    Port.

root(Config, Node, Tail) ->
    iolist_to_binary(filename:join([?config(priv_dir, Config), "file_transfer", Node | Tail])).

start_client(ClientId) ->
    start_client(ClientId, node()).

start_client(ClientId, Node) ->
    Port = tcp_port(Node),
    {ok, Client} = emqtt:start_link([{proto_ver, v5}, {clientid, ClientId}, {port, Port}]),
    {ok, _} = emqtt:connect(Client),
    Client.

upload_file(ClientId, FileId, Name, Data) ->
    upload_file(ClientId, FileId, Name, Data, node()).

upload_file(ClientId, FileId, Name, Data, Node) ->
    C1 = start_client(ClientId, Node),

    Size = byte_size(Data),
    Meta = #{
        name => Name,
        expire_at => erlang:system_time(_Unit = second) + 3600,
        size => Size
    },
    MetaPayload = emqx_utils_json:encode(emqx_ft:encode_filemeta(Meta)),

    ct:pal("MetaPayload = ~ts", [MetaPayload]),

    MetaTopic = <<"$file/", FileId/binary, "/init">>,
    {ok, #{reason_code_name := success}} = emqtt:publish(C1, MetaTopic, MetaPayload, 1),
    {ok, #{reason_code_name := success}} = emqtt:publish(
        C1, <<"$file/", FileId/binary, "/0">>, Data, 1
    ),

    FinTopic = <<"$file/", FileId/binary, "/fin/", (integer_to_binary(Size))/binary>>,
    FinResult =
        case emqtt:publish(C1, FinTopic, <<>>, 1) of
            {ok, #{reason_code_name := success}} ->
                ok;
            {ok, #{reason_code_name := Error}} ->
                {error, Error}
        end,
    ok = emqtt:stop(C1),
    FinResult.

aws_config() ->
    emqx_s3_test_helpers:aws_config(tcp, binary_to_list(?S3_HOST), ?S3_PORT).
