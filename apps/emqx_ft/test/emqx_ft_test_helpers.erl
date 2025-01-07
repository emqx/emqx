%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

config(Storage) ->
    config(Storage, #{}).

config(Storage, FTOptions0) ->
    FTOptions1 = maps:merge(
        #{<<"enable">> => true, <<"storage">> => Storage},
        FTOptions0
    ),
    #{<<"file_transfer">> => FTOptions1}.

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
    iolist_to_binary(filename:join([ft_root(Config), Node | Tail])).

ft_root(Config) ->
    filename:join([?config(priv_dir, Config), "file_transfer"]).

cleanup_ft_root(Config) ->
    file:del_dir_r(emqx_ft_test_helpers:ft_root(Config)).

start_client(ClientId) ->
    start_client(ClientId, node()).

start_client(ClientId, Node) ->
    Port = tcp_port(Node),
    {ok, Client} = emqtt:start_link([{proto_ver, v5}, {clientid, ClientId}, {port, Port}]),
    {ok, _} = emqtt:connect(Client),
    Client.

upload_file(ClientId, FileId, Name, Data) ->
    upload_file(sync, ClientId, FileId, Name, Data).

upload_file(Mode, ClientId, FileId, Name, Data) ->
    upload_file(Mode, ClientId, FileId, Name, Data, node()).

upload_file(Mode, ClientId, FileId, Name, Data, Node) ->
    C1 = start_client(ClientId, Node),

    ReqTopicPrefix = request_topic_prefix(Mode, FileId),
    Size = byte_size(Data),
    Meta = #{
        name => Name,
        expire_at => erlang:system_time(_Unit = second) + 3600,
        size => Size
    },
    MetaPayload = emqx_utils_json:encode(emqx_ft:encode_filemeta(Meta)),

    MetaTopic = <<ReqTopicPrefix/binary, "/init">>,
    {ok, #{reason_code_name := success}} = emqtt:publish(C1, MetaTopic, MetaPayload, 1),
    {ok, #{reason_code_name := success}} = emqtt:publish(
        C1, <<ReqTopicPrefix/binary, "/0">>, Data, 1
    ),

    FinTopic = <<ReqTopicPrefix/binary, "/fin/", (integer_to_binary(Size))/binary>>,
    FinResult = fin_result(Mode, ClientId, C1, FinTopic),
    ok = emqtt:stop(C1),
    FinResult.

fin_result(Mode, ClientId, C, FinTopic) ->
    {ok, _, _} = emqtt:subscribe(C, response_topic(ClientId), 1),
    case emqtt:publish(C, FinTopic, <<>>, 1) of
        {ok, #{reason_code_name := success}} ->
            maybe_wait_for_assemble(Mode, ClientId, FinTopic);
        {ok, #{reason_code_name := Error}} ->
            {error, Error}
    end.

maybe_wait_for_assemble(sync, _ClientId, _FinTopic) ->
    ok;
maybe_wait_for_assemble(async, ClientId, FinTopic) ->
    ResponseTopic = response_topic(ClientId),
    receive
        {publish, #{payload := Payload, topic := ResponseTopic}} ->
            case emqx_utils_json:decode(Payload) of
                #{<<"topic">> := FinTopic, <<"reason_code">> := 0} ->
                    ok;
                #{<<"topic">> := FinTopic, <<"reason_code">> := Code} ->
                    {error, emqx_reason_codes:name(Code)};
                _ ->
                    maybe_wait_for_assemble(async, ClientId, FinTopic)
            end
    end.

response_topic(ClientId) ->
    <<"$file-response/", (to_bin(ClientId))/binary>>.

request_topic_prefix(sync, FileId) ->
    <<"$file/", (to_bin(FileId))/binary>>;
request_topic_prefix(async, FileId) ->
    <<"$file-async/", (to_bin(FileId))/binary>>.

to_bin(Val) ->
    iolist_to_binary(Val).

aws_config() ->
    emqx_s3_test_helpers:aws_config(tcp, binary_to_list(?S3_HOST), ?S3_PORT).

generate_pki_files(Config) ->
    PrivDir = ?config(priv_dir, Config),
    KeyType = ec,
    Opts = #{
        base_tmp_dir => PrivDir,
        key_type => KeyType,
        password => undefined
    },
    emqx_tls_lib_tests:do_setup_ssl_files(Opts).

unique_binary_string() ->
    emqx_guid:to_hexstr(emqx_guid:gen()).
