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

-module(emqx_ft_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(assertRCName(RCName, PublishRes),
    ?assertMatch(
        {ok, #{reason_code_name := RCName}},
        PublishRes
    )
).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    ok = emqx_common_test_helpers:start_apps([emqx_conf, emqx_ft], fun set_special_configs/1),
    ok = emqx_common_test_helpers:maybe_fix_gen_rpc(),
    Config.

end_per_suite(_Config) ->
    ok = emqx_common_test_helpers:stop_apps([emqx_ft, emqx_conf]),
    ok.

set_special_configs(emqx_ft) ->
    {ok, _} = emqx:update_config([file_transfer, storage], #{<<"type">> => <<"local">>}),
    ok;
set_special_configs(_App) ->
    ok.

init_per_testcase(_Case, Config) ->
    _ = file:del_dir_r(filename:join(emqx:data_dir(), "file_transfer")),
    ClientId = <<"client">>,
    {ok, C} = emqtt:start_link([{proto_ver, v5}, {clientid, ClientId}]),
    {ok, _} = emqtt:connect(C),
    [{client, C}, {clientid, ClientId} | Config].

end_per_testcase(_Case, Config) ->
    C = ?config(client, Config),
    ok = emqtt:stop(C),
    ok.

%%--------------------------------------------------------------------
%% Tests
%%--------------------------------------------------------------------

t_invalid_topic_format(Config) ->
    C = ?config(client, Config),

    %% TODO: more invalid topics

    ?assertRCName(
        unspecified_error,
        emqtt:publish(C, <<"$file/XYZ">>, <<>>, 1)
    ),
    ?assertRCName(
        unspecified_error,
        emqtt:publish(C, <<"$file/X/Y/Z">>, <<>>, 1)
    ).

t_simple_transfer(Config) ->
    C = ?config(client, Config),

    Filename = <<"topsecret.pdf">>,
    FileId = <<"f1">>,

    Data = [<<"first">>, <<"second">>, <<"third">>],

    Meta = meta(Filename, Data),
    MetaPayload = emqx_json:encode(Meta),

    MetaTopic = <<"$file/", FileId/binary, "/init">>,
    ?assertRCName(
        success,
        emqtt:publish(C, MetaTopic, MetaPayload, 1)
    ),

    lists:foreach(
        fun({Chunk, Offset}) ->
            OffsetBin = integer_to_binary(Offset),
            SegmentTopic = <<"$file/", FileId/binary, "/", OffsetBin/binary>>,
            ?assertRCName(
                success,
                emqtt:publish(C, SegmentTopic, Chunk, 1)
            )
        end,
        with_offsets(Data)
    ),

    FinTopic = <<"$file/", FileId/binary, "/fin">>,
    ?assertRCName(
        success,
        emqtt:publish(C, FinTopic, <<>>, 1)
    ),

    ReadyTransferId = #{
        <<"fileid">> => FileId,
        <<"clientid">> => ?config(clientid, Config),
        <<"node">> => atom_to_binary(node(), utf8)
    },

    {ok, TableQH} = emqx_ft_storage:get_ready_transfer(ReadyTransferId),

    ?assertEqual(
        iolist_to_binary(Data),
        iolist_to_binary(qlc:eval(TableQH))
    ).

t_meta_conflict(Config) ->
    C = ?config(client, Config),

    Filename = <<"topsecret.pdf">>,
    FileId = <<"f1">>,

    Meta = meta(Filename, [<<"x">>]),
    MetaPayload = emqx_json:encode(Meta),

    MetaTopic = <<"$file/", FileId/binary, "/init">>,
    ?assertRCName(
        success,
        emqtt:publish(C, MetaTopic, MetaPayload, 1)
    ),

    ConflictMeta = Meta#{name => <<"conflict.pdf">>},
    ConflictMetaPayload = emqx_json:encode(ConflictMeta),

    ?assertRCName(
        unspecified_error,
        emqtt:publish(C, MetaTopic, ConflictMetaPayload, 1)
    ).

t_no_meta(Config) ->
    C = ?config(client, Config),

    FileId = <<"f1">>,
    Data = <<"first">>,

    SegmentTopic = <<"$file/", FileId/binary, "/0">>,
    ?assertRCName(
        success,
        emqtt:publish(C, SegmentTopic, Data, 1)
    ),

    FinTopic = <<"$file/", FileId/binary, "/fin">>,
    ?assertRCName(
        unspecified_error,
        emqtt:publish(C, FinTopic, <<>>, 1)
    ).

t_no_segment(Config) ->
    C = ?config(client, Config),

    Filename = <<"topsecret.pdf">>,
    FileId = <<"f1">>,

    Data = [<<"first">>, <<"second">>, <<"third">>],

    Meta = meta(Filename, Data),
    MetaPayload = emqx_json:encode(Meta),

    MetaTopic = <<"$file/", FileId/binary, "/init">>,
    ?assertRCName(
        success,
        emqtt:publish(C, MetaTopic, MetaPayload, 1)
    ),

    lists:foreach(
        fun({Chunk, Offset}) ->
            OffsetBin = integer_to_binary(Offset),
            SegmentTopic = <<"$file/", FileId/binary, "/", OffsetBin/binary>>,
            ?assertRCName(
                success,
                emqtt:publish(C, SegmentTopic, Chunk, 1)
            )
        end,
        %% Skip the first segment
        tl(with_offsets(Data))
    ),

    FinTopic = <<"$file/", FileId/binary, "/fin">>,
    ?assertRCName(
        unspecified_error,
        emqtt:publish(C, FinTopic, <<>>, 1)
    ).

t_invalid_meta(Config) ->
    C = ?config(client, Config),

    FileId = <<"f1">>,

    MetaTopic = <<"$file/", FileId/binary, "/init">>,

    %% Invalid schema
    Meta = #{foo => <<"bar">>},
    MetaPayload = emqx_json:encode(Meta),
    ?assertRCName(
        unspecified_error,
        emqtt:publish(C, MetaTopic, MetaPayload, 1)
    ),

    %% Invalid JSON
    ?assertRCName(
        unspecified_error,
        emqtt:publish(C, MetaTopic, <<"{oops;">>, 1)
    ).

t_invalid_checksum(Config) ->
    C = ?config(client, Config),

    Filename = <<"topsecret.pdf">>,
    FileId = <<"f1">>,

    Data = [<<"first">>, <<"second">>, <<"third">>],

    Meta = meta(Filename, Data),
    MetaPayload = emqx_json:encode(Meta#{checksum => sha256hex(<<"invalid">>)}),

    MetaTopic = <<"$file/", FileId/binary, "/init">>,
    ?assertRCName(
        success,
        emqtt:publish(C, MetaTopic, MetaPayload, 1)
    ),

    lists:foreach(
        fun({Chunk, Offset}) ->
            OffsetBin = integer_to_binary(Offset),
            SegmentTopic = <<"$file/", FileId/binary, "/", OffsetBin/binary>>,
            ?assertRCName(
                success,
                emqtt:publish(C, SegmentTopic, Chunk, 1)
            )
        end,
        with_offsets(Data)
    ),

    FinTopic = <<"$file/", FileId/binary, "/fin">>,
    ?assertRCName(
        unspecified_error,
        emqtt:publish(C, FinTopic, <<>>, 1)
    ).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

with_offsets(Items) ->
    {List, _} = lists:mapfoldl(
        fun(Item, Offset) ->
            {{Item, Offset}, Offset + byte_size(Item)}
        end,
        0,
        Items
    ),
    List.

sha256hex(Data) ->
    binary:encode_hex(crypto:hash(sha256, Data)).

meta(FileName, Data) ->
    FullData = iolist_to_binary(Data),
    #{
        name => FileName,
        checksum => sha256hex(FullData),
        expire_at => erlang:system_time(_Unit = second) + 3600,
        size => byte_size(FullData)
    }.
