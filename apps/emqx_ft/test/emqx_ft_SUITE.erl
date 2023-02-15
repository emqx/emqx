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

all() ->
    [
        {group, single_node},
        {group, cluster}
    ].

groups() ->
    [
        {single_node, [sequence], emqx_common_test_helpers:all(?MODULE) -- [t_switch_node]},
        {cluster, [sequence], [t_switch_node]}
    ].

init_per_suite(Config) ->
    ok = emqx_common_test_helpers:start_apps([emqx_conf, emqx_ft], set_special_configs(Config)),
    ok = emqx_common_test_helpers:set_gen_rpc_stateless(),
    Config.

end_per_suite(_Config) ->
    ok = emqx_common_test_helpers:stop_apps([emqx_ft, emqx_conf]),
    ok.

set_special_configs(Config) ->
    fun
        (emqx_ft) ->
            ok = emqx_config:put([file_transfer, storage], #{
                type => local, root => emqx_ft_test_helpers:ft_root(Config, node())
            });
        (_) ->
            ok
    end.

init_per_testcase(Case, Config) ->
    ClientId = atom_to_binary(Case),
    {ok, C} = emqtt:start_link([{proto_ver, v5}, {clientid, ClientId}]),
    {ok, _} = emqtt:connect(C),
    [{client, C}, {clientid, ClientId} | Config].
end_per_testcase(_Case, Config) ->
    C = ?config(client, Config),
    ok = emqtt:stop(C),
    ok.

init_per_group(cluster, Config) ->
    Node = emqx_ft_test_helpers:start_additional_node(Config, test2),
    [{additional_node, Node} | Config];
init_per_group(_Group, Config) ->
    Config.

end_per_group(cluster, Config) ->
    ok = emqx_ft_test_helpers:stop_additional_node(Config);
end_per_group(_Group, _Config) ->
    ok.

%%--------------------------------------------------------------------
%% Tests
%%--------------------------------------------------------------------

t_invalid_topic_format(Config) ->
    C = ?config(client, Config),

    ?assertRCName(
        unspecified_error,
        emqtt:publish(C, <<"$file/fileid">>, <<>>, 1)
    ),
    ?assertRCName(
        unspecified_error,
        emqtt:publish(C, <<"$file/fileid/">>, <<>>, 1)
    ),
    ?assertRCName(
        unspecified_error,
        emqtt:publish(C, <<"$file/fileid/offset">>, <<>>, 1)
    ),
    ?assertRCName(
        unspecified_error,
        emqtt:publish(C, <<"$file/fileid/fin/offset">>, <<>>, 1)
    ),
    ?assertRCName(
        unspecified_error,
        emqtt:publish(C, <<"$file/">>, <<>>, 1)
    ),
    ?assertRCName(
        unspecified_error,
        emqtt:publish(C, <<"$file/X/Y/Z">>, <<>>, 1)
    ),
    %% should not be handled by `emqx_ft`
    ?assertRCName(
        no_matching_subscribers,
        emqtt:publish(C, <<"$file">>, <<>>, 1)
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
            SegmentTopic = <<"$file/", FileId/binary, "/", Offset/binary>>,
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

    {ok, [{ReadyTransferId, _}]} = emqx_ft_storage:ready_transfers(),
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
            SegmentTopic = <<"$file/", FileId/binary, "/", Offset/binary>>,
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
            SegmentTopic = <<"$file/", FileId/binary, "/", Offset/binary>>,
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

t_switch_node(Config) ->
    AdditionalNodePort = emqx_ft_test_helpers:tcp_port(?config(additional_node, Config)),

    ClientId = <<"t_switch_node-migrating_client">>,

    {ok, C1} = emqtt:start_link([{proto_ver, v5}, {clientid, ClientId}, {port, AdditionalNodePort}]),
    {ok, _} = emqtt:connect(C1),

    Filename = <<"multinode_upload.txt">>,
    FileId = <<"f1">>,

    Data = [<<"first">>, <<"second">>, <<"third">>],
    [{Data0, Offset0}, {Data1, Offset1}, {Data2, Offset2}] = with_offsets(Data),

    %% First, publist metadata and the first segment to the additional node

    Meta = meta(Filename, Data),
    MetaPayload = emqx_json:encode(Meta),

    MetaTopic = <<"$file/", FileId/binary, "/init">>,
    ?assertRCName(
        success,
        emqtt:publish(C1, MetaTopic, MetaPayload, 1)
    ),
    ?assertRCName(
        success,
        emqtt:publish(C1, <<"$file/", FileId/binary, "/", Offset0/binary>>, Data0, 1)
    ),

    %% Then, switch the client to the main node
    %% and publish the rest of the segments

    ok = emqtt:stop(C1),
    {ok, C2} = emqtt:start_link([{proto_ver, v5}, {clientid, ClientId}]),
    {ok, _} = emqtt:connect(C2),

    ?assertRCName(
        success,
        emqtt:publish(C2, <<"$file/", FileId/binary, "/", Offset1/binary>>, Data1, 1)
    ),
    ?assertRCName(
        success,
        emqtt:publish(C2, <<"$file/", FileId/binary, "/", Offset2/binary>>, Data2, 1)
    ),

    FinTopic = <<"$file/", FileId/binary, "/fin">>,
    ?assertRCName(
        success,
        emqtt:publish(C2, FinTopic, <<>>, 1)
    ),

    ok = emqtt:stop(C2),

    %% Now check consistency of the file

    {ok, ReadyTransfers} = emqx_ft_storage:ready_transfers(),
    {ReadyTransferIds, _} = lists:unzip(ReadyTransfers),
    [ReadyTransferId] = [Id || #{<<"clientid">> := CId} = Id <- ReadyTransferIds, CId == ClientId],

    {ok, TableQH} = emqx_ft_storage:get_ready_transfer(ReadyTransferId),

    ?assertEqual(
        iolist_to_binary(Data),
        iolist_to_binary(qlc:eval(TableQH))
    ).

t_assemble_crash(Config) ->
    C = ?config(client, Config),

    meck:new(emqx_ft_storage_fs),
    meck:expect(emqx_ft_storage_fs, assemble, fun(_, _, _) -> meck:exception(error, oops) end),

    ?assertRCName(
        unspecified_error,
        emqtt:publish(C, <<"$file/someid/fin">>, <<>>, 1)
    ).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

with_offsets(Items) ->
    {List, _} = lists:mapfoldl(
        fun(Item, Offset) ->
            {{Item, integer_to_binary(Offset)}, Offset + byte_size(Item)}
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
