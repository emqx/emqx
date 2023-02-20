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
        {single_node, [sequence], emqx_common_test_helpers:all(?MODULE) -- group_cluster()},
        {cluster, [sequence], group_cluster()}
    ].

group_cluster() ->
    [
        t_switch_node,
        t_unreliable_migrating_client
    ].

init_per_suite(Config) ->
    ok = emqx_common_test_helpers:start_apps([emqx_ft], set_special_configs(Config)),
    Config.

end_per_suite(_Config) ->
    ok = emqx_common_test_helpers:stop_apps([emqx_ft]),
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
    case ?config(group, Config) of
        cluster ->
            [{clientid, ClientId} | Config];
        _ ->
            {ok, C} = emqtt:start_link([{proto_ver, v5}, {clientid, ClientId}]),
            {ok, _} = emqtt:connect(C),
            [{client, C}, {clientid, ClientId} | Config]
    end.
end_per_testcase(_Case, Config) ->
    _ = [ok = emqtt:stop(C) || {client, C} <- Config],
    ok.

init_per_group(Group = cluster, Config) ->
    Cluster = mk_cluster_specs(Config),
    ct:pal("Starting ~p", [Cluster]),
    Nodes = [
        emqx_common_test_helpers:start_slave(Name, Opts#{join_to => node()})
     || {Name, Opts} <- Cluster
    ],
    [{group, Group}, {cluster_nodes, Nodes} | Config];
init_per_group(Group, Config) ->
    [{group, Group} | Config].

end_per_group(cluster, Config) ->
    ok = lists:foreach(
        fun emqx_ft_test_helpers:stop_additional_node/1,
        ?config(cluster_nodes, Config)
    );
end_per_group(_Group, _Config) ->
    ok.

mk_cluster_specs(Config) ->
    Specs = [
        {core, emqx_ft_SUITE1, #{listener_ports => [{tcp, 2883}]}},
        {core, emqx_ft_SUITE2, #{listener_ports => [{tcp, 3883}]}}
    ],
    CommOpts = [
        {env, [{emqx, boot_modules, [broker, listeners]}]},
        {apps, [emqx_ft]},
        {conf, [{[listeners, Proto, default, enabled], false} || Proto <- [ssl, ws, wss]]},
        {env_handler, fun
            (emqx_ft) ->
                ok = emqx_config:put([file_transfer, storage], #{
                    type => local,
                    root => emqx_ft_test_helpers:ft_root(Config, node())
                });
            (_) ->
                ok
        end}
    ],
    emqx_common_test_helpers:emqx_cluster(
        Specs,
        CommOpts
    ).

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

t_invalid_fileid(Config) ->
    C = ?config(client, Config),
    ?assertRCName(
        unspecified_error,
        emqtt:publish(C, <<"$file//init">>, <<>>, 1)
    ).

t_simple_transfer(Config) ->
    C = ?config(client, Config),

    Filename = <<"topsecret.pdf">>,
    FileId = <<"f1">>,

    Data = [<<"first">>, <<"second">>, <<"third">>],

    Meta = #{size := Filesize} = meta(Filename, Data),
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

    FinTopic = <<"$file/", FileId/binary, "/fin/", (integer_to_binary(Filesize))/binary>>,
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

    FinTopic = <<"$file/", FileId/binary, "/fin/42">>,
    ?assertRCName(
        unspecified_error,
        emqtt:publish(C, FinTopic, <<>>, 1)
    ).

t_no_segment(Config) ->
    C = ?config(client, Config),

    Filename = <<"topsecret.pdf">>,
    FileId = <<"f1">>,

    Data = [<<"first">>, <<"second">>, <<"third">>],

    Meta = #{size := Filesize} = meta(Filename, Data),
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

    FinTopic = <<"$file/", FileId/binary, "/fin/", (integer_to_binary(Filesize))/binary>>,
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

    Meta = #{size := Filesize} = meta(Filename, Data),
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

    FinTopic = <<"$file/", FileId/binary, "/fin/", (integer_to_binary(Filesize))/binary>>,
    ?assertRCName(
        unspecified_error,
        emqtt:publish(C, FinTopic, <<>>, 1)
    ).

t_switch_node(Config) ->
    [Node | _] = ?config(cluster_nodes, Config),
    AdditionalNodePort = emqx_ft_test_helpers:tcp_port(Node),

    ClientId = <<"t_switch_node-migrating_client">>,

    {ok, C1} = emqtt:start_link([{proto_ver, v5}, {clientid, ClientId}, {port, AdditionalNodePort}]),
    {ok, _} = emqtt:connect(C1),

    Filename = <<"multinode_upload.txt">>,
    FileId = <<"f1">>,

    Data = [<<"first">>, <<"second">>, <<"third">>],
    [{Data0, Offset0}, {Data1, Offset1}, {Data2, Offset2}] = with_offsets(Data),

    %% First, publist metadata and the first segment to the additional node

    Meta = #{size := Filesize} = meta(Filename, Data),
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

    FinTopic = <<"$file/", FileId/binary, "/fin/", (integer_to_binary(Filesize))/binary>>,
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

t_unreliable_migrating_client(Config) ->
    NodeSelf = node(),
    [Node1, Node2] = ?config(cluster_nodes, Config),

    ClientId = ?config(clientid, Config),
    FileId = emqx_guid:to_hexstr(emqx_guid:gen()),
    Filename = "migratory-birds-in-southern-hemisphere-2013.pdf",
    Filesize = 1000,
    Gen = emqx_ft_content_gen:new({{ClientId, FileId}, Filesize}, 16),
    Payload = iolist_to_binary(emqx_ft_content_gen:consume(Gen, fun({Chunk, _, _}) -> Chunk end)),
    Meta = meta(Filename, Payload),

    Context = #{
        clientid => ClientId,
        fileid => FileId,
        filesize => Filesize,
        payload => Payload
    },
    Commands = [
        % Connect to the broker on the current node
        {fun connect_mqtt_client/2, [NodeSelf]},
        % Send filemeta and 3 initial segments
        % (assuming client chose 100 bytes as a desired segment size)
        {fun send_filemeta/2, [Meta]},
        {fun send_segment/3, [0, 100]},
        {fun send_segment/3, [100, 100]},
        {fun send_segment/3, [200, 100]},
        % Disconnect the client cleanly
        {fun stop_mqtt_client/1, []},
        % Connect to the broker on `Node1`
        {fun connect_mqtt_client/2, [Node1]},
        % Connect to the broker on `Node2` without first disconnecting from `Node1`
        % Client forgot the state for some reason and started the transfer again.
        % (assuming this is usual for a client on a device that was rebooted)
        {fun connect_mqtt_client/2, [Node2]},
        {fun send_filemeta/2, [Meta]},
        % This time it chose 200 bytes as a segment size
        {fun send_segment/3, [0, 200]},
        {fun send_segment/3, [200, 200]},
        % But now it downscaled back to 100 bytes segments
        {fun send_segment/3, [400, 100]},
        % Client lost connectivity and reconnected
        % (also had last few segments unacked and decided to resend them)
        {fun connect_mqtt_client/2, [Node2]},
        {fun send_segment/3, [200, 200]},
        {fun send_segment/3, [400, 200]},
        % Client lost connectivity and reconnected, this time to another node
        % (also had last segment unacked and decided to resend it)
        {fun connect_mqtt_client/2, [Node1]},
        {fun send_segment/3, [400, 200]},
        {fun send_segment/3, [600, eof]},
        {fun send_finish/1, []},
        % Client lost connectivity and reconnected, this time to the current node
        % (client had `fin` unacked and decided to resend it)
        {fun connect_mqtt_client/2, [NodeSelf]},
        {fun send_finish/1, []}
    ],
    _Context = run_commands(Commands, Context),

    {ok, ReadyTransfers} = emqx_ft_storage:ready_transfers(),
    ReadyTransferIds =
        [Id || {#{<<"clientid">> := CId} = Id, _Info} <- ReadyTransfers, CId == ClientId],

    % NOTE
    % The cluster had 2 assemblers running on two different nodes, because client sent `fin`
    % twice. This is currently expected, files must be identical anyway.
    Node1Bin = atom_to_binary(Node1),
    NodeSelfBin = atom_to_binary(NodeSelf),
    ?assertMatch(
        [#{<<"node">> := Node1Bin}, #{<<"node">> := NodeSelfBin}],
        lists:sort(ReadyTransferIds)
    ),

    [
        begin
            {ok, TableQH} = emqx_ft_storage:get_ready_transfer(Id),
            ?assertEqual(
                Payload,
                iolist_to_binary(qlc:eval(TableQH))
            )
        end
     || Id <- ReadyTransferIds
    ].

run_commands(Commands, Context) ->
    lists:foldl(fun run_command/2, Context, Commands).

run_command({Command, Args}, Context) ->
    ct:pal("COMMAND ~p ~p", [erlang:fun_info(Command, name), Args]),
    erlang:apply(Command, Args ++ [Context]).

connect_mqtt_client(Node, ContextIn) ->
    Context = #{clientid := ClientId} = disown_mqtt_client(ContextIn),
    NodePort = emqx_ft_test_helpers:tcp_port(Node),
    {ok, Client} = emqtt:start_link([{proto_ver, v5}, {clientid, ClientId}, {port, NodePort}]),
    {ok, _} = emqtt:connect(Client),
    Context#{client => Client}.

stop_mqtt_client(Context = #{client := Client}) ->
    _ = emqtt:stop(Client),
    maps:remove(client, Context).

disown_mqtt_client(Context = #{client := Client}) ->
    _ = erlang:unlink(Client),
    maps:remove(client, Context);
disown_mqtt_client(Context = #{}) ->
    Context.

send_filemeta(Meta, Context = #{client := Client, fileid := FileId}) ->
    Topic = <<"$file/", FileId/binary, "/init">>,
    MetaPayload = emqx_json:encode(Meta),
    ?assertRCName(
        success,
        emqtt:publish(Client, Topic, MetaPayload, 1)
    ),
    Context.

send_segment(Offset, Size, Context = #{client := Client, fileid := FileId, payload := Payload}) ->
    Topic = <<"$file/", FileId/binary, "/", (integer_to_binary(Offset))/binary>>,
    Data =
        case Size of
            eof ->
                binary:part(Payload, Offset, byte_size(Payload) - Offset);
            N ->
                binary:part(Payload, Offset, N)
        end,
    ?assertRCName(
        success,
        emqtt:publish(Client, Topic, Data, 1)
    ),
    Context.

send_finish(Context = #{client := Client, fileid := FileId, filesize := Filesize}) ->
    Topic = <<"$file/", FileId/binary, "/fin/", (integer_to_binary(Filesize))/binary>>,
    ?assertRCName(
        success,
        emqtt:publish(Client, Topic, <<>>, 1)
    ),
    Context.

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
