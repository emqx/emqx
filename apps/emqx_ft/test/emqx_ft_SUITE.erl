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
        {group, async_mode},
        {group, sync_mode},
        {group, cluster}
    ].

groups() ->
    [
        {single_node, [], [
            t_assemble_crash,
            t_corrupted_segment_retry,
            t_invalid_checksum,
            t_invalid_fileid,
            t_invalid_filename,
            t_invalid_meta,
            t_invalid_topic_format,
            t_meta_conflict,
            t_nasty_clientids_fileids,
            t_nasty_filenames,
            t_no_meta,
            t_no_segment,
            t_simple_transfer,
            t_assemble_timeout
        ]},
        {async_mode, [], [
            {group, single_node},
            t_client_disconnect_while_assembling
        ]},
        {sync_mode, [], [
            {group, single_node}
        ]},
        {cluster, [], [
            t_switch_node,
            t_unreliable_migrating_client,
            {g_concurrent_fins, [{repeat_until_any_fail, 8}], [
                t_concurrent_fins
            ]}
        ]}
    ].

suite() ->
    [{timetrap, {seconds, 90}}].

init_per_suite(Config) ->
    % NOTE
    % Inhibit local fs GC to simulate it isn't fast enough to collect
    % complete transfers.
    Storage = emqx_utils_maps:deep_merge(
        emqx_ft_test_helpers:local_storage(Config),
        #{<<"local">> => #{<<"segments">> => #{<<"gc">> => #{<<"interval">> => 0}}}}
    ),
    FTConfig = emqx_ft_test_helpers:config(Storage, #{<<"assemble_timeout">> => <<"2s">>}),
    Apps = emqx_cth_suite:start(
        [
            {emqx_ft, #{config => FTConfig}}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)),
    ok.

init_per_testcase(Case, Config) ->
    ClientId = iolist_to_binary([
        atom_to_binary(Case), <<"-">>, emqx_ft_test_helpers:unique_binary_string()
    ]),
    ok = set_client_specific_ft_dirs(ClientId, Config),
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
    WorkDir = ?config(priv_dir, Config),
    Cluster = mk_cluster_specs(Config),
    Nodes = emqx_cth_cluster:start(Cluster, #{work_dir => WorkDir}),
    [{group, Group}, {cluster_nodes, Nodes} | Config];
init_per_group(_Group = async_mode, Config) ->
    [{mode, async} | Config];
init_per_group(_Group = sync_mode, Config) ->
    [{mode, sync} | Config];
init_per_group(Group, Config) ->
    [{group, Group} | Config].

end_per_group(cluster, Config) ->
    ok = emqx_cth_cluster:stop(?config(cluster_nodes, Config));
end_per_group(_Group, _Config) ->
    ok.

mk_cluster_specs(_Config) ->
    CommonOpts = #{
        role => core,
        join_to => node(),
        apps => [
            {emqx_conf, #{start => false}},
            {emqx, #{override_env => [{boot_modules, [broker, listeners]}]}},
            {emqx_ft, "file_transfer { enable = true }"}
        ]
    },
    [
        {emqx_ft_SUITE1, CommonOpts},
        {emqx_ft_SUITE2, CommonOpts}
    ].

%%--------------------------------------------------------------------
%% Single node tests
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
        emqtt:publish(C, <<"$file/fileid/fin/42/xyz">>, <<>>, 1)
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
        emqtt:publish(C, mk_init_topic(Config, <<>>), <<>>, 1)
    ).

t_invalid_filename(Config) ->
    C = ?config(client, Config),
    ?assertRCName(
        unspecified_error,
        emqtt:publish(C, mk_init_topic(Config, <<"f1">>), encode_meta(meta(".", <<>>)), 1)
    ),
    ?assertRCName(
        unspecified_error,
        emqtt:publish(C, mk_init_topic(Config, <<"f2">>), encode_meta(meta("..", <<>>)), 1)
    ),
    ?assertRCName(
        unspecified_error,
        emqtt:publish(C, mk_init_topic(Config, <<"f2">>), encode_meta(meta("../nice", <<>>)), 1)
    ),
    ?assertRCName(
        unspecified_error,
        emqtt:publish(C, mk_init_topic(Config, <<"f3">>), encode_meta(meta("/etc/passwd", <<>>)), 1)
    ),
    ?assertRCName(
        unspecified_error,
        emqtt:publish(
            C,
            mk_init_topic(Config, <<"f4">>),
            encode_meta(meta(lists:duplicate(1000, $A), <<>>)),
            1
        )
    ).

t_simple_transfer(Config) ->
    C = ?config(client, Config),
    ClientId = ?config(clientid, Config),

    Filename = "topsecret.pdf",
    FileId = <<"f1">>,

    Data = [<<"first">>, <<"second">>, <<"third">>],

    Meta = #{size := Filesize} = meta(Filename, Data),

    ?assertRCName(
        success,
        emqtt:publish(C, mk_init_topic(Config, FileId), encode_meta(Meta), 1)
    ),

    lists:foreach(
        fun({Chunk, Offset}) ->
            ?assertRCName(
                success,
                emqtt:publish(C, mk_segment_topic(Config, FileId, Offset), Chunk, 1)
            )
        end,
        with_offsets(Data)
    ),

    ?assertEqual(
        ok,
        emqx_ft_test_helpers:fin_result(
            mode(Config), ClientId, C, mk_fin_topic(Config, FileId, Filesize)
        )
    ),

    [Export] = list_files(?config(clientid, Config)),
    ?assertEqual(
        {ok, iolist_to_binary(Data)},
        read_export(Export)
    ).

t_nasty_clientids_fileids(Config) ->
    Transfers = [
        {<<".">>, <<".">>},
        {<<"🌚"/utf8>>, <<"🌝"/utf8>>},
        {<<"../..">>, <<"😤"/utf8>>},
        {<<"/etc/passwd">>, <<"whitehat">>},
        {<<"; rm -rf / ;">>, <<"whitehat">>}
    ],

    ok = lists:foreach(
        fun({ClientId, FileId}) ->
            Data = ClientId,
            ok = emqx_ft_test_helpers:upload_file(mode(Config), ClientId, FileId, "justfile", Data),
            [Export] = list_files(ClientId),
            ?assertMatch(#{meta := #{name := "justfile"}}, Export),
            ?assertEqual({ok, Data}, read_export(Export))
        end,
        Transfers
    ).

t_nasty_filenames(Config) ->
    Filenames = [
        {<<"nasty1">>, "146%"},
        {<<"nasty2">>, "🌚"},
        {<<"nasty3">>, "中文.txt"},
        {<<"nasty4">>, _239Bytes = string:join(lists:duplicate(240 div 5, "LONG"), ".")}
    ],
    ok = lists:foreach(
        fun({ClientId, Filename}) ->
            FileId = unicode:characters_to_binary(Filename),
            ok = emqx_ft_test_helpers:upload_file(mode(Config), ClientId, FileId, Filename, FileId),
            [Export] = list_files(ClientId),
            ?assertMatch(#{meta := #{name := Filename}}, Export),
            ?assertEqual({ok, FileId}, read_export(Export))
        end,
        Filenames
    ).

t_meta_conflict(Config) ->
    C = ?config(client, Config),

    Filename = "topsecret.pdf",
    FileId = <<"f1">>,

    Meta = meta(Filename, [<<"x">>]),

    ?assertRCName(
        success,
        emqtt:publish(C, mk_init_topic(Config, FileId), encode_meta(Meta), 1)
    ),

    ConflictMeta = Meta#{name => "conflict.pdf"},

    ?assertRCName(
        unspecified_error,
        emqtt:publish(C, mk_init_topic(Config, FileId), encode_meta(ConflictMeta), 1)
    ).

t_no_meta(Config) ->
    C = ?config(client, Config),
    ClientId = ?config(clientid, Config),

    FileId = <<"f1">>,
    Data = <<"first">>,

    ?assertRCName(
        success,
        emqtt:publish(C, mk_segment_topic(Config, FileId, 0), Data, 1)
    ),

    ?assertEqual(
        {error, unspecified_error},
        emqx_ft_test_helpers:fin_result(mode(Config), ClientId, C, mk_fin_topic(Config, FileId, 42))
    ).

t_no_segment(Config) ->
    C = ?config(client, Config),
    ClientId = ?config(clientid, Config),

    Filename = "topsecret.pdf",
    FileId = <<"f1">>,

    Data = [<<"first">>, <<"second">>, <<"third">>],

    Meta = #{size := Filesize} = meta(Filename, Data),

    ?assertRCName(
        success,
        emqtt:publish(C, mk_init_topic(Config, FileId), encode_meta(Meta), 1)
    ),

    lists:foreach(
        fun({Chunk, Offset}) ->
            ?assertRCName(
                success,
                emqtt:publish(C, mk_segment_topic(Config, FileId, Offset), Chunk, 1)
            )
        end,
        %% Skip the first segment
        tl(with_offsets(Data))
    ),

    ?assertEqual(
        {error, unspecified_error},
        emqx_ft_test_helpers:fin_result(
            mode(Config), ClientId, C, mk_fin_topic(Config, FileId, Filesize)
        )
    ).

t_invalid_meta(Config) ->
    C = ?config(client, Config),

    FileId = <<"f1">>,

    %% Invalid schema
    Meta = #{foo => <<"bar">>},
    MetaPayload = emqx_utils_json:encode(Meta),
    ?assertRCName(
        unspecified_error,
        emqtt:publish(C, mk_init_topic(Config, FileId), MetaPayload, 1)
    ),

    %% Invalid JSON
    ?assertRCName(
        unspecified_error,
        emqtt:publish(C, mk_init_topic(Config, FileId), <<"{oops;">>, 1)
    ).

t_invalid_checksum(Config) ->
    C = ?config(client, Config),
    ClientId = ?config(clientid, Config),

    Filename = "topsecret.pdf",
    FileId = <<"f1">>,

    Data = [<<"first">>, <<"second">>, <<"third">>],

    Meta = #{size := Filesize} = meta(Filename, Data),
    MetaPayload = encode_meta(Meta#{checksum => {sha256, sha256(<<"invalid">>)}}),

    ?assertRCName(
        success,
        emqtt:publish(C, mk_init_topic(Config, FileId), MetaPayload, 1)
    ),

    lists:foreach(
        fun({Chunk, Offset}) ->
            ?assertRCName(
                success,
                emqtt:publish(C, mk_segment_topic(Config, FileId, Offset), Chunk, 1)
            )
        end,
        with_offsets(Data)
    ),

    % Send `fin` w/o checksum, should fail since filemeta checksum is invalid
    FinTopic = mk_fin_topic(Config, FileId, Filesize),

    ?assertEqual(
        {error, unspecified_error},
        emqx_ft_test_helpers:fin_result(mode(Config), ClientId, C, FinTopic)
    ),

    % Send `fin` with the correct checksum
    Checksum = binary:encode_hex(sha256(Data)),
    ?assertEqual(
        ok,
        emqx_ft_test_helpers:fin_result(
            mode(Config), ClientId, C, <<FinTopic/binary, "/", Checksum/binary>>
        )
    ).

t_corrupted_segment_retry(Config) ->
    C = ?config(client, Config),
    ClientId = ?config(clientid, Config),

    Filename = "corruption.pdf",
    FileId = <<"4242-4242">>,

    Data = [<<"first">>, <<"second">>, <<"third">>],
    [
        {Seg1, Offset1},
        {Seg2, Offset2},
        {Seg3, Offset3}
    ] = with_offsets(Data),
    [
        Checksum1,
        Checksum2,
        Checksum3
    ] = [binary:encode_hex(sha256(S)) || S <- Data],

    Meta = #{size := Filesize} = meta(Filename, Data),

    ?assertRCName(success, emqtt:publish(C, mk_init_topic(Config, FileId), encode_meta(Meta), 1)),

    ?assertRCName(
        success,
        emqtt:publish(C, mk_segment_topic(Config, FileId, Offset1, Checksum1), Seg1, 1)
    ),

    % segment is corrupted
    ?assertRCName(
        unspecified_error,
        emqtt:publish(
            C, mk_segment_topic(Config, FileId, Offset2, Checksum2), <<Seg2/binary, 42>>, 1
        )
    ),

    % retry
    ?assertRCName(
        success,
        emqtt:publish(C, mk_segment_topic(Config, FileId, Offset2, Checksum2), Seg2, 1)
    ),

    ?assertRCName(
        success,
        emqtt:publish(C, mk_segment_topic(Config, FileId, Offset3, Checksum3), Seg3, 1)
    ),

    ?assertEqual(
        ok,
        emqx_ft_test_helpers:fin_result(
            mode(Config), ClientId, C, mk_fin_topic(Config, FileId, Filesize)
        )
    ).

t_assemble_crash(Config) ->
    C = ?config(client, Config),

    meck:new(emqx_ft_storage_fs),
    meck:expect(emqx_ft_storage_fs, assemble, fun(_, _, _, _) -> meck:exception(error, oops) end),

    ?assertRCName(
        unspecified_error,
        emqtt:publish(C, <<"$file/someid/fin">>, <<>>, 1)
    ),

    meck:unload(emqx_ft_storage_fs).

t_assemble_timeout(Config) ->
    C = ?config(client, Config),
    ClientId = ?config(clientid, Config),

    SleepForever = fun() ->
        Ref = make_ref(),
        receive
            Ref -> ok
        end
    end,

    ok = meck:new(emqx_ft_storage, [passthrough]),
    ok = meck:expect(emqx_ft_storage, assemble, fun(_, _, _) ->
        {async, spawn_link(SleepForever)}
    end),

    {Time, Res} = timer:tc(
        fun() ->
            emqx_ft_test_helpers:fin_result(
                mode(Config), ClientId, C, <<"$file/someid/fin/9999999">>
            )
        end
    ),

    ok = meck:unload(emqx_ft_storage),

    ?assertEqual(
        {error, unspecified_error},
        Res
    ),

    ?assert(2_000_000 < Time).

t_client_disconnect_while_assembling(Config) ->
    ClientId = atom_to_binary(?FUNCTION_NAME),
    {ok, Client} = emqtt:start_link([{proto_ver, v5}, {clientid, ClientId}]),
    {ok, _} = emqtt:connect(Client),

    Filename = "topsecret.pdf",
    FileId = <<"f1">>,

    Data = <<"data">>,

    Meta = #{size := Filesize} = meta(Filename, Data),

    ?assertRCName(
        success,
        emqtt:publish(Client, mk_init_topic(Config, FileId), encode_meta(Meta), 1)
    ),

    ?assertRCName(
        success,
        emqtt:publish(Client, mk_segment_topic(Config, FileId, 0), Data, 1)
    ),

    ResponseTopic = emqx_ft_test_helpers:response_topic(ClientId),

    {ok, OtherClient} = emqtt:start_link([{proto_ver, v5}, {clientid, <<"other">>}]),
    {ok, _} = emqtt:connect(OtherClient),
    {ok, _, _} = emqtt:subscribe(OtherClient, ResponseTopic, 1),

    FinTopic = mk_fin_topic(Config, FileId, Filesize),

    ?assertRCName(
        success,
        emqtt:publish(Client, FinTopic, <<>>, 1)
    ),

    ok = emqtt:stop(Client),

    ResultReceive = fun Receive() ->
        receive
            {publish, #{payload := Payload, topic := ResponseTopic}} ->
                case emqx_utils_json:decode(Payload) of
                    #{<<"topic">> := FinTopic, <<"reason_code">> := 0} ->
                        ok;
                    #{<<"topic">> := FinTopic, <<"reason_code">> := _} = FTResult ->
                        ct:fail("unexpected fin result: ~p", [FTResult]);
                    _ ->
                        Receive()
                end
        after 1000 ->
            ct:fail("timeout waiting for fin result")
        end
    end,
    ResultReceive(),

    ok = emqtt:stop(OtherClient).

%%--------------------------------------------------------------------
%% Cluster tests
%%--------------------------------------------------------------------

t_switch_node(Config) ->
    [Node | _] = ?config(cluster_nodes, Config),
    AdditionalNodePort = emqx_ft_test_helpers:tcp_port(Node),

    ClientId = <<"t_switch_node-migrating_client">>,

    {ok, C1} = emqtt:start_link([{proto_ver, v5}, {clientid, ClientId}, {port, AdditionalNodePort}]),
    {ok, _} = emqtt:connect(C1),

    Filename = "multinode_upload.txt",
    FileId = <<"f1">>,

    Data = [<<"first">>, <<"second">>, <<"third">>],
    [{Data0, Offset0}, {Data1, Offset1}, {Data2, Offset2}] = with_offsets(Data),

    %% First, publist metadata and the first segment to the additional node

    Meta = #{size := Filesize} = meta(Filename, Data),

    ?assertRCName(
        success,
        emqtt:publish(C1, mk_init_topic(Config, FileId), encode_meta(Meta), 1)
    ),
    ?assertRCName(
        success,
        emqtt:publish(C1, mk_segment_topic(Config, FileId, Offset0), Data0, 1)
    ),

    %% Then, switch the client to the main node
    %% and publish the rest of the segments

    ok = emqtt:stop(C1),
    {ok, C2} = emqtt:start_link([{proto_ver, v5}, {clientid, ClientId}]),
    {ok, _} = emqtt:connect(C2),

    ?assertRCName(
        success,
        emqtt:publish(C2, mk_segment_topic(Config, FileId, Offset1), Data1, 1)
    ),
    ?assertRCName(
        success,
        emqtt:publish(C2, mk_segment_topic(Config, FileId, Offset2), Data2, 1)
    ),

    ?assertRCName(
        success,
        emqtt:publish(C2, mk_fin_topic(Config, FileId, Filesize), <<>>, 1)
    ),

    ok = emqtt:stop(C2),

    %% Now check consistency of the file

    [Export] = list_files(ClientId),
    ?assertEqual(
        {ok, iolist_to_binary(Data)},
        read_export(Export)
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
        {fun send_filemeta/3, [Config, Meta]},
        {fun send_segment/4, [Config, 0, 100]},
        {fun send_segment/4, [Config, 100, 100]},
        {fun send_segment/4, [Config, 200, 100]},
        % Disconnect the client cleanly
        {fun stop_mqtt_client/1, []},
        % Connect to the broker on `Node1`
        {fun connect_mqtt_client/2, [Node1]},
        % Connect to the broker on `Node2` without first disconnecting from `Node1`
        % Client forgot the state for some reason and started the transfer again.
        % (assuming this is usual for a client on a device that was rebooted)
        {fun connect_mqtt_client/2, [Node2]},
        {fun send_filemeta/3, [Config, Meta]},
        % This time it chose 200 bytes as a segment size
        {fun send_segment/4, [Config, 0, 200]},
        {fun send_segment/4, [Config, 200, 200]},
        % But now it downscaled back to 100 bytes segments
        {fun send_segment/4, [Config, 400, 100]},
        % Client lost connectivity and reconnected
        % (also had last few segments unacked and decided to resend them)
        {fun connect_mqtt_client/2, [Node2]},
        {fun send_segment/4, [Config, 200, 200]},
        {fun send_segment/4, [Config, 400, 200]},
        % Client lost connectivity and reconnected, this time to another node
        % (also had last segment unacked and decided to resend it)
        {fun connect_mqtt_client/2, [Node1]},
        {fun send_segment/4, [Config, 400, 200]},
        {fun send_segment/4, [Config, 600, eof]},
        {fun send_finish/2, [Config]},
        % Client lost connectivity and reconnected, this time to the current node
        % (client had `fin` unacked and decided to resend it)
        {fun connect_mqtt_client/2, [NodeSelf]},
        {fun send_finish/2, [Config]}
    ],
    _Context = run_commands(Commands, Context),

    Exports = list_files(?config(clientid, Config)),

    Node1Str = atom_to_list(Node1),
    % TODO: this testcase is specific to local fs storage backend
    ?assertMatch(
        [#{"node" := Node1Str}],
        fs_exported_file_attributes(Exports)
    ),

    [
        ?assertEqual({ok, Payload}, read_export(Export))
     || Export <- Exports
    ].

t_concurrent_fins(Config) ->
    ct:timetrap({seconds, 10}),

    NodeSelf = node(),
    [Node1, Node2] = ?config(cluster_nodes, Config),

    ClientId = iolist_to_binary([
        ?config(clientid, Config),
        integer_to_list(erlang:unique_integer())
    ]),
    FileId = emqx_guid:to_hexstr(emqx_guid:gen()),
    Filename = "migratory-birds-in-southern-hemisphere-2013.pdf",
    Filesize = 100,
    Gen = emqx_ft_content_gen:new({{ClientId, FileId}, Filesize}, 16),
    Payload = iolist_to_binary(emqx_ft_content_gen:consume(Gen, fun({Chunk, _, _}) -> Chunk end)),
    Meta = meta(Filename, Payload),

    %% Send filemeta and segments to Node1
    Context0 = #{
        clientid => ClientId,
        fileid => FileId,
        filesize => Filesize,
        payload => Payload
    },

    Context1 = run_commands(
        [
            {fun connect_mqtt_client/2, [Node1]},
            {fun send_filemeta/3, [Config, Meta]},
            {fun send_segment/4, [Config, 0, 100]},
            {fun stop_mqtt_client/1, []}
        ],
        Context0
    ),

    %% Now send fins concurrently to the 3 nodes
    Nodes = [Node1, Node2, NodeSelf],
    SendFin = fun(Node) ->
        run_commands(
            [
                {fun connect_mqtt_client/2, [Node]},
                {fun send_finish/2, [Config]}
            ],
            Context1
        )
    end,

    PidMons = lists:map(
        fun(Node) ->
            erlang:spawn_monitor(fun F() ->
                _ = erlang:process_flag(trap_exit, true),
                try
                    SendFin(Node)
                catch
                    C:E ->
                        % NOTE: random delay to avoid livelock conditions
                        ct:pal("Node ~p did not send finish successfully: ~p:~p", [Node, C, E]),
                        ok = timer:sleep(rand:uniform(10)),
                        F()
                end
            end)
        end,
        Nodes
    ),
    ok = lists:foreach(
        fun({Pid, MRef}) ->
            receive
                {'DOWN', MRef, process, Pid, normal} -> ok
            end
        end,
        PidMons
    ),

    %% Only one node should have the file
    Exports = list_files(ClientId),
    case fs_exported_file_attributes(Exports) of
        [#{"node" := _Node}] ->
            ok;
        [#{"node" := _Node} | _] = Files ->
            % ...But we can't really guarantee that
            ct:comment({multiple_files_on_different_nodes, Files})
    end.

%%------------------------------------------------------------------------------
%% Command helpers
%%------------------------------------------------------------------------------

%% Command runners

run_commands(Commands, Context) ->
    lists:foldl(fun run_command/2, Context, Commands).

run_command({Command, Args}, Context) ->
    ct:pal("COMMAND ~p ~p", [erlang:fun_info(Command, name), Args]),
    erlang:apply(Command, Args ++ [Context]).

%% Commands

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

send_filemeta(Config, Meta, Context = #{client := Client, fileid := FileId}) ->
    ?assertRCName(
        success,
        emqtt:publish(Client, mk_init_topic(Config, FileId), encode_meta(Meta), 1)
    ),
    Context.

send_segment(
    Config, Offset, Size, Context = #{client := Client, fileid := FileId, payload := Payload}
) ->
    Data =
        case Size of
            eof ->
                binary:part(Payload, Offset, byte_size(Payload) - Offset);
            N ->
                binary:part(Payload, Offset, N)
        end,
    ?assertRCName(
        success,
        emqtt:publish(Client, mk_segment_topic(Config, FileId, Offset), Data, 1)
    ),
    Context.

send_finish(Config, Context = #{client := Client, fileid := FileId, filesize := Filesize}) ->
    ?assertRCName(
        success,
        emqtt:publish(Client, mk_fin_topic(Config, FileId, Filesize), <<>>, 1)
    ),
    Context.

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

fs_exported_file_attributes(FSExports) ->
    lists:map(
        fun(#{uri := URIString}) ->
            #{query := QS} = uri_string:parse(URIString),
            maps:from_list(uri_string:dissect_query(QS))
        end,
        lists:sort(FSExports)
    ).

mk_init_topic(Config, FileId) ->
    RequestTopicPrefix = request_topic_prefix(Config, FileId),
    <<RequestTopicPrefix/binary, "/init">>.

mk_segment_topic(Config, FileId, Offset) when is_integer(Offset) ->
    mk_segment_topic(Config, FileId, integer_to_binary(Offset));
mk_segment_topic(Config, FileId, Offset) when is_binary(Offset) ->
    RequestTopicPrefix = request_topic_prefix(Config, FileId),
    <<RequestTopicPrefix/binary, "/", Offset/binary>>.

mk_segment_topic(Config, FileId, Offset, Checksum) when is_integer(Offset) ->
    mk_segment_topic(Config, FileId, integer_to_binary(Offset), Checksum);
mk_segment_topic(Config, FileId, Offset, Checksum) when is_binary(Offset) ->
    RequestTopicPrefix = request_topic_prefix(Config, FileId),
    <<RequestTopicPrefix/binary, "/", Offset/binary, "/", Checksum/binary>>.

mk_fin_topic(Config, FileId, Size) when is_integer(Size) ->
    mk_fin_topic(Config, FileId, integer_to_binary(Size));
mk_fin_topic(Config, FileId, Size) when is_binary(Size) ->
    RequestTopicPrefix = request_topic_prefix(Config, FileId),
    <<RequestTopicPrefix/binary, "/fin/", Size/binary>>.

request_topic_prefix(Config, FileId) ->
    emqx_ft_test_helpers:request_topic_prefix(mode(Config), FileId).

with_offsets(Items) ->
    {List, _} = lists:mapfoldl(
        fun(Item, Offset) ->
            {{Item, integer_to_binary(Offset)}, Offset + byte_size(Item)}
        end,
        0,
        Items
    ),
    List.

sha256(Data) ->
    crypto:hash(sha256, Data).

meta(FileName, Data) ->
    FullData = iolist_to_binary(Data),
    #{
        name => FileName,
        checksum => {sha256, sha256(FullData)},
        expire_at => erlang:system_time(_Unit = second) + 3600,
        size => byte_size(FullData)
    }.

encode_meta(Meta) ->
    emqx_utils_json:encode(emqx_ft:encode_filemeta(Meta)).

list_files(ClientId) ->
    {ok, #{items := Files}} = emqx_ft_storage:files(),
    [File || File = #{transfer := {CId, _}} <- Files, CId == ClientId].

read_export(#{path := AbsFilepath}) ->
    % TODO: only works for the local filesystem exporter right now
    file:read_file(AbsFilepath).

set_client_specific_ft_dirs(ClientId, Config) ->
    FTRoot = emqx_ft_test_helpers:ft_root(Config),
    ok = emqx_config:put(
        [file_transfer, storage, local, segments, root],
        filename:join([FTRoot, ClientId, segments])
    ),
    ok = emqx_config:put(
        [file_transfer, storage, local, exporter, local, root],
        filename:join([FTRoot, ClientId, exports])
    ).

mode(Config) ->
    proplists:get_value(mode, Config, sync).
