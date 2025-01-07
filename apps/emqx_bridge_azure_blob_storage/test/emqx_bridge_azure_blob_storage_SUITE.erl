%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_azure_blob_storage_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-import(emqx_common_test_helpers, [on_exit/1]).
-import(emqx_utils_conv, [bin/1, str/1]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("erlazure/include/erlazure.hrl").
-include("../src/emqx_bridge_azure_blob_storage.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").

-define(ACCOUNT_NAME_BIN, <<"devstoreaccount1">>).
-define(ACCOUNT_KEY_BIN, <<
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsu"
    "Fq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
>>).

-define(CONF_MAX_RECORDS, 100).
-define(CONF_COLUMN_ORDER, ?CONF_COLUMN_ORDER([])).
-define(CONF_COLUMN_ORDER(T), [
    <<"publish_received_at">>,
    <<"clientid">>,
    <<"topic">>,
    <<"payload">>,
    <<"empty">>
    | T
]).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Endpoint = os:getenv("AZURITE_ENDPOINT", "http://toxiproxy:10000/"),
    #{host := Host, port := Port} = uri_string:parse(Endpoint),
    ProxyHost = os:getenv("PROXY_HOST", "toxiproxy"),
    ProxyPort = list_to_integer(os:getenv("PROXY_PORT", "8474")),
    ProxyName = "azurite_plain",
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    case emqx_common_test_helpers:is_tcp_server_available(Host, Port) of
        true ->
            Apps = emqx_cth_suite:start(
                [
                    emqx,
                    emqx_conf,
                    emqx_bridge_azure_blob_storage,
                    emqx_bridge,
                    emqx_rule_engine,
                    emqx_management,
                    emqx_mgmt_api_test_util:emqx_dashboard()
                ],
                #{work_dir => emqx_cth_suite:work_dir(Config)}
            ),
            {ok, _Api} = emqx_common_test_http:create_default_app(),
            [
                {apps, Apps},
                {proxy_name, ProxyName},
                {proxy_host, ProxyHost},
                {proxy_port, ProxyPort},
                {endpoint, Endpoint}
                | Config
            ];
        false ->
            case os:getenv("IS_CI") of
                "yes" ->
                    throw(no_azurite);
                _ ->
                    {skip, no_azurite}
            end
    end.

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(TestCase, Config0) ->
    ct:timetrap(timer:seconds(31)),
    Endpoint = ?config(endpoint, Config0),
    UniqueNum = integer_to_binary(erlang:unique_integer()),
    Name = <<(atom_to_binary(TestCase))/binary, UniqueNum/binary>>,
    ConnectorConfig = connector_config(Name, Endpoint),
    ContainerName = container_name(Name),
    %% TODO: switch based on test
    ActionConfig0 =
        case lists:member(TestCase, direct_action_cases()) of
            true ->
                direct_action_config(#{
                    connector => Name,
                    parameters => #{container => ContainerName}
                });
            false ->
                aggreg_action_config(#{
                    connector => Name,
                    parameters => #{container => ContainerName}
                })
        end,
    ActionConfig = emqx_bridge_v2_testlib:parse_and_check(?ACTION_TYPE_BIN, Name, ActionConfig0),
    Client = new_control_driver(Endpoint),
    ct:pal("container name: ~s", [ContainerName]),
    ok = ensure_new_container(ContainerName, Client),
    Config = [
        {bridge_kind, action},
        {action_type, ?ACTION_TYPE_BIN},
        {action_name, Name},
        {action_config, ActionConfig},
        {connector_name, Name},
        {connector_type, ?CONNECTOR_TYPE_BIN},
        {connector_config, ConnectorConfig},
        {container_name, ContainerName},
        {client, Client}
        | Config0
    ],
    Config.

end_per_testcase(_Testcase, Config) ->
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    emqx_common_test_helpers:call_janitor(),
    ok = snabbkaffe:stop(),
    ok.

direct_action_cases() ->
    [
        t_sync_query,
        t_sync_query_down,
        t_max_block_size_direct_transfer
    ].

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

new_control_driver(Endpoint) ->
    {ok, Client} = erlazure:new(#{
        endpoint => Endpoint,
        account => binary_to_list(?ACCOUNT_NAME_BIN),
        key => binary_to_list(?ACCOUNT_KEY_BIN)
    }),
    Client.

container_name(Name) ->
    IOList = re:replace(bin(Name), <<"[^a-z0-9-]">>, <<"-">>, [global]),
    iolist_to_binary(IOList).

ensure_new_container(Name0, Client) ->
    Name = str(Name0),
    case erlazure:create_container(Client, Name) of
        {ok, created} ->
            ok;
        {error, #{code := "ContainerAlreadyExists"}} ->
            {ok, deleted} = erlazure:delete_container(Client, Name),
            {ok, created} = erlazure:create_container(Client, Name),
            ok
    end,
    on_exit(fun() -> {ok, deleted} = erlazure:delete_container(Client, Name) end),
    ok.

connector_config(Name, Endpoint) ->
    InnerConfigMap0 =
        #{
            <<"enable">> => true,
            <<"tags">> => [<<"bridge">>],
            <<"description">> => <<"my cool bridge">>,
            <<"endpoint">> => Endpoint,
            %% Default Azurite credentials
            %% See: https://github.com/Azure/Azurite/blob/main/README.md#default-storage-account
            <<"account_name">> => ?ACCOUNT_NAME_BIN,
            <<"account_key">> => ?ACCOUNT_KEY_BIN,
            <<"resource_opts">> =>
                #{
                    <<"health_check_interval">> => <<"1s">>,
                    <<"start_after_created">> => true,
                    <<"start_timeout">> => <<"5s">>
                }
        },
    emqx_bridge_v2_testlib:parse_and_check_connector(?CONNECTOR_TYPE_BIN, Name, InnerConfigMap0).

direct_action_config(Overrides0) ->
    Overrides = emqx_utils_maps:binary_key_map(Overrides0),
    CommonConfig =
        #{
            <<"enable">> => true,
            <<"connector">> => <<"please override">>,
            <<"parameters">> =>
                #{
                    <<"mode">> => <<"direct">>,
                    <<"container">> => <<"${payload.c}">>,
                    <<"blob">> => <<"${payload.b}">>,
                    <<"content">> => <<"${.}">>
                },
            <<"resource_opts">> => #{
                <<"batch_size">> => 1,
                <<"batch_time">> => <<"0ms">>,
                <<"buffer_mode">> => <<"memory_only">>,
                <<"buffer_seg_bytes">> => <<"10MB">>,
                <<"health_check_interval">> => <<"1s">>,
                <<"inflight_window">> => 100,
                <<"max_buffer_bytes">> => <<"256MB">>,
                <<"metrics_flush_interval">> => <<"1s">>,
                <<"query_mode">> => <<"sync">>,
                <<"request_ttl">> => <<"15s">>,
                <<"resume_interval">> => <<"1s">>,
                <<"worker_pool_size">> => <<"1">>
            }
        },
    emqx_utils_maps:deep_merge(CommonConfig, Overrides).

aggreg_action_config(Overrides0) ->
    Overrides = emqx_utils_maps:binary_key_map(Overrides0),
    CommonConfig =
        #{
            <<"enable">> => true,
            <<"connector">> => <<"please override">>,
            <<"parameters">> =>
                #{
                    <<"mode">> => <<"aggregated">>,
                    <<"aggregation">> => #{
                        <<"container">> => #{
                            <<"type">> => <<"csv">>,
                            <<"column_order">> => ?CONF_COLUMN_ORDER
                        },
                        <<"time_interval">> => <<"4s">>,
                        <<"max_records">> => ?CONF_MAX_RECORDS
                    },
                    <<"container">> => <<"mycontainer">>,
                    <<"blob">> => <<"${action}/${node}/${datetime.rfc3339}/${sequence}">>
                },
            <<"resource_opts">> => #{
                <<"batch_size">> => 100,
                <<"batch_time">> => <<"10ms">>,
                <<"buffer_mode">> => <<"memory_only">>,
                <<"buffer_seg_bytes">> => <<"10MB">>,
                <<"health_check_interval">> => <<"1s">>,
                <<"inflight_window">> => 100,
                <<"max_buffer_bytes">> => <<"256MB">>,
                <<"metrics_flush_interval">> => <<"1s">>,
                <<"query_mode">> => <<"sync">>,
                <<"request_ttl">> => <<"15s">>,
                <<"resume_interval">> => <<"1s">>,
                <<"worker_pool_size">> => <<"1">>
            }
        },
    emqx_utils_maps:deep_merge(CommonConfig, Overrides).

aggreg_id(BridgeName) ->
    {?ACTION_TYPE_BIN, BridgeName}.

mk_message_event(ClientId, Topic, Payload) ->
    Message = emqx_message:make(bin(ClientId), bin(Topic), Payload),
    {Event, _} = emqx_rule_events:eventmsg_publish(Message),
    emqx_utils_maps:binary_key_map(Event).

mk_message({ClientId, Topic, Payload}) ->
    emqx_message:make(bin(ClientId), bin(Topic), Payload).

publish_messages(MessageEvents) ->
    lists:foreach(fun emqx:publish/1, MessageEvents).

publish_messages_delayed(MessageEvents, Delay) ->
    lists:foreach(
        fun(Msg) ->
            emqx:publish(Msg),
            ct:sleep(Delay)
        end,
        MessageEvents
    ).

list_blobs(Config) ->
    Client = ?config(client, Config),
    ContainerName = ?config(container_name, Config),
    {Blobs, _} = erlazure:list_blobs(Client, str(ContainerName)),
    Blobs.

get_blob(BlobName, Config) ->
    Client = ?config(client, Config),
    ContainerName = ?config(container_name, Config),
    {ok, Blob} = erlazure:get_blob(Client, str(ContainerName), str(BlobName)),
    Blob.

get_and_decode_event(BlobName, Config) ->
    maps:update_with(
        <<"payload">>,
        fun(Raw) -> emqx_utils_json:decode(Raw, [return_maps]) end,
        emqx_utils_json:decode(get_blob(BlobName, Config), [return_maps])
    ).

list_committed_blocks(Config) ->
    Client = ?config(client, Config),
    Container0 = ?config(container_name, Config),
    Container = emqx_utils_conv:str(Container0),
    {Blobs, _} = erlazure:list_blobs(Client, Container),
    lists:map(
        fun(#cloud_blob{name = BlobName}) ->
            {Blocks, _} = erlazure:get_block_list(Client, Container, BlobName),
            {BlobName, [{Id, Type} || #blob_block{id = Id, type = Type} <- Blocks]}
        end,
        Blobs
    ).

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_start_stop(Config) ->
    ok = emqx_bridge_v2_testlib:t_start_stop(Config, azure_blob_storage_stop),
    ok.

t_create_via_http(Config) ->
    ok = emqx_bridge_v2_testlib:t_create_via_http(Config),
    ok.

t_on_get_status(Config) ->
    ok = emqx_bridge_v2_testlib:t_on_get_status(Config),
    ok.

%% Testing non-aggregated / direct action
t_sync_query(Config) ->
    ContainerName = ?config(container_name, Config),
    Topic = <<"t/a">>,
    Payload0 = #{
        <<"b">> => <<"myblob">>,
        <<"c">> => ContainerName,
        <<"x">> => <<"first data">>
    },
    Payload0Bin = emqx_utils_json:encode(Payload0),
    ClientId = <<"some_client">>,
    MsgEvent0 = mk_message_event(ClientId, Topic, Payload0Bin),
    ok = emqx_bridge_v2_testlib:t_sync_query(
        Config,
        fun() -> MsgEvent0 end,
        fun(Res) -> ?assertMatch(ok, Res) end,
        azure_blob_storage_bridge_connector_upload_ok
    ),
    Decoded0 = get_and_decode_event(<<"myblob">>, Config),
    ?assertMatch(#{<<"payload">> := #{<<"x">> := <<"first data">>}}, Decoded0),
    %% Test sending the same payload again, so that the same blob is written to.
    ResourceId = emqx_bridge_v2_testlib:resource_id(Config),
    BridgeId = emqx_bridge_v2_testlib:bridge_id(Config),
    Payload1 = Payload0#{<<"x">> => <<"new data">>},
    Payload1Bin = emqx_utils_json:encode(Payload1),
    MsgEvent1 = mk_message_event(ClientId, Topic, Payload1Bin),
    Message = {BridgeId, MsgEvent1},
    ?assertMatch(ok, emqx_resource:simple_sync_query(ResourceId, Message)),
    Decoded1 = get_and_decode_event(<<"myblob">>, Config),
    ?assertMatch(#{<<"payload">> := #{<<"x">> := <<"new data">>}}, Decoded1),
    ok.

%% Testing non-aggregated / direct action
t_sync_query_down(Config) ->
    ContainerName = ?config(container_name, Config),
    Payload0 = #{
        <<"b">> => <<"myblob">>,
        <<"c">> => ContainerName,
        <<"x">> => <<"first data">>
    },
    Payload0Bin = emqx_utils_json:encode(Payload0),
    ClientId = <<"some_client">>,
    ok = emqx_bridge_v2_testlib:t_sync_query_down(
        Config,
        #{
            make_message_fn => fun(Topic) -> mk_message({ClientId, Topic, Payload0Bin}) end,
            enter_tp_filter =>
                ?match_event(#{
                    ?snk_kind := azure_blob_storage_bridge_on_query_enter,
                    mode := direct
                }),
            error_tp_filter =>
                ?match_event(#{?snk_kind := azure_blob_storage_bridge_direct_upload_error}),
            success_tp_filter =>
                ?match_event(#{?snk_kind := azure_blob_storage_bridge_connector_upload_ok})
        }
    ),
    ok.

t_aggreg_upload(Config) ->
    ActionName = ?config(action_name, Config),
    AggregId = aggreg_id(ActionName),
    ?check_trace(
        #{timetrap => timer:seconds(30)},
        begin
            ActionNameString = unicode:characters_to_list(ActionName),
            NodeString = atom_to_list(node()),
            %% Create a bridge with the sample configuration.
            ?assertMatch({ok, _Bridge}, emqx_bridge_v2_testlib:create_bridge_api(Config)),
            {ok, _Rule} =
                emqx_bridge_v2_testlib:create_rule_and_action_http(
                    ?ACTION_TYPE_BIN, <<"">>, Config, #{
                        sql => <<
                            "SELECT"
                            "  *,"
                            "  strlen(payload) as psize,"
                            "  unix_ts_to_rfc3339(publish_received_at, 'millisecond') as publish_received_at"
                            "  FROM 'abs/#'"
                        >>
                    }
                ),
            Messages = lists:map(fun mk_message/1, [
                {<<"C1">>, T1 = <<"abs/a/b/c">>, P1 = <<"{\"hello\":\"world\"}">>},
                {<<"C2">>, T2 = <<"abs/foo/bar">>, P2 = <<"baz">>},
                {<<"C3">>, T3 = <<"abs/t/42">>, P3 = <<"">>},
                %% Won't match rule filter
                {<<"C4">>, <<"t/42">>, <<"won't appear in results">>}
            ]),
            ok = publish_messages(Messages),
            %% Wait until the delivery is completed.
            ?block_until(#{?snk_kind := connector_aggreg_delivery_completed, action := AggregId}),
            %% Check the uploaded objects.
            _Uploads =
                [#cloud_blob{name = BlobName, properties = UploadProps}] = list_blobs(Config),
            ?assertMatch(#{content_type := "text/csv"}, maps:from_list(UploadProps)),
            ?assertMatch(
                [ActionNameString, NodeString, _Datetime, _Seq = "0"],
                string:split(BlobName, "/", all)
            ),
            Content = get_blob(BlobName, Config),
            %% Verify that column order is respected.
            ?assertMatch(
                {ok, [
                    ?CONF_COLUMN_ORDER(_),
                    [_TS1, <<"C1">>, T1, P1, <<>> | _],
                    [_TS2, <<"C2">>, T2, P2, <<>> | _],
                    [_TS3, <<"C3">>, T3, P3, <<>> | _]
                ]},
                erl_csv:decode(Content)
            ),
            ok
        end,
        []
    ),
    ok.

%% This test verifies that the bridge will reuse existing aggregation buffer after a
%% restart.
t_aggreg_upload_restart(Config) ->
    ActionName = ?config(action_name, Config),
    AggregId = aggreg_id(ActionName),
    ?check_trace(
        #{timetrap => timer:seconds(30)},
        begin
            ActionNameString = unicode:characters_to_list(ActionName),
            NodeString = atom_to_list(node()),
            %% Create a bridge with the sample configuration.
            ?assertMatch({ok, _Bridge}, emqx_bridge_v2_testlib:create_bridge_api(Config)),
            {ok, _Rule} =
                emqx_bridge_v2_testlib:create_rule_and_action_http(
                    ?ACTION_TYPE_BIN, <<"">>, Config, #{
                        sql => <<
                            "SELECT"
                            "  *,"
                            "  strlen(payload) as psize,"
                            "  unix_ts_to_rfc3339(publish_received_at, 'millisecond') as publish_received_at"
                            "  FROM 'abs/#'"
                        >>
                    }
                ),
            Messages = lists:map(fun mk_message/1, [
                {<<"C1">>, T1 = <<"abs/a/b/c">>, P1 = <<"{\"hello\":\"world\"}">>},
                {<<"C2">>, T2 = <<"abs/foo/bar">>, P2 = <<"baz">>},
                {<<"C3">>, T3 = <<"abs/t/42">>, P3 = <<"">>}
            ]),
            ok = publish_messages(Messages),
            {ok, _} = ?block_until(#{
                ?snk_kind := connector_aggreg_records_written, action := AggregId
            }),
            {ok, _} =
                ?wait_async_action(
                    begin
                        %% Restart the bridge.
                        {ok, {{_, 204, _}, _, _}} = emqx_bridge_v2_testlib:disable_kind_http_api(
                            Config
                        ),
                        {ok, {{_, 204, _}, _, _}} = emqx_bridge_v2_testlib:enable_kind_http_api(
                            Config
                        ),
                        %% Send some more messages.
                        ok = publish_messages(Messages)
                    end,
                    #{?snk_kind := connector_aggreg_records_written, action := AggregId}
                ),
            %% Wait until the delivery is completed.
            ?block_until(#{?snk_kind := connector_aggreg_delivery_completed, action := AggregId}),

            %% Check there's still only one upload.
            [#cloud_blob{name = BlobName, properties = UploadProps}] = list_blobs(Config),
            ?assertMatch(#{content_type := "text/csv"}, maps:from_list(UploadProps)),
            ?assertMatch(
                [ActionNameString, NodeString, _Datetime, _Seq = "0"],
                string:split(BlobName, "/", all)
            ),
            Content = get_blob(BlobName, Config),
            ?assertMatch(
                {ok, [
                    ?CONF_COLUMN_ORDER(_),
                    [_TS1, <<"C1">>, T1, P1, <<>> | _],
                    [_TS2, <<"C2">>, T2, P2, <<>> | _],
                    [_TS3, <<"C3">>, T3, P3, <<>> | _],
                    [_TS1, <<"C1">>, T1, P1, <<>> | _],
                    [_TS2, <<"C2">>, T2, P2, <<>> | _],
                    [_TS3, <<"C3">>, T3, P3, <<>> | _]
                ]},
                erl_csv:decode(Content)
            ),
            ok
        end,
        []
    ),
    ok.

%% This test verifies that the bridge can recover from a buffer file corruption, and does
%% so while preserving uncompromised data.
t_aggreg_upload_restart_corrupted(Config) ->
    ActionName = ?config(action_name, Config),
    BatchSize = ?CONF_MAX_RECORDS div 2,
    Opts = #{
        aggreg_id => aggreg_id(ActionName),
        batch_size => BatchSize,
        rule_sql => <<
            "SELECT"
            "  *,"
            "  strlen(payload) as psize,"
            "  unix_ts_to_rfc3339(publish_received_at, 'millisecond') as publish_received_at"
            "  FROM 'abs/#'"
        >>,
        make_message_fn => fun(N) ->
            mk_message(
                {integer_to_binary(N), <<"abs/a/b/c">>, <<"{\"hello\":\"world\"}">>}
            )
        end,
        message_check_fn => fun(Context) ->
            #{
                messages_before := Messages1,
                messages_after := Messages2
            } = Context,
            [#cloud_blob{name = BlobName}] = list_blobs(Config),
            {ok, CSV = [_Header | Rows]} = erl_csv:decode(get_blob(BlobName, Config)),
            NRows = length(Rows),
            ?assert(NRows > BatchSize, CSV),
            Expected = [
                {ClientId, Topic, Payload}
             || #message{
                    from = ClientId,
                    topic = Topic,
                    payload = Payload
                } <- lists:sublist(Messages1, NRows - BatchSize) ++ Messages2
            ],
            ?assertEqual(
                Expected,
                [{ClientID, Topic, Payload} || [_TS, ClientID, Topic, Payload | _] <- Rows],
                CSV
            ),

            ok
        end
    },
    emqx_bridge_v2_testlib:t_aggreg_upload_restart_corrupted(Config, Opts),
    ok.

%% This test verifies that the bridge will finish uploading a buffer file after a restart.
t_aggreg_pending_upload_restart(Config) ->
    ActionName = ?config(action_name, Config),
    AggregId = aggreg_id(ActionName),
    ?check_trace(
        #{timetrap => timer:seconds(30)},
        begin
            %% Create a bridge with the sample configuration.
            ?assertMatch(
                {ok, _Bridge},
                emqx_bridge_v2_testlib:create_bridge_api(
                    Config,
                    #{
                        <<"parameters">> =>
                            #{
                                <<"min_block_size">> => <<"1024B">>
                            }
                    }
                )
            ),
            {ok, _Rule} =
                emqx_bridge_v2_testlib:create_rule_and_action_http(
                    ?ACTION_TYPE_BIN, <<"">>, Config, #{
                        sql => <<
                            "SELECT"
                            "  *,"
                            "  strlen(payload) as psize,"
                            "  unix_ts_to_rfc3339(publish_received_at, 'millisecond') as publish_received_at"
                            "  FROM 'abs/#'"
                        >>
                    }
                ),

            %% Send few large messages that will require multipart upload.
            %% Ensure that they span multiple batch queries.
            Payload0 = iolist_to_binary(lists:duplicate(128, "PAYLOAD!")),
            %% Payload0 = iolist_to_binary(lists:duplicate(128 * 1024, "PAYLOAD!")),
            Messages = [
                {integer_to_binary(N), <<"abs/a/b/c">>, Payload0}
             || N <- lists:seq(1, 10)
            ],

            {ok, {ok, _}} =
                ?wait_async_action(
                    publish_messages_delayed(lists:map(fun mk_message/1, Messages), 10),
                    %% Wait until the multipart upload is started.
                    #{?snk_kind := azure_blob_storage_will_write_chunk}
                ),
            ct:pal("published messages"),

            %% Stop the bridge.
            {ok, {{_, 204, _}, _, _}} = emqx_bridge_v2_testlib:disable_kind_http_api(
                Config
            ),
            ct:pal("stopped bridge"),
            %% Verify that pending uploads have been gracefully aborted.
            ?assertMatch([{_Name, []}], list_committed_blocks(Config)),

            %% Restart the bridge.
            {ok, {{_, 204, _}, _, _}} = emqx_bridge_v2_testlib:enable_kind_http_api(
                Config
            ),
            ct:pal("restarted bridge"),

            %% Wait until the delivery is completed.
            {ok, _} = ?block_until(#{
                ?snk_kind := connector_aggreg_delivery_completed, action := AggregId
            }),
            ct:pal("delivery complete"),

            %% Check that delivery contains all the messages.
            ?assertMatch([{_Name, [_ | _]}], list_committed_blocks(Config)),
            [#cloud_blob{name = BlobName}] = list_blobs(Config),
            {ok, CSV = [_Header | Rows]} = erl_csv:decode(get_blob(BlobName, Config)),
            ?assertEqual(
                Messages,
                [{ClientID, Topic, Payload} || [_TS, ClientID, Topic, Payload | _] <- Rows],
                CSV
            ),

            ok
        end,
        []
    ),
    ok.

%% Checks that we return an unrecoverable error if the payload exceeds `max_block_size'.
t_max_block_size_direct_transfer(Config) ->
    {ok, _Bridge} = emqx_bridge_v2_testlib:create_bridge_api(
        Config,
        #{<<"parameters">> => #{<<"max_block_size">> => <<"1B">>}}
    ),
    Topic = <<"t/a">>,
    ClientId = <<"myclient">>,
    ResourceId = emqx_bridge_v2_testlib:resource_id(Config),
    BridgeId = emqx_bridge_v2_testlib:bridge_id(Config),
    Payload = <<"too large">>,
    PayloadBin = emqx_utils_json:encode(Payload),
    MsgEvent = mk_message_event(ClientId, Topic, PayloadBin),
    Message = {BridgeId, MsgEvent},
    ?assertMatch(
        {error, {unrecoverable_error, payload_too_large}},
        emqx_resource:simple_sync_query(ResourceId, Message)
    ),
    ok.

%% Checks that account keys that are not base64 encoded return a friendly error.
t_bad_account_key(Config) ->
    ?check_trace(
        begin
            ?assertMatch(
                {400, #{
                    <<"message">> := #{
                        <<"kind">> := <<"validation_error">>,
                        <<"reason">> := <<"bad account key", _/binary>>
                    }
                }},
                emqx_bridge_v2_testlib:simplify_result(
                    emqx_bridge_v2_testlib:create_connector_api(
                        Config,
                        #{<<"account_key">> => <<"aaa">>}
                    )
                )
            ),
            ?assertMatch(
                {400, #{
                    <<"message">> := #{
                        <<"kind">> := <<"validation_error">>,
                        <<"reason">> := <<"bad account key", _/binary>>
                    }
                }},
                emqx_bridge_v2_testlib:simplify_result(
                    emqx_bridge_v2_testlib:probe_connector_api(
                        Config,
                        #{<<"account_key">> => <<"aaa">>}
                    )
                )
            ),
            ok
        end,
        []
    ),
    ok.

%% Checks that account names that are non-existent return a friendly error.
t_bad_account_name(Config) ->
    ConnectorConfig0 = ?config(connector_config, Config),
    ConnectorConfig = maps:remove(<<"endpoint">>, ConnectorConfig0),
    ?check_trace(
        begin
            Res0 = emqx_bridge_v2_testlib:simplify_result(
                emqx_bridge_v2_testlib:probe_connector_api(
                    [{connector_config, ConnectorConfig} | Config],
                    #{<<"account_name">> => <<"idontexistzzzzzaa">>}
                )
            ),
            ?assertMatch({400, #{<<"message">> := _}}, Res0),
            {400, #{<<"message">> := Msg}} = Res0,
            ?assertEqual(match, re:run(Msg, <<"failed_connect">>, [{capture, none}])),
            ?assertEqual(match, re:run(Msg, <<"nxdomain">>, [{capture, none}])),
            ok
        end,
        []
    ),
    ok.

t_deobfuscate_connector(Config) ->
    emqx_bridge_v2_testlib:?FUNCTION_NAME(Config).

%% Checks that we verify at runtime that the provided account key is a valid base64 string.
t_create_connector_with_obfuscated_key(Config0) ->
    ?check_trace(
        begin
            RedactedValue = <<"******">>,
            Config = emqx_bridge_v2_testlib:proplist_update(Config0, connector_config, fun(Old) ->
                Old#{<<"account_key">> := RedactedValue}
            end),
            ?assertMatch(
                {201, #{
                    <<"status">> := <<"disconnected">>,
                    <<"status_reason">> := <<"bad account key", _/binary>>
                }},
                emqx_bridge_v2_testlib:simplify_result(
                    emqx_bridge_v2_testlib:create_connector_api(Config)
                )
            ),
            ok
        end,
        []
    ),
    ok.
