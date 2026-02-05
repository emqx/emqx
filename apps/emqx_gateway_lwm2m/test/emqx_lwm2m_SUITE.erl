%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_lwm2m_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-import(
    emqx_gateway_test_utils,
    [
        request/2,
        request/3
    ]
).

-define(PORT, 5783).

-define(LOGT(Format, Args), ct:pal("TEST_SUITE: " ++ Format, Args)).

-include("emqx_lwm2m.hrl").
-include_lib("emqx_gateway_coap/include/emqx_coap.hrl").
-include_lib("emqx/include/emqx_config.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").

-record(coap_content, {content_format, payload = <<>>}).

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() ->
    [
        {group, test_grp_0_register},
        {group, test_grp_1_read},
        {group, test_grp_2_write},
        {group, test_grp_create},
        {group, test_grp_delete},
        {group, test_grp_3_execute},
        {group, test_grp_4_discover},
        {group, test_grp_5_write_attr},
        {group, test_grp_6_observe},
        %% {group, test_grp_8_object_19}
        {group, test_grp_9_psm_queue_mode},
        {group, test_grp_10_rest_api},
        {group, test_grp_11_internal}
    ].

suite() -> [{timetrap, {seconds, 90}}].

groups() ->
    RepeatOpt = {repeat_until_all_ok, 1},
    [
        {test_grp_0_register, [RepeatOpt], [
            case01_register,
            case01_auth_expire,
            case01_update_not_restart_listener,
            case01_register_additional_opts,
            %% TODO now we can't handle partial decode packet
            %% case01_register_incorrect_opts,
            case01_register_report,
            case02_update_deregister,
            case02_update_no_lt,
            case02_update_lifetime_zero,
            case03_register_wrong_version,
            case03_register_missing_version,
            case03_register_version_1_1,
            case04_register_and_lifetime_timeout,
            case05_register_wrong_epn,
            %% case06_register_wrong_lifetime, %% now, will ignore wrong lifetime
            case07_register_alternate_path_01,
            case07_register_alternate_path_02,
            case07_register_alternate_path_unprefixed,
            case08_reregister,
            case09_auto_observe,
            case09_auto_observe_list,
            case02_update_publish_condition
        ]},
        {test_grp_1_read, [RepeatOpt], [
            case10_read,
            case10_read_bad_request,
            case10_read_separate_ack,
            case11_read_object_tlv,
            case11_read_object_json,
            case12_read_resource_opaque,
            case13_read_no_xml,
            case14_read_text_types,
            case15_read_tlv_types,
            case16_read_tlv_object_instances,
            case17_read_invalid_tlv,
            case18_read_error_codes,
            case19_read_text_invalid_basename,
            case20_read_tlv_variants
            %% NOTE: TLV 24-bit length requires payload > 64KB; not feasible over UDP.
        ]},
        {test_grp_2_write, [RepeatOpt], [
            case20_write,
            case21_write_object,
            case22_write_error,
            case20_single_write,
            case23_write_multi_resource_instance,
            case24_write_content_response,
            case25_write_invalid_json,
            case26_write_object_instance_mixed,
            case27_write_hex_encoding,
            case27_write_hex_encoding_invalid
        ]},
        {test_grp_create, [RepeatOpt], [
            case_create_basic,
            case_create_with_content
        ]},
        {test_grp_delete, [RepeatOpt], [
            case_delete_basic
        ]},
        {test_grp_3_execute, [RepeatOpt], [
            case30_execute, case31_execute_error
        ]},
        {test_grp_4_discover, [RepeatOpt], [
            case40_discover,
            case41_discover_error
        ]},
        {test_grp_5_write_attr, [RepeatOpt], [
            case50_write_attribute,
            case51_write_attr_error
        ]},
        {test_grp_6_observe, [RepeatOpt], [
            case60_observe,
            case61_observe_error,
            case62_cancel_observe_error,
            case63_observe_notify_object5,
            case64_coap_reset
        ]},
        {test_grp_9_psm_queue_mode, [RepeatOpt], [
            case90_psm_mode,
            case90_queue_mode
        ]},
        {test_grp_10_rest_api, [RepeatOpt], [
            case100_clients_api,
            case100_subscription_api
        ]},
        {test_grp_11_internal, [RepeatOpt], [
            case110_xml_object_helpers,
            case111_gateway_error_paths,
            case112_channel_unexpected_messages,
            case113_request_invalid_paths,
            case114_execute_args_undefined,
            case115_cmd_record_overflow,
            case116_command_headers,
            case116_command_headers_invalid_mheaders,
            case117_auth_failure,
            case118_authorize_denied,
            case119_open_session_error,
            case120_post_missing_uri_path,
            case121_delete_missing_uri_path,
            case122_connect_hook_error,
            case123_tlv_internal,
            case124_cmd_code_internal,
            case125_message_internal_errors,
            case126_message_insert_resource,
            case127_channel_internal_branches,
            case128_session_internal_branches,
            case129_write_hex_encoding,
            case130_auto_observe_list_config
        ]}
    ].

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx_gateway_lwm2m,
            emqx_gateway,
            emqx_auth,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(suite_apps, Config)),
    ok.

init_per_testcase(TestCase, Config) ->
    snabbkaffe:start_trace(),
    GatewayConfig =
        case TestCase of
            case09_auto_observe ->
                default_config(#{auto_observe => true});
            case09_auto_observe_list ->
                default_config_with_auto_observe_raw("[\"/3/0\"]");
            _ ->
                default_config()
        end,
    ok = emqx_conf_cli:load_config(?global_ns, GatewayConfig, #{mode => replace}),
    ensure_gateway_loaded(),

    {ok, ClientUdpSock} = gen_udp:open(0, [binary, {active, false}]),

    {ok, C} = emqtt:start_link([
        {host, "localhost"},
        {port, 1883},
        {clientid, <<"c1">>}
    ]),
    {ok, _} = emqtt:connect(C),
    timer:sleep(100),

    [{sock, ClientUdpSock}, {emqx_c, C} | Config].

end_per_testcase(_AllTestCase, Config) ->
    timer:sleep(300),
    gen_udp:close(?config(sock, Config)),
    emqtt:disconnect(?config(emqx_c, Config)),
    snabbkaffe:stop(),
    ok.

ensure_gateway_loaded() ->
    case emqx_gateway:lookup(lwm2m) of
        undefined ->
            {ok, _} = emqx_gateway:load(lwm2m, emqx:get_config([gateway, lwm2m])),
            ok;
        _ ->
            ok
    end.

default_config() ->
    default_config(#{}).

default_config(Overrides) ->
    XmlDir = filename:join(
        [
            emqx_common_test_helpers:proj_root(),
            "apps",
            "emqx_gateway_lwm2m",
            "lwm2m_xml"
        ]
    ),
    iolist_to_binary(
        io_lib:format(
            "\n"
            "gateway.lwm2m {\n"
            "  xml_dir = \"~s\"\n"
            "  lifetime_min = 1s\n"
            "  lifetime_max = 86400s\n"
            "  qmode_time_window = 22s\n"
            "  auto_observe = ~w\n"
            "  mountpoint = \"lwm2m/${username}\"\n"
            "  translators {\n"
            "    command = {topic = \"/dn/#\", qos = 0}\n"
            "    response = {topic = \"/up/resp\", qos = 0}\n"
            "    notify = {topic = \"/up/notify\", qos = 0}\n"
            "    register = {topic = \"/up/resp\", qos = 0}\n"
            "    update = {topic = \"/up/resp\", qos = 0}\n"
            "  }\n"
            "  listeners.udp.default {\n"
            "    bind = ~w\n"
            "  }\n"
            "}\n",
            [
                XmlDir,
                maps:get(auto_observe, Overrides, false),
                maps:get(bind, Overrides, ?PORT)
            ]
        )
    ).

default_config_with_auto_observe_raw(AutoObserveRaw) ->
    XmlDir = filename:join(
        [
            emqx_common_test_helpers:proj_root(),
            "apps",
            "emqx_gateway_lwm2m",
            "lwm2m_xml"
        ]
    ),
    iolist_to_binary(
        io_lib:format(
            "\n"
            "gateway.lwm2m {\n"
            "  xml_dir = \"~s\"\n"
            "  lifetime_min = 1s\n"
            "  lifetime_max = 86400s\n"
            "  qmode_time_window = 22s\n"
            "  auto_observe = ~s\n"
            "  mountpoint = \"lwm2m/${username}\"\n"
            "  translators {\n"
            "    command = {topic = \"/dn/#\", qos = 0}\n"
            "    response = {topic = \"/up/resp\", qos = 0}\n"
            "    notify = {topic = \"/up/notify\", qos = 0}\n"
            "    register = {topic = \"/up/resp\", qos = 0}\n"
            "    update = {topic = \"/up/resp\", qos = 0}\n"
            "  }\n"
            "  listeners.udp.default {\n"
            "    bind = ~w\n"
            "  }\n"
            "}\n",
            [XmlDir, AutoObserveRaw, ?PORT]
        )
    ).

default_config_with_update_condition_raw(UpdateConditionRaw) ->
    XmlDir = filename:join(
        [
            emqx_common_test_helpers:proj_root(),
            "apps",
            "emqx_gateway_lwm2m",
            "lwm2m_xml"
        ]
    ),
    iolist_to_binary(
        io_lib:format(
            "\n"
            "gateway.lwm2m {\n"
            "  xml_dir = \"~s\"\n"
            "  lifetime_min = 1s\n"
            "  lifetime_max = 86400s\n"
            "  qmode_time_window = 22s\n"
            "  auto_observe = false\n"
            "  mountpoint = \"lwm2m/${username}\"\n"
            "  update_msg_publish_condition = ~s\n"
            "  translators {\n"
            "    command = {topic = \"/dn/#\", qos = 0}\n"
            "    response = {topic = \"/up/resp\", qos = 0}\n"
            "    notify = {topic = \"/up/notify\", qos = 0}\n"
            "    register = {topic = \"/up/resp\", qos = 0}\n"
            "    update = {topic = \"/up/resp\", qos = 0}\n"
            "  }\n"
            "  listeners.udp.default {\n"
            "    bind = ~w\n"
            "  }\n"
            "}\n",
            [XmlDir, UpdateConditionRaw, ?PORT]
        )
    ).

default_port() ->
    ?PORT.

update_lwm2m_with_idle_timeout(IdleTimeout) ->
    Conf = emqx:get_raw_config([gateway, lwm2m]),
    emqx_gateway_conf:update_gateway(
        lwm2m,
        Conf#{<<"idle_timeout">> => IdleTimeout}
    ).

%%--------------------------------------------------------------------
%% Cases
%%--------------------------------------------------------------------

case01_register(Config) ->
    %%----------------------------------------
    %% REGISTER command
    %%----------------------------------------
    UdpSock = ?config(sock, Config),
    Epn = "urn:oma:lwm2m:oma:83",
    MsgId = 12,
    SubTopic = list_to_binary("lwm2m/" ++ Epn ++ "/dn/#"),

    emqx_gateway_test_utils:meck_emqx_hook_calls(),

    test_send_coap_request(
        UdpSock,
        post,
        sprintf("coap://127.0.0.1:~b/rd?ep=~ts&lt=345&lwm2m=1", [?PORT, Epn]),
        #coap_content{
            content_format = <<"text/plain">>,
            payload = <<"</1>, </2>, </3>, </4>, </5>">>
        },
        [],
        MsgId
    ),

    %% checkpoint 1 - called client.connect hook
    ?assertMatch(
        ['client.connect' | _],
        emqx_gateway_test_utils:collect_emqx_hooks_calls()
    ),

    %% checkpoint 2 - response
    #coap_message{type = Type, method = Method, id = RspId, options = Opts} =
        test_recv_coap_response(UdpSock),
    ack = Type,
    {ok, created} = Method,
    RspId = MsgId,
    Location = maps:get(location_path, Opts),
    ?assertNotEqual(undefined, Location),

    %% checkpoint 3 - verify subscribed topics
    timer:sleep(100),
    ?LOGT("all topics: ~p", [test_mqtt_broker:get_subscrbied_topics()]),
    true = lists:member(SubTopic, test_mqtt_broker:get_subscrbied_topics()),

    %%----------------------------------------
    %% DE-REGISTER command
    %%----------------------------------------
    ?LOGT("start to send DE-REGISTER command", []),
    MsgId3 = 52,
    test_send_coap_request(
        UdpSock,
        delete,
        sprintf("coap://127.0.0.1:~b~ts", [?PORT, join_path(Location, <<>>)]),
        #coap_content{payload = <<>>},
        [],
        MsgId3
    ),
    #coap_message{
        type = ack,
        id = RspId3,
        method = Method3
    } = test_recv_coap_response(UdpSock),
    {ok, deleted} = Method3,
    MsgId3 = RspId3,
    timer:sleep(50),
    false = lists:member(SubTopic, test_mqtt_broker:get_subscrbied_topics()).

case01_auth_expire(Config) ->
    ok = meck:new(emqx_access_control, [passthrough, no_history]),
    ok = meck:expect(
        emqx_access_control,
        authenticate,
        fun(_) ->
            {ok, #{is_superuser => false, expire_at => erlang:system_time(millisecond) + 500}}
        end
    ),
    try
        %%----------------------------------------
        %% REGISTER command
        %%----------------------------------------
        UdpSock = ?config(sock, Config),
        Epn = "urn:oma:lwm2m:oma:183",
        EpnBin = list_to_binary(Epn),
        MsgId = 12,

        ?assertWaitEvent(
            test_send_coap_request(
                UdpSock,
                post,
                sprintf("coap://127.0.0.1:~b/rd?ep=~ts&lt=345&lwm2m=1", [?PORT, Epn]),
                #coap_content{
                    content_format = <<"text/plain">>,
                    payload = <<"</1>, </2>, </3>, </4>, </5>">>
                },
                [],
                MsgId
            ),
            #{
                ?snk_kind := conn_process_terminated,
                clientid := EpnBin,
                reason := {shutdown, expired}
            },
            5000
        )
    after
        meck:unload(emqx_access_control)
    end.

case01_update_not_restart_listener(Config) ->
    %%----------------------------------------
    %% REGISTER command
    %%----------------------------------------
    UdpSock = ?config(sock, Config),
    Epn = "urn:oma:lwm2m:oma:3",
    MsgId = 12,

    test_send_coap_request(
        UdpSock,
        post,
        sprintf("coap://127.0.0.1:~b/rd?ep=~ts&lt=345&lwm2m=1", [?PORT, Epn]),
        #coap_content{
            content_format = <<"text/plain">>,
            payload = <<"</1>, </2>, </3>, </4>, </5>">>
        },
        [],
        MsgId
    ),

    %% checkpoint 1 - register success
    #coap_message{type = Type, method = Method, id = RspId, options = Opts} =
        test_recv_coap_response(UdpSock),
    ack = Type,
    {ok, created} = Method,
    RspId = MsgId,
    Location = maps:get(location_path, Opts),
    ?assertNotEqual(undefined, Location),

    update_lwm2m_with_idle_timeout(<<"20s">>),

    %% checkpoint 2 - deregister success after gateway update
    %%----------------------------------------
    %% DE-REGISTER command
    %%----------------------------------------
    ?LOGT("start to send DE-REGISTER command", []),
    MsgId3 = 52,
    test_send_coap_request(
        UdpSock,
        delete,
        sprintf("coap://127.0.0.1:~b~ts", [?PORT, join_path(Location, <<>>)]),
        #coap_content{payload = <<>>},
        [],
        MsgId3
    ),
    #coap_message{
        type = ack,
        id = RspId3,
        method = Method3
    } = test_recv_coap_response(UdpSock),
    {ok, deleted} = Method3,
    MsgId3 = RspId3,
    ok.

case01_register_additional_opts(Config) ->
    %%----------------------------------------
    %% REGISTER command
    %%----------------------------------------
    UdpSock = ?config(sock, Config),
    Epn = "urn:oma:lwm2m:oma:3",
    MsgId = 12,
    SubTopic = list_to_binary("lwm2m/" ++ Epn ++ "/dn/#"),

    AddOpts =
        "ep=~ts&lt=345&lwm2m=1&apn=psmA.eDRX0.ctnb&cust_opt=shawn&"
        "im=123&ct=1.4&mt=mdm9620&mv=1.2",
    test_send_coap_request(
        UdpSock,
        post,
        sprintf("coap://127.0.0.1:~b/rd?" ++ AddOpts, [?PORT, Epn]),
        #coap_content{
            content_format = <<"text/plain">>,
            payload = <<"</1>, </2>, </3>, </4>, </5>">>
        },
        [],
        MsgId
    ),

    %% checkpoint 1 - response
    #coap_message{type = Type, method = Method, id = RspId, options = Opts} =
        test_recv_coap_response(UdpSock),
    Type = ack,
    Method = {ok, created},
    RspId = MsgId,
    Location = maps:get(location_path, Opts),
    ?assertNotEqual(undefined, Location),

    %% checkpoint 2 - verify subscribed topics
    timer:sleep(50),

    true = lists:member(SubTopic, test_mqtt_broker:get_subscrbied_topics()),

    %%----------------------------------------
    %% DE-REGISTER command
    %%----------------------------------------
    ?LOGT("start to send DE-REGISTER command", []),
    MsgId3 = 52,
    test_send_coap_request(
        UdpSock,
        delete,
        sprintf("coap://127.0.0.1:~b~ts", [?PORT, join_path(Location, <<>>)]),
        #coap_content{payload = <<>>},
        [],
        MsgId3
    ),
    #coap_message{
        type = ack,
        id = RspId3,
        method = Method3
    } = test_recv_coap_response(UdpSock),
    {ok, deleted} = Method3,
    MsgId3 = RspId3,
    timer:sleep(50),
    false = lists:member(SubTopic, test_mqtt_broker:get_subscrbied_topics()).

case01_register_incorrect_opts(Config) ->
    %%----------------------------------------
    %% REGISTER command
    %%----------------------------------------
    UdpSock = ?config(sock, Config),
    Epn = "urn:oma:lwm2m:oma:3",
    MsgId = 12,

    AddOpts = "ep=~ts&lt=345&lwm2m=1&incorrect_opt",
    test_send_coap_request(
        UdpSock,
        post,
        sprintf("coap://127.0.0.1:~b/rd?" ++ AddOpts, [?PORT, Epn]),
        #coap_content{
            content_format = <<"text/plain">>,
            payload = <<"</1>, </2>, </3>, </4>, </5>">>
        },
        [],
        MsgId
    ),

    %% checkpoint 1 - response
    #coap_message{type = ack, method = Method, id = MsgId} =
        test_recv_coap_response(UdpSock),
    ?assertEqual({error, bad_request}, Method).

case01_register_report(Config) ->
    %%----------------------------------------
    %% REGISTER command
    %%----------------------------------------
    UdpSock = ?config(sock, Config),
    Epn = "urn:oma:lwm2m:oma:3",
    MsgId = 12,
    SubTopic = list_to_binary("lwm2m/" ++ Epn ++ "/dn/#"),
    ReportTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), ReportTopic, qos0),
    timer:sleep(200),

    test_send_coap_request(
        UdpSock,
        post,
        sprintf("coap://127.0.0.1:~b/rd?ep=~ts&lt=345&lwm2m=1", [?PORT, Epn]),
        #coap_content{
            content_format = <<"text/plain">>,
            payload = <<"</1>, </2>, </3>, </4>, </5>">>
        },
        [],
        MsgId
    ),

    #coap_message{type = Type, method = Method, id = RspId, options = Opts} =
        test_recv_coap_response(UdpSock),
    Type = ack,
    Method = {ok, created},
    RspId = MsgId,
    Location = maps:get(location_path, Opts),
    ?assertNotEqual(undefined, Location),

    timer:sleep(50),
    true = lists:member(SubTopic, test_mqtt_broker:get_subscrbied_topics()),

    ReadResult = emqx_utils_json:encode(
        #{
            <<"msgType">> => <<"register">>,
            <<"data">> => #{
                <<"alternatePath">> => <<"/">>,
                <<"ep">> => list_to_binary(Epn),
                <<"lt">> => 345,
                <<"lwm2m">> => <<"1">>,
                <<"objectList">> => [
                    <<"/1">>,
                    <<"/2">>,
                    <<"/3">>,
                    <<"/4">>,
                    <<"/5">>
                ]
            }
        }
    ),
    ?assertEqual(ReadResult, test_recv_mqtt_response(ReportTopic)),

    %%----------------------------------------
    %% DE-REGISTER command
    %%----------------------------------------
    ?LOGT("start to send DE-REGISTER command", []),
    MsgId3 = 52,
    test_send_coap_request(
        UdpSock,
        delete,
        sprintf("coap://127.0.0.1:~b~ts", [?PORT, join_path(Location, <<>>)]),
        #coap_content{payload = <<>>},
        [],
        MsgId3
    ),
    #coap_message{
        type = ack,
        id = RspId3,
        method = Method3
    } = test_recv_coap_response(UdpSock),
    {ok, deleted} = Method3,
    MsgId3 = RspId3,
    timer:sleep(50),
    false = lists:member(SubTopic, test_mqtt_broker:get_subscrbied_topics()).

case02_update_deregister(Config) ->
    %%----------------------------------------
    %% REGISTER command
    %%----------------------------------------
    UdpSock = ?config(sock, Config),
    Epn = "urn:oma:lwm2m:oma:3",
    MsgId = 12,
    SubTopic = list_to_binary("lwm2m/" ++ Epn ++ "/dn/#"),
    ReportTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), ReportTopic, qos0),
    timer:sleep(200),

    test_send_coap_request(
        UdpSock,
        post,
        sprintf("coap://127.0.0.1:~b/rd?ep=~ts&lt=345&lwm2m=1", [?PORT, Epn]),
        #coap_content{
            content_format = <<"text/plain">>,
            payload = <<"</1>, </2>, </3>, </4>, </5>">>
        },
        [],
        MsgId
    ),
    timer:sleep(100),
    #coap_message{
        type = ack,
        method = Method,
        options = Opts
    } = test_recv_coap_response(UdpSock),
    ?assertEqual({ok, created}, Method),

    ?LOGT("Options got: ~p", [Opts]),
    Location = maps:get(location_path, Opts),
    Register = emqx_utils_json:encode(
        #{
            <<"msgType">> => <<"register">>,
            <<"data">> => #{
                <<"alternatePath">> => <<"/">>,
                <<"ep">> => list_to_binary(Epn),
                <<"lt">> => 345,
                <<"lwm2m">> => <<"1">>,
                <<"objectList">> => [
                    <<"/1">>,
                    <<"/2">>,
                    <<"/3">>,
                    <<"/4">>,
                    <<"/5">>
                ]
            }
        }
    ),
    ?assertEqual(Register, test_recv_mqtt_response(ReportTopic)),

    %%----------------------------------------
    %% UPDATE command
    %%----------------------------------------
    ?LOGT("start to send UPDATE command", []),
    MsgId2 = 27,
    test_send_coap_request(
        UdpSock,
        post,
        sprintf("coap://127.0.0.1:~b~ts?lt=789", [?PORT, join_path(Location, <<>>)]),
        #coap_content{
            content_format = <<"text/plain">>,
            payload = <<"</1>, </2>, </3>, </4>, </5>, </6>">>
        },
        [],
        MsgId2
    ),
    #coap_message{
        type = ack,
        id = RspId2,
        method = Method2
    } = test_recv_coap_response(UdpSock),
    {ok, changed} = Method2,
    MsgId2 = RspId2,
    Update = emqx_utils_json:encode(
        #{
            <<"msgType">> => <<"update">>,
            <<"data">> => #{
                <<"alternatePath">> => <<"/">>,
                <<"ep">> => list_to_binary(Epn),
                <<"lt">> => 789,
                <<"lwm2m">> => <<"1">>,
                <<"objectList">> => [
                    <<"/1">>,
                    <<"/2">>,
                    <<"/3">>,
                    <<"/4">>,
                    <<"/5">>,
                    <<"/6">>
                ]
            }
        }
    ),
    ?assertEqual(Update, test_recv_mqtt_response(ReportTopic)),

    %%----------------------------------------
    %% DE-REGISTER command
    %%----------------------------------------
    ?LOGT("start to send DE-REGISTER command", []),
    MsgId3 = 52,
    test_send_coap_request(
        UdpSock,
        delete,
        sprintf("coap://127.0.0.1:~b~ts", [?PORT, join_path(Location, <<>>)]),
        #coap_content{payload = <<>>},
        [],
        MsgId3
    ),
    #coap_message{
        type = ack,
        id = RspId3,
        method = Method3
    } = test_recv_coap_response(UdpSock),
    {ok, deleted} = Method3,
    MsgId3 = RspId3,

    timer:sleep(50),
    false = lists:member(SubTopic, test_mqtt_broker:get_subscrbied_topics()).

case02_update_no_lt(Config) ->
    %%----------------------------------------
    %% REGISTER command
    %%----------------------------------------
    UdpSock = ?config(sock, Config),
    Epn = "urn:oma:lwm2m:oma:3",
    MsgId = 12,
    ReportTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), ReportTopic, qos0),
    timer:sleep(200),

    test_send_coap_request(
        UdpSock,
        post,
        sprintf("coap://127.0.0.1:~b/rd?ep=~ts&lt=345&lwm2m=1", [?PORT, Epn]),
        #coap_content{
            content_format = <<"text/plain">>,
            payload = <<"</1>, </2>, </3>, </4>, </5>">>
        },
        [],
        MsgId
    ),
    #coap_message{method = Method, options = Opts} = test_recv_coap_response(UdpSock),
    ?assertEqual({ok, created}, Method),
    Location = maps:get(location_path, Opts),
    _ = test_recv_mqtt_response(ReportTopic),

    %%----------------------------------------
    %% UPDATE without lt
    %%----------------------------------------
    MsgId2 = 28,
    test_send_coap_request(
        UdpSock,
        post,
        sprintf("coap://127.0.0.1:~b~ts", [?PORT, join_path(Location, <<>>)]),
        #coap_content{
            content_format = <<"text/plain">>,
            payload = <<"</1>, </2>, </3>, </4>, </5>, </6>">>
        },
        [],
        MsgId2
    ),
    #coap_message{method = Method2} = test_recv_coap_response(UdpSock),
    ?assertEqual({ok, changed}, Method2),
    Update = emqx_utils_json:decode(test_recv_mqtt_response(ReportTopic)),
    UpdateData = maps:get(<<"data">>, Update),
    ?assertEqual(345, maps:get(<<"lt">>, UpdateData)).

case02_update_lifetime_zero(Config) ->
    %%----------------------------------------
    %% REGISTER command
    %%----------------------------------------
    UdpSock = ?config(sock, Config),
    Epn = "urn:oma:lwm2m:oma:3",
    MsgId = 12,
    ReportTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), ReportTopic, qos0),
    timer:sleep(200),

    test_send_coap_request(
        UdpSock,
        post,
        sprintf("coap://127.0.0.1:~b/rd?ep=~ts&lt=345&lwm2m=1", [?PORT, Epn]),
        #coap_content{
            content_format = <<"text/plain">>,
            payload = <<"</1>, </2>, </3>, </4>, </5>">>
        },
        [],
        MsgId
    ),
    #coap_message{method = Method, options = Opts} = test_recv_coap_response(UdpSock),
    ?assertEqual({ok, created}, Method),
    Location = maps:get(location_path, Opts),
    _ = test_recv_mqtt_response(ReportTopic),

    %%----------------------------------------
    %% UPDATE with lt=0
    %%----------------------------------------
    MsgId2 = 29,
    test_send_coap_request(
        UdpSock,
        post,
        sprintf("coap://127.0.0.1:~b~ts?lt=0", [?PORT, join_path(Location, <<>>)]),
        #coap_content{
            content_format = <<"text/plain">>,
            payload = <<"</1>, </2>">>
        },
        [],
        MsgId2
    ),
    #coap_message{method = Method2} = test_recv_coap_response(UdpSock),
    ?assertEqual({ok, changed}, Method2),
    Update = emqx_utils_json:decode(test_recv_mqtt_response(ReportTopic)),
    UpdateData = maps:get(<<"data">>, Update),
    ?assertEqual(0, maps:get(<<"lt">>, UpdateData)).

case02_update_publish_condition(Config) ->
    UdpSock = ?config(sock, Config),
    Epn = "urn:oma:lwm2m:oma:9",
    MsgId = 40,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),

    test_send_coap_request(
        UdpSock,
        post,
        sprintf("coap://127.0.0.1:~b/rd?ep=~ts&lt=345&lwm2m=1", [?PORT, Epn]),
        #coap_content{
            content_format = <<"text/plain">>,
            payload = <<"</1>, </2>, </3>, </4>, </5>">>
        },
        [],
        MsgId
    ),
    #coap_message{method = {ok, created}, options = Opts} = test_recv_coap_response(UdpSock),
    Location = maps:get(location_path, Opts),
    _ = test_recv_mqtt_response(RespTopic),

    %% update without objectList should publish by default (always)
    MsgId2 = 41,
    test_send_coap_request(
        UdpSock,
        post,
        sprintf("coap://127.0.0.1:~b~ts?lt=789", [?PORT, join_path(Location, <<>>)]),
        #coap_content{payload = <<>>},
        [],
        MsgId2
    ),
    #coap_message{type = ack, id = MsgId2, method = {ok, changed}} =
        test_recv_coap_response(UdpSock),
    UpdateBin1 = test_recv_mqtt_response(RespTopic),
    ?assertNotEqual(timeout_test_recv_mqtt_response, UpdateBin1),
    UpdateMsg1 = emqx_utils_json:decode(UpdateBin1),
    ?assertEqual(<<"update">>, maps:get(<<"msgType">>, UpdateMsg1)),

    %% switch to contains_object_list, update without objectList should NOT publish
    ok = emqx_conf_cli:load_config(
        ?global_ns, default_config_with_update_condition_raw("\"contains_object_list\""), #{mode => replace}
    ),
    ensure_gateway_loaded(),

    MsgId3 = 42,
    test_send_coap_request(
        UdpSock,
        post,
        sprintf("coap://127.0.0.1:~b~ts?lt=789", [?PORT, join_path(Location, <<>>)]),
        #coap_content{payload = <<>>},
        [],
        MsgId3
    ),
    #coap_message{type = ack, id = MsgId3, method = {ok, changed}} =
        test_recv_coap_response(UdpSock),
    ?assertEqual(timeout_test_recv_mqtt_response, test_recv_mqtt_response(RespTopic)).

case03_register_wrong_version(Config) ->
    %%----------------------------------------
    %% REGISTER command
    %%----------------------------------------
    UdpSock = ?config(sock, Config),
    Epn = "urn:oma:lwm2m:oma:103",
    MsgId = 12,
    SubTopic = list_to_binary("lwm2m/" ++ Epn ++ "/dn/#"),
    test_send_coap_request(
        UdpSock,
        post,
        sprintf("coap://127.0.0.1:~b/rd?ep=~ts&lt=345&lwm2m=8.3", [?PORT, Epn]),
        #coap_content{
            content_format = <<"text/plain">>,
            payload = <<"</1>, </2>, </3>, </4>, </5>">>
        },
        [],
        MsgId
    ),
    #coap_message{type = ack, method = Method} = test_recv_coap_response(UdpSock),
    ?assertEqual({error, bad_request}, Method),
    timer:sleep(50),

    false = lists:member(SubTopic, test_mqtt_broker:get_subscrbied_topics()).

case03_register_missing_version(Config) ->
    %%----------------------------------------
    %% REGISTER command without lwm2m version
    %%----------------------------------------
    UdpSock = ?config(sock, Config),
    Epn = "urn:oma:lwm2m:oma:203",
    MsgId = 12,
    SubTopic = list_to_binary("lwm2m/" ++ Epn ++ "/dn/#"),
    test_send_coap_request(
        UdpSock,
        post,
        sprintf("coap://127.0.0.1:~b/rd?ep=~ts&lt=345", [?PORT, Epn]),
        #coap_content{
            content_format = <<"text/plain">>,
            payload = <<"</1>, </2>, </3>, </4>, </5>">>
        },
        [],
        MsgId
    ),
    #coap_message{type = ack, method = Method} = test_recv_coap_response(UdpSock),
    ?assertEqual({error, bad_request}, Method),
    timer:sleep(50),

    false = lists:member(SubTopic, test_mqtt_broker:get_subscrbied_topics()).

case03_register_version_1_1(Config) ->
    %%----------------------------------------
    %% REGISTER command
    %%----------------------------------------
    UdpSock = ?config(sock, Config),
    Epn = "urn:oma:lwm2m:oma:3",
    MsgId = 13,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),

    test_send_coap_request(
        UdpSock,
        post,
        sprintf("coap://127.0.0.1:~b/rd?ep=~ts&lt=345&lwm2m=1.1", [?PORT, Epn]),
        #coap_content{
            content_format = <<"text/plain">>,
            payload = <<"</1>, </2>, </3>, </4>, </5>">>
        },
        [],
        MsgId
    ),
    #coap_message{type = ack, method = Method} = test_recv_coap_response(UdpSock),
    ?assertEqual({ok, created}, Method),
    test_recv_mqtt_response(RespTopic).

case04_register_and_lifetime_timeout(Config) ->
    %%----------------------------------------
    %% REGISTER command
    %%----------------------------------------
    UdpSock = ?config(sock, Config),
    Epn = "urn:oma:lwm2m:oma:3",
    MsgId = 12,
    SubTopic = list_to_binary("lwm2m/" ++ Epn ++ "/dn/#"),

    test_send_coap_request(
        UdpSock,
        post,
        sprintf("coap://127.0.0.1:~b/rd?ep=~ts&lt=2&lwm2m=1", [?PORT, Epn]),
        #coap_content{
            content_format = <<"text/plain">>,
            payload = <<"</1>, </2>, </3>, </4>, </5>">>
        },
        [],
        MsgId
    ),
    timer:sleep(100),
    #coap_message{type = ack, method = Method} = test_recv_coap_response(UdpSock),
    ?assertEqual({ok, created}, Method),

    true = lists:member(SubTopic, test_mqtt_broker:get_subscrbied_topics()),

    %%----------------------------------------
    %% lifetime timeout
    %%----------------------------------------
    timer:sleep(4000),

    false = lists:member(SubTopic, test_mqtt_broker:get_subscrbied_topics()).

case05_register_wrong_epn(Config) ->
    %%----------------------------------------
    %% REGISTER command
    %%----------------------------------------
    MsgId = 12,
    UdpSock = ?config(sock, Config),

    test_send_coap_request(
        UdpSock,
        post,
        sprintf("coap://127.0.0.1:~b/rd?lt=345&lwm2m=1.0", [?PORT]),
        #coap_content{
            content_format = <<"text/plain">>,
            payload = <<"</1>, </2>, </3>, </4>, </5>">>
        },
        [],
        MsgId
    ),
    #coap_message{type = ack, method = Method} = test_recv_coap_response(UdpSock),
    ?assertEqual({error, bad_request}, Method).

%% case06_register_wrong_lifetime(Config) ->
%%     %%----------------------------------------
%%    %% REGISTER command
%%     %%----------------------------------------
%%     UdpSock = ?config(sock, Config),
%%     Epn = "urn:oma:lwm2m:oma:3",
%%     MsgId = 12,

%%     test_send_coap_request(
%%       UdpSock,
%%       post,
%%       sprintf("coap://127.0.0.1:~b/rd?ep=~ts&lwm2m=1", [?PORT, Epn]),
%%       #coap_content{content_format = <<"text/plain">>,
%%                     payload = <<"</1>, </2>, </3>, </4>, </5>">>},
%%       [],
%%       MsgId),
%%     #coap_message{type = ack,
%%                   method = Method} = test_recv_coap_response(UdpSock),
%%     ?assertEqual({error,bad_request}, Method),
%%     timer:sleep(50),
%%     ?assertEqual([], test_mqtt_broker:get_subscrbied_topics()).

case07_register_alternate_path_01(Config) ->
    %%----------------------------------------
    %% REGISTER command
    %%----------------------------------------
    UdpSock = ?config(sock, Config),
    Epn = "urn:oma:lwm2m:oma:3",
    MsgId = 12,
    SubTopic = list_to_binary("lwm2m/" ++ Epn ++ "/dn/#"),
    ReportTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), ReportTopic, qos0),
    timer:sleep(200),

    test_send_coap_request(
        UdpSock,
        post,
        sprintf("coap://127.0.0.1:~b/rd?ep=~ts&lt=345&lwm2m=1", [?PORT, Epn]),
        #coap_content{
            content_format = <<"text/plain">>,
            payload = <<
                "</>;rt=\"oma.lwm2m\";ct=11543,"
                "</lwm2m/1/0>,</lwm2m/2/0>,</lwm2m/3/0>"
            >>
        },
        [],
        MsgId
    ),
    timer:sleep(50),
    true = lists:member(SubTopic, test_mqtt_broker:get_subscrbied_topics()).

case07_register_alternate_path_02(Config) ->
    %%----------------------------------------
    %% REGISTER command
    %%----------------------------------------
    UdpSock = ?config(sock, Config),
    Epn = "urn:oma:lwm2m:oma:3",
    MsgId = 12,
    SubTopic = list_to_binary("lwm2m/" ++ Epn ++ "/dn/#"),
    ReportTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), ReportTopic, qos0),
    timer:sleep(200),

    test_send_coap_request(
        UdpSock,
        post,
        sprintf("coap://127.0.0.1:~b/rd?ep=~ts&lt=345&lwm2m=1", [?PORT, Epn]),
        #coap_content{
            content_format = <<"text/plain">>,
            payload = <<
                "</lwm2m>;rt=\"oma.lwm2m\";ct=11543,"
                "</lwm2m/1/0>,</lwm2m/2/0>,</lwm2m/3/0>"
            >>
        },
        [],
        MsgId
    ),
    timer:sleep(50),
    true = lists:member(SubTopic, test_mqtt_broker:get_subscrbied_topics()).

case07_register_alternate_path_unprefixed(Config) ->
    %%----------------------------------------
    %% REGISTER command
    %%----------------------------------------
    UdpSock = ?config(sock, Config),
    Epn = "urn:oma:lwm2m:oma:3",
    MsgId = 12,
    ReportTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), ReportTopic, qos0),
    timer:sleep(200),

    test_send_coap_request(
        UdpSock,
        post,
        sprintf("coap://127.0.0.1:~b/rd?ep=~ts&lt=345&lwm2m=1", [?PORT, Epn]),
        #coap_content{
            content_format = <<"text/plain">>,
            payload = <<
                "</lwm2m>;rt=\"oma.lwm2m\";ct=11543,"
                "</lwm2m/1/0>,</3/0>"
            >>
        },
        [],
        MsgId
    ),
    #coap_message{method = Method} = test_recv_coap_response(UdpSock),
    ?assertEqual({ok, created}, Method),
    Register = emqx_utils_json:decode(test_recv_mqtt_response(ReportTopic)),
    RegData = maps:get(<<"data">>, Register),
    ObjList = maps:get(<<"objectList">>, RegData),
    ?assertEqual(<<"/lwm2m">>, maps:get(<<"alternatePath">>, RegData)),
    ?assert(lists:member(<<"/1/0">>, ObjList)),
    ?assert(lists:member(<<"/3/0">>, ObjList)).

case08_reregister(Config) ->
    %%----------------------------------------
    %% REGISTER command
    %%----------------------------------------
    UdpSock = ?config(sock, Config),
    Epn = "urn:oma:lwm2m:oma:3",
    MsgId = 12,
    SubTopic = list_to_binary("lwm2m/" ++ Epn ++ "/dn/#"),
    ReportTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), ReportTopic, qos0),
    timer:sleep(200),

    test_send_coap_request(
        UdpSock,
        post,
        sprintf("coap://127.0.0.1:~b/rd?ep=~ts&lt=345&lwm2m=1", [?PORT, Epn]),
        #coap_content{
            content_format = <<"text/plain">>,
            payload = <<
                "</lwm2m>;rt=\"oma.lwm2m\";ct=11543,"
                "</lwm2m/1/0>,</lwm2m/2/0>,</lwm2m/3/0>"
            >>
        },
        [],
        MsgId
    ),
    timer:sleep(50),
    true = lists:member(SubTopic, test_mqtt_broker:get_subscrbied_topics()),

    ReadResult = emqx_utils_json:encode(
        #{
            <<"msgType">> => <<"register">>,
            <<"data">> => #{
                <<"alternatePath">> => <<"/lwm2m">>,
                <<"ep">> => list_to_binary(Epn),
                <<"lt">> => 345,
                <<"lwm2m">> => <<"1">>,
                <<"objectList">> => [<<"/1/0">>, <<"/2/0">>, <<"/3/0">>]
            }
        }
    ),
    ?assertEqual(ReadResult, test_recv_mqtt_response(ReportTopic)),
    timer:sleep(1000),

    %% the same lwm2mc client registers to server again
    test_send_coap_request(
        UdpSock,
        post,
        sprintf("coap://127.0.0.1:~b/rd?ep=~ts&lt=345&lwm2m=1", [?PORT, Epn]),
        #coap_content{
            content_format = <<"text/plain">>,
            payload = <<
                "</lwm2m>;rt=\"oma.lwm2m\";ct=11543,"
                "</lwm2m/1/0>,</lwm2m/2/0>,</lwm2m/3/0>"
            >>
        },
        [],
        MsgId + 1
    ),

    %% verify the lwm2m client is still online
    ?assertEqual(ReadResult, test_recv_mqtt_response(ReportTopic)).

case09_auto_observe(Config) ->
    UdpSock = ?config(sock, Config),
    Epn = "urn:oma:lwm2m:oma:3",
    MsgId1 = 15,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),

    ok = snabbkaffe:start_trace(),

    %% step 1, device register ...
    test_send_coap_request(
        UdpSock,
        post,
        sprintf("coap://127.0.0.1:~b/rd?ep=~ts&lt=345&lwm2m=1", [?PORT, Epn]),
        #coap_content{
            content_format = <<"text/plain">>,
            payload = <<
                "</lwm2m>;rt=\"oma.lwm2m\";ct=11543,"
                "</lwm2m/1/0>,</lwm2m/2/0>,</lwm2m/3/0>,</lwm2m/59102/0>"
            >>
        },
        [],
        MsgId1
    ),
    #coap_message{method = Method1} = test_recv_coap_response(UdpSock),
    ?assertEqual({ok, created}, Method1),

    #coap_message{
        method = Method2,
        token = Token2,
        options = Options2
    } = test_recv_coap_request(UdpSock),
    ?assertEqual(get, Method2),
    ?assertNotEqual(<<>>, Token2),
    ?assertMatch(
        #{
            observe := 0,
            uri_path := [<<"lwm2m">>, <<"3">>, <<"0">>]
        },
        Options2
    ),

    {ok, _} = ?block_until(#{?snk_kind := ignore_observer_resource}, 1000),
    ok.

case09_auto_observe_list(Config) ->
    UdpSock = ?config(sock, Config),
    Epn = "urn:oma:lwm2m:oma:4",
    MsgId1 = 16,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),

    std_register(
        UdpSock,
        Epn,
        <<"</1>, </2>">>,
        MsgId1,
        RespTopic
    ),

    #coap_message{method = Method1, options = Options1} = test_recv_coap_request(UdpSock),
    ?assertEqual(get, Method1),
    ?assertEqual(0, get_coap_observe(Options1)),
    ?assertEqual(<<"/3/0">>, get_coap_path(Options1)).

case10_read(Config) ->
    UdpSock = ?config(sock, Config),
    Epn = "urn:oma:lwm2m:oma:3",
    MsgId1 = 15,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),
    %% step 1, device register ...
    test_send_coap_request(
        UdpSock,
        post,
        sprintf("coap://127.0.0.1:~b/rd?ep=~ts&lt=345&lwm2m=1", [?PORT, Epn]),
        #coap_content{
            content_format = <<"text/plain">>,
            payload = <<
                "</lwm2m>;rt=\"oma.lwm2m\";ct=11543,"
                "</lwm2m/1/0>,</lwm2m/2/0>,</lwm2m/3/0>"
            >>
        },
        [],
        MsgId1
    ),
    #coap_message{method = Method1} = test_recv_coap_response(UdpSock),
    ?assertEqual({ok, created}, Method1),
    test_recv_mqtt_response(RespTopic),

    %% step2,  send a READ command to device
    CmdId = 206,
    CommandTopic = <<"lwm2m/", (list_to_binary(Epn))/binary, "/dn/dm">>,
    Command = #{
        <<"requestID">> => CmdId,
        <<"cacheID">> => CmdId,
        <<"msgType">> => <<"read">>,
        <<"data">> => #{
            <<"path">> => <<"/3/0/0">>
        }
    },
    CommandJson = emqx_utils_json:encode(Command),
    ?LOGT("CommandJson=~p", [CommandJson]),
    test_mqtt_broker:publish(CommandTopic, CommandJson, 0),
    timer:sleep(50),
    Request2 = test_recv_coap_request(UdpSock),
    #coap_message{
        method = Method2,
        options = Options2,
        payload = Payload2
    } = Request2,
    ?LOGT("LwM2M client got ~p", [Request2]),

    ?assertEqual(get, Method2),
    ?assertEqual(<<"/lwm2m/3/0/0">>, get_coap_path(Options2)),
    ?assertEqual(<<>>, Payload2),
    timer:sleep(50),

    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {ok, content},
        #coap_content{
            content_format = <<"text/plain">>,
            payload = <<"EMQ">>
        },
        Request2,
        true
    ),
    timer:sleep(100),

    ReadResult = emqx_utils_json:encode(
        #{
            <<"requestID">> => CmdId,
            <<"cacheID">> => CmdId,
            <<"msgType">> => <<"read">>,
            <<"data">> => #{
                <<"code">> => <<"2.05">>,
                <<"codeMsg">> => <<"content">>,
                <<"reqPath">> => <<"/3/0/0">>,
                <<"content">> => [
                    #{
                        path => <<"/3/0/0">>,
                        value => <<"EMQ">>
                    }
                ]
            }
        }
    ),
    ?assertEqual(ReadResult, test_recv_mqtt_response(RespTopic)).

case18_read_error_codes(Config) ->
    %% step 1, device register ...
    Epn = "urn:oma:lwm2m:oma:18",
    MsgId1 = 15,
    UdpSock = ?config(sock, Config),
    ObjectList = <<"</1>, </3/0>">>,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    CommandTopic = <<"lwm2m/", (list_to_binary(Epn))/binary, "/dn/dm">>,
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),

    std_register(UdpSock, Epn, ObjectList, MsgId1, RespTopic),

    Codes = [
        {unauthorized, <<"4.01">>},
        {bad_option, <<"4.02">>},
        {forbidden, <<"4.03">>},
        {not_found, <<"4.04">>},
        {method_not_allowed, <<"4.05">>},
        {not_acceptable, <<"4.06">>},
        {request_entity_incomplete, <<"4.08">>},
        {precondition_failed, <<"4.12">>},
        {request_entity_too_large, <<"4.13">>},
        {unsupported_content_format, <<"4.15">>},
        {internal_server_error, <<"5.00">>},
        {not_implemented, <<"5.01">>},
        {bad_gateway, <<"5.02">>},
        {service_unavailable, <<"5.03">>},
        {gateway_timeout, <<"5.04">>},
        {proxying_not_supported, <<"5.05">>}
    ],
    lists:foreach(
        fun({Code, CodeStr}) ->
            ReqId = erlang:unique_integer([monotonic, positive]),
            Command = #{
                <<"requestID">> => ReqId,
                <<"cacheID">> => ReqId,
                <<"msgType">> => <<"read">>,
                <<"data">> => #{<<"path">> => <<"/3/0/0">>}
            },
            test_mqtt_broker:publish(CommandTopic, emqx_utils_json:encode(Command), 0),
            timer:sleep(50),
            Request = test_recv_coap_request(UdpSock),
            #coap_message{method = Method} = Request,
            ?assertEqual(get, Method),
            test_send_coap_response(
                UdpSock,
                "127.0.0.1",
                ?PORT,
                {error, Code},
                #coap_content{content_format = <<"text/plain">>, payload = <<>>},
                Request,
                true
            ),
            timer:sleep(100),
            ReadResult = emqx_utils_json:encode(
                #{
                    <<"requestID">> => ReqId,
                    <<"cacheID">> => ReqId,
                    <<"msgType">> => <<"read">>,
                    <<"data">> => #{
                        <<"reqPath">> => <<"/3/0/0">>,
                        <<"code">> => CodeStr,
                        <<"codeMsg">> => atom_to_binary(Code, utf8)
                    }
                }
            ),
            ?assertEqual(ReadResult, test_recv_mqtt_response(RespTopic))
        end,
        Codes
    ).

case19_read_text_invalid_basename(Config) ->
    %% step 1, device register ...
    Epn = "urn:oma:lwm2m:oma:19",
    MsgId1 = 15,
    UdpSock = ?config(sock, Config),
    ObjectList = <<"</1>, </3/0>">>,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    CommandTopic = <<"lwm2m/", (list_to_binary(Epn))/binary, "/dn/dm">>,
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),

    std_register(UdpSock, Epn, ObjectList, MsgId1, RespTopic),

    CmdId = 510,
    Command = #{
        <<"requestID">> => CmdId,
        <<"cacheID">> => CmdId,
        <<"msgType">> => <<"read">>,
        <<"data">> => #{<<"path">> => <<"/3">>}
    },
    test_mqtt_broker:publish(CommandTopic, emqx_utils_json:encode(Command), 0),
    timer:sleep(50),
    Request = test_recv_coap_request(UdpSock),
    #coap_message{method = Method} = Request,
    ?assertEqual(get, Method),
    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {ok, content},
        #coap_content{content_format = <<"text/plain">>, payload = <<"1">>},
        Request,
        true
    ),
    timer:sleep(100),
    ReadResult = emqx_utils_json:encode(
        #{
            <<"requestID">> => CmdId,
            <<"cacheID">> => CmdId,
            <<"msgType">> => <<"read">>,
            <<"data">> => #{
                <<"reqPath">> => <<"/3">>,
                <<"code">> => <<"4.00">>,
                <<"codeMsg">> => <<"bad_request">>
            }
        }
    ),
    ?assertEqual(ReadResult, test_recv_mqtt_response(RespTopic)).

case20_read_tlv_variants(Config) ->
    %% step 1, device register ...
    Epn = "urn:oma:lwm2m:oma:20",
    MsgId1 = 15,
    UdpSock = ?config(sock, Config),
    ObjectList = <<"</1/0>, </3/0>">>,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    CommandTopic = <<"lwm2m/", (list_to_binary(Epn))/binary, "/dn/dm">>,
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),

    std_register(UdpSock, Epn, ObjectList, MsgId1, RespTopic),

    %% boolean TLV false with basename /1/0/6
    CmdId1 = 610,
    Command1 = #{
        <<"requestID">> => CmdId1,
        <<"cacheID">> => CmdId1,
        <<"msgType">> => <<"read">>,
        <<"data">> => #{<<"path">> => <<"/1/0/6">>}
    },
    test_mqtt_broker:publish(CommandTopic, emqx_utils_json:encode(Command1), 0),
    timer:sleep(50),
    Request1 = test_recv_coap_request(UdpSock),
    #coap_message{method = Method1} = Request1,
    ?assertEqual(get, Method1),
    BoolFalseTlv = <<3:2, 0:1, 0:2, 1:3, 6, 0>>,
    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {ok, content},
        #coap_content{
            content_format = <<"application/vnd.oma.lwm2m+tlv">>,
            payload = BoolFalseTlv
        },
        Request1,
        true
    ),
    timer:sleep(100),
    ReadResult1 = emqx_utils_json:encode(
        #{
            <<"requestID">> => CmdId1,
            <<"cacheID">> => CmdId1,
            <<"msgType">> => <<"read">>,
            <<"data">> => #{
                <<"code">> => <<"2.05">>,
                <<"codeMsg">> => <<"content">>,
                <<"reqPath">> => <<"/1/0/6">>,
                <<"content">> => [#{path => <<"/1/0/6">>, value => false}]
            }
        }
    ),
    ?assertEqual(ReadResult1, test_recv_mqtt_response(RespTopic)),

    %% integer TLV with basename /3/0/6
    CmdId2 = 611,
    Command2 = #{
        <<"requestID">> => CmdId2,
        <<"cacheID">> => CmdId2,
        <<"msgType">> => <<"read">>,
        <<"data">> => #{<<"path">> => <<"/3/0/6">>}
    },
    test_mqtt_broker:publish(CommandTopic, emqx_utils_json:encode(Command2), 0),
    timer:sleep(50),
    Request2 = test_recv_coap_request(UdpSock),
    #coap_message{method = Method2} = Request2,
    ?assertEqual(get, Method2),
    IntTlv = <<3:2, 0:1, 0:2, 2:3, 6, 0, 42>>,
    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {ok, content},
        #coap_content{
            content_format = <<"application/vnd.oma.lwm2m+tlv">>,
            payload = IntTlv
        },
        Request2,
        true
    ),
    timer:sleep(100),
    ReadResult2 = emqx_utils_json:encode(
        #{
            <<"requestID">> => CmdId2,
            <<"cacheID">> => CmdId2,
            <<"msgType">> => <<"read">>,
            <<"data">> => #{
                <<"code">> => <<"2.05">>,
                <<"codeMsg">> => <<"content">>,
                <<"reqPath">> => <<"/3/0/6">>,
                <<"content">> => [#{path => <<"/3/0/6">>, value => 42}]
            }
        }
    ),
    ?assertEqual(ReadResult2, test_recv_mqtt_response(RespTopic)),

    %% multiple resource first with basename /3/0/6
    CmdId3 = 612,
    Command3 = #{
        <<"requestID">> => CmdId3,
        <<"cacheID">> => CmdId3,
        <<"msgType">> => <<"read">>,
        <<"data">> => #{<<"path">> => <<"/3/0/6">>}
    },
    test_mqtt_broker:publish(CommandTopic, emqx_utils_json:encode(Command3), 0),
    timer:sleep(50),
    Request3 = test_recv_coap_request(UdpSock),
    #coap_message{method = Method3} = Request3,
    ?assertEqual(get, Method3),
    ResInst0 = <<1:2, 0:1, 0:2, 1:3, 0, 1>>,
    ResInst1 = <<1:2, 0:1, 0:2, 1:3, 1, 2>>,
    MultiValue = <<ResInst0/binary, ResInst1/binary>>,
    MultiResTlv = <<2:2, 0:1, 0:2, 6:3, 6, MultiValue/binary>>,
    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {ok, content},
        #coap_content{
            content_format = <<"application/vnd.oma.lwm2m+tlv">>,
            payload = MultiResTlv
        },
        Request3,
        true
    ),
    timer:sleep(100),
    ReadResult3 = emqx_utils_json:encode(
        #{
            <<"requestID">> => CmdId3,
            <<"cacheID">> => CmdId3,
            <<"msgType">> => <<"read">>,
            <<"data">> => #{
                <<"code">> => <<"2.05">>,
                <<"codeMsg">> => <<"content">>,
                <<"reqPath">> => <<"/3/0/6">>,
                <<"content">> =>
                    [
                        #{path => <<"/3/0/6/0">>, value => 1},
                        #{path => <<"/3/0/6/1">>, value => 2}
                    ]
            }
        }
    ),
    ?assertEqual(ReadResult3, test_recv_mqtt_response(RespTopic)),

    %% single object instance with basename /3
    CmdId4 = 613,
    Command4 = #{
        <<"requestID">> => CmdId4,
        <<"cacheID">> => CmdId4,
        <<"msgType">> => <<"read">>,
        <<"data">> => #{<<"path">> => <<"/3">>}
    },
    test_mqtt_broker:publish(CommandTopic, emqx_utils_json:encode(Command4), 0),
    timer:sleep(50),
    Request4 = test_recv_coap_request(UdpSock),
    #coap_message{method = Method4} = Request4,
    ?assertEqual(get, Method4),
    Res0 = <<3:2, 0:1, 0:2, 4:3, 0, "ACME">>,
    ObjInst = <<0:2, 0:1, 0:2, (byte_size(Res0)):3, 0, Res0/binary>>,
    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {ok, content},
        #coap_content{
            content_format = <<"application/vnd.oma.lwm2m+tlv">>,
            payload = ObjInst
        },
        Request4,
        true
    ),
    timer:sleep(100),
    ReadResult4 = emqx_utils_json:encode(
        #{
            <<"requestID">> => CmdId4,
            <<"cacheID">> => CmdId4,
            <<"msgType">> => <<"read">>,
            <<"data">> => #{
                <<"code">> => <<"2.05">>,
                <<"codeMsg">> => <<"content">>,
                <<"reqPath">> => <<"/3">>,
                <<"content">> => [#{path => <<"/3/0/0">>, value => <<"ACME">>}]
            }
        }
    ),
    ?assertEqual(ReadResult4, test_recv_mqtt_response(RespTopic)),

    %% object instance list with basename /3/0/6/0 (object_id 4 segments)
    CmdId5 = 614,
    Command5 = #{
        <<"requestID">> => CmdId5,
        <<"cacheID">> => CmdId5,
        <<"msgType">> => <<"read">>,
        <<"data">> => #{<<"path">> => <<"/3/0/6/0">>}
    },
    test_mqtt_broker:publish(CommandTopic, emqx_utils_json:encode(Command5), 0),
    timer:sleep(50),
    Request5 = test_recv_coap_request(UdpSock),
    #coap_message{method = Method5} = Request5,
    ?assertEqual(get, Method5),
    Res1 = <<3:2, 0:1, 0:2, 4:3, 0, "ACME">>,
    ObjInst0 = <<0:2, 0:1, 0:2, (byte_size(Res1)):3, 0, Res1/binary>>,
    ObjInst1 = <<0:2, 0:1, 0:2, (byte_size(Res1)):3, 1, Res1/binary>>,
    ObjInstList = <<ObjInst0/binary, ObjInst1/binary>>,
    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {ok, content},
        #coap_content{
            content_format = <<"application/vnd.oma.lwm2m+tlv">>,
            payload = ObjInstList
        },
        Request5,
        true
    ),
    timer:sleep(100),
    ReadResult5 = emqx_utils_json:encode(
        #{
            <<"requestID">> => CmdId5,
            <<"cacheID">> => CmdId5,
            <<"msgType">> => <<"read">>,
            <<"data">> => #{
                <<"code">> => <<"2.05">>,
                <<"codeMsg">> => <<"content">>,
                <<"reqPath">> => <<"/3/0/6/0">>,
                <<"content">> =>
                    [
                        #{path => <<"3/0/0">>, value => <<"ACME">>},
                        #{path => <<"3/1/0">>, value => <<"ACME">>}
                    ]
            }
        }
    ),
    ?assertEqual(ReadResult5, test_recv_mqtt_response(RespTopic)),

    %% length type 16 for TLV
    CmdId6 = 615,
    Command6 = #{
        <<"requestID">> => CmdId6,
        <<"cacheID">> => CmdId6,
        <<"msgType">> => <<"read">>,
        <<"data">> => #{<<"path">> => <<"/3/0/0">>}
    },
    test_mqtt_broker:publish(CommandTopic, emqx_utils_json:encode(Command6), 0),
    timer:sleep(50),
    Request6 = test_recv_coap_request(UdpSock),
    #coap_message{method = Method6} = Request6,
    ?assertEqual(get, Method6),
    LongValue = binary:copy(<<"A">>, 256),
    LongTlv = <<3:2, 0:1, 2:2, 0:3, 0, 256:16, LongValue/binary>>,
    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {ok, content},
        #coap_content{
            content_format = <<"application/vnd.oma.lwm2m+tlv">>,
            payload = LongTlv
        },
        Request6,
        true
    ),
    timer:sleep(100),
    Response6 = emqx_utils_json:decode(test_recv_mqtt_response(RespTopic)),
    [#{<<"value">> := LongValue0}] = maps:get(<<"content">>, maps:get(<<"data">>, Response6)),
    ?assertEqual(256, byte_size(LongValue0)).

case10_read_bad_request(Config) ->
    UdpSock = ?config(sock, Config),
    Epn = "urn:oma:lwm2m:oma:3",
    MsgId1 = 15,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),
    % step 1, device register ...
    test_send_coap_request(
        UdpSock,
        post,
        sprintf("coap://127.0.0.1:~b/rd?ep=~s&lt=345&lwm2m=1", [?PORT, Epn]),
        #coap_content{
            content_format = <<"text/plain">>,
            payload =
                <<"</lwm2m>;rt=\"oma.lwm2m\";ct=11543,</lwm2m/1/0>,</lwm2m/2/0>,</lwm2m/3/0>">>
        },
        [],
        MsgId1
    ),
    #coap_message{method = Method1} = test_recv_coap_response(UdpSock),
    ?assertEqual({ok, created}, Method1),
    test_recv_mqtt_response(RespTopic),

    % step2,  send a READ command to device
    CmdId = 206,
    CommandTopic = <<"lwm2m/", (list_to_binary(Epn))/binary, "/dn/dm">>,
    Command = #{
        <<"requestID">> => CmdId,
        <<"cacheID">> => CmdId,
        <<"msgType">> => <<"read">>,
        <<"data">> => #{
            <<"path">> => <<"/3333/0/0">>
        }
    },
    CommandJson = emqx_utils_json:encode(Command),
    ?LOGT("CommandJson=~p", [CommandJson]),
    test_mqtt_broker:publish(CommandTopic, CommandJson, 0),
    timer:sleep(50),
    Request2 = test_recv_coap_request(UdpSock),
    #coap_message{method = Method2, payload = Payload2} = Request2,
    ?LOGT("LwM2M client got ~p", [Request2]),
    ?assertEqual(get, Method2),
    ?assertEqual(<<>>, Payload2),
    timer:sleep(50),

    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {ok, content},
        #coap_content{content_format = <<"text/plain">>, payload = <<"EMQ">>},
        Request2,
        true
    ),
    timer:sleep(100),

    ReadResult = emqx_utils_json:encode(#{
        <<"requestID">> => CmdId,
        <<"cacheID">> => CmdId,
        <<"msgType">> => <<"read">>,
        <<"data">> => #{
            <<"code">> => <<"4.00">>,
            <<"codeMsg">> => <<"bad_request">>,
            <<"reqPath">> => <<"/3333/0/0">>
        }
    }),
    ?assertEqual(ReadResult, test_recv_mqtt_response(RespTopic)).

case10_read_separate_ack(Config) ->
    UdpSock = ?config(sock, Config),
    Epn = "urn:oma:lwm2m:oma:3",
    MsgId1 = 15,
    ObjectList = <<"</1>, </2>, </3/0>, </4>, </5>">>,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),

    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),

    %% step 1, device register ...
    std_register(UdpSock, Epn, ObjectList, MsgId1, RespTopic),

    %% step2,  send a READ command to device
    CmdId = 206,
    CommandTopic = <<"lwm2m/", (list_to_binary(Epn))/binary, "/dn/dm">>,
    Command = #{
        <<"requestID">> => CmdId,
        <<"cacheID">> => CmdId,
        <<"msgType">> => <<"read">>,
        <<"data">> => #{
            <<"path">> => <<"/3/0/0">>
        }
    },
    CommandJson = emqx_utils_json:encode(Command),
    ?LOGT("CommandJson=~p", [CommandJson]),
    test_mqtt_broker:publish(CommandTopic, CommandJson, 0),
    timer:sleep(50),
    Request2 = test_recv_coap_request(UdpSock),
    #coap_message{
        method = Method2,
        options = Options2,
        payload = Payload2
    } = Request2,
    ?LOGT("LwM2M client got ~p", [Request2]),

    ?assertEqual(get, Method2),
    ?assertEqual(<<"/3/0/0">>, get_coap_path(Options2)),
    ?assertEqual(<<>>, Payload2),

    test_send_empty_ack(UdpSock, "127.0.0.1", ?PORT, Request2),
    ReadResultACK = emqx_utils_json:encode(
        #{
            <<"requestID">> => CmdId,
            <<"cacheID">> => CmdId,
            <<"msgType">> => <<"ack">>,
            <<"data">> => #{<<"path">> => <<"/3/0/0">>}
        }
    ),
    ?assertEqual(ReadResultACK, test_recv_mqtt_response(RespTopic)),
    timer:sleep(100),

    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {ok, content},
        #coap_content{
            content_format = <<"text/plain">>,
            payload = <<"EMQ">>
        },
        Request2,
        false
    ),
    timer:sleep(100),

    ReadResult = emqx_utils_json:encode(
        #{
            <<"requestID">> => CmdId,
            <<"cacheID">> => CmdId,
            <<"msgType">> => <<"read">>,
            <<"data">> => #{
                <<"code">> => <<"2.05">>,
                <<"codeMsg">> => <<"content">>,
                <<"reqPath">> => <<"/3/0/0">>,
                <<"content">> => [
                    #{
                        path => <<"/3/0/0">>,
                        value => <<"EMQ">>
                    }
                ]
            }
        }
    ),
    ?assertEqual(ReadResult, test_recv_mqtt_response(RespTopic)).

case11_read_object_tlv(Config) ->
    %% step 1, device register ...
    Epn = "urn:oma:lwm2m:oma:3",
    MsgId1 = 15,
    UdpSock = ?config(sock, Config),
    ObjectList = <<"</1>, </2>, </3/0>, </4>, </5>">>,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),

    std_register(UdpSock, Epn, ObjectList, MsgId1, RespTopic),

    %% step2,  send a READ command to device
    CmdId = 207,
    CommandTopic = <<"lwm2m/", (list_to_binary(Epn))/binary, "/dn/dm">>,
    Command = #{
        <<"requestID">> => CmdId,
        <<"cacheID">> => CmdId,
        <<"msgType">> => <<"read">>,
        <<"data">> => #{
            <<"path">> => <<"/3/0">>
        }
    },
    CommandJson = emqx_utils_json:encode(Command),
    ?LOGT("CommandJson=~p", [CommandJson]),
    test_mqtt_broker:publish(CommandTopic, CommandJson, 0),
    timer:sleep(50),
    Request2 = test_recv_coap_request(UdpSock),
    #coap_message{method = Method2} = Request2,
    ?LOGT("LwM2M client got ~p", [Request2]),

    ?assertEqual(get, Method2),
    timer:sleep(50),

    Tlv =
        <<16#08, 16#00, 16#3C, 16#C8, 16#00, 16#14, 16#4F, 16#70, 16#65, 16#6E, 16#20, 16#4D, 16#6F,
            16#62, 16#69, 16#6C, 16#65, 16#20, 16#41, 16#6C, 16#6C, 16#69, 16#61, 16#6E, 16#63,
            16#65, 16#C8, 16#01, 16#16, 16#4C, 16#69, 16#67, 16#68, 16#74, 16#77, 16#65, 16#69,
            16#67, 16#68, 16#74, 16#20, 16#4D, 16#32, 16#4D, 16#20, 16#43, 16#6C, 16#69, 16#65,
            16#6E, 16#74, 16#C8, 16#02, 16#09, 16#33, 16#34, 16#35, 16#30, 16#30, 16#30, 16#31,
            16#32, 16#33>>,
    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {ok, content},
        #coap_content{
            content_format = <<"application/vnd.oma.lwm2m+tlv">>,
            payload = Tlv
        },
        Request2,
        true
    ),
    timer:sleep(100),

    ReadResult = emqx_utils_json:encode(
        #{
            <<"requestID">> => CmdId,
            <<"cacheID">> => CmdId,
            <<"msgType">> => <<"read">>,
            <<"data">> => #{
                <<"code">> => <<"2.05">>,
                <<"codeMsg">> => <<"content">>,
                <<"reqPath">> => <<"/3/0">>,
                <<"content">> =>
                    [
                        #{
                            path => <<"/3/0/0">>,
                            value => <<"Open Mobile Alliance">>
                        },
                        #{
                            path => <<"/3/0/1">>,
                            value => <<"Lightweight M2M Client">>
                        },
                        #{
                            path => <<"/3/0/2">>,
                            value => <<"345000123">>
                        }
                    ]
            }
        }
    ),
    ?assertEqual(ReadResult, test_recv_mqtt_response(RespTopic)).

case11_read_object_json(Config) ->
    %% step 1, device register ...
    UdpSock = ?config(sock, Config),
    Epn = "urn:oma:lwm2m:oma:3",
    MsgId1 = 15,

    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    ObjectList = <<"</1>, </2>, </3/0>, </4>, </5>">>,
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),

    std_register(UdpSock, Epn, ObjectList, MsgId1, RespTopic),

    %% step2,  send a READ command to device
    CmdId = 206,
    CommandTopic = <<"lwm2m/", (list_to_binary(Epn))/binary, "/dn/dm">>,
    Command = #{
        <<"requestID">> => CmdId,
        <<"cacheID">> => CmdId,
        <<"msgType">> => <<"read">>,
        <<"data">> => #{
            <<"path">> => <<"/3/0">>
        }
    },
    CommandJson = emqx_utils_json:encode(Command),
    ?LOGT("CommandJson=~p", [CommandJson]),
    test_mqtt_broker:publish(CommandTopic, CommandJson, 0),
    timer:sleep(50),
    Request2 = test_recv_coap_request(UdpSock),
    #coap_message{method = Method2} = Request2,
    ?LOGT("LwM2M client got ~p", [Request2]),

    ?assertEqual(get, Method2),
    timer:sleep(50),

    Json = <<
        "{\"bn\":\"/3/0\",\"e\":["
        "{\"n\":\"0\",\"sv\":\"Open Mobile Alliance\"},"
        "{\"n\":\"1\",\"sv\":\"Lightweight M2M Client\"},"
        "{\"n\":\"2\",\"sv\":\"345000123\"},"
        "{\"n\":\"13\",\"v\":1700000000},"
        "{\"n\":\"6\",\"bv\":true},"
        "{\"n\":\"22\",\"ov\":\"1:2\"},"
        "{\"n\":\"15\",\"t\":123},"
        "{\"n\":\"99\"}"
        "]}"
    >>,
    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {ok, content},
        #coap_content{
            content_format = <<"application/vnd.oma.lwm2m+json">>,
            payload = Json
        },
        Request2,
        true
    ),
    timer:sleep(100),

    ReadResult = emqx_utils_json:encode(
        #{
            <<"requestID">> => CmdId,
            <<"cacheID">> => CmdId,
            <<"msgType">> => <<"read">>,
            <<"data">> => #{
                <<"code">> => <<"2.05">>,
                <<"codeMsg">> => <<"content">>,
                <<"reqPath">> => <<"/3/0">>,
                <<"content">> =>
                    [
                        #{
                            path => <<"/3/0/0">>,
                            value => <<"Open Mobile Alliance">>
                        },
                        #{
                            path => <<"/3/0/1">>,
                            value => <<"Lightweight M2M Client">>
                        },
                        #{
                            path => <<"/3/0/2">>,
                            value => <<"345000123">>
                        },
                        #{
                            path => <<"/3/0/13">>,
                            value => 1700000000
                        },
                        #{
                            path => <<"/3/0/6">>,
                            value => true
                        },
                        #{
                            path => <<"/3/0/22">>,
                            value => <<"1:2">>
                        },
                        #{
                            path => <<"/3/0/15">>,
                            value => 123
                        },
                        #{
                            path => <<"/3/0/99">>,
                            value => null
                        }
                    ]
            }
        }
    ),
    ?assertEqual(ReadResult, test_recv_mqtt_response(RespTopic)).

case12_read_resource_opaque(Config) ->
    %% step 1, device register ...
    UdpSock = ?config(sock, Config),
    Epn = "urn:oma:lwm2m:oma:3",
    MsgId1 = 15,
    ObjectList = <<"</1>, </2>, </3/0>, </4>, </5>">>,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),

    std_register(UdpSock, Epn, ObjectList, MsgId1, RespTopic),

    %% step2,  send a READ command to device
    CmdId = 206,
    CommandTopic = <<"lwm2m/", (list_to_binary(Epn))/binary, "/dn/dm">>,
    Command = #{
        <<"requestID">> => CmdId,
        <<"cacheID">> => CmdId,
        <<"msgType">> => <<"read">>,
        <<"data">> => #{
            <<"path">> => <<"/3/0/8">>
        }
    },
    CommandJson = emqx_utils_json:encode(Command),
    ?LOGT("CommandJson=~p", [CommandJson]),
    test_mqtt_broker:publish(CommandTopic, CommandJson, 0),
    timer:sleep(50),
    Request2 = test_recv_coap_request(UdpSock),
    #coap_message{method = Method2} = Request2,
    ?LOGT("LwM2M client got ~p", [Request2]),

    ?assertEqual(get, Method2),
    timer:sleep(50),

    Opaque = <<20, 21, 22, 23>>,
    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {ok, content},
        #coap_content{
            content_format = <<"application/octet-stream">>,
            payload = Opaque
        },
        Request2,
        true
    ),
    timer:sleep(100),

    ReadResult = emqx_utils_json:encode(
        #{
            <<"requestID">> => CmdId,
            <<"cacheID">> => CmdId,
            <<"msgType">> => <<"read">>,
            <<"data">> => #{
                <<"code">> => <<"2.05">>,
                <<"codeMsg">> => <<"content">>,
                <<"reqPath">> => <<"/3/0/8">>,
                <<"content">> =>
                    [
                        #{
                            path => <<"/3/0/8">>,
                            value => base64:encode(Opaque)
                        }
                    ]
            }
        }
    ),
    ?assertEqual(ReadResult, test_recv_mqtt_response(RespTopic)).

case13_read_no_xml(Config) ->
    %% step 1, device register ...
    Epn = "urn:oma:lwm2m:oma:3",
    MsgId1 = 15,
    UdpSock = ?config(sock, Config),
    ObjectList = <<"</1>, </2>, </3/0>, </4>, </5>">>,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),

    std_register(UdpSock, Epn, ObjectList, MsgId1, RespTopic),

    %% step2,  send a READ command to device
    CmdId = 206,
    CommandTopic = <<"lwm2m/", (list_to_binary(Epn))/binary, "/dn/dm">>,
    Command = #{
        <<"requestID">> => CmdId,
        <<"cacheID">> => CmdId,
        <<"msgType">> => <<"read">>,
        <<"data">> => #{<<"path">> => <<"/9723/0/0">>}
    },
    CommandJson = emqx_utils_json:encode(Command),
    ?LOGT("CommandJson=~p", [CommandJson]),
    test_mqtt_broker:publish(CommandTopic, CommandJson, 0),
    timer:sleep(50),
    Request2 = test_recv_coap_request(UdpSock),
    #coap_message{method = Method2} = Request2,
    ?LOGT("LwM2M client got ~p", [Request2]),

    ?assertEqual(get, Method2),
    timer:sleep(50),

    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {ok, content},
        #coap_content{
            content_format = <<"text/plain">>,
            payload = <<"EMQ">>
        },
        Request2,
        true
    ),
    timer:sleep(100),

    ReadResult = emqx_utils_json:encode(
        #{
            <<"requestID">> => CmdId,
            <<"cacheID">> => CmdId,
            <<"msgType">> => <<"read">>,
            <<"data">> => #{
                <<"reqPath">> => <<"/9723/0/0">>,
                <<"code">> => <<"4.00">>,
                <<"codeMsg">> => <<"bad_request">>
            }
        }
    ),
    ?assertEqual(ReadResult, test_recv_mqtt_response(RespTopic)).

case14_read_text_types(Config) ->
    %% step 1, device register ...
    Epn = "urn:oma:lwm2m:oma:3",
    MsgId1 = 15,
    UdpSock = ?config(sock, Config),
    ObjectList = <<"</1>, </3/0>, </6/0>">>,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    CommandTopic = <<"lwm2m/", (list_to_binary(Epn))/binary, "/dn/dm">>,
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),

    std_register(UdpSock, Epn, ObjectList, MsgId1, RespTopic),

    %% boolean text/plain
    CmdId1 = 310,
    Command1 = #{
        <<"requestID">> => CmdId1,
        <<"cacheID">> => CmdId1,
        <<"msgType">> => <<"read">>,
        <<"data">> => #{<<"path">> => <<"/1/0/6">>}
    },
    test_mqtt_broker:publish(CommandTopic, emqx_utils_json:encode(Command1), 0),
    timer:sleep(50),
    Request1 = test_recv_coap_request(UdpSock),
    #coap_message{method = Method1, options = Options1, payload = Payload1} = Request1,
    ?assertEqual(get, Method1),
    ?assertEqual(<<"/1/0/6">>, get_coap_path(Options1)),
    ?assertEqual(<<>>, Payload1),
    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {ok, content},
        #coap_content{content_format = <<"text/plain">>, payload = <<"true">>},
        Request1,
        true
    ),
    timer:sleep(100),
    ReadResult1 = emqx_utils_json:encode(
        #{
            <<"requestID">> => CmdId1,
            <<"cacheID">> => CmdId1,
            <<"msgType">> => <<"read">>,
            <<"data">> => #{
                <<"code">> => <<"2.05">>,
                <<"codeMsg">> => <<"content">>,
                <<"reqPath">> => <<"/1/0/6">>,
                <<"content">> => [#{path => <<"/1/0/6">>, value => true}]
            }
        }
    ),
    ?assertEqual(ReadResult1, test_recv_mqtt_response(RespTopic)),

    %% boolean text/plain false
    CmdId1b = 314,
    Command1b = #{
        <<"requestID">> => CmdId1b,
        <<"cacheID">> => CmdId1b,
        <<"msgType">> => <<"read">>,
        <<"data">> => #{<<"path">> => <<"/1/0/6">>}
    },
    test_mqtt_broker:publish(CommandTopic, emqx_utils_json:encode(Command1b), 0),
    timer:sleep(50),
    Request1b = test_recv_coap_request(UdpSock),
    #coap_message{method = Method1b} = Request1b,
    ?assertEqual(get, Method1b),
    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {ok, content},
        #coap_content{content_format = <<"text/plain">>, payload = <<"false">>},
        Request1b,
        true
    ),
    timer:sleep(100),
    ReadResult1b = emqx_utils_json:encode(
        #{
            <<"requestID">> => CmdId1b,
            <<"cacheID">> => CmdId1b,
            <<"msgType">> => <<"read">>,
            <<"data">> => #{
                <<"code">> => <<"2.05">>,
                <<"codeMsg">> => <<"content">>,
                <<"reqPath">> => <<"/1/0/6">>,
                <<"content">> => [#{path => <<"/1/0/6">>, value => false}]
            }
        }
    ),
    ?assertEqual(ReadResult1b, test_recv_mqtt_response(RespTopic)),

    %% opaque text/plain
    CmdId1c = 315,
    Command1c = #{
        <<"requestID">> => CmdId1c,
        <<"cacheID">> => CmdId1c,
        <<"msgType">> => <<"read">>,
        <<"data">> => #{<<"path">> => <<"/6/0/4">>}
    },
    test_mqtt_broker:publish(CommandTopic, emqx_utils_json:encode(Command1c), 0),
    timer:sleep(50),
    Request1c = test_recv_coap_request(UdpSock),
    #coap_message{method = Method1c} = Request1c,
    ?assertEqual(get, Method1c),
    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {ok, content},
        #coap_content{content_format = <<"text/plain">>, payload = <<"abc">>},
        Request1c,
        true
    ),
    timer:sleep(100),
    ReadResult1c = emqx_utils_json:encode(
        #{
            <<"requestID">> => CmdId1c,
            <<"cacheID">> => CmdId1c,
            <<"msgType">> => <<"read">>,
            <<"data">> => #{
                <<"code">> => <<"2.05">>,
                <<"codeMsg">> => <<"content">>,
                <<"reqPath">> => <<"/6/0/4">>,
                <<"content">> => [#{path => <<"/6/0/4">>, value => <<"YWJj">>}]
            }
        }
    ),
    ?assertEqual(ReadResult1c, test_recv_mqtt_response(RespTopic)),

    %% float text/plain with query
    CmdId2 = 311,
    Command2 = #{
        <<"requestID">> => CmdId2,
        <<"cacheID">> => CmdId2,
        <<"msgType">> => <<"read">>,
        <<"data">> => #{<<"path">> => <<"/6/0/0?foo=bar&baz=1">>}
    },
    test_mqtt_broker:publish(CommandTopic, emqx_utils_json:encode(Command2), 0),
    timer:sleep(50),
    Request2 = test_recv_coap_request(UdpSock),
    #coap_message{method = Method2, options = Options2, payload = Payload2} = Request2,
    ?assertEqual(get, Method2),
    ?assertEqual(<<"/6/0/0">>, get_coap_path(Options2)),
    Query2 = get_coap_query(Options2),
    ?assert(Query2 =/= #{}),
    ?assert(Query2 =/= []),
    ?assertEqual(<<>>, Payload2),
    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {ok, content},
        #coap_content{content_format = <<"text/plain">>, payload = <<"12.5">>},
        Request2,
        true
    ),
    timer:sleep(100),
    ReadResult2 = emqx_utils_json:encode(
        #{
            <<"requestID">> => CmdId2,
            <<"cacheID">> => CmdId2,
            <<"msgType">> => <<"read">>,
            <<"data">> => #{
                <<"code">> => <<"2.05">>,
                <<"codeMsg">> => <<"content">>,
                <<"reqPath">> => <<"/6/0/0">>,
                <<"content">> => [#{path => <<"/6/0/0">>, value => 12.5}]
            }
        }
    ),
    ?assertEqual(ReadResult2, test_recv_mqtt_response(RespTopic)),

    %% objlnk text/plain with resource instance
    CmdId3 = 312,
    Command3 = #{
        <<"requestID">> => CmdId3,
        <<"cacheID">> => CmdId3,
        <<"msgType">> => <<"read">>,
        <<"data">> => #{<<"path">> => <<"/3/0/22/0">>}
    },
    test_mqtt_broker:publish(CommandTopic, emqx_utils_json:encode(Command3), 0),
    timer:sleep(50),
    Request3 = test_recv_coap_request(UdpSock),
    #coap_message{method = Method3, options = Options3, payload = Payload3} = Request3,
    ?assertEqual(get, Method3),
    ?assertEqual(<<"/3/0/22/0">>, get_coap_path(Options3)),
    ?assertEqual(<<>>, Payload3),
    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {ok, content},
        #coap_content{content_format = <<"text/plain">>, payload = <<"1:2">>},
        Request3,
        true
    ),
    timer:sleep(100),
    ReadResult3 = emqx_utils_json:encode(
        #{
            <<"requestID">> => CmdId3,
            <<"cacheID">> => CmdId3,
            <<"msgType">> => <<"read">>,
            <<"data">> => #{
                <<"code">> => <<"2.05">>,
                <<"codeMsg">> => <<"content">>,
                <<"reqPath">> => <<"/3/0/22/0">>,
                <<"content">> => [#{path => <<"/3/0/22/0">>, value => <<"1:2">>}]
            }
        }
    ),
    ?assertEqual(ReadResult3, test_recv_mqtt_response(RespTopic)),

    %% time text/plain
    CmdId4 = 313,
    Command4 = #{
        <<"requestID">> => CmdId4,
        <<"cacheID">> => CmdId4,
        <<"msgType">> => <<"read">>,
        <<"data">> => #{<<"path">> => <<"/3/0/13">>}
    },
    test_mqtt_broker:publish(CommandTopic, emqx_utils_json:encode(Command4), 0),
    timer:sleep(50),
    Request4 = test_recv_coap_request(UdpSock),
    #coap_message{method = Method4, options = Options4, payload = Payload4} = Request4,
    ?assertEqual(get, Method4),
    ?assertEqual(<<"/3/0/13">>, get_coap_path(Options4)),
    ?assertEqual(<<>>, Payload4),
    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {ok, content},
        #coap_content{content_format = <<"text/plain">>, payload = <<"1700000000">>},
        Request4,
        true
    ),
    timer:sleep(100),
    ReadResult4 = emqx_utils_json:encode(
        #{
            <<"requestID">> => CmdId4,
            <<"cacheID">> => CmdId4,
            <<"msgType">> => <<"read">>,
            <<"data">> => #{
                <<"code">> => <<"2.05">>,
                <<"codeMsg">> => <<"content">>,
                <<"reqPath">> => <<"/3/0/13">>,
                <<"content">> => [#{path => <<"/3/0/13">>, value => 1700000000}]
            }
        }
    ),
    ?assertEqual(ReadResult4, test_recv_mqtt_response(RespTopic)).

case15_read_tlv_types(Config) ->
    %% step 1, device register ...
    Epn = "urn:oma:lwm2m:oma:3",
    MsgId1 = 15,
    UdpSock = ?config(sock, Config),
    ObjectList = <<"</1>, </3/0>, </6/0>">>,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    CommandTopic = <<"lwm2m/", (list_to_binary(Epn))/binary, "/dn/dm">>,
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),

    std_register(UdpSock, Epn, ObjectList, MsgId1, RespTopic),

    %% boolean TLV
    CmdId1 = 320,
    Command1 = #{
        <<"requestID">> => CmdId1,
        <<"cacheID">> => CmdId1,
        <<"msgType">> => <<"read">>,
        <<"data">> => #{<<"path">> => <<"/1/0">>}
    },
    test_mqtt_broker:publish(CommandTopic, emqx_utils_json:encode(Command1), 0),
    timer:sleep(50),
    Request1 = test_recv_coap_request(UdpSock),
    #coap_message{method = Method1} = Request1,
    ?assertEqual(get, Method1),
    BoolTlv = <<3:2, 0:1, 0:2, 1:3, 6, 1>>,
    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {ok, content},
        #coap_content{
            content_format = <<"application/vnd.oma.lwm2m+tlv">>,
            payload = BoolTlv
        },
        Request1,
        true
    ),
    timer:sleep(100),
    ReadResult1 = emqx_utils_json:encode(
        #{
            <<"requestID">> => CmdId1,
            <<"cacheID">> => CmdId1,
            <<"msgType">> => <<"read">>,
            <<"data">> => #{
                <<"code">> => <<"2.05">>,
                <<"codeMsg">> => <<"content">>,
                <<"reqPath">> => <<"/1/0">>,
                <<"content">> => [#{path => <<"/1/0/6">>, value => true}]
            }
        }
    ),
    ?assertEqual(ReadResult1, test_recv_mqtt_response(RespTopic)),

    %% time + objlnk TLV
    CmdId2 = 321,
    Command2 = #{
        <<"requestID">> => CmdId2,
        <<"cacheID">> => CmdId2,
        <<"msgType">> => <<"read">>,
        <<"data">> => #{<<"path">> => <<"/3/0">>}
    },
    test_mqtt_broker:publish(CommandTopic, emqx_utils_json:encode(Command2), 0),
    timer:sleep(50),
    Request2 = test_recv_coap_request(UdpSock),
    #coap_message{method = Method2} = Request2,
    ?assertEqual(get, Method2),
    TimeVal = <<0, 0, 0, 10>>,
    ResTime = <<3:2, 0:1, 0:2, 4:3, 13, TimeVal/binary>>,
    Objlnk0 = <<1:16, 2:16>>,
    Objlnk1 = <<1:16, 3:16>>,
    ResInst0 = <<1:2, 0:1, 0:2, 4:3, 0, Objlnk0/binary>>,
    ResInst1 = <<1:2, 0:1, 0:2, 4:3, 1, Objlnk1/binary>>,
    ObjlnkValue = <<ResInst0/binary, ResInst1/binary>>,
    ResObjlnk = <<2:2, 0:1, 1:2, 0:3, 22, (byte_size(ObjlnkValue)), ObjlnkValue/binary>>,
    DeviceTlv = <<ResTime/binary, ResObjlnk/binary>>,
    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {ok, content},
        #coap_content{
            content_format = <<"application/vnd.oma.lwm2m+tlv">>,
            payload = DeviceTlv
        },
        Request2,
        true
    ),
    timer:sleep(100),
    ReadResult2 = emqx_utils_json:encode(
        #{
            <<"requestID">> => CmdId2,
            <<"cacheID">> => CmdId2,
            <<"msgType">> => <<"read">>,
            <<"data">> => #{
                <<"code">> => <<"2.05">>,
                <<"codeMsg">> => <<"content">>,
                <<"reqPath">> => <<"/3/0">>,
                <<"content">> =>
                    [
                        #{path => <<"/3/0/13">>, value => 10},
                        #{path => <<"/3/0/22/0">>, value => <<"1:2">>},
                        #{path => <<"/3/0/22/1">>, value => <<"1:3">>}
                    ]
            }
        }
    ),
    ?assertEqual(ReadResult2, test_recv_mqtt_response(RespTopic)),

    %% float + opaque + time TLV
    CmdId3 = 322,
    Command3 = #{
        <<"requestID">> => CmdId3,
        <<"cacheID">> => CmdId3,
        <<"msgType">> => <<"read">>,
        <<"data">> => #{<<"path">> => <<"/6/0">>}
    },
    test_mqtt_broker:publish(CommandTopic, emqx_utils_json:encode(Command3), 0),
    timer:sleep(50),
    Request3 = test_recv_coap_request(UdpSock),
    #coap_message{method = Method3} = Request3,
    ?assertEqual(get, Method3),
    FloatVal = <<1.5:32/float>>,
    ResFloat = <<3:2, 0:1, 0:2, 4:3, 0, FloatVal/binary>>,
    OpaqueVal = <<1, 2, 3>>,
    ResOpaque = <<3:2, 0:1, 0:2, 3:3, 4, OpaqueVal/binary>>,
    TimeVal2 = <<0, 0, 0, 20>>,
    ResTime2 = <<3:2, 0:1, 0:2, 4:3, 5, TimeVal2/binary>>,
    LocationTlv = <<ResFloat/binary, ResOpaque/binary, ResTime2/binary>>,
    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {ok, content},
        #coap_content{
            content_format = <<"application/vnd.oma.lwm2m+tlv">>,
            payload = LocationTlv
        },
        Request3,
        true
    ),
    timer:sleep(100),
    ReadResult3 = emqx_utils_json:encode(
        #{
            <<"requestID">> => CmdId3,
            <<"cacheID">> => CmdId3,
            <<"msgType">> => <<"read">>,
            <<"data">> => #{
                <<"code">> => <<"2.05">>,
                <<"codeMsg">> => <<"content">>,
                <<"reqPath">> => <<"/6/0">>,
                <<"content">> =>
                    [
                        #{path => <<"/6/0/0">>, value => 1.5},
                        #{path => <<"/6/0/4">>, value => base64:encode(OpaqueVal)},
                        #{path => <<"/6/0/5">>, value => 20}
                    ]
            }
        }
    ),
    ?assertEqual(ReadResult3, test_recv_mqtt_response(RespTopic)).

case16_read_tlv_object_instances(Config) ->
    %% step 1, device register ...
    Epn = "urn:oma:lwm2m:oma:3",
    MsgId1 = 15,
    UdpSock = ?config(sock, Config),
    ObjectList = <<"</1>, </2>, </3/0>, </4>, </5>">>,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    CommandTopic = <<"lwm2m/", (list_to_binary(Epn))/binary, "/dn/dm">>,
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),

    std_register(UdpSock, Epn, ObjectList, MsgId1, RespTopic),

    %% read object with object instance TLV list
    CmdId = 330,
    Command = #{
        <<"requestID">> => CmdId,
        <<"cacheID">> => CmdId,
        <<"msgType">> => <<"read">>,
        <<"data">> => #{<<"path">> => <<"/3">>}
    },
    test_mqtt_broker:publish(CommandTopic, emqx_utils_json:encode(Command), 0),
    timer:sleep(50),
    Request = test_recv_coap_request(UdpSock),
    #coap_message{method = Method} = Request,
    ?assertEqual(get, Method),
    Res0A = <<3:2, 0:1, 0:2, 1:3, 0, "A">>,
    ObjInst0 = <<0:2, 0:1, 0:2, (byte_size(Res0A)):3, 0, Res0A/binary>>,
    Res0B = <<3:2, 0:1, 0:2, 1:3, 0, "B">>,
    ObjInst1 = <<0:2, 0:1, 0:2, (byte_size(Res0B)):3, 1, Res0B/binary>>,
    ObjTlv = <<ObjInst0/binary, ObjInst1/binary>>,
    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {ok, content},
        #coap_content{
            content_format = <<"application/vnd.oma.lwm2m+tlv">>,
            payload = ObjTlv
        },
        Request,
        true
    ),
    timer:sleep(100),
    ReadResult = emqx_utils_json:encode(
        #{
            <<"requestID">> => CmdId,
            <<"cacheID">> => CmdId,
            <<"msgType">> => <<"read">>,
            <<"data">> => #{
                <<"code">> => <<"2.05">>,
                <<"codeMsg">> => <<"content">>,
                <<"reqPath">> => <<"/3">>,
                <<"content">> =>
                    [
                        #{path => <<"3/0/0">>, value => <<"A">>},
                        #{path => <<"3/1/0">>, value => <<"B">>}
                    ]
            }
        }
    ),
    ?assertEqual(ReadResult, test_recv_mqtt_response(RespTopic)).

case17_read_invalid_tlv(Config) ->
    %% step 1, device register ...
    Epn = "urn:oma:lwm2m:oma:3",
    MsgId1 = 15,
    UdpSock = ?config(sock, Config),
    ObjectList = <<"</1>, </2>, </3/0>, </4>, </5>">>,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    CommandTopic = <<"lwm2m/", (list_to_binary(Epn))/binary, "/dn/dm">>,
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),

    std_register(UdpSock, Epn, ObjectList, MsgId1, RespTopic),

    %% invalid TLV resource id
    CmdId = 340,
    Command = #{
        <<"requestID">> => CmdId,
        <<"cacheID">> => CmdId,
        <<"msgType">> => <<"read">>,
        <<"data">> => #{<<"path">> => <<"/3/0">>}
    },
    test_mqtt_broker:publish(CommandTopic, emqx_utils_json:encode(Command), 0),
    timer:sleep(50),
    Request = test_recv_coap_request(UdpSock),
    #coap_message{method = Method} = Request,
    ?assertEqual(get, Method),
    BadTlv = <<3:2, 1:1, 0:2, 1:3, 300:16, 1>>,
    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {ok, content},
        #coap_content{
            content_format = <<"application/vnd.oma.lwm2m+tlv">>,
            payload = BadTlv
        },
        Request,
        true
    ),
    timer:sleep(100),
    ReadResult = emqx_utils_json:encode(
        #{
            <<"requestID">> => CmdId,
            <<"cacheID">> => CmdId,
            <<"msgType">> => <<"read">>,
            <<"data">> => #{
                <<"reqPath">> => <<"/3/0">>,
                <<"code">> => <<"4.00">>,
                <<"codeMsg">> => <<"bad_request">>
            }
        }
    ),
    ?assertEqual(ReadResult, test_recv_mqtt_response(RespTopic)).

case20_single_write(Config) ->
    %% step 1, device register ...
    Epn = "urn:oma:lwm2m:oma:3",
    MsgId1 = 15,
    UdpSock = ?config(sock, Config),
    ObjectList = <<"</1>, </2>, </3/0>, </4>, </5>">>,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),

    std_register(UdpSock, Epn, ObjectList, MsgId1, RespTopic),

    %% step2,  send a WRITE command to device
    CommandTopic = <<"lwm2m/", (list_to_binary(Epn))/binary, "/dn/dm">>,
    CmdId = 307,
    Command = #{
        <<"requestID">> => CmdId,
        <<"cacheID">> => CmdId,
        <<"msgType">> => <<"write">>,
        <<"data">> => #{
            <<"path">> => <<"/3/0/13">>,
            <<"type">> => <<"Integer">>,
            <<"value">> => <<"12345">>
        }
    },
    CommandJson = emqx_utils_json:encode(Command),
    test_mqtt_broker:publish(CommandTopic, CommandJson, 0),
    timer:sleep(50),
    Request2 = test_recv_coap_request(UdpSock),
    #coap_message{
        method = Method2,
        options = Options2,
        payload = Payload2
    } = Request2,
    Path2 = get_coap_path(Options2),
    ?assertEqual(put, Method2),
    ?assertEqual(<<"/3/0/13">>, Path2),
    Tlv_Value = <<3:2, 0:1, 0:2, 2:3, 13, 12345:16>>,
    ?assertEqual(Tlv_Value, Payload2),
    timer:sleep(50),

    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {ok, changed},
        #coap_content{},
        Request2,
        true
    ),
    timer:sleep(100),

    ReadResult = emqx_utils_json:encode(
        #{
            <<"requestID">> => CmdId,
            <<"cacheID">> => CmdId,
            <<"data">> => #{
                <<"reqPath">> => <<"/3/0/13">>,
                <<"code">> => <<"2.04">>,
                <<"codeMsg">> => <<"changed">>
            },
            <<"msgType">> => <<"write">>
        }
    ),
    ?assertEqual(ReadResult, test_recv_mqtt_response(RespTopic)).

case20_write(Config) ->
    %% step 1, device register ...
    Epn = "urn:oma:lwm2m:oma:3",
    MsgId1 = 15,
    UdpSock = ?config(sock, Config),
    ObjectList = <<"</1>, </2>, </3/0>, </4>, </5>">>,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),

    std_register(UdpSock, Epn, ObjectList, MsgId1, RespTopic),

    %% step2,  send a WRITE command to device
    CommandTopic = <<"lwm2m/", (list_to_binary(Epn))/binary, "/dn/dm">>,
    CmdId = 307,
    Command = #{
        <<"requestID">> => CmdId,
        <<"cacheID">> => CmdId,
        <<"msgType">> => <<"write">>,
        <<"data">> => #{
            <<"basePath">> => <<"/3/0/13">>,
            <<"content">> =>
                [
                    #{
                        type => <<"Float">>,
                        value => <<"12345.0">>
                    }
                ]
        }
    },
    CommandJson = emqx_utils_json:encode(Command),
    test_mqtt_broker:publish(CommandTopic, CommandJson, 0),
    timer:sleep(50),
    Request2 = test_recv_coap_request(UdpSock),
    #coap_message{
        method = Method2,
        options = Options2,
        payload = Payload2
    } = Request2,
    Path2 = get_coap_path(Options2),
    ?assertEqual(put, Method2),
    ?assertEqual(<<"/3/0/13">>, Path2),
    Tlv_Value = <<200, 13, 8, 64, 200, 28, 128, 0, 0, 0, 0>>,
    ?assertEqual(Tlv_Value, Payload2),
    timer:sleep(50),

    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {ok, changed},
        #coap_content{},
        Request2,
        true
    ),
    timer:sleep(100),

    WriteResult = emqx_utils_json:encode(
        #{
            <<"requestID">> => CmdId,
            <<"cacheID">> => CmdId,
            <<"data">> => #{
                <<"reqPath">> => <<"/3/0/13">>,
                <<"code">> => <<"2.04">>,
                <<"codeMsg">> => <<"changed">>
            },
            <<"msgType">> => <<"write">>
        }
    ),
    ?assertEqual(WriteResult, test_recv_mqtt_response(RespTopic)).

case27_write_hex_encoding(Config) ->
    Epn = "urn:oma:lwm2m:oma:6",
    MsgId1 = 15,
    UdpSock = ?config(sock, Config),
    ObjectList = <<"</1>, </2>, </3/0>, </4>, </5>">>,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),

    std_register(UdpSock, Epn, ObjectList, MsgId1, RespTopic),

    CommandTopic = <<"lwm2m/", (list_to_binary(Epn))/binary, "/dn/dm">>,
    CmdId = 308,
    Command = #{
        <<"requestID">> => CmdId,
        <<"cacheID">> => CmdId,
        <<"msgType">> => <<"write">>,
        <<"encoding">> => <<"hex">>,
        <<"data">> => #{
            <<"path">> => <<"/3/0/1">>,
            <<"type">> => <<"String">>,
            <<"value">> => <<"48656C6C6F">>
        }
    },
    CommandJson = emqx_utils_json:encode(Command),
    test_mqtt_broker:publish(CommandTopic, CommandJson, 0),
    timer:sleep(50),
    Request = test_recv_coap_request(UdpSock),
    #coap_message{payload = Payload} = Request,
    HexBin = emqx_utils:hexstr_to_bin(<<"48656C6C6F">>),
    Data1 = #{
        <<"path">> => <<"/3/0/1">>,
        <<"type">> => <<"String">>,
        <<"value">> => HexBin
    },
    {PathList, _QueryList} = emqx_lwm2m_cmd:path_list(<<"/3/0/1">>),
    TlvData = emqx_lwm2m_message:json_to_tlv(PathList, [Data1]),
    Expected = emqx_lwm2m_tlv:encode(TlvData),
    ?assertEqual(Expected, Payload),

    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {ok, changed},
        #coap_content{},
        Request,
        true
    ),
    WriteBin = test_recv_mqtt_response(RespTopic),
    WriteMap = emqx_utils_json:decode(WriteBin),
    ?assertEqual(<<"hex">>, maps:get(<<"encoding">>, WriteMap)),
    ?assertEqual(<<"write">>, maps:get(<<"msgType">>, WriteMap)),
    ?assertEqual(CmdId, maps:get(<<"requestID">>, WriteMap)),
    ?assertEqual(CmdId, maps:get(<<"cacheID">>, WriteMap)),
    WriteData = maps:get(<<"data">>, WriteMap),
    ?assertEqual(<<"/3/0/1">>, maps:get(<<"reqPath">>, WriteData)),
    ?assertEqual(<<"2.04">>, maps:get(<<"code">>, WriteData)),
    ?assertEqual(<<"changed">>, maps:get(<<"codeMsg">>, WriteData)).

case27_write_hex_encoding_invalid(Config) ->
    Epn = "urn:oma:lwm2m:oma:7",
    MsgId1 = 16,
    UdpSock = ?config(sock, Config),
    ObjectList = <<"</1>, </2>, </3/0>, </4>, </5>">>,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),

    std_register(UdpSock, Epn, ObjectList, MsgId1, RespTopic),

    CommandTopic = <<"lwm2m/", (list_to_binary(Epn))/binary, "/dn/dm">>,
    CmdId = 309,
    Command = #{
        <<"requestID">> => CmdId,
        <<"cacheID">> => CmdId,
        <<"msgType">> => <<"write">>,
        <<"encoding">> => <<"hex">>,
        <<"data">> => #{
            <<"path">> => <<"/3/0/1">>,
            <<"type">> => <<"String">>,
            <<"value">> => <<"ZZ">>
        }
    },
    CommandJson = emqx_utils_json:encode(Command),
    test_mqtt_broker:publish(CommandTopic, CommandJson, 0),
    timer:sleep(50),

    ?assertEqual(timeout_test_recv_coap_request, test_recv_coap_request(UdpSock)),

    WriteBin = test_recv_mqtt_response(RespTopic),
    ?assertNotEqual(timeout_test_recv_mqtt_response, WriteBin),
    WriteMap = emqx_utils_json:decode(WriteBin),
    ?assertEqual(<<"write">>, maps:get(<<"msgType">>, WriteMap)),
    ?assertEqual(CmdId, maps:get(<<"requestID">>, WriteMap)),
    ?assertEqual(CmdId, maps:get(<<"cacheID">>, WriteMap)),
    WriteData = maps:get(<<"data">>, WriteMap),
    ?assertEqual(<<"/3/0/1">>, maps:get(<<"reqPath">>, WriteData)),
    ?assertEqual(<<"4.00">>, maps:get(<<"code">>, WriteData)),
    ?assertEqual(<<"bad_request">>, maps:get(<<"codeMsg">>, WriteData)).

case21_write_object(Config) ->
    %% step 1, device register ...
    Epn = "urn:oma:lwm2m:oma:3",
    MsgId1 = 15,
    UdpSock = ?config(sock, Config),
    ObjectList = <<"</1>, </2>, </3/0>, </4>, </5>">>,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),

    std_register(UdpSock, Epn, ObjectList, MsgId1, RespTopic),

    %% step2,  send a WRITE command to device
    CommandTopic = <<"lwm2m/", (list_to_binary(Epn))/binary, "/dn/dm">>,
    CmdId = 307,
    Command = #{
        <<"requestID">> => CmdId,
        <<"cacheID">> => CmdId,
        <<"msgType">> => <<"write">>,
        <<"data">> => #{
            <<"basePath">> => <<"/3/0/">>,
            <<"content">> =>
                [
                    #{
                        path => <<"13">>,
                        type => <<"Integer">>,
                        value => <<"12345">>
                    },
                    #{
                        path => <<"14">>,
                        type => <<"String">>,
                        value => <<"87x">>
                    },
                    #{
                        path => <<"6/0">>,
                        type => <<"Integer">>,
                        value => <<"1">>
                    }
                ]
        }
    },
    CommandJson = emqx_utils_json:encode(Command),
    test_mqtt_broker:publish(CommandTopic, CommandJson, 0),
    timer:sleep(50),
    Request2 = test_recv_coap_request(UdpSock),
    #coap_message{
        method = Method2,
        options = Options2,
        payload = Payload2
    } = Request2,
    Path2 = get_coap_path(Options2),
    ?assertEqual(post, Method2),
    ?assertEqual(<<"/3/0">>, Path2),
    Tlv_Value =
        <<3:2, 0:1, 0:2, 2:3, 13, 12345:16, 3:2, 0:1, 0:2, 3:3, 14, "87x", 2:2, 0:1, 0:2, 3:3, 6,
            1:2, 0:1, 0:2, 1:3, 0, 1>>,
    ?assertEqual(Tlv_Value, Payload2),
    timer:sleep(50),

    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {ok, changed},
        #coap_content{},
        Request2,
        true
    ),
    timer:sleep(100),

    ReadResult = emqx_utils_json:encode(
        #{
            <<"requestID">> => CmdId,
            <<"cacheID">> => CmdId,
            <<"msgType">> => <<"write">>,
            <<"data">> => #{
                <<"reqPath">> => <<"/3/0/">>,
                <<"code">> => <<"2.04">>,
                <<"codeMsg">> => <<"changed">>
            }
        }
    ),
    ?assertEqual(ReadResult, test_recv_mqtt_response(RespTopic)).

case22_write_error(Config) ->
    %% step 1, device register ...
    Epn = "urn:oma:lwm2m:oma:3",
    MsgId1 = 15,
    UdpSock = ?config(sock, Config),
    ObjectList = <<"</1>, </2>, </3/0>, </4>, </5>">>,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),

    std_register(UdpSock, Epn, ObjectList, MsgId1, RespTopic),

    %% step2,  send a WRITE command to device
    CommandTopic = <<"lwm2m/", (list_to_binary(Epn))/binary, "/dn/dm">>,
    CmdId = 307,
    Command = #{
        <<"requestID">> => CmdId,
        <<"cacheID">> => CmdId,
        <<"msgType">> => <<"write">>,
        <<"data">> => #{
            <<"basePath">> => <<"/3/0/1">>,
            <<"content">> =>
                [
                    #{
                        type => <<"Integer">>,
                        value => <<"12345">>
                    }
                ]
        }
    },
    CommandJson = emqx_utils_json:encode(Command),
    test_mqtt_broker:publish(CommandTopic, CommandJson, 0),
    timer:sleep(50),
    Request2 = test_recv_coap_request(UdpSock),
    #coap_message{method = Method2, options = Options2} = Request2,
    Path2 = get_coap_path(Options2),
    ?assertEqual(put, Method2),
    ?assertEqual(<<"/3/0/1">>, Path2),
    timer:sleep(50),

    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {error, bad_request},
        #coap_content{},
        Request2,
        true
    ),
    timer:sleep(100),

    ReadResult = emqx_utils_json:encode(
        #{
            <<"requestID">> => CmdId,
            <<"cacheID">> => CmdId,
            <<"data">> => #{
                <<"reqPath">> => <<"/3/0/1">>,
                <<"code">> => <<"4.00">>,
                <<"codeMsg">> => <<"bad_request">>
            },
            <<"msgType">> => <<"write">>
        }
    ),
    ?assertEqual(ReadResult, test_recv_mqtt_response(RespTopic)).

case23_write_multi_resource_instance(Config) ->
    %% step 1, device register ...
    Epn = "urn:oma:lwm2m:oma:3",
    MsgId1 = 15,
    UdpSock = ?config(sock, Config),
    ObjectList = <<"</1>, </2>, </3/0>, </4>, </5>">>,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    CommandTopic = <<"lwm2m/", (list_to_binary(Epn))/binary, "/dn/dm">>,
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),

    std_register(UdpSock, Epn, ObjectList, MsgId1, RespTopic),

    %% step2, send a WRITE command with resource instances
    CmdId = 400,
    Command = #{
        <<"requestID">> => CmdId,
        <<"cacheID">> => CmdId,
        <<"msgType">> => <<"write">>,
        <<"mheaders">> => #{<<"ttl">> => 0},
        <<"data">> => #{
            <<"basePath">> => <<"/3/0/22">>,
            <<"content">> =>
                [
                    #{path => <<"0">>, type => <<"Objlnk">>, value => <<"1:2">>},
                    #{path => <<"1">>, type => <<"Objlnk">>, value => <<"1:3">>},
                    #{path => <<"1">>, type => <<"Objlnk">>, value => <<"1:4">>}
                ]
        }
    },
    test_mqtt_broker:publish(CommandTopic, emqx_utils_json:encode(Command), 0),
    timer:sleep(50),
    Request2 = test_recv_coap_request(UdpSock),
    #coap_message{
        method = Method2,
        options = Options2,
        payload = Payload2
    } = Request2,
    ?assertEqual(put, Method2),
    ?assertEqual(<<"/3/0/22">>, get_coap_path(Options2)),
    Objlnk0 = <<1:16, 2:16>>,
    Objlnk1 = <<1:16, 4:16>>,
    ResInst0 = <<1:2, 0:1, 0:2, 4:3, 0, Objlnk0/binary>>,
    ResInst1 = <<1:2, 0:1, 0:2, 4:3, 1, Objlnk1/binary>>,
    ObjlnkValue = <<ResInst0/binary, ResInst1/binary>>,
    ExpectedPayload = <<2:2, 0:1, 1:2, 0:3, 22, (byte_size(ObjlnkValue)), ObjlnkValue/binary>>,
    ?assertEqual(ExpectedPayload, Payload2),
    timer:sleep(50),

    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {ok, changed},
        #coap_content{},
        Request2,
        true
    ),
    timer:sleep(100),

    WriteResult = emqx_utils_json:encode(
        #{
            <<"requestID">> => CmdId,
            <<"cacheID">> => CmdId,
            <<"mheaders">> => #{<<"ttl">> => 0},
            <<"data">> => #{
                <<"reqPath">> => <<"/3/0/22">>,
                <<"code">> => <<"2.04">>,
                <<"codeMsg">> => <<"changed">>
            },
            <<"msgType">> => <<"write">>
        }
    ),
    ?assertEqual(WriteResult, test_recv_mqtt_response(RespTopic)).

case24_write_content_response(Config) ->
    %% step 1, device register ...
    Epn = "urn:oma:lwm2m:oma:3",
    MsgId1 = 15,
    UdpSock = ?config(sock, Config),
    ObjectList = <<"</1>, </2>, </3/0>, </4>, </5>">>,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    CommandTopic = <<"lwm2m/", (list_to_binary(Epn))/binary, "/dn/dm">>,
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),

    std_register(UdpSock, Epn, ObjectList, MsgId1, RespTopic),

    %% content response with empty payload
    CmdId1 = 410,
    Command1 = #{
        <<"requestID">> => CmdId1,
        <<"cacheID">> => CmdId1,
        <<"msgType">> => <<"write">>,
        <<"data">> => #{
            <<"path">> => <<"/3/0/13">>,
            <<"type">> => <<"Integer">>,
            <<"value">> => <<"1">>
        }
    },
    test_mqtt_broker:publish(CommandTopic, emqx_utils_json:encode(Command1), 0),
    timer:sleep(50),
    Request1 = test_recv_coap_request(UdpSock),
    #coap_message{method = Method1, options = Options1} = Request1,
    ?assertEqual(put, Method1),
    ?assertEqual(<<"/3/0/13">>, get_coap_path(Options1)),
    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {ok, content},
        #coap_content{content_format = <<"text/plain">>, payload = <<>>},
        Request1,
        true
    ),
    timer:sleep(100),
    WriteResult1 = emqx_utils_json:encode(
        #{
            <<"requestID">> => CmdId1,
            <<"cacheID">> => CmdId1,
            <<"data">> => #{
                <<"reqPath">> => <<"/3/0/13">>,
                <<"code">> => <<"4.05">>,
                <<"codeMsg">> => <<"method_not_allowed">>
            },
            <<"msgType">> => <<"write">>
        }
    ),
    ?assertEqual(WriteResult1, test_recv_mqtt_response(RespTopic)),

    %% content response with payload
    CmdId2 = 411,
    Command2 = #{
        <<"requestID">> => CmdId2,
        <<"cacheID">> => CmdId2,
        <<"msgType">> => <<"write">>,
        <<"data">> => #{
            <<"path">> => <<"/3/0/13">>,
            <<"type">> => <<"Integer">>,
            <<"value">> => <<"2">>
        }
    },
    test_mqtt_broker:publish(CommandTopic, emqx_utils_json:encode(Command2), 0),
    timer:sleep(50),
    Request2 = test_recv_coap_request(UdpSock),
    #coap_message{method = Method2, options = Options2} = Request2,
    ?assertEqual(put, Method2),
    ?assertEqual(<<"/3/0/13">>, get_coap_path(Options2)),
    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {ok, content},
        #coap_content{content_format = <<"text/plain">>, payload = <<"OK">>},
        Request2,
        true
    ),
    timer:sleep(100),
    WriteResult2 = emqx_utils_json:encode(
        #{
            <<"requestID">> => CmdId2,
            <<"cacheID">> => CmdId2,
            <<"data">> => #{
                <<"reqPath">> => <<"/3/0/13">>,
                <<"code">> => <<"2.04">>,
                <<"codeMsg">> => <<"changed">>
            },
            <<"msgType">> => <<"write">>
        }
    ),
    ?assertEqual(WriteResult2, test_recv_mqtt_response(RespTopic)).

case25_write_invalid_json(Config) ->
    %% step 1, device register ...
    Epn = "urn:oma:lwm2m:oma:3",
    MsgId1 = 15,
    UdpSock = ?config(sock, Config),
    ObjectList = <<"</1>, </2>, </3/0>, </4>, </5>">>,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    CommandTopic = <<"lwm2m/", (list_to_binary(Epn))/binary, "/dn/dm">>,
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),

    std_register(UdpSock, Epn, ObjectList, MsgId1, RespTopic),

    %% invalid JSON should be dropped
    test_mqtt_broker:publish(CommandTopic, <<"{bad json">>, 0),
    ?assertEqual(timeout_test_recv_coap_request, test_recv_coap_request(UdpSock)).

case26_write_object_instance_mixed(Config) ->
    %% step 1, device register ...
    Epn = "urn:oma:lwm2m:oma:26",
    MsgId1 = 15,
    UdpSock = ?config(sock, Config),
    ObjectList = <<"</1>, </2>, </3/0>, </4>, </5>">>,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    CommandTopic = <<"lwm2m/", (list_to_binary(Epn))/binary, "/dn/dm">>,
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),

    std_register(UdpSock, Epn, ObjectList, MsgId1, RespTopic),

    LargeOpaque = base64:encode(binary:copy(<<"B">>, 256)),
    CmdId = 560,
    Command = #{
        <<"requestID">> => CmdId,
        <<"cacheID">> => CmdId,
        <<"msgType">> => <<"write">>,
        <<"data">> => #{
            <<"basePath">> => <<"/3/0">>,
            <<"content">> => [
                #{path => <<"/6/0">>, type => <<"Integer">>, value => 1},
                #{path => <<"6/1">>, type => <<"Integer">>, value => 2},
                #{path => <<"13">>, type => <<"Time">>, value => 10},
                #{path => <<"13">>, type => <<"Time">>, value => 11},
                #{path => <<"11">>, type => <<"Boolean">>, value => <<"true">>},
                #{path => <<"12">>, type => <<"Boolean">>, value => <<"false">>},
                #{path => <<"14">>, type => <<"Float">>, value => 1.5},
                #{path => <<"300">>, type => <<"Opaque">>, value => LargeOpaque}
            ]
        }
    },
    test_mqtt_broker:publish(CommandTopic, emqx_utils_json:encode(Command), 0),
    timer:sleep(50),
    Request2 = test_recv_coap_request(UdpSock),
    #coap_message{method = Method2, options = Options2, payload = Payload2} = Request2,
    ?assertEqual(post, Method2),
    ?assertEqual(<<"/3/0">>, get_coap_path(Options2)),
    ?assert(byte_size(Payload2) > 0),
    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {ok, changed},
        #coap_content{},
        Request2,
        true
    ),
    timer:sleep(100),
    WriteResult = emqx_utils_json:encode(
        #{
            <<"requestID">> => CmdId,
            <<"cacheID">> => CmdId,
            <<"data">> => #{
                <<"reqPath">> => <<"/3/0">>,
                <<"code">> => <<"2.04">>,
                <<"codeMsg">> => <<"changed">>
            },
            <<"msgType">> => <<"write">>
        }
    ),
    ?assertEqual(WriteResult, test_recv_mqtt_response(RespTopic)).

case_create_basic(Config) ->
    %% step 1, device register ...
    Epn = "urn:oma:lwm2m:oma:3",
    MsgId1 = 15,
    UdpSock = ?config(sock, Config),
    ObjectList = <<"</1>, </2>, </3/0>, </4>">>,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),

    std_register(UdpSock, Epn, ObjectList, MsgId1, RespTopic),

    %% step2, send a CREATE command to device
    CommandTopic = <<"lwm2m/", (list_to_binary(Epn))/binary, "/dn/dm">>,
    CmdId = 307,
    Command = #{
        <<"msgType">> => <<"create">>,
        <<"requestID">> => CmdId,
        <<"cacheID">> => CmdId,
        <<"data">> => #{
            <<"content">> => [],
            <<"basePath">> => <<"/5">>
        }
    },
    CommandJson = emqx_utils_json:encode(Command),
    test_mqtt_broker:publish(CommandTopic, CommandJson, 0),
    timer:sleep(50),
    Request2 = test_recv_coap_request(UdpSock),
    #coap_message{
        method = Method2,
        options = Options2,
        payload = Payload2
    } = Request2,
    Path2 = get_coap_path(Options2),
    ?assertEqual(post, Method2),
    ?assertEqual(<<"/5">>, Path2),
    ?assertEqual(<<"">>, Payload2),
    timer:sleep(50),

    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {ok, created},
        #coap_content{},
        Request2,
        true
    ),
    timer:sleep(100),

    ReadResult = emqx_utils_json:encode(
        #{
            <<"requestID">> => CmdId,
            <<"cacheID">> => CmdId,
            <<"data">> => #{
                <<"reqPath">> => <<"/5">>,
                <<"code">> => <<"2.01">>,
                <<"codeMsg">> => <<"created">>
            },
            <<"msgType">> => <<"create">>
        }
    ),
    ?assertEqual(ReadResult, test_recv_mqtt_response(RespTopic)).

case_create_with_content(Config) ->
    %% step 1, device register ...
    Epn = "urn:oma:lwm2m:oma:3",
    MsgId1 = 15,
    UdpSock = ?config(sock, Config),
    ObjectList = <<"</1>, </2>, </3/0>, </4>">>,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),

    std_register(UdpSock, Epn, ObjectList, MsgId1, RespTopic),

    %% step2, send a CREATE command with content
    CommandTopic = <<"lwm2m/", (list_to_binary(Epn))/binary, "/dn/dm">>,
    CmdId = 308,
    Command = #{
        <<"msgType">> => <<"create">>,
        <<"requestID">> => CmdId,
        <<"cacheID">> => CmdId,
        <<"data">> => #{
            <<"content">> =>
                [
                    #{path => <<"0/0">>, type => <<"Float">>, value => <<"1.5">>},
                    #{path => <<"0/4">>, type => <<"Opaque">>, value => <<"AQID">>},
                    #{path => <<"0/21">>, type => <<"Integer">>, value => <<"-129">>},
                    #{path => <<"1/0">>, type => <<"Float">>, value => <<"2.5">>}
                ],
            <<"basePath">> => <<"/6">>
        }
    },
    test_mqtt_broker:publish(CommandTopic, emqx_utils_json:encode(Command), 0),
    timer:sleep(50),
    Request2 = test_recv_coap_request(UdpSock),
    #coap_message{
        method = Method2,
        options = Options2,
        payload = Payload2
    } = Request2,
    Path2 = get_coap_path(Options2),
    ?assertEqual(post, Method2),
    ?assertEqual(<<"/6">>, Path2),
    ?assertNotEqual(<<>>, Payload2),
    timer:sleep(50),

    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {ok, created},
        #coap_content{},
        Request2,
        true
    ),
    timer:sleep(100),

    ReadResult = emqx_utils_json:encode(
        #{
            <<"requestID">> => CmdId,
            <<"cacheID">> => CmdId,
            <<"data">> => #{
                <<"reqPath">> => <<"/6">>,
                <<"code">> => <<"2.01">>,
                <<"codeMsg">> => <<"created">>
            },
            <<"msgType">> => <<"create">>
        }
    ),
    ?assertEqual(ReadResult, test_recv_mqtt_response(RespTopic)).

case_delete_basic(Config) ->
    %% step 1, device register ...
    Epn = "urn:oma:lwm2m:oma:3",
    MsgId1 = 15,
    UdpSock = ?config(sock, Config),
    ObjectList = <<"</1>, </2>, </3/0>, </4>, </5>, </5/0>">>,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),

    std_register(UdpSock, Epn, ObjectList, MsgId1, RespTopic),

    %% step2, send a CREATE command to device
    CommandTopic = <<"lwm2m/", (list_to_binary(Epn))/binary, "/dn/dm">>,
    CmdId = 307,
    Command = #{
        <<"requestID">> => CmdId,
        <<"cacheID">> => CmdId,
        <<"msgType">> => <<"delete">>,
        <<"data">> => #{<<"path">> => <<"/5/0">>}
    },
    CommandJson = emqx_utils_json:encode(Command),
    test_mqtt_broker:publish(CommandTopic, CommandJson, 0),
    timer:sleep(50),
    Request2 = test_recv_coap_request(UdpSock),
    #coap_message{
        method = Method2,
        options = Options2,
        payload = Payload2
    } = Request2,
    Path2 = get_coap_path(Options2),
    ?assertEqual(delete, Method2),
    ?assertEqual(<<"/5/0">>, Path2),
    ?assertEqual(<<"">>, Payload2),
    timer:sleep(50),

    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {ok, deleted},
        #coap_content{},
        Request2,
        true
    ),
    timer:sleep(100),

    ReadResult = emqx_utils_json:encode(
        #{
            <<"requestID">> => CmdId,
            <<"cacheID">> => CmdId,
            <<"data">> => #{
                <<"reqPath">> => <<"/5/0">>,
                <<"code">> => <<"2.02">>,
                <<"codeMsg">> => <<"deleted">>
            },
            <<"msgType">> => <<"delete">>
        }
    ),
    ?assertEqual(ReadResult, test_recv_mqtt_response(RespTopic)).

case30_execute(Config) ->
    %% step 1, device register ...
    Epn = "urn:oma:lwm2m:oma:3",
    MsgId1 = 15,
    UdpSock = ?config(sock, Config),
    ObjectList = <<"</1>, </2>, </3/0>, </4>, </5>">>,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),

    std_register(UdpSock, Epn, ObjectList, MsgId1, RespTopic),

    %% step2,  send a WRITE command to device
    CommandTopic = <<"lwm2m/", (list_to_binary(Epn))/binary, "/dn/dm">>,
    CmdId = 307,
    Command = #{
        <<"requestID">> => CmdId,
        <<"cacheID">> => CmdId,
        <<"msgType">> => <<"execute">>,
        <<"data">> => #{
            <<"path">> => <<"/3/0/4">>,
            %% ensure undefined args are treated as empty payload
            <<"args">> => <<"undefined">>
        }
    },
    CommandJson = emqx_utils_json:encode(Command),
    test_mqtt_broker:publish(CommandTopic, CommandJson, 0),
    timer:sleep(50),
    Request2 = test_recv_coap_request(UdpSock),
    #coap_message{
        method = Method2,
        options = Options2,
        payload = Payload2
    } = Request2,
    Path2 = get_coap_path(Options2),
    ?assertEqual(post, Method2),
    ?assertEqual(<<"/3/0/4">>, Path2),
    ?assertEqual(<<>>, Payload2),
    timer:sleep(50),

    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {ok, changed},
        #coap_content{},
        Request2,
        true
    ),
    timer:sleep(100),

    ReadResult = emqx_utils_json:encode(
        #{
            <<"requestID">> => CmdId,
            <<"cacheID">> => CmdId,
            <<"data">> => #{
                <<"reqPath">> => <<"/3/0/4">>,
                <<"code">> => <<"2.04">>,
                <<"codeMsg">> => <<"changed">>
            },
            <<"msgType">> => <<"execute">>
        }
    ),
    ?assertEqual(ReadResult, test_recv_mqtt_response(RespTopic)).

case31_execute_error(Config) ->
    %% step 1, device register ...
    Epn = "urn:oma:lwm2m:oma:3",
    MsgId1 = 15,
    UdpSock = ?config(sock, Config),
    ObjectList = <<"</1>, </2>, </3/0>, </4>, </5>">>,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),

    std_register(UdpSock, Epn, ObjectList, MsgId1, RespTopic),

    %% step2,  send a WRITE command to device
    CommandTopic = <<"lwm2m/", (list_to_binary(Epn))/binary, "/dn/dm">>,
    CmdId = 307,
    Command = #{
        <<"requestID">> => CmdId,
        <<"cacheID">> => CmdId,
        <<"msgType">> => <<"execute">>,
        <<"data">> => #{
            <<"path">> => <<"/3/0/4">>,
            <<"args">> => <<"2,7">>
        }
    },
    CommandJson = emqx_utils_json:encode(Command),
    test_mqtt_broker:publish(CommandTopic, CommandJson, 0),
    timer:sleep(50),
    Request2 = test_recv_coap_request(UdpSock),
    #coap_message{
        method = Method2,
        options = Options2,
        payload = Payload2
    } = Request2,
    Path2 = get_coap_path(Options2),
    ?assertEqual(post, Method2),
    ?assertEqual(<<"/3/0/4">>, Path2),
    ?assertEqual(<<"2,7">>, Payload2),
    timer:sleep(50),

    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {error, unauthorized},
        #coap_content{},
        Request2,
        true
    ),
    timer:sleep(100),

    ReadResult = emqx_utils_json:encode(
        #{
            <<"requestID">> => CmdId,
            <<"cacheID">> => CmdId,
            <<"data">> => #{
                <<"reqPath">> => <<"/3/0/4">>,
                <<"code">> => <<"4.01">>,
                <<"codeMsg">> => <<"unauthorized">>
            },
            <<"msgType">> => <<"execute">>
        }
    ),
    ?assertEqual(ReadResult, test_recv_mqtt_response(RespTopic)).

case40_discover(Config) ->
    %% step 1, device register ...
    Epn = "urn:oma:lwm2m:oma:3",
    MsgId1 = 15,
    UdpSock = ?config(sock, Config),
    ObjectList = <<"</1>, </2>, </3/0>, </4>, </5>">>,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),

    std_register(UdpSock, Epn, ObjectList, MsgId1, RespTopic),

    %% step2,  send a WRITE command to device
    CommandTopic = <<"lwm2m/", (list_to_binary(Epn))/binary, "/dn/dm">>,
    CmdId = 307,
    Command = #{
        <<"requestID">> => CmdId,
        <<"cacheID">> => CmdId,
        <<"msgType">> => <<"discover">>,
        <<"data">> => #{
            <<"path">> => <<"/3/0/7">>
        }
    },
    CommandJson = emqx_utils_json:encode(Command),
    test_mqtt_broker:publish(CommandTopic, CommandJson, 0),
    timer:sleep(50),
    Request2 = test_recv_coap_request(UdpSock),
    #coap_message{
        method = Method2,
        options = Options2,
        payload = Payload2
    } = Request2,
    Path2 = get_coap_path(Options2),
    ?assertEqual(get, Method2),
    ?assertEqual(<<"/3/0/7">>, Path2),
    ?assertEqual(<<>>, Payload2),
    timer:sleep(50),

    PayloadDiscover = <<"</3/0/7>;dim=8;pmin=10;pmax=60;gt=50;lt=42.2,</3/0/8>">>,
    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {ok, content},
        #coap_content{
            content_format = <<"application/link-format">>,
            payload = PayloadDiscover
        },
        Request2,
        true
    ),
    timer:sleep(100),

    ReadResult = emqx_utils_json:encode(
        #{
            <<"requestID">> => CmdId,
            <<"cacheID">> => CmdId,
            <<"msgType">> => <<"discover">>,
            <<"data">> => #{
                <<"reqPath">> => <<"/3/0/7">>,
                <<"code">> => <<"2.05">>,
                <<"codeMsg">> => <<"content">>,
                <<"content">> =>
                    [
                        <<"</3/0/7>;dim=8;pmin=10;pmax=60;gt=50;lt=42.2">>,
                        <<"</3/0/8>">>
                    ]
            }
        }
    ),
    ?assertEqual(ReadResult, test_recv_mqtt_response(RespTopic)).

case41_discover_error(Config) ->
    %% step 1, device register ...
    Epn = "urn:oma:lwm2m:oma:41",
    MsgId1 = 15,
    UdpSock = ?config(sock, Config),
    ObjectList = <<"</1>, </3/0>">>,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),

    std_register(UdpSock, Epn, ObjectList, MsgId1, RespTopic),

    %% send a DISCOVER command and return error
    CommandTopic = <<"lwm2m/", (list_to_binary(Epn))/binary, "/dn/dm">>,
    CmdId = 407,
    Command = #{
        <<"requestID">> => CmdId,
        <<"cacheID">> => CmdId,
        <<"msgType">> => <<"discover">>,
        <<"data">> => #{<<"path">> => <<"/3/0/7">>}
    },
    test_mqtt_broker:publish(CommandTopic, emqx_utils_json:encode(Command), 0),
    timer:sleep(50),
    Request2 = test_recv_coap_request(UdpSock),
    #coap_message{method = Method2} = Request2,
    ?assertEqual(get, Method2),
    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {error, not_found},
        #coap_content{content_format = <<"text/plain">>, payload = <<>>},
        Request2,
        true
    ),
    timer:sleep(100),
    ReadResult = emqx_utils_json:encode(
        #{
            <<"requestID">> => CmdId,
            <<"cacheID">> => CmdId,
            <<"msgType">> => <<"discover">>,
            <<"data">> => #{
                <<"reqPath">> => <<"/3/0/7">>,
                <<"code">> => <<"4.04">>,
                <<"codeMsg">> => <<"not_found">>
            }
        }
    ),
    ?assertEqual(ReadResult, test_recv_mqtt_response(RespTopic)).

case50_write_attribute(Config) ->
    %% step 1, device register ...
    Epn = "urn:oma:lwm2m:oma:3",
    MsgId1 = 15,
    UdpSock = ?config(sock, Config),
    ObjectList = <<"</1>, </2>, </3/0>, </4>, </5>">>,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),

    std_register(UdpSock, Epn, ObjectList, MsgId1, RespTopic),

    %% step2,  send a WRITE command to device
    CommandTopic = <<"lwm2m/", (list_to_binary(Epn))/binary, "/dn/dm">>,
    CmdId = 307,
    Command = #{
        <<"requestID">> => CmdId,
        <<"cacheID">> => CmdId,
        <<"msgType">> => <<"write-attr">>,
        <<"data">> => #{
            <<"path">> => <<"/3/0/9">>,
            <<"pmin">> => <<"1">>,
            <<"pmax">> => <<"5">>,
            <<"lt">> => <<"5">>,
            <<"st">> => null,
            <<"ignored">> => <<"1">>
        }
    },
    CommandJson = emqx_utils_json:encode(Command),
    test_mqtt_broker:publish(CommandTopic, CommandJson, 0),
    timer:sleep(100),
    Request2 = test_recv_coap_request(UdpSock),
    #coap_message{
        method = Method2,
        options = Options2,
        payload = Payload2
    } = Request2,
    ?LOGT("got options: ~p", [Options2]),
    Path2 = get_coap_path(Options2),
    Query2 = lists:sort(maps:to_list(get_coap_query(Options2))),
    ?assertEqual(put, Method2),
    ?assertEqual(<<"/3/0/9">>, Path2),
    ?assertEqual(
        lists:sort([
            {<<"pmax">>, <<"5">>},
            {<<"lt">>, <<"5">>},
            {<<"pmin">>, <<"1">>}
        ]),
        Query2
    ),
    ?assertEqual(<<>>, Payload2),
    timer:sleep(50),

    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {ok, changed},
        #coap_content{},
        Request2,
        true
    ),
    timer:sleep(100),

    ReadResult = emqx_utils_json:encode(
        #{
            <<"requestID">> => CmdId,
            <<"cacheID">> => CmdId,
            <<"data">> => #{
                <<"reqPath">> => <<"/3/0/9">>,
                <<"code">> => <<"2.04">>,
                <<"codeMsg">> => <<"changed">>
            },
            <<"msgType">> => <<"write-attr">>
        }
    ),
    ?assertEqual(ReadResult, test_recv_mqtt_response(RespTopic)).

case51_write_attr_error(Config) ->
    %% step 1, device register ...
    Epn = "urn:oma:lwm2m:oma:51",
    MsgId1 = 15,
    UdpSock = ?config(sock, Config),
    ObjectList = <<"</1>, </2>, </3/0>">>,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),

    std_register(UdpSock, Epn, ObjectList, MsgId1, RespTopic),

    CommandTopic = <<"lwm2m/", (list_to_binary(Epn))/binary, "/dn/dm">>,
    CmdId = 507,
    Command = #{
        <<"requestID">> => CmdId,
        <<"cacheID">> => CmdId,
        <<"msgType">> => <<"write-attr">>,
        <<"data">> => #{<<"path">> => <<"/3/0/9">>, <<"pmin">> => <<"1">>}
    },
    test_mqtt_broker:publish(CommandTopic, emqx_utils_json:encode(Command), 0),
    timer:sleep(50),
    Request2 = test_recv_coap_request(UdpSock),
    #coap_message{method = Method2} = Request2,
    ?assertEqual(put, Method2),
    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {error, bad_request},
        #coap_content{},
        Request2,
        true
    ),
    timer:sleep(100),
    ReadResult = emqx_utils_json:encode(
        #{
            <<"requestID">> => CmdId,
            <<"cacheID">> => CmdId,
            <<"data">> => #{
                <<"reqPath">> => <<"/3/0/9">>,
                <<"code">> => <<"4.00">>,
                <<"codeMsg">> => <<"bad_request">>
            },
            <<"msgType">> => <<"write-attr">>
        }
    ),
    ?assertEqual(ReadResult, test_recv_mqtt_response(RespTopic)).

case60_observe(Config) ->
    %% step 1, device register ...
    Epn = "urn:oma:lwm2m:oma:3",
    MsgId1 = 15,
    UdpSock = ?config(sock, Config),
    ObjectList = <<"</1>, </2>, </3/0>, </4>, </5>">>,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    RespTopicAD = list_to_binary("lwm2m/" ++ Epn ++ "/up/notify"),
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    emqtt:subscribe(?config(emqx_c, Config), RespTopicAD, qos0),
    timer:sleep(200),

    std_register(UdpSock, Epn, ObjectList, MsgId1, RespTopic),

    %% step2,  send a OBSERVE command to device
    CommandTopic = <<"lwm2m/", (list_to_binary(Epn))/binary, "/dn/dm">>,
    CmdId = 307,
    Command = #{
        <<"requestID">> => CmdId,
        <<"cacheID">> => CmdId,
        <<"msgType">> => <<"observe">>,
        <<"data">> => #{<<"path">> => <<"/3/0/10">>}
    },
    CommandJson = emqx_utils_json:encode(Command),
    test_mqtt_broker:publish(CommandTopic, CommandJson, 0),
    timer:sleep(50),
    Request2 = test_recv_coap_request(UdpSock),
    #coap_message{
        method = Method2,
        options = Options2,
        payload = Payload2
    } = Request2,
    Path2 = get_coap_path(Options2),
    Observe = get_coap_observe(Options2),
    ?assertEqual(get, Method2),
    ?assertEqual(<<"/3/0/10">>, Path2),
    ?assertEqual(Observe, 0),
    ?assertEqual(<<>>, Payload2),
    timer:sleep(50),

    test_send_coap_observe_ack(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {ok, content},
        #coap_content{content_format = <<"text/plain">>, payload = <<"2048">>},
        Request2
    ),
    timer:sleep(100),

    ReadResult = emqx_utils_json:encode(
        #{
            <<"requestID">> => CmdId,
            <<"cacheID">> => CmdId,
            <<"msgType">> => <<"observe">>,
            <<"data">> => #{
                <<"reqPath">> => <<"/3/0/10">>,
                <<"code">> => <<"2.05">>,
                <<"codeMsg">> => <<"content">>,
                <<"content">> =>
                    [
                        #{
                            path => <<"/3/0/10">>,
                            value => 2048
                        }
                    ]
            }
        }
    ),
    ?assertEqual(ReadResult, test_recv_mqtt_response(RespTopic)),

    %% step3 the notifications
    timer:sleep(200),
    ObSeq = 3,
    test_send_coap_notif(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        #coap_content{content_format = <<"text/plain">>, payload = <<"4096">>},
        ObSeq,
        Request2
    ),
    timer:sleep(100),
    #coap_message{} = test_recv_coap_response(UdpSock),

    ReadResult2 = emqx_utils_json:encode(
        #{
            <<"requestID">> => CmdId,
            <<"cacheID">> => CmdId,
            <<"msgType">> => <<"notify">>,
            <<"seqNum">> => ObSeq,
            <<"data">> => #{
                <<"reqPath">> => <<"/3/0/10">>,
                <<"code">> => <<"2.05">>,
                <<"codeMsg">> => <<"content">>,
                <<"content">> =>
                    [
                        #{
                            path => <<"/3/0/10">>,
                            value => 4096
                        }
                    ]
            }
        }
    ),
    ?assertEqual(ReadResult2, test_recv_mqtt_response(RespTopicAD)),

    %% Step3. cancel observe
    CmdId3 = 308,
    Command3 = #{
        <<"requestID">> => CmdId3,
        <<"cacheID">> => CmdId3,
        <<"msgType">> => <<"cancel-observe">>,
        <<"data">> => #{
            <<"path">> => <<"/3/0/10">>
        }
    },
    CommandJson3 = emqx_utils_json:encode(Command3),
    test_mqtt_broker:publish(CommandTopic, CommandJson3, 0),
    timer:sleep(50),
    Request3 = test_recv_coap_request(UdpSock),
    #coap_message{
        method = Method3,
        options = Options3,
        payload = Payload3
    } = Request3,
    Path3 = get_coap_path(Options3),
    Observe3 = get_coap_observe(Options3),
    ?assertEqual(get, Method3),
    ?assertEqual(<<"/3/0/10">>, Path3),
    ?assertEqual(Observe3, 1),
    ?assertEqual(<<>>, Payload3),
    timer:sleep(50),

    test_send_coap_observe_ack(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {ok, content},
        #coap_content{content_format = <<"text/plain">>, payload = <<"1150">>},
        Request3
    ),
    timer:sleep(100),

    ReadResult3 = emqx_utils_json:encode(
        #{
            <<"requestID">> => CmdId3,
            <<"cacheID">> => CmdId3,
            <<"msgType">> => <<"cancel-observe">>,
            <<"data">> => #{
                <<"reqPath">> => <<"/3/0/10">>,
                <<"code">> => <<"2.05">>,
                <<"codeMsg">> => <<"content">>,
                <<"content">> =>
                    [
                        #{
                            path => <<"/3/0/10">>,
                            value => 1150
                        }
                    ]
            }
        }
    ),
    ?assertEqual(ReadResult3, test_recv_mqtt_response(RespTopic)).

case61_observe_error(Config) ->
    %% step 1, device register ...
    Epn = "urn:oma:lwm2m:oma:3",
    MsgId1 = 15,
    UdpSock = ?config(sock, Config),
    ObjectList = <<"</1>, </2>, </3/0>, </4>, </5>">>,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),

    std_register(UdpSock, Epn, ObjectList, MsgId1, RespTopic),

    %% step2, send an OBSERVE command to device and reply with error
    CommandTopic = <<"lwm2m/", (list_to_binary(Epn))/binary, "/dn/dm">>,
    CmdId = 309,
    Command = #{
        <<"requestID">> => CmdId,
        <<"cacheID">> => CmdId,
        <<"msgType">> => <<"observe">>,
        <<"data">> => #{<<"path">> => <<"/3/0/10">>}
    },
    test_mqtt_broker:publish(CommandTopic, emqx_utils_json:encode(Command), 0),
    timer:sleep(50),
    Request2 = test_recv_coap_request(UdpSock),
    #coap_message{method = Method2, options = Options2} = Request2,
    ?assertEqual(get, Method2),
    ?assertEqual(<<"/3/0/10">>, get_coap_path(Options2)),
    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {error, unauthorized},
        #coap_content{},
        Request2,
        true
    ),
    timer:sleep(100),

    ReadResult = emqx_utils_json:encode(
        #{
            <<"requestID">> => CmdId,
            <<"cacheID">> => CmdId,
            <<"msgType">> => <<"observe">>,
            <<"data">> => #{
                <<"reqPath">> => <<"/3/0/10">>,
                <<"code">> => <<"4.01">>,
                <<"codeMsg">> => <<"unauthorized">>
            }
        }
    ),
    ?assertEqual(ReadResult, test_recv_mqtt_response(RespTopic)).

case62_cancel_observe_error(Config) ->
    %% step 1, device register ...
    Epn = "urn:oma:lwm2m:oma:62",
    MsgId1 = 15,
    UdpSock = ?config(sock, Config),
    ObjectList = <<"</1>, </3/0>, </5/0>">>,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),

    std_register(UdpSock, Epn, ObjectList, MsgId1, RespTopic),

    %% send a CANCEL-OBSERVE command and reply with error
    CommandTopic = <<"lwm2m/", (list_to_binary(Epn))/binary, "/dn/dm">>,
    CmdId = 610,
    Command = #{
        <<"requestID">> => CmdId,
        <<"cacheID">> => CmdId,
        <<"msgType">> => <<"cancel-observe">>,
        <<"data">> => #{<<"path">> => <<"/3/0/10">>}
    },
    test_mqtt_broker:publish(CommandTopic, emqx_utils_json:encode(Command), 0),
    timer:sleep(50),
    Request2 = test_recv_coap_request(UdpSock),
    #coap_message{method = Method2} = Request2,
    ?assertEqual(get, Method2),
    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {error, not_found},
        #coap_content{},
        Request2,
        true
    ),
    timer:sleep(100),
    ReadResult = emqx_utils_json:encode(
        #{
            <<"requestID">> => CmdId,
            <<"cacheID">> => CmdId,
            <<"msgType">> => <<"cancel-observe">>,
            <<"data">> => #{
                <<"reqPath">> => <<"/3/0/10">>,
                <<"code">> => <<"4.04">>,
                <<"codeMsg">> => <<"not_found">>
            }
        }
    ),
    ?assertEqual(ReadResult, test_recv_mqtt_response(RespTopic)).

case63_observe_notify_object5(Config) ->
    %% step 1, device register ...
    Epn = "urn:oma:lwm2m:oma:63",
    MsgId1 = 15,
    UdpSock = ?config(sock, Config),
    ObjectList = <<"</5/0>">>,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    NotifyTopic = list_to_binary("lwm2m/" ++ Epn ++ "/v1/up/dm"),
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    emqtt:subscribe(?config(emqx_c, Config), NotifyTopic, qos0),
    timer:sleep(200),

    std_register(UdpSock, Epn, ObjectList, MsgId1, RespTopic),

    %% observe firmware update object
    CommandTopic = <<"lwm2m/", (list_to_binary(Epn))/binary, "/dn/dm">>,
    CmdId = 611,
    Command = #{
        <<"requestID">> => CmdId,
        <<"cacheID">> => CmdId,
        <<"msgType">> => <<"observe">>,
        <<"data">> => #{<<"path">> => <<"/5/0/0">>}
    },
    test_mqtt_broker:publish(CommandTopic, emqx_utils_json:encode(Command), 0),
    timer:sleep(50),
    Request2 = test_recv_coap_request(UdpSock),
    #coap_message{method = Method2} = Request2,
    ?assertEqual(get, Method2),
    test_send_coap_observe_ack(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {ok, content},
        #coap_content{content_format = <<"text/plain">>, payload = <<"1">>},
        Request2
    ),
    timer:sleep(100),
    _ = test_recv_mqtt_response(RespTopic),

    %% confirmable notification should use notify topic
    test_send_coap_notif_con(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        #coap_content{content_format = <<"text/plain">>, payload = <<"2">>},
        1,
        Request2
    ),
    timer:sleep(100),
    #coap_message{} = test_recv_coap_response(UdpSock),
    NotifyPayload = emqx_utils_json:decode(test_recv_mqtt_response(NotifyTopic)),
    ?assertEqual(<<"notify">>, maps:get(<<"msgType">>, NotifyPayload)).

case64_coap_reset(Config) ->
    %% step 1, device register ...
    Epn = "urn:oma:lwm2m:oma:64",
    MsgId1 = 15,
    UdpSock = ?config(sock, Config),
    ObjectList = <<"</1>, </3/0>">>,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),

    std_register(UdpSock, Epn, ObjectList, MsgId1, RespTopic),

    CommandTopic = <<"lwm2m/", (list_to_binary(Epn))/binary, "/dn/dm">>,
    CmdId = 612,
    Command = #{
        <<"requestID">> => CmdId,
        <<"cacheID">> => CmdId,
        <<"msgType">> => <<"read">>,
        <<"data">> => #{<<"path">> => <<"/3/0/0">>}
    },
    test_mqtt_broker:publish(CommandTopic, emqx_utils_json:encode(Command), 0),
    timer:sleep(50),
    Request2 = test_recv_coap_request(UdpSock),
    #coap_message{method = Method2} = Request2,
    ?assertEqual(get, Method2),
    test_send_coap_reset(UdpSock, "127.0.0.1", ?PORT, Request2),
    timer:sleep(100),
    ResetPayload = emqx_utils_json:decode(test_recv_mqtt_response(RespTopic)),
    ?assertEqual(<<"coap_reset">>, maps:get(<<"msgType">>, ResetPayload)).

%% case80_specail_object_19_0_0_notify(Config) ->
%%     %% step 1, device register, with extra register options
%%     Epn = "urn:oma:lwm2m:oma:3",
%%     RegOptionWangYi = "&apn=psmA.eDRX0.ctnb&im=13456&ct=2.0&mt=MDM9206&mv=4.0",
%%     MsgId1 = 15,
%%     UdpSock = ?config(sock, Config),

%%     RespTopic = list_to_binary("lwm2m/"++Epn++"/up/resp"),
%%     emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
%%     timer:sleep(200),
%%
%%     test_send_coap_request(
%%       UdpSock,
%%       post,
%%       sprintf("coap://127.0.0.1:~b/rd?ep=~ts&lt=345&lwm2m=1"++RegOptionWangYi, [?PORT, Epn]),
%%       #coap_content{content_format = <<"text/plain">>,
%%                     payload = <<"</1>, </2>, </3>, </4>, </5>">>},
%%       [],
%%       MsgId1),
%%      #coap_message{method = Method1} = test_recv_coap_response(UdpSock),
%%      ?assertEqual({ok,created}, Method1),
%%      ReadResult = emqx_utils_json:encode(
%%                     #{<<"msgType">> => <<"register">>,
%%                       <<"data">> => #{
%%                           <<"alternatePath">> => <<"/">>,
%%                           <<"ep">> => list_to_binary(Epn),
%%                           <<"lt">> => 345,
%%                           <<"lwm2m">> => <<"1">>,
%%                           <<"objectList">> => [<<"/1">>, <<"/2">>, <<"/3">>,
%%                                                <<"/4">>, <<"/5">>],
%%                           <<"apn">> => <<"psmA.eDRX0.ctnb">>,
%%                           <<"im">> => <<"13456">>,
%%                           <<"ct">> => <<"2.0">>,
%%                           <<"mt">> => <<"MDM9206">>,
%%                           <<"mv">> => <<"4.0">>}
%%                      }),
%%      ?assertEqual(ReadResult, test_recv_mqtt_response(RespTopic)),
%%
%%      %% step2,  send a OBSERVE command to device
%%      CommandTopic = <<"lwm2m/", (list_to_binary(Epn))/binary, "/dn/dm">>,
%%      CmdId = 307,
%%      Command = #{<<"requestID">> => CmdId, <<"cacheID">> => CmdId,
%%                  <<"msgType">> => <<"observe">>,
%%                  <<"data">> => #{
%%                                  <<"path">> => <<"/19/0/0">>
%%                                 }
%%                 },
%%      CommandJson = emqx_utils_json:encode(Command),
%%      test_mqtt_broker:publish(CommandTopic, CommandJson, 0),
%%      timer:sleep(50),
%%      Request2 = test_recv_coap_request(UdpSock),
%%      #coap_message{method = Method2,
%%                    options = Options2,
%%                    payload = Payload2} = Request2,
%%      Path2 = get_coap_path(Options2),
%%      Observe = get_coap_observe(Options2),
%%      ?assertEqual(get, Method2),
%%      ?assertEqual(<<"/19/0/0">>, Path2),
%%      ?assertEqual(Observe, 0),
%%      ?assertEqual(<<>>, Payload2),
%%      timer:sleep(50),
%%
%%      test_send_coap_observe_ack(
%%        UdpSock,
%%        "127.0.0.1",
%%        ?PORT,
%%        {ok, content},
%%        #coap_content{content_format = <<"text/plain">>, payload = <<"2048">>},
%%        Request2),
%%      timer:sleep(100).
%%
%% case80_specail_object_19_1_0_write(Config) ->
%%     Epn = "urn:oma:lwm2m:oma:3",
%%     RegOptionWangYi = "&apn=psmA.eDRX0.ctnb&im=13456&ct=2.0&mt=MDM9206&mv=4.0",
%%     MsgId1 = 15,
%%     UdpSock = ?config(sock, Config),
%%     RespTopic = list_to_binary("lwm2m/"++Epn++"/up/resp"),
%%     emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
%%     timer:sleep(200),
%%
%%     test_send_coap_request(
%%       UdpSock,
%%       post,
%%       sprintf("coap://127.0.0.1:~b/rd?ep=~ts&lt=345&lwm2m=1"++RegOptionWangYi, [?PORT, Epn]),
%%       #coap_content{content_format = <<"text/plain">>,
%%                     payload = <<"</1>, </2>, </3>, </4>, </5>">>},
%%       [],
%%       MsgId1),
%%     #coap_message{method = Method1} = test_recv_coap_response(UdpSock),
%%     ?assertEqual({ok,created}, Method1),
%%     test_recv_mqtt_response(RespTopic),
%%
%%     %% step2,  send a WRITE command to device
%%     CommandTopic = <<"lwm2m/", (list_to_binary(Epn))/binary, "/dn/dm">>,
%%     CmdId = 307,
%%     Command = #{<<"requestID">> => CmdId,
%%                 <<"cacheID">> => CmdId,
%%                 <<"msgType">> => <<"write">>,
%%                 <<"data">> => #{
%%                     <<"path">> => <<"/19/1/0">>,
%%                     <<"type">> => <<"Opaque">>,
%%                     <<"value">> => base64:encode(<<12345:32>>)
%%                }},
%%
%%     CommandJson = emqx_utils_json:encode(Command),
%%     test_mqtt_broker:publish(CommandTopic, CommandJson, 0),
%%     timer:sleep(50),
%%     Request2 = test_recv_coap_request(UdpSock),
%%     #coap_message{method = Method2,
%%                   options = Options2,
%%                   payload = Payload2} = Request2,
%%     Path2 = get_coap_path(Options2),
%%     ?assertEqual(put, Method2),
%%     ?assertEqual(<<"/19/1/0">>, Path2),
%%     ?assertEqual(<<3:2, 0:1, 0:2, 4:3, 0, 12345:32>>, Payload2),
%%     timer:sleep(50),
%%
%%     test_send_coap_response(UdpSock, "127.0.0.1", ?PORT,
%%                             {ok, changed}, #coap_content{}, Request2, true),
%%     timer:sleep(100),
%%
%%     ReadResult = emqx_utils_json:encode(
%%                    #{<<"requestID">> => CmdId,
%%                      <<"cacheID">> => CmdId,
%%                      <<"data">> => #{
%%                         <<"reqPath">> => <<"/19/1/0">>,
%%                         <<"code">> => <<"2.04">>,
%%                         <<"codeMsg">> => <<"changed">>},
%%                      <<"msgType">> => <<"write">>
%%                    }),
%%     ?assertEqual(ReadResult, test_recv_mqtt_response(RespTopic)).

case90_psm_mode(Config) ->
    server_cache_mode(Config, "ep=~ts&lt=345&lwm2m=1&apn=psmA.eDRX0.ctnb").

case90_queue_mode(Config) ->
    server_cache_mode(Config, "ep=~ts&lt=345&lwm2m=1&b=UQ").

server_cache_mode(Config, RegOption) ->
    #{lwm2m := LwM2M} = Gateway = emqx:get_config([gateway]),
    Gateway2 = Gateway#{lwm2m := LwM2M#{qmode_time_window => 2}},
    emqx_config:put([gateway], Gateway2),
    %% step 1, device register, with apn indicates "PSM" mode
    Epn = "urn:oma:lwm2m:oma:3",

    MsgId1 = 15,
    UdpSock = ?config(sock, Config),
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),

    test_send_coap_request(
        UdpSock,
        post,
        sprintf("coap://127.0.0.1:~b/rd?" ++ RegOption, [?PORT, Epn]),
        #coap_content{
            content_format = <<"text/plain">>,
            payload = <<"</1>, </2>, </3>, </4>, </5>">>
        },
        [],
        MsgId1
    ),
    #coap_message{
        type = ack,
        method = Method1,
        options = Opts
    } = test_recv_coap_response(UdpSock),
    ?assertEqual({ok, created}, Method1),
    ?LOGT("Options got: ~p", [Opts]),
    Location = maps:get(location_path, Opts),
    test_recv_mqtt_response(RespTopic),

    %% server not in PSM mode
    send_read_command_1(0, UdpSock),
    verify_read_response_1(0, UdpSock),

    %% server inters into PSM mode
    timer:sleep(2500),

    %% verify server caches downlink commands
    send_read_command_1(1, UdpSock),
    send_read_command_1(2, UdpSock),
    send_read_command_1(3, UdpSock),

    ?assertEqual(
        timeout_test_recv_coap_request,
        test_recv_coap_request(UdpSock)
    ),

    device_update_1(UdpSock, Location),

    verify_read_response_1(1, UdpSock),
    verify_read_response_1(2, UdpSock),
    verify_read_response_1(3, UdpSock).

case100_clients_api(Config) ->
    Epn = "urn:oma:lwm2m:oma:3",
    MsgId1 = 15,
    UdpSock = ?config(sock, Config),
    ObjectList = <<"</1>, </2>, </3/0>, </4>, </5>">>,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    std_register(UdpSock, Epn, ObjectList, MsgId1, RespTopic),

    %% list
    {200, #{data := ClientList}} = request(get, "/gateways/lwm2m/clients"),
    %% searching
    {200, #{data := [Client2]}} =
        request(
            get,
            "/gateways/lwm2m/clients",
            [{<<"endpoint_name">>, list_to_binary(Epn)}]
        ),
    {200, #{data := [Client3]}} =
        request(
            get,
            "/gateways/lwm2m/clients",
            [
                {<<"like_endpoint_name">>, list_to_binary(Epn)},
                {<<"gte_lifetime">>, <<"1">>}
            ]
        ),
    %% lookup
    ClientId = maps:get(clientid, Client2),
    {200, Client4} =
        request(get, "/gateways/lwm2m/clients/" ++ binary_to_list(ClientId)),
    %% assert
    true = lists:member(Client2, ClientList),
    Client2 = Client3 = Client4,
    %% assert keepalive
    ?assertEqual(345, maps:get(keepalive, Client4)),
    %% kickout
    {204, _} =
        request(delete, "/gateways/lwm2m/clients/" ++ binary_to_list(ClientId)),
    timer:sleep(100),
    {200, #{data := ClientList2}} = request(get, "/gateways/lwm2m/clients"),
    ?assertEqual(
        false,
        lists:any(
            fun(Client) ->
                maps:get(clientid, Client, undefined) =:= ClientId
            end,
            ClientList2
        )
    ).

case100_subscription_api(Config) ->
    Epn = "urn:oma:lwm2m:oma:3",
    MsgId1 = 15,
    UdpSock = ?config(sock, Config),
    ObjectList = <<"</1>, </2>, </3/0>, </4>, </5>">>,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    std_register(UdpSock, Epn, ObjectList, MsgId1, RespTopic),

    {200, #{data := [Client1]}} =
        request(
            get,
            "/gateways/lwm2m/clients",
            [{<<"endpoint_name">>, list_to_binary(Epn)}]
        ),
    ClientId = maps:get(clientid, Client1),
    Path =
        "/gateways/lwm2m/clients/" ++
            binary_to_list(ClientId) ++
            "/subscriptions",

    %% list
    {200, [InitSub]} = request(get, Path),
    ?assertEqual(
        <<"lwm2m/", (list_to_binary(Epn))/binary, "/dn/#">>,
        maps:get(topic, InitSub)
    ),

    %% create
    SubReq = #{
        topic => <<"tx">>,
        qos => 1,
        nl => 0,
        rap => 0,
        rh => 0
    },
    {201, _} = request(post, Path, SubReq),
    {200, _} = request(get, Path),

    %% check subscription_cnt
    {200, #{subscriptions_cnt := 2}} = request(
        get, "/gateways/lwm2m/clients/" ++ binary_to_list(ClientId)
    ),

    {204, _} = request(delete, Path ++ "/tx"),
    {200, [InitSub]} = request(get, Path).

case110_xml_object_helpers(_Config) ->
    ObjDefinition = emqx_lwm2m_xml_object:get_obj_def(3, true),
    ?assert(is_tuple(ObjDefinition)),
    ?assertEqual("3", emqx_lwm2m_xml_object:get_object_id(ObjDefinition)),
    ?assertEqual("Device", emqx_lwm2m_xml_object:get_object_name(ObjDefinition)),
    {ObjId, ResId} =
        emqx_lwm2m_xml_object:get_object_and_resource_id(<<"Manufacturer">>, ObjDefinition),
    ?assertEqual("3", ObjId),
    ?assertEqual("0", ResId),
    ?assertEqual("Manufacturer", emqx_lwm2m_xml_object:get_resource_name(0, ObjDefinition)),
    ?assertEqual("R", emqx_lwm2m_xml_object:get_resource_operations(0, ObjDefinition)),
    ?assert(is_tuple(emqx_lwm2m_xml_object:get_obj_def("Device", false))),
    ?assert(is_tuple(emqx_lwm2m_xml_object_db:find_objectid("3"))),
    ?assert(is_tuple(emqx_lwm2m_xml_object_db:find_name("Device"))),
    ?assert(is_tuple(emqx_lwm2m_xml_object_db:find_name(<<"Device">>))),
    ?assertEqual({error, no_xml_definition}, emqx_lwm2m_xml_object_db:find_objectid(999)),
    ?assertEqual({error, no_xml_definition}, emqx_lwm2m_xml_object_db:find_name("Missing")),
    ?assertEqual(undefined, emqx_lwm2m_schema:desc(unknown)),
    ?assertEqual(ignored, gen_server:call(emqx_lwm2m_xml_object_db, ping)),
    ok = gen_server:cast(emqx_lwm2m_xml_object_db, ping),
    emqx_lwm2m_xml_object_db ! ping,
    ?assertEqual({ok, ok_state}, emqx_lwm2m_xml_object_db:code_change(v1, ok_state, extra)),
    timer:sleep(50).

case111_gateway_error_paths(Config) ->
    _ = Config,
    _ = catch emqx_lwm2m_xml_object_db:stop(),
    case emqx_gateway:unload(lwm2m) of
        ok -> ok;
        {error, not_found} -> ok
    end,
    Conf = emqx:get_config([gateway, lwm2m]),
    BadConf = Conf#{xml_dir => "/no/such/dir"},
    ?assertMatch({error, _}, emqx_gateway:load(lwm2m, BadConf)),
    {ok, PortSock} = gen_udp:open(0, [binary, {active, false}]),
    {ok, {_, BusyPort}} = inet:sockname(PortSock),
    Listeners = maps:get(listeners, Conf),
    UdpListeners = maps:get(udp, Listeners),
    DefaultListener = maps:get(default, UdpListeners),
    BusyListener = DefaultListener#{bind => BusyPort},
    BusyLoadConf = Conf#{listeners => Listeners#{udp => UdpListeners#{default => BusyListener}}},
    ?assertMatch({error, _}, emqx_gateway:load(lwm2m, BusyLoadConf)),
    ok = emqx_conf_cli:load_config(?global_ns, default_config(), #{mode => replace}),
    ensure_gateway_loaded(),
    BusyUpdateListeners = UdpListeners#{extra => BusyListener},
    BusyUpdateConf = Conf#{listeners => Listeners#{udp => BusyUpdateListeners}},
    ?assertMatch({error, _}, emqx_gateway:update(lwm2m, BusyUpdateConf)),
    gen_udp:close(PortSock),
    ok = emqx_conf_cli:load_config(?global_ns, default_config(), #{mode => replace}).

case112_channel_unexpected_messages(Config) ->
    %% step 1, device register ...
    Epn = "urn:oma:lwm2m:oma:112",
    MsgId1 = 15,
    UdpSock = ?config(sock, Config),
    ObjectList = <<"</1>, </3/0>">>,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),
    std_register(UdpSock, Epn, ObjectList, MsgId1, RespTopic),
    [Channel | _] = emqx_gateway_cm_registry:lookup_channels(lwm2m, list_to_binary(Epn)),
    ?assertEqual(ignored, gen_server:call(Channel, unexpected_call)),
    ok = gen_server:cast(Channel, unexpected_cast),
    Channel ! {subscribe, #{topic => <<"unexpected">>}},
    Channel ! unexpected_info,
    timer:sleep(100).

case113_request_invalid_paths(Config) ->
    UdpSock = ?config(sock, Config),
    %% invalid POST path
    MsgId = 90,
    test_send_coap_request(
        UdpSock,
        post,
        sprintf("coap://127.0.0.1:~b/bad", [?PORT]),
        #coap_content{content_format = <<"text/plain">>, payload = <<>>},
        [],
        MsgId
    ),
    #coap_message{method = Method} = test_recv_coap_response(UdpSock),
    ?assertEqual({error, not_found}, Method),

    %% register for update/delete checks
    Epn = "urn:oma:lwm2m:oma:113",
    MsgId1 = 15,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    test_send_coap_request(
        UdpSock,
        post,
        sprintf("coap://127.0.0.1:~b/rd?ep=~ts&lt=345&lwm2m=1", [?PORT, Epn]),
        #coap_content{content_format = <<"text/plain">>, payload = <<"</3/0>">>},
        [],
        MsgId1
    ),
    #coap_message{method = {ok, created}} = test_recv_coap_response(UdpSock),
    _ = test_recv_mqtt_response(RespTopic),
    timer:sleep(100),

    %% update wrong location
    MsgId2 = 91,
    test_send_coap_request(
        UdpSock,
        post,
        sprintf("coap://127.0.0.1:~b/rd/bad", [?PORT]),
        #coap_content{content_format = <<"text/plain">>, payload = <<>>},
        [],
        MsgId2
    ),
    #coap_message{method = Method2} = test_recv_coap_response(UdpSock),
    ?assertEqual({error, not_found}, Method2),

    %% delete wrong location
    MsgId3 = 92,
    test_send_coap_request(
        UdpSock,
        delete,
        sprintf("coap://127.0.0.1:~b/rd/bad", [?PORT]),
        #coap_content{payload = <<>>},
        [],
        MsgId3
    ),
    #coap_message{method = Method3} = test_recv_coap_response(UdpSock),
    ?assertEqual({error, not_found}, Method3).

case114_execute_args_undefined(Config) ->
    %% step 1, device register ...
    Epn = "urn:oma:lwm2m:oma:114",
    MsgId1 = 15,
    UdpSock = ?config(sock, Config),
    ObjectList = <<"</3/0>">>,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),
    std_register(UdpSock, Epn, ObjectList, MsgId1, RespTopic),
    [Channel | _] = emqx_gateway_cm_registry:lookup_channels(lwm2m, list_to_binary(Epn)),
    CmdId = 700,
    Cmd = #{
        <<"requestID">> => CmdId,
        <<"cacheID">> => CmdId,
        <<"msgType">> => <<"execute">>,
        <<"data">> => #{<<"path">> => <<"/3/0/4">>, <<"args">> => undefined}
    },
    ok = emqx_lwm2m_channel:send_cmd(Channel, Cmd),
    Request2 = test_recv_coap_request(UdpSock),
    #coap_message{method = Method2, payload = Payload2} = Request2,
    ?assertEqual(post, Method2),
    ?assertEqual(<<>>, Payload2),
    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {ok, changed},
        #coap_content{},
        Request2,
        true
    ),
    timer:sleep(100),
    ExecuteResult = emqx_utils_json:encode(
        #{
            <<"requestID">> => CmdId,
            <<"cacheID">> => CmdId,
            <<"data">> => #{
                <<"reqPath">> => <<"/3/0/4">>,
                <<"code">> => <<"2.04">>,
                <<"codeMsg">> => <<"changed">>
            },
            <<"msgType">> => <<"execute">>
        }
    ),
    ?assertEqual(ExecuteResult, test_recv_mqtt_response(RespTopic)).

case115_cmd_record_overflow(Config) ->
    %% step 1, device register ...
    Epn = "urn:oma:lwm2m:oma:115",
    MsgId1 = 15,
    UdpSock = ?config(sock, Config),
    ObjectList = <<"</3/0>">>,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),

    std_register(UdpSock, Epn, ObjectList, MsgId1, RespTopic),
    [Channel | _] = emqx_gateway_cm_registry:lookup_channels(lwm2m, list_to_binary(Epn)),
    lists:foreach(
        fun(I) ->
            CmdId = 9000 + I,
            Path = <<"/3/0/", (integer_to_binary(I))/binary>>,
            Cmd = #{
                <<"requestID">> => CmdId,
                <<"cacheID">> => CmdId,
                <<"msgType">> => <<"read">>,
                <<"data">> => #{<<"path">> => Path}
            },
            ok = emqx_lwm2m_channel:send_cmd(Channel, Cmd)
        end,
        lists:seq(1, 101)
    ),
    timer:sleep(100),
    {ok, Result} = gen_server:call(Channel, {lookup_cmd, <<"/3/0/1">>, <<"read">>}),
    ?assertEqual(undefined, Result).

case116_command_headers(Config) ->
    %% step 1, device register ...
    Epn = "urn:oma:lwm2m:oma:116",
    MsgId1 = 15,
    UdpSock = ?config(sock, Config),
    ObjectList = <<"</1>, </2>, </3/0>, </4>, </5>">>,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    CommandTopic = <<"lwm2m/", (list_to_binary(Epn))/binary, "/dn/dm">>,
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),

    std_register(UdpSock, Epn, ObjectList, MsgId1, RespTopic),

    CmdId = 800,
    Command = #{
        <<"requestID">> => CmdId,
        <<"cacheID">> => CmdId,
        <<"msgType">> => <<"read">>,
        <<"mheaders">> => #{<<"ttl">> => 0},
        <<"data">> => #{<<"path">> => <<"/3/0/0">>}
    },
    Payload = emqx_utils_json:encode(Command),
    Headers = #{properties => #{'Message-Expiry-Interval' => 1}},
    Msg = emqx_message:make(<<"lwm2m_test_suite">>, 0, CommandTopic, Payload, #{}, Headers),
    emqx:publish(Msg),
    timer:sleep(50),
    Request2 = test_recv_coap_request(UdpSock),
    ?assertEqual(get, Request2#coap_message.method),
    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {ok, content},
        #coap_content{content_format = <<"text/plain">>, payload = <<"EMQ">>},
        Request2,
        true
    ),
    Response = emqx_utils_json:decode(test_recv_mqtt_response(RespTopic)),
    Data = maps:get(<<"data">>, Response),
    ?assertEqual(<<"/3/0/0">>, maps:get(<<"reqPath">>, Data)).

case116_command_headers_invalid_mheaders(Config) ->
    %% step 1, device register ...
    Epn = "urn:oma:lwm2m:oma:1161",
    MsgId1 = 15,
    UdpSock = ?config(sock, Config),
    ObjectList = <<"</1>, </2>, </3/0>, </4>, </5>">>,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    CommandTopic = <<"lwm2m/", (list_to_binary(Epn))/binary, "/dn/dm">>,
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),

    std_register(UdpSock, Epn, ObjectList, MsgId1, RespTopic),

    CmdId = 801,
    Command = #{
        <<"requestID">> => CmdId,
        <<"cacheID">> => CmdId,
        <<"msgType">> => <<"read">>,
        <<"mheaders">> => <<"invalid">>,
        <<"data">> => #{<<"path">> => <<"/3/0/0">>}
    },
    Payload = emqx_utils_json:encode(Command),
    Msg = emqx_message:make(<<"lwm2m_test_suite">>, 0, CommandTopic, Payload, #{}, #{}),
    emqx:publish(Msg),
    timer:sleep(50),
    Request2 = test_recv_coap_request(UdpSock),
    ?assertEqual(get, Request2#coap_message.method),
    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {ok, content},
        #coap_content{content_format = <<"text/plain">>, payload = <<"EMQ">>},
        Request2,
        true
    ),
    Response = emqx_utils_json:decode(test_recv_mqtt_response(RespTopic)),
    Data = maps:get(<<"data">>, Response),
    ?assertEqual(<<"/3/0/0">>, maps:get(<<"reqPath">>, Data)).

case117_auth_failure(Config) ->
    ok = meck:new(emqx_gateway_ctx, [passthrough, no_history]),
    ok = meck:expect(emqx_gateway_ctx, authenticate, fun(_, _) -> {error, unauthorized} end),
    try
        UdpSock = ?config(sock, Config),
        Epn = "urn:oma:lwm2m:oma:117",
        MsgId = 12,
        test_send_coap_request(
            UdpSock,
            post,
            sprintf("coap://127.0.0.1:~b/rd?ep=~ts&lt=345&lwm2m=1", [?PORT, Epn]),
            #coap_content{
                content_format = <<"text/plain">>,
                payload = <<"</1>, </2>, </3>, </4>, </5>">>
            },
            [],
            MsgId
        ),
        #coap_message{method = Method} = test_recv_coap_response(UdpSock),
        ?assertEqual({error, bad_request}, Method)
    after
        meck:unload(emqx_gateway_ctx)
    end.

case118_authorize_denied(Config) ->
    ok = meck:new(emqx_gateway_ctx, [passthrough, no_history]),
    ok = meck:expect(
        emqx_gateway_ctx,
        authenticate,
        fun(_, ClientInfo) -> {ok, ClientInfo#{auth_expire_at => undefined}} end
    ),
    ok = meck:expect(emqx_gateway_ctx, authorize, fun(_, _, _, _) -> deny end),
    try
        UdpSock = ?config(sock, Config),
        Epn = "urn:oma:lwm2m:oma:118",
        MsgId = 12,
        SubTopic = list_to_binary("lwm2m/" ++ Epn ++ "/dn/#"),
        test_send_coap_request(
            UdpSock,
            post,
            sprintf("coap://127.0.0.1:~b/rd?ep=~ts&lt=345&lwm2m=1", [?PORT, Epn]),
            #coap_content{
                content_format = <<"text/plain">>,
                payload = <<"</1>, </2>, </3>, </4>, </5>">>
            },
            [],
            MsgId
        ),
        #coap_message{method = Method} = test_recv_coap_response(UdpSock),
        ?assertEqual({ok, created}, Method),
        timer:sleep(50),
        ?assertEqual(false, lists:member(SubTopic, test_mqtt_broker:get_subscrbied_topics()))
    after
        meck:unload(emqx_gateway_ctx)
    end.

case119_open_session_error(Config) ->
    ok = meck:new(emqx_gateway_ctx, [passthrough, no_history]),
    ok = meck:expect(emqx_gateway_ctx, open_session, fun(_, _, _, _, _, _) ->
        {error, no_session}
    end),
    try
        UdpSock = ?config(sock, Config),
        Epn = "urn:oma:lwm2m:oma:119",
        MsgId = 12,
        test_send_coap_request(
            UdpSock,
            post,
            sprintf("coap://127.0.0.1:~b/rd?ep=~ts&lt=345&lwm2m=1", [?PORT, Epn]),
            #coap_content{
                content_format = <<"text/plain">>,
                payload = <<"</1>, </2>, </3>, </4>, </5>">>
            },
            [],
            MsgId
        ),
        #coap_message{method = Method} = test_recv_coap_response(UdpSock),
        ?assertEqual({error, bad_request}, Method)
    after
        meck:unload(emqx_gateway_ctx)
    end.

case120_post_missing_uri_path(Config) ->
    UdpSock = ?config(sock, Config),
    MsgId = 120,
    Request0 = emqx_coap_message:request(con, post, <<>>, [{content_format, <<"text/plain">>}]),
    Request = Request0#coap_message{id = MsgId},
    Packet = emqx_coap_frame:serialize_pkt(Request, undefined),
    ok = gen_udp:send(UdpSock, {127, 0, 0, 1}, ?PORT, Packet),
    #coap_message{method = Method} = test_recv_coap_response(UdpSock),
    ?assertEqual({error, not_found}, Method).

case121_delete_missing_uri_path(Config) ->
    UdpSock = ?config(sock, Config),
    MsgId = 121,
    Request0 = emqx_coap_message:request(con, delete, <<>>, []),
    Request = Request0#coap_message{id = MsgId},
    Packet = emqx_coap_frame:serialize_pkt(Request, undefined),
    ok = gen_udp:send(UdpSock, {127, 0, 0, 1}, ?PORT, Packet),
    #coap_message{method = Method} = test_recv_coap_response(UdpSock),
    ?assertEqual({error, bad_request}, Method).

case122_connect_hook_error(Config) ->
    ok = meck:new(emqx_hooks, [passthrough, no_history]),
    ok = meck:expect(emqx_hooks, run_fold, fun(_, _, _) -> {error, hook_failed} end),
    try
        UdpSock = ?config(sock, Config),
        Epn = "urn:oma:lwm2m:oma:122",
        MsgId = 12,
        test_send_coap_request(
            UdpSock,
            post,
            sprintf("coap://127.0.0.1:~b/rd?ep=~ts&lt=345&lwm2m=1", [?PORT, Epn]),
            #coap_content{
                content_format = <<"text/plain">>,
                payload = <<"</1>, </2>, </3>, </4>, </5>">>
            },
            [],
            MsgId
        ),
        #coap_message{method = Method} = test_recv_coap_response(UdpSock),
        ?assertEqual({error, bad_request}, Method)
    after
        meck:unload(emqx_hooks)
    end.

case123_tlv_internal(_Config) ->
    LargeBinary = binary:copy(<<0>>, 65536),
    TlvList = [
        #{tlv_resource_with_value => 1, value => "abc"},
        #{tlv_resource_with_value => 2, value => true},
        #{tlv_resource_with_value => 3, value => false},
        #{tlv_resource_with_value => 4, value => 10},
        #{tlv_resource_with_value => 5, value => 1000},
        #{tlv_resource_with_value => 6, value => 70000},
        #{tlv_resource_with_value => 7, value => 3.14},
        #{tlv_resource_with_value => 8, value => LargeBinary}
    ],
    Encoded = emqx_lwm2m_tlv:encode(TlvList),
    ?assert(is_binary(Encoded)),
    Tlv24 = <<3:2, 0:1, 3:2, 0:3, 1:8, 1:24, 0>>,
    ?assertMatch(
        [#{tlv_resource_with_value := 1, value := <<0>>}],
        emqx_lwm2m_tlv:parse(Tlv24)
    ),
    ?assertException(
        error,
        _,
        emqx_lwm2m_tlv:encode([#{tlv_resource_with_value => 9, value => #{}}])
    ),
    ?assertEqual("FF ", emqx_lwm2m_tlv:binary_to_hex_string(<<255>>)).

case124_cmd_code_internal(_Config) ->
    Ref = #{<<"msgType">> => <<"create">>, <<"path">> => <<"/3/0/1">>},
    lists:foreach(
        fun(Code) ->
            Resp = emqx_lwm2m_cmd:coap_to_mqtt({ok, Code}, <<>>, #{}, Ref),
            Data = maps:get(<<"data">>, Resp),
            ?assertEqual(<<"/3/0/1">>, maps:get(<<"reqPath">>, Data)),
            ?assert(is_binary(maps:get(<<"code">>, Data)))
        end,
        [get, post, put, delete, valid, continue]
    ).

case125_message_internal_errors(_Config) ->
    TlvBin = emqx_lwm2m_tlv:encode([#{tlv_resource_with_value => 0, value => <<"1">>}]),
    ?assertException(error, _, emqx_lwm2m_message:tlv_to_json(<<"/3">>, TlvBin)),
    ?assertException(
        error,
        {invalid_basename, <<"/3/0">>},
        emqx_lwm2m_message:text_to_json(<<"/3/0">>, <<"abc">>)
    ).

case126_message_insert_resource(_Config) ->
    Result = emqx_lwm2m_message:insert(
        resource,
        #{<<"path">> => <<"/0">>, <<"type">> => <<"String">>, <<"value">> => <<"v">>},
        []
    ),
    ?assertMatch([#{tlv_resource_instance := 0, value := <<"v">>}], Result).

case127_channel_internal_branches(_Config) ->
    ok = meck:new(esockd_peercert, [passthrough, no_history]),
    ok = meck:expect(esockd_peercert, subject, fun(_) -> <<"DN">> end),
    ok = meck:expect(esockd_peercert, common_name, fun(_) -> <<"CN">> end),
    try
        CmPid = whereis(emqx_gateway_lwm2m_cm),
        ?assert(is_pid(CmPid)),
        Ctx = #{gwname => lwm2m, cm => CmPid},
        ConnInfo = #{
            peername => {{127, 0, 0, 1}, 56830},
            sockname => {{127, 0, 0, 1}, 56830},
            peercert => dummy_cert
        },
        Channel = emqx_lwm2m_channel:init(ConnInfo, #{ctx => Ctx}),
        ?assertMatch(
            {shutdown, test_error, _}, emqx_lwm2m_channel:handle_frame_error(test_error, Channel)
        ),
        Msg = emqx_coap_message:request(con, get, <<>>, []),
        TimeoutMsg = {timeout_seq, timeout, Msg},
        ?assertMatch(
            {ok, _},
            emqx_lwm2m_channel:handle_timeout(undefined, {transport, TimeoutMsg}, Channel)
        ),
        ?assertMatch(
            {shutdown, normal, _}, emqx_lwm2m_channel:handle_timeout(undefined, disconnect, Channel)
        ),
        ?assertMatch({ok, _, _}, emqx_lwm2m_channel:do_takeover(<<"id">>, Msg, Channel)),
        MsgOk = #coap_message{
            options = #{uri_query => #{<<"ep">> => <<"ep">>, <<"lt">> => <<"60">>}}
        },
        ?assertMatch({ok, _}, emqx_lwm2m_channel:enrich_clientinfo(MsgOk, Channel)),
        MsgBad = #coap_message{options = #{uri_query => #{<<"ep">> => <<"ep">>}}},
        ?assertMatch(
            {error, "invalid queries", _}, emqx_lwm2m_channel:enrich_clientinfo(MsgBad, Channel)
        ),
        Reply = emqx_coap_message:reset(Msg),
        {ok, [{outgoing, Outs} | _], _} =
            emqx_lwm2m_channel:process_out([Msg], #{reply => Reply}, Channel, ignored),
        ?assert(lists:member(Reply, Outs)),
        ?assertMatch({ok, _}, emqx_lwm2m_channel:process_nothing(undefined, #{}, Channel))
    after
        meck:unload(esockd_peercert)
    end.

case128_session_internal_branches(_Config) ->
    ok = emqx_conf_cli:load_config(?global_ns, default_config(#{auto_observe => true}), #{
        mode => replace
    }),
    WithContext = with_context_stub(),
    Session0 = emqx_lwm2m_session:new(),
    AltPayload = <<"</alt>;rt=\"oma.lwm2m\"">>,
    QueryWithLt = #{<<"ep">> => <<"ep128">>, <<"lt">> => <<"60">>, <<"b">> => <<"UQ">>},
    RegMsg = #coap_message{options = #{uri_query => QueryWithLt}, payload = AltPayload},
    InitResult = emqx_lwm2m_session:init(RegMsg, <<>>, WithContext, Session0),
    Session1 = session_from_result(InitResult),
    _ = emqx_lwm2m_session:timeout(
        {transport, {timeout_seq, timeout, RegMsg}}, undefined, Session1
    ),
    ?assertEqual({<<"/">>, <<>>}, emqx_lwm2m_session:parse_object_list(<<>>)),
    ?assertException(throw, _, emqx_lwm2m_session:parse_object_list(<<"</alt>;=bad">>)),
    RegMsgNoLt = #coap_message{
        options = #{uri_query => #{<<"ep">> => <<"ep128_nolt">>}}, payload = <<>>
    },
    InitNoLtResult = emqx_lwm2m_session:init(RegMsgNoLt, <<>>, WithContext, Session0),
    SessionNoLt = session_from_result(InitNoLtResult),
    Cmd1 = #{<<"msgType">> => <<"read">>, <<"data">> => #{<<"path">> => <<"/3/0/0">>}},
    SendResult = emqx_lwm2m_session:send_cmd(Cmd1, undefined, Session1),
    Session2 = session_from_result(SendResult),
    Session2Cache = setelement(11, Session2, 0),
    UpdateMsg = #coap_message{options = #{uri_query => #{}}, payload = <<>>},
    _ = emqx_lwm2m_session:update(UpdateMsg, WithContext, Session2),
    _ = emqx_lwm2m_session:update(UpdateMsg, WithContext, SessionNoLt),
    Cmd2 = #{<<"msgType">> => <<"read">>, <<"data">> => #{<<"path">> => <<"/3/0/1">>}},
    _ = emqx_lwm2m_session:handle_protocol_in(
        {ack_failure, {Cmd1, RegMsg}}, WithContext, Session2Cache
    ),
    _ = emqx_lwm2m_session:handle_protocol_in(
        {ack_failure, {Cmd2, RegMsg}}, WithContext, Session2Cache
    ),
    _ = emqx_lwm2m_session:handle_protocol_in({ack, {Cmd2, RegMsg}}, WithContext, Session2),
    TermData = #{
        <<"msgType">> => <<"read">>,
        <<"data">> => #{<<"path">> => <<"/3/0/0">>},
        mheaders => #{ttl => 1}
    },
    Headers = #{properties => #{'Message-Expiry-Interval' => 0}},
    Msg0 = emqx_message:make(<<"lwm2m_internal">>, 0, <<"topic">>, <<>>, #{}, Headers),
    Msg = Msg0#message{payload = TermData, timestamp = 1},
    DeliverResult =
        emqx_lwm2m_session:handle_deliver([{deliver, <<"topic">>, Msg}], WithContext, Session2),
    Session3 = session_from_result(DeliverResult),
    _ = emqx_lwm2m_session:handle_protocol_in({ack, {Cmd1, RegMsg}}, WithContext, Session3),
    ok.

case129_write_hex_encoding(_Config) ->
    InputCmd = #{
        <<"msgType">> => <<"write">>,
        <<"encoding">> => <<"hex">>,
        <<"data">> => #{
            <<"path">> => <<"/3/0/1">>,
            <<"type">> => <<"String">>,
            <<"value">> => <<"48656C6C6F">>
        }
    },
    {Req, _Ctx} = emqx_lwm2m_cmd:mqtt_to_coap(<<"/">>, InputCmd),
    #coap_message{payload = Payload} = Req,
    HexBin = emqx_utils:hexstr_to_bin(<<"48656C6C6F">>),
    Data1 = #{
        <<"path">> => <<"/3/0/1">>,
        <<"type">> => <<"String">>,
        <<"value">> => HexBin
    },
    {PathList, _QueryList} = emqx_lwm2m_cmd:path_list(<<"/3/0/1">>),
    TlvData = emqx_lwm2m_message:json_to_tlv(PathList, [Data1]),
    Expected = emqx_lwm2m_tlv:encode(TlvData),
    ?assertEqual(Expected, Payload).

case130_auto_observe_list_config(_Config) ->
    RegInfo = #{<<"objectList">> => [<<"/1/0">>]},
    ListRaw = "[\"/3/0\",\"/3/0/1\"]",
    ok = emqx_conf_cli:load_config(
        ?global_ns, default_config_with_auto_observe_raw(ListRaw), #{mode => replace}
    ),
    ?assertEqual(
        [<<"/3/0">>, <<"/3/0/1">>],
        emqx_lwm2m_session:auto_observe_object_list(RegInfo)
    ),
    OnRaw = "\"on\"",
    ok = emqx_conf_cli:load_config(
        ?global_ns, default_config_with_auto_observe_raw(OnRaw), #{mode => replace}
    ),
    ?assertEqual(
        [<<"/1/0">>],
        emqx_lwm2m_session:auto_observe_object_list(RegInfo)
    ),
    OffRaw = "\"off\"",
    ok = emqx_conf_cli:load_config(
        ?global_ns, default_config_with_auto_observe_raw(OffRaw), #{mode => replace}
    ),
    ?assertEqual([], emqx_lwm2m_session:auto_observe_object_list(RegInfo)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Internal Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

send_read_command_1(CmdId, _UdpSock) ->
    Epn = "urn:oma:lwm2m:oma:3",
    CommandTopic = <<"lwm2m/", (list_to_binary(Epn))/binary, "/dn/dm">>,
    Command = #{
        <<"requestID">> => CmdId,
        <<"cacheID">> => CmdId,
        <<"msgType">> => <<"read">>,
        <<"data">> => #{<<"path">> => <<"/3/0/0">>}
    },
    CommandJson = emqx_utils_json:encode(Command),
    test_mqtt_broker:publish(CommandTopic, CommandJson, 0),
    timer:sleep(50).

verify_read_response_1(CmdId, UdpSock) ->
    Epn = "urn:oma:lwm2m:oma:3",
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),

    %% device receives a command
    Request = test_recv_coap_request(UdpSock),
    ?LOGT("LwM2M client got ~p", [Request]),

    %% device replies the command
    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {ok, content},
        #coap_content{
            content_format = <<"text/plain">>,
            payload = <<"EMQ">>
        },
        Request,
        true
    ),

    ReadResult = emqx_utils_json:encode(
        #{
            <<"requestID">> => CmdId,
            <<"cacheID">> => CmdId,
            <<"msgType">> => <<"read">>,
            <<"data">> => #{
                <<"reqPath">> => <<"/3/0/0">>,
                <<"code">> => <<"2.05">>,
                <<"codeMsg">> => <<"content">>,
                <<"content">> =>
                    [
                        #{
                            path => <<"/3/0/0">>,
                            value => <<"EMQ">>
                        }
                    ]
            }
        }
    ),
    ?assertEqual(ReadResult, test_recv_mqtt_response(RespTopic)).

device_update_1(UdpSock, Location) ->
    Epn = "urn:oma:lwm2m:oma:3",
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    ?LOGT("send UPDATE command", []),
    MsgId2 = 27,
    test_send_coap_request(
        UdpSock,
        post,
        sprintf("coap://127.0.0.1:~b~ts?lt=789", [?PORT, join_path(Location, <<>>)]),
        #coap_content{payload = <<>>},
        [],
        MsgId2
    ),
    #coap_message{
        type = ack,
        id = MsgId2,
        method = Method2
    } = test_recv_coap_response(UdpSock),
    {ok, changed} = Method2,
    test_recv_mqtt_response(RespTopic).

test_recv_mqtt_response(RespTopic) ->
    receive
        {publish, #{topic := RespTopic, payload := RM}} ->
            ?LOGT("test_recv_mqtt_response Response=~p", [RM]),
            RM
    after 1000 -> timeout_test_recv_mqtt_response
    end.

test_send_coap_request(UdpSock, Method, Uri, Content, Options, MsgId) ->
    is_record(Content, coap_content) orelse
        error("Content must be a #coap_content!"),
    is_list(Options) orelse
        error("Options must be a list"),
    case resolve_uri(Uri) of
        {coap, {IpAddr, Port}, Path, Query} ->
            Request0 = request(
                con,
                Method,
                Content,
                [{uri_path, Path}, {uri_query, Query} | Options]
            ),
            Request = Request0#coap_message{id = MsgId},
            ?LOGT("send_coap_request Request=~p", [Request]),

            RequestBinary = emqx_coap_frame:serialize_pkt(Request, undefined),
            ?LOGT(
                "test udp socket send to ~p:~p, data=~p",
                [IpAddr, Port, RequestBinary]
            ),
            ok = gen_udp:send(UdpSock, IpAddr, Port, RequestBinary);
        {SchemeDiff, ChIdDiff, _, _} ->
            error(
                lists:flatten(
                    io_lib:format(
                        "scheme ~ts or ChId ~ts does not match with socket",
                        [SchemeDiff, ChIdDiff]
                    )
                )
            )
    end.

test_recv_coap_response(UdpSock) ->
    {ok, {Address, Port, Packet}} = gen_udp:recv(UdpSock, 0, 5000),
    {ok, Response, _, _} = emqx_coap_frame:parse(Packet, undefined),
    ?LOGT(
        "test udp receive from ~p:~p, data1=~p, Response=~p",
        [Address, Port, Packet, Response]
    ),
    #coap_message{
        type = ack,
        method = Method,
        id = Id,
        token = Token,
        options = Options,
        payload = Payload
    } = Response,
    ?LOGT(
        "receive coap response Method=~p, Id=~p, Token=~p, "
        "Options=~p, Payload=~p",
        [Method, Id, Token, Options, Payload]
    ),
    Response.

test_recv_coap_request(UdpSock) ->
    case gen_udp:recv(UdpSock, 0, 2000) of
        {ok, {_Address, _Port, Packet}} ->
            {ok, Request, _, _} = emqx_coap_frame:parse(Packet, undefined),
            #coap_message{
                type = con,
                id = Id,
                method = Method,
                token = Token,
                payload = Payload,
                options = Options
            } = Request,
            ?LOGT(
                "receive coap request Method=~p, Id=~p, Token=~p, "
                "Options=~p, Payload=~p",
                [Method, Id, Token, Options, Payload]
            ),
            Request;
        {error, Reason} ->
            ?LOGT("test_recv_coap_request failed, Reason=~p", [Reason]),
            timeout_test_recv_coap_request
    end.

test_send_coap_response(UdpSock, Host, Port, Code, Content, Request, Ack) ->
    is_record(Content, coap_content) orelse
        error("Content must be a #coap_content!"),
    is_list(Host) orelse error("Host is not a string"),

    {ok, IpAddr} = inet:getaddr(Host, inet),
    Response = response(Code, Content, Request),
    Response2 =
        case Ack of
            true -> Response#coap_message{type = ack};
            false -> Response
        end,
    ?LOGT("test_send_coap_response Response=~p", [Response2]),
    ok = gen_udp:send(
        UdpSock,
        IpAddr,
        Port,
        emqx_coap_frame:serialize_pkt(Response2, undefined)
    ).

test_send_empty_ack(UdpSock, Host, Port, Request) ->
    is_list(Host) orelse error("Host is not a string"),
    {ok, IpAddr} = inet:getaddr(Host, inet),
    EmptyACK = emqx_coap_message:ack(Request),
    ?LOGT("test_send_empty_ack EmptyACK=~p", [EmptyACK]),
    ok = gen_udp:send(
        UdpSock,
        IpAddr,
        Port,
        emqx_coap_frame:serialize_pkt(EmptyACK, undefined)
    ).

test_send_coap_observe_ack(UdpSock, Host, Port, Code, Content, Request) ->
    is_record(Content, coap_content) orelse
        error("Content must be a #coap_content!"),
    is_list(Host) orelse error("Host is not a string"),

    {ok, IpAddr} = inet:getaddr(Host, inet),
    Response = response(Code, Content, Request),
    Response1 = emqx_coap_message:set(observe, 0, Response),
    Response2 = Response1#coap_message{type = ack},

    ?LOGT("test_send_coap_observe_ack Response=~p", [Response2]),
    ResponseBinary = emqx_coap_frame:serialize_pkt(Response2, undefined),
    ok = gen_udp:send(UdpSock, IpAddr, Port, ResponseBinary).

test_send_coap_notif(UdpSock, Host, Port, Content, ObSeq, Request) ->
    is_record(Content, coap_content) orelse
        error("Content must be a #coap_content!"),
    is_list(Host) orelse error("Host is not a string"),

    {ok, IpAddr} = inet:getaddr(Host, inet),
    Notif = response({ok, content}, Content, Request),
    NewNotif = emqx_coap_message:set(observe, ObSeq, Notif),
    ?LOGT("test_send_coap_notif Response=~p", [NewNotif]),
    NotifBinary = emqx_coap_frame:serialize_pkt(NewNotif, undefined),
    ?LOGT("test udp socket send to ~p:~p, data=~p", [IpAddr, Port, NotifBinary]),
    ok = gen_udp:send(UdpSock, IpAddr, Port, NotifBinary).

test_send_coap_notif_con(UdpSock, Host, Port, Content, ObSeq, Request) ->
    is_record(Content, coap_content) orelse
        error("Content must be a #coap_content!"),
    is_list(Host) orelse error("Host is not a string"),

    {ok, IpAddr} = inet:getaddr(Host, inet),
    Notif = response({ok, content}, Content, Request),
    NewNotif = emqx_coap_message:set(observe, ObSeq, Notif),
    ConNotif = NewNotif#coap_message{type = con},
    ?LOGT("test_send_coap_notif_con Response=~p", [ConNotif]),
    NotifBinary = emqx_coap_frame:serialize_pkt(ConNotif, undefined),
    ok = gen_udp:send(UdpSock, IpAddr, Port, NotifBinary).

test_send_coap_reset(UdpSock, Host, Port, Request) ->
    is_list(Host) orelse error("Host is not a string"),
    {ok, IpAddr} = inet:getaddr(Host, inet),
    Reset = emqx_coap_message:reset(Request),
    ?LOGT("test_send_coap_reset Reset=~p", [Reset]),
    ok = gen_udp:send(
        UdpSock,
        IpAddr,
        Port,
        emqx_coap_frame:serialize_pkt(Reset, undefined)
    ).

std_register(UdpSock, Epn, ObjectList, MsgId1, RespTopic) ->
    test_send_coap_request(
        UdpSock,
        post,
        sprintf("coap://127.0.0.1:~b/rd?ep=~ts&lt=345&lwm2m=1", [?PORT, Epn]),
        #coap_content{content_format = <<"text/plain">>, payload = ObjectList},
        [],
        MsgId1
    ),
    #coap_message{method = {ok, created}} = test_recv_coap_response(UdpSock),
    test_recv_mqtt_response(RespTopic),
    timer:sleep(100).

resolve_uri(Uri) ->
    {ok,
        #{
            scheme := Scheme,
            host := Host,
            port := PortNo,
            path := Path
        } = URIMap} = emqx_http_lib:uri_parse(Uri),
    Query = maps:get(query, URIMap, ""),
    {ok, PeerIP} = inet:getaddr(Host, inet),
    {Scheme, {PeerIP, PortNo}, split_path(Path), split_query(Query)}.

split_path([]) -> [];
split_path([$/]) -> [];
split_path([$/ | Path]) -> split_segments(Path, $/, []).

split_query([]) -> [];
split_query(Path) -> split_segments(Path, $&, []).

split_segments(Path, Char, Acc) ->
    case string:rchr(Path, Char) of
        0 ->
            [make_segment(Path) | Acc];
        N when N > 0 ->
            split_segments(
                string:substr(Path, 1, N - 1),
                Char,
                [make_segment(string:substr(Path, N + 1)) | Acc]
            )
    end.

make_segment(Seg) ->
    list_to_binary(emqx_http_lib:uri_decode(Seg)).

get_coap_path(Options) ->
    Seps = maps:get(uri_path, Options, []),
    lists:foldl(
        fun(Sep, Acc) ->
            <<Acc/binary, $/, Sep/binary>>
        end,
        <<>>,
        Seps
    ).

get_coap_query(Options) ->
    maps:get(uri_query, Options, #{}).

get_coap_observe(Options) ->
    maps:get(observe, Options, undefined).

join_path([], Acc) -> Acc;
join_path([<<"/">> | T], Acc) -> join_path(T, Acc);
join_path([H | T], Acc) -> join_path(T, <<Acc/binary, $/, H/binary>>).

sprintf(Format, Args) ->
    lists:flatten(io_lib:format(Format, Args)).

response(Code, #coap_content{content_format = Format, payload = Payload}, Req) ->
    Msg =
        #coap_message{options = Opts} =
        emqx_coap_message:response(Code, Payload, Req),
    Msg#coap_message{options = Opts#{content_format => Format}}.

request(
    Type,
    Method,
    #coap_content{
        content_format = Format,
        payload = Payload
    },
    Opts
) ->
    emqx_coap_message:request(
        Type,
        Method,
        Payload,
        [{content_format, Format} | Opts]
    ).

with_context_stub() ->
    fun
        (publish, [_Topic, _Msg]) -> ok;
        (subscribe, [_Topic, _Opts]) -> ok;
        (metrics, _Name) -> ok;
        (_, _) -> ok
    end.

session_from_result(Result) ->
    case maps:get(return, Result, undefined) of
        {_, Session} ->
            Session;
        _ ->
            error({missing_return, Result})
    end.
