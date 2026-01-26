%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_lwm2m_api_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-define(PORT, 5783).

-define(LOGT(Format, Args), ct:pal("TEST_SUITE: " ++ Format, Args)).

-include("emqx_lwm2m.hrl").
-include("../../emqx_gateway_coap/include/emqx_coap.hrl").
-include_lib("emqx/include/emqx_config.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(assertExists(Map, Key),
    ?assertNotEqual(maps:get(Key, Map, undefined), undefined)
).

-record(coap_content, {content_format, payload = <<>>}).

-import(emqx_lwm2m_SUITE, [
    request/4,
    response/3,
    test_send_coap_response/7,
    test_recv_coap_request/1,
    test_recv_coap_response/1,
    test_send_coap_request/6,
    test_recv_mqtt_response/1,
    std_register/5,
    split_path/1,
    split_query/1,
    join_path/2,
    sprintf/2
]).

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

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
    ok = emqx_conf_cli:load_config(?global_ns, emqx_lwm2m_SUITE:default_config(), #{mode => replace}),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(suite_apps, Config)),
    ok.

init_per_testcase(_AllTestCase, Config) ->
    ok = emqx_conf_cli:load_config(?global_ns, emqx_lwm2m_SUITE:default_config(), #{mode => replace}),
    {ok, ClientUdpSock} = gen_udp:open(0, [binary, {active, false}]),

    {ok, C} = emqtt:start_link([{host, "localhost"}, {port, 1883}, {clientid, <<"c1">>}]),
    {ok, _} = emqtt:connect(C),
    timer:sleep(100),

    [{sock, ClientUdpSock}, {emqx_c, C} | Config].

end_per_testcase(_AllTestCase, Config) ->
    gen_udp:close(?config(sock, Config)),
    emqtt:disconnect(?config(emqx_c, Config)),
    timer:sleep(300).

%%--------------------------------------------------------------------
%% Cases
%%--------------------------------------------------------------------
t_lookup_read(Config) ->
    UdpSock = ?config(sock, Config),
    Epn = "urn:oma:lwm2m:oma:1",
    MsgId1 = 15,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),
    %% step 1, device register ...
    test_send_coap_request(
        UdpSock,
        post,
        sprintf("coap://127.0.0.1:~b/rd?ep=~ts&lt=600&lwm2m=1", [?PORT, Epn]),
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

    timer:sleep(100),
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

    timer:sleep(200),
    no_received_request(Epn, <<"/3/0/0">>, <<"read">>),

    Request2 = test_recv_coap_request(UdpSock),
    ?LOGT("LwM2M client got ~p", [Request2]),
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

    timer:sleep(200),
    normal_received_request(Epn, <<"/3/0/0">>, <<"read">>).

t_lookup_discover(Config) ->
    %% step 1, device register ...
    Epn = "urn:oma:lwm2m:oma:2",
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

    timer:sleep(200),
    no_received_request(Epn, <<"/3/0/7">>, <<"discover">>),

    timer:sleep(50),
    Request2 = test_recv_coap_request(UdpSock),
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
    timer:sleep(200),
    discover_received_request(Epn, <<"/3/0/7">>, <<"discover">>).

t_lookup_missing_client(_Config) ->
    {404, Response} = call_lookup_api_with_status("missing-client", <<"/3/0/0">>, <<"read">>),
    ?assertEqual(#{<<"code">> => <<"CLIENT_NOT_FOUND">>}, emqx_utils_json:decode(Response)).

t_lookup_discover_unknown_object(Config) ->
    %% step 1, device register ...
    Epn = "urn:oma:lwm2m:oma:21",
    MsgId1 = 15,
    UdpSock = ?config(sock, Config),
    ObjectList = <<"</999/0>">>,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),

    std_register(UdpSock, Epn, ObjectList, MsgId1, RespTopic),

    %% step2, send a DISCOVER command to device
    CommandTopic = <<"lwm2m/", (list_to_binary(Epn))/binary, "/dn/dm">>,
    CmdId = 401,
    Command = #{
        <<"requestID">> => CmdId,
        <<"cacheID">> => CmdId,
        <<"msgType">> => <<"discover">>,
        <<"data">> => #{<<"path">> => <<"/999/0/1">>}
    },
    test_mqtt_broker:publish(CommandTopic, emqx_utils_json:encode(Command), 0),
    timer:sleep(200),
    no_received_request(Epn, <<"/999/0/1">>, <<"discover">>),
    Request2 = test_recv_coap_request(UdpSock),

    PayloadDiscover = <<"</999>,</999/0/4>">>,
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
    timer:sleep(200),
    Response = call_lookup_api(Epn, <<"/999/0/1">>, <<"discover">>),
    RCont = emqx_utils_json:decode(Response),
    ?assertEqual([#{<<"/999/0/4">> => <<"/999/0/4">>}], maps:get(<<"content">>, RCont)).

t_lookup_discover_obj_instance(Config) ->
    %% step 1, device register ...
    Epn = "urn:oma:lwm2m:oma:22",
    MsgId1 = 15,
    UdpSock = ?config(sock, Config),
    ObjectList = <<"</1>, </2>, </3/0>, </4>, </5>">>,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),

    std_register(UdpSock, Epn, ObjectList, MsgId1, RespTopic),

    %% step2, send a DISCOVER command to device
    CommandTopic = <<"lwm2m/", (list_to_binary(Epn))/binary, "/dn/dm">>,
    CmdId = 402,
    Command = #{
        <<"requestID">> => CmdId,
        <<"cacheID">> => CmdId,
        <<"msgType">> => <<"discover">>,
        <<"data">> => #{<<"path">> => <<"/3/0/7">>}
    },
    test_mqtt_broker:publish(CommandTopic, emqx_utils_json:encode(Command), 0),
    timer:sleep(200),
    no_received_request(Epn, <<"/3/0/7">>, <<"discover">>),
    Request2 = test_recv_coap_request(UdpSock),

    PayloadDiscover = <<"</3/0>,</3/0/7>">>,
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
    timer:sleep(200),
    discover_received_request(Epn, <<"/3/0/7">>, <<"discover">>).

t_lookup_discover_operations_e(Config) ->
    %% step 1, device register ...
    Epn = "urn:oma:lwm2m:oma:23",
    MsgId1 = 15,
    UdpSock = ?config(sock, Config),
    ObjectList = <<"</1>, </2>, </3/0>, </4>, </5>">>,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),

    std_register(UdpSock, Epn, ObjectList, MsgId1, RespTopic),

    %% step2, send a DISCOVER command to device
    CommandTopic = <<"lwm2m/", (list_to_binary(Epn))/binary, "/dn/dm">>,
    CmdId = 403,
    Command = #{
        <<"requestID">> => CmdId,
        <<"cacheID">> => CmdId,
        <<"msgType">> => <<"discover">>,
        <<"data">> => #{<<"path">> => <<"/3/0/4">>}
    },
    test_mqtt_broker:publish(CommandTopic, emqx_utils_json:encode(Command), 0),
    timer:sleep(200),
    no_received_request(Epn, <<"/3/0/4">>, <<"discover">>),
    Request2 = test_recv_coap_request(UdpSock),

    PayloadDiscover = <<"</3/0/4/0>,</3/0/4>">>,
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
    timer:sleep(200),
    Response = call_lookup_api(Epn, <<"/3/0/4">>, <<"discover">>),
    RCont = emqx_utils_json:decode(Response),
    [Res | _] = maps:get(<<"content">>, RCont),
    ?assertEqual(<<"/3/0/4">>, maps:get(<<"path">>, Res, undefined)),
    ?assertEqual(<<"E">>, maps:get(<<"operations">>, Res, undefined)),
    ?assertExists(Res, <<"name">>),
    ?assertEqual(undefined, maps:get(<<"dataType">>, Res, undefined)).

t_lookup_content_empty(Config) ->
    %% step 1, device register ...
    Epn = "urn:oma:lwm2m:oma:24",
    MsgId1 = 15,
    UdpSock = ?config(sock, Config),
    ObjectList = <<"</1>, </3/0>">>,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),

    std_register(UdpSock, Epn, ObjectList, MsgId1, RespTopic),

    %% step2, send a WRITE command and return changed
    CommandTopic = <<"lwm2m/", (list_to_binary(Epn))/binary, "/dn/dm">>,
    CmdId = 410,
    Command = #{
        <<"requestID">> => CmdId,
        <<"cacheID">> => CmdId,
        <<"msgType">> => <<"write">>,
        <<"data">> => #{
            <<"basePath">> => <<"/3/0/13">>,
            <<"content">> => [#{type => <<"Integer">>, value => <<"42">>}]
        }
    },
    test_mqtt_broker:publish(CommandTopic, emqx_utils_json:encode(Command), 0),
    timer:sleep(200),
    Request2 = test_recv_coap_request(UdpSock),
    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {ok, changed},
        #coap_content{},
        Request2,
        true
    ),
    timer:sleep(200),
    _ = test_recv_mqtt_response(RespTopic),
    Response = call_lookup_api(Epn, <<"/3/0/13">>, <<"write">>),
    RCont = emqx_utils_json:decode(Response),
    ?assertEqual(undefined, maps:get(<<"content">>, RCont, undefined)).

t_lookup_discover_empty_object_list(Config) ->
    %% step 1, device register ...
    Epn = "urn:oma:lwm2m:oma:25",
    MsgId1 = 15,
    UdpSock = ?config(sock, Config),
    ObjectList = <<"</1>, </3/0>">>,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),

    std_register(UdpSock, Epn, ObjectList, MsgId1, RespTopic),

    %% step2, send a DISCOVER command with single link
    CommandTopic = <<"lwm2m/", (list_to_binary(Epn))/binary, "/dn/dm">>,
    CmdId = 411,
    Command = #{
        <<"requestID">> => CmdId,
        <<"cacheID">> => CmdId,
        <<"msgType">> => <<"discover">>,
        <<"data">> => #{<<"path">> => <<"/3/0/7">>}
    },
    test_mqtt_broker:publish(CommandTopic, emqx_utils_json:encode(Command), 0),
    timer:sleep(200),
    Request2 = test_recv_coap_request(UdpSock),
    PayloadDiscover = <<"</3/0/7>">>,
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
    timer:sleep(200),
    Response = call_lookup_api(Epn, <<"/3/0/7">>, <<"discover">>),
    RCont = emqx_utils_json:decode(Response),
    ?assertEqual([], maps:get(<<"content">>, RCont)).

t_observe_enable(Config) ->
    UdpSock = ?config(sock, Config),
    Epn = "urn:oma:lwm2m:oma:26",
    MsgId1 = 15,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),
    %% step 1, device register ...
    test_send_coap_request(
        UdpSock,
        post,
        sprintf("coap://127.0.0.1:~b/rd?ep=~ts&lt=600&lwm2m=1", [?PORT, Epn]),
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

    timer:sleep(100),
    test_recv_mqtt_response(RespTopic),

    %% step2, call observe API (enable=true)
    ?assertMatch({204, []}, call_send_api(Epn, "observe", "path=/3/0/1&enable=true")),
    timer:sleep(100),
    #coap_message{type = Type, method = Method, options = Opts} = test_recv_coap_request(UdpSock),
    ?assertEqual(con, Type),
    ?assertEqual(get, Method),
    ?assertEqual([<<"lwm2m">>, <<"3">>, <<"0">>, <<"1">>], maps:get(uri_path, Opts)),
    ?assertEqual(0, maps:get(observe, Opts)).

t_send_cmd_missing_client(_Config) ->
    ApiPath = emqx_mgmt_api_test_util:api_path([
        "gateways/lwm2m/clients", "missing-client", "read"
    ]),
    Auth = emqx_mgmt_api_test_util:auth_header_(),
    Opts = #{return_all => true},
    Result = emqx_mgmt_api_test_util:request_api(post, ApiPath, "path=/3/0/0", Auth, [], Opts),
    {StatusCode, _Response} =
        case Result of
            {ok, {{"HTTP/1.1", Code, _}, _Headers, Body}} ->
                {Code, Body};
            {error, {{"HTTP/1.1", Code, _}, _Headers, Body}} ->
                {Code, Body}
        end,
    ?assertEqual(404, StatusCode).

t_read(Config) ->
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
        sprintf("coap://127.0.0.1:~b/rd?ep=~ts&lt=600&lwm2m=1", [?PORT, Epn]),
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

    timer:sleep(100),
    test_recv_mqtt_response(RespTopic),

    %% step2, call Read API
    ?assertMatch({204, []}, call_send_api(Epn, "read", "path=/3/0/0")),
    timer:sleep(100),
    #coap_message{type = Type, method = Method, options = Opts} = test_recv_coap_request(UdpSock),
    ?assertEqual(con, Type),
    ?assertEqual(get, Method),
    ?assertEqual([<<"lwm2m">>, <<"3">>, <<"0">>, <<"0">>], maps:get(uri_path, Opts)).

t_write(Config) ->
    UdpSock = ?config(sock, Config),
    Epn = "urn:oma:lwm2m:oma:4",
    MsgId1 = 15,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),
    %% step 1, device register ...
    test_send_coap_request(
        UdpSock,
        post,
        sprintf("coap://127.0.0.1:~b/rd?ep=~ts&lt=600&lwm2m=1", [?PORT, Epn]),
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

    timer:sleep(100),
    test_recv_mqtt_response(RespTopic),

    %% step2, call write API
    ?assertMatch({204, []}, call_send_api(Epn, "write", "path=/3/0/13&type=Integer&value=123")),
    timer:sleep(100),
    #coap_message{type = Type, method = Method, options = Opts} = test_recv_coap_request(UdpSock),
    ?assertEqual(con, Type),
    ?assertEqual(put, Method),
    ?assertEqual([<<"lwm2m">>, <<"3">>, <<"0">>, <<"13">>], maps:get(uri_path, Opts)),
    ?assertEqual(<<"application/vnd.oma.lwm2m+tlv">>, maps:get(content_format, Opts)).

t_observe(Config) ->
    UdpSock = ?config(sock, Config),
    Epn = "urn:oma:lwm2m:oma:5",
    MsgId1 = 15,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),
    %% step 1, device register ...
    test_send_coap_request(
        UdpSock,
        post,
        sprintf("coap://127.0.0.1:~b/rd?ep=~ts&lt=600&lwm2m=1", [?PORT, Epn]),
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

    timer:sleep(100),
    test_recv_mqtt_response(RespTopic),

    %% step2, call observe API
    ?assertMatch({204, []}, call_send_api(Epn, "observe", "path=/3/0/1&enable=false")),
    timer:sleep(100),
    #coap_message{type = Type, method = Method, options = Opts} = test_recv_coap_request(UdpSock),
    ?assertEqual(con, Type),
    ?assertEqual(get, Method),
    ?assertEqual([<<"lwm2m">>, <<"3">>, <<"0">>, <<"1">>], maps:get(uri_path, Opts)),
    ?assertEqual(1, maps:get(observe, Opts)).

t_observe_enable_numeric(Config) ->
    UdpSock = ?config(sock, Config),
    Epn = "urn:oma:lwm2m:oma:6",
    MsgId1 = 15,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),
    %% step 1, device register ...
    test_send_coap_request(
        UdpSock,
        post,
        sprintf("coap://127.0.0.1:~b/rd?ep=~ts&lt=600&lwm2m=1", [?PORT, Epn]),
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

    timer:sleep(100),
    test_recv_mqtt_response(RespTopic),

    %% step2, call observe API (enable=1)
    ?assertMatch({204, []}, call_send_api(Epn, "observe", "path=/3/0/1&enable=1")),
    timer:sleep(100),
    Request2 = test_recv_coap_request(UdpSock),
    #coap_message{method = Method2, options = Opts2} = Request2,
    ?assertEqual(get, Method2),
    ?assertEqual(0, maps:get(observe, Opts2)),
    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {ok, content},
        #coap_content{content_format = <<"text/plain">>, payload = <<"1">>},
        Request2,
        true
    ),

    %% step3, call observe API (enable=0)
    ?assertMatch({204, []}, call_send_api(Epn, "observe", "path=/3/0/1&enable=0")),
    timer:sleep(100),
    Request3 = test_recv_coap_request(UdpSock),
    #coap_message{method = Method3, options = Opts3} = Request3,
    ?assertEqual(get, Method3),
    ?assertEqual(1, maps:get(observe, Opts3)),
    test_send_coap_response(
        UdpSock,
        "127.0.0.1",
        ?PORT,
        {ok, content},
        #coap_content{content_format = <<"text/plain">>, payload = <<"0">>},
        Request3,
        true
    ).

t_observe_enable_types(Config) ->
    UdpSock = ?config(sock, Config),
    Epn = "urn:oma:lwm2m:oma:27",
    MsgId1 = 15,
    RespTopic = list_to_binary("lwm2m/" ++ Epn ++ "/up/resp"),
    emqtt:subscribe(?config(emqx_c, Config), RespTopic, qos0),
    timer:sleep(200),
    %% step 1, device register ...
    test_send_coap_request(
        UdpSock,
        post,
        sprintf("coap://127.0.0.1:~b/rd?ep=~ts&lt=600&lwm2m=1", [?PORT, Epn]),
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

    timer:sleep(100),
    test_recv_mqtt_response(RespTopic),

    ClientId = list_to_binary(Epn),
    Path = <<"/3/0/1">>,
    Cases = [
        {true, 0},
        {false, 1},
        {"true", 0},
        {"false", 1},
        {"1", 0},
        {"0", 1},
        {1, 0},
        {0, 1},
        {undefined, 1}
    ],
    lists:foreach(
        fun({Enable, ObserveValue}) ->
            {204} = emqx_lwm2m_api:observe(post, #{
                bindings => #{clientid => ClientId},
                query_string => #{<<"path">> => Path, <<"enable">> => Enable}
            }),
            Request = test_recv_coap_request(UdpSock),
            #coap_message{method = Method, options = Opts} = Request,
            ?assertEqual(get, Method),
            ?assertEqual(ObserveValue, maps:get(observe, Opts)),
            test_send_coap_response(
                UdpSock,
                "127.0.0.1",
                ?PORT,
                {ok, content},
                #coap_content{content_format = <<"text/plain">>, payload = <<"1">>},
                Request,
                true
            ),
            timer:sleep(50)
        end,
        Cases
    ).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Internal Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
call_lookup_api(ClientId, Path, Action) ->
    ApiPath = emqx_mgmt_api_test_util:api_path(["gateways/lwm2m/clients", ClientId, "lookup"]),
    Auth = emqx_mgmt_api_test_util:auth_header_(),
    Query = io_lib:format("path=~ts&action=~ts", [Path, Action]),
    {ok, Response} = emqx_mgmt_api_test_util:request_api(get, ApiPath, Query, Auth),
    ?LOGT("rest api response:~ts~n", [Response]),
    Response.

call_lookup_api_with_status(ClientId, Path, Action) ->
    ApiPath = emqx_mgmt_api_test_util:api_path(["gateways/lwm2m/clients", ClientId, "lookup"]),
    Auth = emqx_mgmt_api_test_util:auth_header_(),
    Query = io_lib:format("path=~ts&action=~ts", [Path, Action]),
    Opts = #{return_all => true},
    Result = emqx_mgmt_api_test_util:request_api(get, ApiPath, Query, Auth, [], Opts),
    {StatusCode, Response} =
        case Result of
            {ok, {{"HTTP/1.1", Code, _}, _Headers, Body}} ->
                {Code, Body};
            {error, {{"HTTP/1.1", Code, _}, _Headers, Body}} ->
                {Code, Body}
        end,
    ?LOGT("rest api response:~ts~n", [Response]),
    {StatusCode, Response}.

call_send_api(ClientId, Cmd, Query) ->
    call_send_api(ClientId, Cmd, Query, "gateways/lwm2m/clients").

call_send_api(ClientId, Cmd, Query, API) ->
    ApiPath = emqx_mgmt_api_test_util:api_path([API, ClientId, Cmd]),
    Auth = emqx_mgmt_api_test_util:auth_header_(),
    Opts = #{return_all => true},
    {ok, {{"HTTP/1.1", StatusCode, _}, _Headers, Response}} = emqx_mgmt_api_test_util:request_api(
        post, ApiPath, Query, Auth, [], Opts
    ),
    ?LOGT("rest api response:~ts~n", [Response]),
    {StatusCode, Response}.

no_received_request(ClientId, Path, Action) ->
    Response = call_lookup_api(ClientId, Path, Action),
    NotReceived = #{
        <<"clientid">> => list_to_binary(ClientId),
        <<"action">> => Action,
        <<"code">> => <<"6.01">>,
        <<"codeMsg">> => <<"reply_not_received">>,
        <<"path">> => Path
    },
    ?assertEqual(NotReceived, emqx_utils_json:decode(Response)).
normal_received_request(ClientId, Path, Action) ->
    Response = call_lookup_api(ClientId, Path, Action),
    RCont = emqx_utils_json:decode(Response),
    ?assertEqual(list_to_binary(ClientId), maps:get(<<"clientid">>, RCont, undefined)),
    ?assertEqual(Path, maps:get(<<"path">>, RCont, undefined)),
    ?assertEqual(Action, maps:get(<<"action">>, RCont, undefined)),
    ?assertExists(RCont, <<"code">>),
    ?assertExists(RCont, <<"codeMsg">>),
    ?assertExists(RCont, <<"content">>),
    RCont.

discover_received_request(ClientId, Path, Action) ->
    RCont = normal_received_request(ClientId, Path, Action),
    [Res | _] = maps:get(<<"content">>, RCont),
    ?assertExists(Res, <<"path">>),
    ?assertExists(Res, <<"name">>),
    ?assertExists(Res, <<"operations">>).
