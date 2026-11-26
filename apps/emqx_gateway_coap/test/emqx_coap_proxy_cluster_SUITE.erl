%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_coap_proxy_cluster_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("er_coap_client/include/coap.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").

-define(HOST, {127, 0, 0, 1}).
-define(MQTT_PREFIX, "coap://127.0.0.1/mqtt").
-define(PS_PREFIX, "coap://127.0.0.1/ps").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    {Nodes, Port1, Port2} = start_coap_cluster(Config),
    [
        {cluster_nodes, Nodes},
        {coap_port1, Port1},
        {coap_port2, Port2}
        | Config
    ].

end_per_suite(Config) ->
    emqx_cth_cluster:stop(?config(cluster_nodes, Config)).

t_udp_downlink_migration_across_nodes(Config) ->
    ClientId = <<"cluster-coap-udp-migration">>,
    Topic = <<"cluster/coap/udp/migration">>,
    [Node1, Node2] = ?config(cluster_nodes, Config),
    Port1 = ?config(coap_port1, Config),
    Port2 = ?config(coap_port2, Config),
    {ok, Sock1, Channel1} = er_coap_udp_socket:connect(?HOST, Port1),
    {ok, Sock2} = gen_udp:open(0, [binary, {active, false}]),
    try
        Token = connection(Channel1, ClientId),
        observe_topic(Channel1, ClientId, Token, Topic),
        ?retry(
            100,
            20,
            [_] = erpc:call(Node2, emqx_gateway_cm_registry, lookup_channels, [coap, ClientId])
        ),
        [ConnPid] = erpc:call(Node2, emqx_gateway_cm_registry, lookup_channels, [
            coap, ClientId
        ]),
        ?assertEqual(Node1, node(ConnPid)),

        InvalidURI = pubsub_uri(Topic, ClientId, <<"wrong-token">>),
        ?assertMatch(
            {error, unauthorized, _},
            raw_udp_request(Sock2, Port2, InvalidURI, make_req(post, <<"rejected">>))
        ),

        RejectedPayload = <<"after-cross-node-reject">>,
        publish(Node1, Topic, RejectedPayload),
        ?assertEqual({error, timeout}, gen_udp:recv(Sock2, 0, 300)),
        _ = emqx_coap_SUITE:assert_notify(Channel1, non, RejectedPayload),

        ValidURI = pubsub_uri(<<"cluster/coap/udp/migration/request">>, ClientId, Token),
        ?assertMatch(
            {ok, changed, _},
            raw_udp_request(Sock2, Port2, ValidURI, make_req(post, <<"accepted">>))
        ),

        CommittedPayload = <<"after-cross-node-commit">>,
        publish(Node1, Topic, CommittedPayload),
        ?assertEqual({error, timeout}, emqx_coap_SUITE:with_message_response(Channel1, 300)),
        {ok, {?HOST, Port2, NotificationData}} = gen_udp:recv(Sock2, 0, 2000),
        #coap_message{payload = CommittedPayload} =
            emqx_coap_SUITE:parse_udp_message(NotificationData)
    after
        _ = catch erpc:call(Node1, emqx_gateway_cm, kick_session, [coap, ClientId]),
        er_coap_channel:close(Channel1),
        er_coap_udp_socket:close(Sock1),
        gen_udp:close(Sock2)
    end.

start_coap_cluster(Config) ->
    Port1 = emqx_common_test_helpers:select_free_port(udp),
    Port2 = emqx_common_test_helpers:select_free_port(udp),
    NodeSpecs = emqx_cth_cluster:mk_nodespecs(
        [
            {cluster_node_name(?FUNCTION_NAME, 1), #{apps => coap_apps(Port1)}},
            {cluster_node_name(?FUNCTION_NAME, 2), #{apps => coap_apps(Port2)}}
        ],
        #{work_dir => emqx_cth_suite:work_dir(?FUNCTION_NAME, Config)}
    ),
    [Node1, Node2] = Nodes = emqx_cth_cluster:start(NodeSpecs),
    ?retry(
        50,
        20,
        true = is_pid(erpc:call(Node1, erlang, whereis, [emqx_gateway_sup]))
    ),
    ?retry(
        50,
        20,
        true = is_pid(erpc:call(Node2, erlang, whereis, [emqx_gateway_sup]))
    ),
    {Nodes, Port1, Port2}.

cluster_node_name(TestCase, N) ->
    binary_to_atom(iolist_to_binary(io_lib:format("~s_~B", [TestCase, N]))).

coap_apps(Port) ->
    [
        {emqx_conf, coap_conf(Port)},
        emqx_gateway,
        emqx_auth
    ].

coap_conf(Port) ->
    iolist_to_binary(
        io_lib:format(
            ~S"""
            gateway.coap {
                idle_timeout = 30s
                enable_stats = false
                mountpoint = ""
                notify_type = qos
                connection_required = true
                subscribe_qos = qos1
                publish_qos = qos1
                listeners.udp.default {
                    bind = "127.0.0.1:~B"
                    enable_authn = false
                }
            }
            """,
            [Port]
        )
    ).

connection(Channel, ClientId) ->
    URI = emqx_coap_SUITE:compose_uri(
        ?MQTT_PREFIX ++ "/connection",
        #{
            "clientid" => ClientId,
            "username" => <<"admin">>,
            "password" => <<"public">>
        },
        false
    ),
    {ok, created, #coap_content{payload = Token}} =
        emqx_coap_SUITE:do_request(Channel, URI, make_req(post)),
    Token.

observe_topic(Channel, ClientId, Token, Topic) ->
    URI = pubsub_uri(Topic, ClientId, Token),
    Req = (make_req(get, <<>>, [{observe, 0}]))#coap_message{token = <<"cluster-observe">>},
    {ok, content, _} = emqx_coap_SUITE:do_request(Channel, URI, Req),
    ok.

pubsub_uri(Topic, ClientId, Token) ->
    emqx_coap_SUITE:compose_uri(
        ?PS_PREFIX ++ "/" ++ binary_to_list(Topic),
        #{"clientid" => ClientId, "token" => Token},
        false
    ).

make_req(Method) ->
    make_req(Method, <<>>).

make_req(Method, Payload) ->
    make_req(Method, Payload, []).

make_req(Method, Payload, Opts) ->
    er_coap_message:request(con, Method, Payload, Opts).

raw_udp_request(Socket, Port, URI, #coap_message{options = Opts} = Req) ->
    {_, _, Path, Query} = er_coap_client:resolve_uri(URI),
    Req1 = Req#coap_message{
        id = erlang:unique_integer([positive]) band 16#FFFF,
        token = crypto:strong_rand_bytes(4),
        options = [{uri_path, Path}, {uri_query, Query} | Opts]
    },
    ok = gen_udp:send(Socket, ?HOST, Port, er_coap_message_parser:encode(Req1)),
    emqx_coap_SUITE:recv_raw_udp_response(Socket).

publish(Node, Topic, Payload) ->
    Msg = emqx_message:make(<<"coap-cluster-ct">>, 0, Topic, Payload),
    _ = erpc:call(Node, emqx, publish, [Msg]),
    ok.
