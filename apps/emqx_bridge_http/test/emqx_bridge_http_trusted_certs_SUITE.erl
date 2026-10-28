%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_http_trusted_certs_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(CONNECTOR_TYPE, http).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(TCConfig) ->
    TCConfig.

end_per_suite(_TCConfig) ->
    ok.

init_per_testcase(TestCase, TCConfig) ->
    TrustedCerts = trusted_certs(TestCase),
    Apps = emqx_cth_suite:start(
        [
            emqx,
            {emqx_conf, #{config => #{<<"node">> => #{<<"trusted_certs">> => TrustedCerts}}}},
            emqx_bridge_http,
            emqx_bridge,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(TestCase, TCConfig)}
    ),
    ServerSSLOpts = [{verify, verify_none} | server_certs()],
    {ok, {HTTPPort, _Pid}} = emqx_bridge_http_connector_test_server:start_link(
        random, <<"/path">>, ServerSSLOpts, #{host => "localhost"}
    ),
    ok = emqx_bridge_http_connector_test_server:set_handler(fun ok_handler/2),
    ConnectorConfig = emqx_bridge_schema_testlib:http_connector_config(#{
        <<"url">> => iolist_to_binary(io_lib:format("https://localhost:~b", [HTTPPort])),
        <<"connect_timeout">> => <<"2s">>,
        <<"ssl">> => #{<<"enable">> => true, <<"verify">> => <<"verify_peer">>}
    }),
    [
        {apps, Apps},
        {bridge_kind, action},
        {connector_type, ?CONNECTOR_TYPE},
        {connector_name, atom_to_binary(TestCase)},
        {connector_config, ConnectorConfig}
        | TCConfig
    ].

end_per_testcase(_TestCase, TCConfig) ->
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    emqx_bridge_http_connector_test_server:stop(),
    emqx_cth_suite:stop(?config(apps, TCConfig)),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

trusted_certs(t_verify_peer_trusts_node_trusted_certs) ->
    <<"file://", (list_to_binary(filename:join(certs_dir(), "cacert.pem")))/binary>>;
trusted_certs(t_verify_peer_rejects_ca_not_in_trusted_certs) ->
    <<"system_default">>.

%% `etc/certs' holds only a README on this branch, so take the generated test
%% certificates. `test_cert/1' generates them when they are missing.
certs_dir() ->
    filename:dirname(emqx_common_test_helpers:test_cert("cacert.pem")).

server_certs() ->
    [
        {keyfile, filename:join(certs_dir(), "key.pem")},
        {certfile, filename:join(certs_dir(), "cert.pem")}
    ].

ok_handler(Req0, State) ->
    {ok, Req0, State}.

create_connector(TCConfig) ->
    emqx_bridge_v2_testlib:create_connector_api2(TCConfig, #{}).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

-doc """
A connector that sets `verify = verify_peer` and no `cacertfile` verifies the server
with the CA file that `node.trusted_certs` names, and connects.
""".
t_verify_peer_trusts_node_trusted_certs(TCConfig) ->
    ?assertMatch(
        {201, #{<<"status">> := <<"connected">>}},
        create_connector(TCConfig)
    ),
    ok.

-doc """
A connector that sets `verify = verify_peer` and no `cacertfile` does not connect when
the CA of the server is not in the trust store of `node.trusted_certs`.
""".
t_verify_peer_rejects_ca_not_in_trusted_certs(TCConfig) ->
    {201, #{<<"status">> := Status} = Res} = create_connector(TCConfig),
    ct:pal("connector: ~p", [Res]),
    ?assertNotEqual(<<"connected">>, Status),
    ?assertMatch({match, _}, re:run(maps:get(<<"status_reason">>, Res, <<>>), "unknown_ca")),
    ok.
