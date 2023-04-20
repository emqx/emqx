%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_conf_schema_tests).

-include_lib("eunit/include/eunit.hrl").
-define(BASE_CONF,
    ""
    "\n"
    "     node {\n"
    "        name = \"emqx1@127.0.0.1\"\n"
    "        cookie = \"emqxsecretcookie\"\n"
    "        data_dir = \"data\"\n"
    "     }\n"
    "     cluster {\n"
    "        name = emqxcl\n"
    "        discovery_strategy = static\n"
    "        static.seeds = ~p\n"
    "        core_nodes = ~p\n"
    "     }\n"
    ""
).
array_nodes_test() ->
    ExpectNodes = ['emqx1@127.0.0.1', 'emqx2@127.0.0.1'],
    lists:foreach(
        fun(Nodes) ->
            ConfFile = iolist_to_binary(io_lib:format(?BASE_CONF, [Nodes, Nodes])),
            {ok, Conf} = hocon:binary(ConfFile, #{format => richmap}),
            ConfList = hocon_tconf:generate(emqx_conf_schema, Conf),
            ClusterDiscovery = proplists:get_value(
                cluster_discovery, proplists:get_value(ekka, ConfList)
            ),
            ?assertEqual(
                {static, [{seeds, ExpectNodes}]},
                ClusterDiscovery,
                Nodes
            ),
            ?assertEqual(
                ExpectNodes,
                proplists:get_value(core_nodes, proplists:get_value(mria, ConfList)),
                Nodes
            )
        end,
        [["emqx1@127.0.0.1", "emqx2@127.0.0.1"], "emqx1@127.0.0.1, emqx2@127.0.0.1"]
    ),
    ok.

authn_validations_test() ->
    BaseConf = iolist_to_binary(io_lib:format(?BASE_CONF, ["emqx1@127.0.0.1", "emqx1@127.0.0.1"])),
    DisableSSLWithHttps =
        ""
        "\n"
        "authentication = [\n"
        "{backend = \"http\"\n"
        "body {password = \"${password}\", username = \"${username}\"}\n"
        "connect_timeout = \"15s\"\n"
        "enable_pipelining = 100\n"
        "headers {\"content-type\" = \"application/json\"}\n"
        "mechanism = \"password_based\"\n"
        "method = \"post\"\n"
        "pool_size = 8\n"
        "request_timeout = \"5s\"\n"
        "ssl {enable = false, verify = \"verify_peer\"}\n"
        "url = \"https://127.0.0.1:8080\"\n"
        "}\n"
        "]\n"
        "",
    Conf = <<BaseConf/binary, (list_to_binary(DisableSSLWithHttps))/binary>>,
    {ok, ConfMap} = hocon:binary(Conf, #{format => richmap}),
    ?assertThrow(
        {emqx_conf_schema, [
            #{
                kind := validation_error,
                reason := integrity_validation_failure,
                result := _,
                validation_name := check_http_ssl_opts
            }
        ]},
        hocon_tconf:generate(emqx_conf_schema, ConfMap)
    ),
    BadHeader =
        ""
        "\n"
        "authentication = [\n"
        "{backend = \"http\"\n"
        "body {password = \"${password}\", username = \"${username}\"}\n"
        "connect_timeout = \"15s\"\n"
        "enable_pipelining = 100\n"
        "headers {\"content-type\" = \"application/json\"}\n"
        "mechanism = \"password_based\"\n"
        "method = \"get\"\n"
        "pool_size = 8\n"
        "request_timeout = \"5s\"\n"
        "ssl {enable = false, verify = \"verify_peer\"}\n"
        "url = \"http://127.0.0.1:8080\"\n"
        "}\n"
        "]\n"
        "",
    Conf1 = <<BaseConf/binary, (list_to_binary(BadHeader))/binary>>,
    {ok, ConfMap1} = hocon:binary(Conf1, #{format => richmap}),
    ?assertThrow(
        {emqx_conf_schema, [
            #{
                kind := validation_error,
                reason := integrity_validation_failure,
                result := _,
                validation_name := check_http_headers
            }
        ]},
        hocon_tconf:generate(emqx_conf_schema, ConfMap1)
    ),
    BadHeader2 =
        ""
        "\n"
        "authentication = \n"
        "{backend = \"http\"\n"
        "body {password = \"${password}\", username = \"${username}\"}\n"
        "connect_timeout = \"15s\"\n"
        "enable_pipelining = 100\n"
        "headers {\"content-type\" = \"application/json\"}\n"
        "mechanism = \"password_based\"\n"
        "method = \"get\"\n"
        "pool_size = 8\n"
        "request_timeout = \"5s\"\n"
        "ssl {enable = false, verify = \"verify_peer\"}\n"
        "url = \"http://127.0.0.1:8080\"\n"
        "}\n"
        "\n"
        "",
    Conf2 = <<BaseConf/binary, (list_to_binary(BadHeader2))/binary>>,
    {ok, ConfMap2} = hocon:binary(Conf2, #{format => richmap}),
    ?assertThrow(
        {emqx_conf_schema, [
            #{
                kind := validation_error,
                reason := integrity_validation_failure,
                result := _,
                validation_name := check_http_headers
            }
        ]},
        hocon_tconf:generate(emqx_conf_schema, ConfMap2)
    ),
    ok.

doc_gen_test() ->
    %% the json file too large to encode.
    {
        timeout,
        60,
        fun() ->
            Dir = "tmp",
            ok = filelib:ensure_dir(filename:join("tmp", foo)),
            I18nFile = filename:join([
                "_build",
                "test",
                "lib",
                "emqx_dashboard",
                "priv",
                "i18n.conf"
            ]),
            _ = emqx_conf:dump_schema(Dir, emqx_conf_schema, I18nFile),
            ok
        end
    }.
