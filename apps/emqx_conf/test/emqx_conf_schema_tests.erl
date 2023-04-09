%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_conf_schema_tests).

-include_lib("eunit/include/eunit.hrl").
array_nodes_test() ->
    ExpectNodes = ['emqx1@127.0.0.1', 'emqx2@127.0.0.1'],
    BaseConf =
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
        "   "
        "",
    lists:foreach(
        fun(Nodes) ->
            ConfFile = iolist_to_binary(io_lib:format(BaseConf, [Nodes, Nodes])),
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
