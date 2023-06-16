%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_bridge_api_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-import(emqx_mgmt_api_test_util, [uri/1]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/test_macros.hrl").

-define(SUITE_APPS, [emqx_conf, emqx_authn, emqx_management, emqx_rule_engine, emqx_bridge]).

-define(BRIDGE_TYPE_HTTP, <<"webhook">>).
-define(BRIDGE_NAME, (atom_to_binary(?FUNCTION_NAME))).
-define(URL(PORT, PATH),
    list_to_binary(
        io_lib:format(
            "http://localhost:~s/~s",
            [integer_to_list(PORT), PATH]
        )
    )
).
-define(BRIDGE(NAME, TYPE), #{
    <<"ssl">> => #{<<"enable">> => false},
    <<"type">> => TYPE,
    <<"name">> => NAME
}).

-define(BRIDGE_TYPE_MQTT, <<"mqtt">>).
-define(MQTT_BRIDGE(SERVER, NAME), ?BRIDGE(NAME, ?BRIDGE_TYPE_MQTT)#{
    <<"server">> => SERVER,
    <<"username">> => <<"user1">>,
    <<"password">> => <<"">>,
    <<"proto_ver">> => <<"v5">>,
    <<"egress">> => #{
        <<"remote">> => #{
            <<"topic">> => <<"emqx/${topic}">>,
            <<"qos">> => <<"${qos}">>,
            <<"retain">> => false
        }
    }
}).
-define(MQTT_BRIDGE(SERVER), ?MQTT_BRIDGE(SERVER, <<"mqtt_egress_test_bridge">>)).

-define(HTTP_BRIDGE(URL, NAME), ?BRIDGE(NAME, ?BRIDGE_TYPE_HTTP)#{
    <<"url">> => URL,
    <<"local_topic">> => <<"emqx_webhook/#">>,
    <<"method">> => <<"post">>,
    <<"body">> => <<"${payload}">>,
    <<"headers">> => #{
        % NOTE
        % The Pascal-Case is important here.
        % The reason is kinda ridiculous: `emqx_bridge_resource:create_dry_run/2` converts
        % bridge config keys into atoms, and the atom 'Content-Type' exists in the ERTS
        % when this happens (while the 'content-type' does not).
        <<"Content-Type">> => <<"application/json">>
    }
}).
-define(HTTP_BRIDGE(URL), ?HTTP_BRIDGE(URL, ?BRIDGE_NAME)).

all() ->
    [
        {group, single},
        {group, cluster_later_join},
        {group, cluster}
    ].

groups() ->
    AllTCs = emqx_common_test_helpers:all(?MODULE),
    SingleOnlyTests = [
        t_broken_bpapi_vsn,
        t_old_bpapi_vsn,
        t_bridges_probe
    ],
    ClusterLaterJoinOnlyTCs = [t_cluster_later_join_metrics],
    [
        {single, [], AllTCs -- ClusterLaterJoinOnlyTCs},
        {cluster_later_join, [], ClusterLaterJoinOnlyTCs},
        {cluster, [], (AllTCs -- SingleOnlyTests) -- ClusterLaterJoinOnlyTCs}
    ].

suite() ->
    [{timetrap, {seconds, 60}}].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(cluster, Config) ->
    Cluster = mk_cluster_specs(Config),
    ct:pal("Starting ~p", [Cluster]),
    Nodes = [
        emqx_common_test_helpers:start_slave(Name, Opts)
     || {Name, Opts} <- Cluster
    ],
    [NodePrimary | NodesRest] = Nodes,
    ok = erpc:call(NodePrimary, fun() -> init_node(primary) end),
    _ = [ok = erpc:call(Node, fun() -> init_node(regular) end) || Node <- NodesRest],
    [{group, cluster}, {cluster_nodes, Nodes}, {api_node, NodePrimary} | Config];
init_per_group(cluster_later_join, Config) ->
    Cluster = mk_cluster_specs(Config, #{join_to => undefined}),
    ct:pal("Starting ~p", [Cluster]),
    Nodes = [
        emqx_common_test_helpers:start_slave(Name, Opts)
     || {Name, Opts} <- Cluster
    ],
    [NodePrimary | NodesRest] = Nodes,
    ok = erpc:call(NodePrimary, fun() -> init_node(primary) end),
    _ = [ok = erpc:call(Node, fun() -> init_node(regular) end) || Node <- NodesRest],
    [{group, cluster_later_join}, {cluster_nodes, Nodes}, {api_node, NodePrimary} | Config];
init_per_group(_, Config) ->
    ok = emqx_mgmt_api_test_util:init_suite(?SUITE_APPS),
    ok = load_suite_config(emqx_rule_engine),
    ok = load_suite_config(emqx_bridge),
    [{group, single}, {api_node, node()} | Config].

mk_cluster_specs(Config) ->
    mk_cluster_specs(Config, #{}).

mk_cluster_specs(Config, Opts) ->
    Specs = [
        {core, emqx_bridge_api_SUITE1, #{}},
        {core, emqx_bridge_api_SUITE2, #{}}
    ],
    CommonOpts = Opts#{
        env => [{emqx, boot_modules, [broker]}],
        apps => [],
        % NOTE
        % We need to start all those apps _after_ the cluster becomes stable, in the
        % `init_node/1`. This is because usual order is broken in very subtle way:
        % 1. Node starts apps including `mria` and `emqx_conf` which starts `emqx_cluster_rpc`.
        % 2. The `emqx_cluster_rpc` sets up a mnesia table subscription during initialization.
        % 3. In the meantime `mria` joins the cluster and notices it should restart.
        % 4. Mnesia subscription becomes lost during restarts (god knows why).
        % Yet we need to load them before, so that mria / mnesia will know which tables
        % should be created in the cluster.
        % TODO
        % We probably should hide these intricacies behind the `emqx_common_test_helpers`.
        load_apps => ?SUITE_APPS ++ [emqx_dashboard],
        env_handler => fun load_suite_config/1,
        load_schema => false,
        priv_data_dir => ?config(priv_dir, Config)
    },
    emqx_common_test_helpers:emqx_cluster(Specs, CommonOpts).

init_node(Type) ->
    ok = emqx_common_test_helpers:start_apps(?SUITE_APPS, fun load_suite_config/1),
    case Type of
        primary ->
            ok = emqx_dashboard_desc_cache:init(),
            ok = emqx_config:put(
                [dashboard, listeners],
                #{http => #{bind => 18083, proxy_header => false}}
            ),
            ok = emqx_dashboard:start_listeners(),
            ready = emqx_dashboard_listener:regenerate_minirest_dispatch(),
            emqx_common_test_http:create_default_app();
        regular ->
            ok
    end.

load_suite_config(emqx_rule_engine) ->
    ok = emqx_common_test_helpers:load_config(
        emqx_rule_engine_schema,
        <<"rule_engine { rules {} }">>
    );
load_suite_config(emqx_bridge) ->
    ok = emqx_common_test_helpers:load_config(
        emqx_bridge_schema,
        <<"bridges {}">>
    );
load_suite_config(_) ->
    ok.

end_per_group(Group, Config) when
    Group =:= cluster;
    Group =:= cluster_later_join
->
    ok = lists:foreach(
        fun(Node) ->
            _ = erpc:call(Node, emqx_common_test_helpers, stop_apps, [?SUITE_APPS]),
            emqx_common_test_helpers:stop_slave(Node)
        end,
        ?config(cluster_nodes, Config)
    );
end_per_group(_, _Config) ->
    emqx_mgmt_api_test_util:end_suite(?SUITE_APPS),
    ok.

init_per_testcase(t_broken_bpapi_vsn, Config) ->
    meck:new(emqx_bpapi, [passthrough]),
    meck:expect(emqx_bpapi, supported_version, 1, -1),
    meck:expect(emqx_bpapi, supported_version, 2, -1),
    init_per_testcase(commong, Config);
init_per_testcase(t_old_bpapi_vsn, Config) ->
    meck:new(emqx_bpapi, [passthrough]),
    meck:expect(emqx_bpapi, supported_version, 1, 1),
    meck:expect(emqx_bpapi, supported_version, 2, 1),
    init_per_testcase(common, Config);
init_per_testcase(_, Config) ->
    {Port, Sock, Acceptor} = start_http_server(fun handle_fun_200_ok/2),
    [{port, Port}, {sock, Sock}, {acceptor, Acceptor} | Config].

end_per_testcase(t_broken_bpapi_vsn, Config) ->
    meck:unload([emqx_bpapi]),
    end_per_testcase(common, Config);
end_per_testcase(t_old_bpapi_vsn, Config) ->
    meck:unload([emqx_bpapi]),
    end_per_testcase(common, Config);
end_per_testcase(_, Config) ->
    Sock = ?config(sock, Config),
    Acceptor = ?config(acceptor, Config),
    Node = ?config(api_node, Config),
    ok = emqx_common_test_helpers:call_janitor(),
    ok = stop_http_server(Sock, Acceptor),
    ok = erpc:call(Node, fun clear_resources/0),
    ok.

clear_resources() ->
    lists:foreach(
        fun(#{type := Type, name := Name}) ->
            {ok, _} = emqx_bridge:remove(Type, Name)
        end,
        emqx_bridge:list()
    ).

%%------------------------------------------------------------------------------
%% HTTP server for testing
%%------------------------------------------------------------------------------
start_http_server(HandleFun) ->
    process_flag(trap_exit, true),
    Parent = self(),
    {Port, Sock} = listen_on_random_port(),
    Acceptor = spawn_link(fun() ->
        accept_loop(Sock, HandleFun, Parent)
    end),
    timer:sleep(100),
    {Port, Sock, Acceptor}.

stop_http_server(Sock, Acceptor) ->
    exit(Acceptor, kill),
    gen_tcp:close(Sock).

listen_on_random_port() ->
    SockOpts = [binary, {active, false}, {packet, raw}, {reuseaddr, true}, {backlog, 1000}],
    case gen_tcp:listen(0, SockOpts) of
        {ok, Sock} ->
            {ok, Port} = inet:port(Sock),
            {Port, Sock};
        {error, Reason} when Reason /= eaddrinuse ->
            {error, Reason}
    end.

accept_loop(Sock, HandleFun, Parent) ->
    process_flag(trap_exit, true),
    {ok, Conn} = gen_tcp:accept(Sock),
    Handler = spawn_link(fun() -> HandleFun(Conn, Parent) end),
    gen_tcp:controlling_process(Conn, Handler),
    accept_loop(Sock, HandleFun, Parent).

make_response(CodeStr, Str) ->
    B = iolist_to_binary(Str),
    iolist_to_binary(
        io_lib:fwrite(
            "HTTP/1.0 ~s\r\nContent-Type: text/html\r\nContent-Length: ~p\r\n\r\n~s",
            [CodeStr, size(B), B]
        )
    ).

handle_fun_200_ok(Conn, Parent) ->
    case gen_tcp:recv(Conn, 0) of
        {ok, ReqStr} ->
            ct:pal("the http handler got request: ~p", [ReqStr]),
            Req = parse_http_request(ReqStr),
            Parent ! {http_server, received, Req},
            gen_tcp:send(Conn, make_response("200 OK", "Request OK")),
            handle_fun_200_ok(Conn, Parent);
        {error, Reason} ->
            ct:pal("the http handler recv error: ~p", [Reason]),
            timer:sleep(100),
            gen_tcp:close(Conn)
    end.

parse_http_request(ReqStr0) ->
    [Method, ReqStr1] = string:split(ReqStr0, " ", leading),
    [Path, ReqStr2] = string:split(ReqStr1, " ", leading),
    [_ProtoVsn, ReqStr3] = string:split(ReqStr2, "\r\n", leading),
    [_HeaderStr, Body] = string:split(ReqStr3, "\r\n\r\n", leading),
    #{method => Method, path => Path, body => Body}.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_http_crud_apis(Config) ->
    Port = ?config(port, Config),
    %% assert we there's no bridges at first
    {ok, 200, []} = request_json(get, uri(["bridges"]), Config),

    {ok, 404, _} = request(get, uri(["bridges", "foo"]), Config),
    {ok, 404, _} = request(get, uri(["bridges", "webhook:foo"]), Config),

    %% then we add a webhook bridge, using POST
    %% POST /bridges/ will create a bridge
    URL1 = ?URL(Port, "path1"),
    Name = ?BRIDGE_NAME,
    ?assertMatch(
        {ok, 201, #{
            <<"type">> := ?BRIDGE_TYPE_HTTP,
            <<"name">> := Name,
            <<"enable">> := true,
            <<"status">> := _,
            <<"node_status">> := [_ | _],
            <<"url">> := URL1
        }},
        request_json(
            post,
            uri(["bridges"]),
            ?HTTP_BRIDGE(URL1, Name),
            Config
        )
    ),

    BridgeID = emqx_bridge_resource:bridge_id(?BRIDGE_TYPE_HTTP, Name),
    %% send an message to emqx and the message should be forwarded to the HTTP server
    Body = <<"my msg">>,
    _ = publish_message(<<"emqx_webhook/1">>, Body, Config),
    ?assert(
        receive
            {http_server, received, #{
                method := <<"POST">>,
                path := <<"/path1">>,
                body := Body
            }} ->
                true;
            Msg ->
                ct:pal("error: http got unexpected request: ~p", [Msg]),
                false
        after 100 ->
            false
        end
    ),
    %% update the request-path of the bridge
    URL2 = ?URL(Port, "path2"),
    ?assertMatch(
        {ok, 200, #{
            <<"type">> := ?BRIDGE_TYPE_HTTP,
            <<"name">> := Name,
            <<"enable">> := true,
            <<"status">> := _,
            <<"node_status">> := [_ | _],
            <<"url">> := URL2
        }},
        request_json(
            put,
            uri(["bridges", BridgeID]),
            ?HTTP_BRIDGE(URL2, Name),
            Config
        )
    ),

    %% list all bridges again, assert Bridge2 is in it
    ?assertMatch(
        {ok, 200, [
            #{
                <<"type">> := ?BRIDGE_TYPE_HTTP,
                <<"name">> := Name,
                <<"enable">> := true,
                <<"status">> := _,
                <<"node_status">> := [_ | _],
                <<"url">> := URL2
            }
        ]},
        request_json(get, uri(["bridges"]), Config)
    ),

    %% get the bridge by id
    ?assertMatch(
        {ok, 200, #{
            <<"type">> := ?BRIDGE_TYPE_HTTP,
            <<"name">> := Name,
            <<"enable">> := true,
            <<"status">> := _,
            <<"node_status">> := [_ | _],
            <<"url">> := URL2
        }},
        request_json(get, uri(["bridges", BridgeID]), Config)
    ),

    %% send an message to emqx again, check the path has been changed
    _ = publish_message(<<"emqx_webhook/1">>, Body, Config),
    ?assert(
        receive
            {http_server, received, #{path := <<"/path2">>}} ->
                true;
            Msg2 ->
                ct:pal("error: http got unexpected request: ~p", [Msg2]),
                false
        after 100 ->
            false
        end
    ),

    %% Test bad updates
    %% ================

    %% Add bridge with a name that is too long
    %% We only support bridge names up to 255 characters
    LongName = list_to_binary(lists:duplicate(256, $a)),
    NameTooLongRequestResult = request_json(
        post,
        uri(["bridges"]),
        ?HTTP_BRIDGE(URL1, LongName),
        Config
    ),
    ?assertMatch(
        {ok, 400, _},
        NameTooLongRequestResult
    ),
    {ok, 400, #{<<"message">> := NameTooLongMessage}} = NameTooLongRequestResult,
    %% Use regex to check that the message contains the name
    Match = re:run(NameTooLongMessage, LongName),
    ?assertMatch({match, _}, Match),
    %% Add bridge without the URL field
    {ok, 400, PutFail1} = request_json(
        put,
        uri(["bridges", BridgeID]),
        maps:remove(<<"url">>, ?HTTP_BRIDGE(URL2, Name)),
        Config
    ),
    ?assertMatch(
        #{<<"reason">> := <<"required_field">>},
        json(maps:get(<<"message">>, PutFail1))
    ),
    {ok, 400, PutFail2} = request_json(
        put,
        uri(["bridges", BridgeID]),
        maps:put(<<"curl">>, URL2, maps:remove(<<"url">>, ?HTTP_BRIDGE(URL2, Name))),
        Config
    ),
    ?assertMatch(
        #{
            <<"reason">> := <<"unknown_fields">>,
            <<"unknown">> := <<"curl">>
        },
        json(maps:get(<<"message">>, PutFail2))
    ),
    {ok, 400, _} = request_json(
        put,
        uri(["bridges", BridgeID]),
        ?HTTP_BRIDGE(<<"localhost:1234/foo">>, Name),
        Config
    ),
    {ok, 400, _} = request_json(
        put,
        uri(["bridges", BridgeID]),
        ?HTTP_BRIDGE(<<"htpp://localhost:12341234/foo">>, Name),
        Config
    ),

    %% delete the bridge
    {ok, 204, <<>>} = request(delete, uri(["bridges", BridgeID]), Config),
    {ok, 200, []} = request_json(get, uri(["bridges"]), Config),

    %% update a deleted bridge returns an error
    ?assertMatch(
        {ok, 404, #{
            <<"code">> := <<"NOT_FOUND">>,
            <<"message">> := _
        }},
        request_json(
            put,
            uri(["bridges", BridgeID]),
            ?HTTP_BRIDGE(URL2, Name),
            Config
        )
    ),

    %% try delete bad bridge id
    ?assertMatch(
        {ok, 404, #{
            <<"code">> := <<"NOT_FOUND">>,
            <<"message">> := <<"Invalid bridge ID", _/binary>>
        }},
        request_json(delete, uri(["bridges", "foo"]), Config)
    ),

    %% Deleting a non-existing bridge should result in an error
    ?assertMatch(
        {ok, 404, #{
            <<"code">> := <<"NOT_FOUND">>,
            <<"message">> := _
        }},
        request_json(delete, uri(["bridges", BridgeID]), Config)
    ),

    %% Create non working bridge
    BrokenURL = ?URL(Port + 1, "/foo"),
    {ok, 201, BrokenBridge} = request(
        post,
        uri(["bridges"]),
        ?HTTP_BRIDGE(BrokenURL, Name),
        fun json/1,
        Config
    ),
    ?assertMatch(
        #{
            <<"type">> := ?BRIDGE_TYPE_HTTP,
            <<"name">> := Name,
            <<"enable">> := true,
            <<"status">> := <<"disconnected">>,
            <<"status_reason">> := <<"Connection refused">>,
            <<"node_status">> := [
                #{
                    <<"status">> := <<"disconnected">>,
                    <<"status_reason">> := <<"Connection refused">>
                }
                | _
            ],
            <<"url">> := BrokenURL
        },
        BrokenBridge
    ),

    {ok, 200, FixedBridge} = request_json(
        put,
        uri(["bridges", BridgeID]),
        ?HTTP_BRIDGE(URL1),
        Config
    ),
    ?assertMatch(
        #{
            <<"status">> := <<"connected">>,
            <<"node_status">> := [FixedNodeStatus = #{<<"status">> := <<"connected">>} | _]
        } when
            not is_map_key(<<"status_reason">>, FixedBridge) andalso
                not is_map_key(<<"status_reason">>, FixedNodeStatus),
        FixedBridge
    ),

    %% Try create bridge with bad characters as name
    {ok, 400, _} = request(post, uri(["bridges"]), ?HTTP_BRIDGE(URL1, <<"隋达"/utf8>>), Config),

    %% Missing scheme in URL
    {ok, 400, _} = request(
        post,
        uri(["bridges"]),
        ?HTTP_BRIDGE(<<"localhost:1234/foo">>, <<"missing_url_scheme">>),
        Config
    ),

    %% Invalid port
    {ok, 400, _} = request(
        post,
        uri(["bridges"]),
        ?HTTP_BRIDGE(<<"http://localhost:12341234/foo">>, <<"invalid_port">>),
        Config
    ),

    {ok, 204, <<>>} = request(delete, uri(["bridges", BridgeID]), Config).

t_http_bridges_local_topic(Config) ->
    Port = ?config(port, Config),
    %% assert we there's no bridges at first
    {ok, 200, []} = request_json(get, uri(["bridges"]), Config),

    %% then we add a webhook bridge, using POST
    %% POST /bridges/ will create a bridge
    URL1 = ?URL(Port, "path1"),
    Name1 = <<"t_http_bridges_with_local_topic1">>,
    Name2 = <<"t_http_bridges_without_local_topic1">>,
    %% create one http bridge with local_topic
    {ok, 201, _} = request(
        post,
        uri(["bridges"]),
        ?HTTP_BRIDGE(URL1, Name1),
        Config
    ),
    %% and we create another one without local_topic
    {ok, 201, _} = request(
        post,
        uri(["bridges"]),
        maps:remove(<<"local_topic">>, ?HTTP_BRIDGE(URL1, Name2)),
        Config
    ),
    BridgeID1 = emqx_bridge_resource:bridge_id(?BRIDGE_TYPE_HTTP, Name1),
    BridgeID2 = emqx_bridge_resource:bridge_id(?BRIDGE_TYPE_HTTP, Name2),
    %% Send an message to emqx and the message should be forwarded to the HTTP server.
    %% This is to verify we can have 2 bridges with and without local_topic fields
    %% at the same time.
    Body = <<"my msg">>,
    _ = publish_message(<<"emqx_webhook/1">>, Body, Config),
    ?assert(
        receive
            {http_server, received, #{
                method := <<"POST">>,
                path := <<"/path1">>,
                body := Body
            }} ->
                true;
            Msg ->
                ct:pal("error: http got unexpected request: ~p", [Msg]),
                false
        after 100 ->
            false
        end
    ),
    %% delete the bridge
    {ok, 204, <<>>} = request(delete, uri(["bridges", BridgeID1]), Config),
    {ok, 204, <<>>} = request(delete, uri(["bridges", BridgeID2]), Config).

t_check_dependent_actions_on_delete(Config) ->
    Port = ?config(port, Config),
    %% assert we there's no bridges at first
    {ok, 200, []} = request_json(get, uri(["bridges"]), Config),

    %% then we add a webhook bridge, using POST
    %% POST /bridges/ will create a bridge
    URL1 = ?URL(Port, "path1"),
    Name = <<"t_http_crud_apis">>,
    BridgeID = emqx_bridge_resource:bridge_id(?BRIDGE_TYPE_HTTP, Name),
    {ok, 201, _} = request(
        post,
        uri(["bridges"]),
        ?HTTP_BRIDGE(URL1, Name),
        Config
    ),
    {ok, 201, #{<<"id">> := RuleId}} = request_json(
        post,
        uri(["rules"]),
        #{
            <<"name">> => <<"t_http_crud_apis">>,
            <<"enable">> => true,
            <<"actions">> => [BridgeID],
            <<"sql">> => <<"SELECT * from \"t\"">>
        },
        Config
    ),
    %% deleting the bridge should fail because there is a rule that depends on it
    {ok, 400, _} = request(
        delete, uri(["bridges", BridgeID]) ++ "?also_delete_dep_actions=false", Config
    ),
    %% delete the rule first
    {ok, 204, <<>>} = request(delete, uri(["rules", RuleId]), Config),
    %% then delete the bridge is OK
    {ok, 204, <<>>} = request(delete, uri(["bridges", BridgeID]), Config),
    {ok, 200, []} = request_json(get, uri(["bridges"]), Config).

t_cascade_delete_actions(Config) ->
    Port = ?config(port, Config),
    %% assert we there's no bridges at first
    {ok, 200, []} = request_json(get, uri(["bridges"]), Config),

    %% then we add a webhook bridge, using POST
    %% POST /bridges/ will create a bridge
    URL1 = ?URL(Port, "path1"),
    Name = <<"t_http_crud_apis">>,
    BridgeID = emqx_bridge_resource:bridge_id(?BRIDGE_TYPE_HTTP, Name),
    {ok, 201, _} = request(
        post,
        uri(["bridges"]),
        ?HTTP_BRIDGE(URL1, Name),
        Config
    ),
    {ok, 201, #{<<"id">> := RuleId}} = request_json(
        post,
        uri(["rules"]),
        #{
            <<"name">> => <<"t_http_crud_apis">>,
            <<"enable">> => true,
            <<"actions">> => [BridgeID],
            <<"sql">> => <<"SELECT * from \"t\"">>
        },
        Config
    ),
    %% delete the bridge will also delete the actions from the rules
    {ok, 204, _} = request(
        delete,
        uri(["bridges", BridgeID]) ++ "?also_delete_dep_actions=true",
        Config
    ),
    {ok, 200, []} = request_json(get, uri(["bridges"]), Config),
    ?assertMatch(
        {ok, 200, #{<<"actions">> := []}},
        request_json(get, uri(["rules", RuleId]), Config)
    ),
    {ok, 204, <<>>} = request(delete, uri(["rules", RuleId]), Config),

    {ok, 201, _} = request(
        post,
        uri(["bridges"]),
        ?HTTP_BRIDGE(URL1, Name),
        Config
    ),
    {ok, 201, _} = request(
        post,
        uri(["rules"]),
        #{
            <<"name">> => <<"t_http_crud_apis">>,
            <<"enable">> => true,
            <<"actions">> => [BridgeID],
            <<"sql">> => <<"SELECT * from \"t\"">>
        },
        Config
    ),

    {ok, 204, _} = request(
        delete,
        uri(["bridges", BridgeID]) ++ "?also_delete_dep_actions",
        Config
    ),
    {ok, 200, []} = request_json(get, uri(["bridges"]), Config).

t_broken_bpapi_vsn(Config) ->
    Port = ?config(port, Config),
    URL1 = ?URL(Port, "abc"),
    Name = <<"t_bad_bpapi_vsn">>,
    {ok, 201, _Bridge} = request(
        post,
        uri(["bridges"]),
        ?HTTP_BRIDGE(URL1, Name),
        Config
    ),
    BridgeID = emqx_bridge_resource:bridge_id(?BRIDGE_TYPE_HTTP, Name),
    %% still works since we redirect to 'restart'
    {ok, 501, <<>>} = request(post, {operation, cluster, start, BridgeID}, Config),
    {ok, 501, <<>>} = request(post, {operation, node, start, BridgeID}, Config),
    ok.

t_old_bpapi_vsn(Config) ->
    Port = ?config(port, Config),
    URL1 = ?URL(Port, "abc"),
    Name = <<"t_bad_bpapi_vsn">>,
    {ok, 201, _Bridge} = request(
        post,
        uri(["bridges"]),
        ?HTTP_BRIDGE(URL1, Name),
        Config
    ),
    BridgeID = emqx_bridge_resource:bridge_id(?BRIDGE_TYPE_HTTP, Name),
    {ok, 204, <<>>} = request(post, {operation, cluster, stop, BridgeID}, Config),
    {ok, 204, <<>>} = request(post, {operation, node, stop, BridgeID}, Config),
    %% still works since we redirect to 'restart'
    {ok, 204, <<>>} = request(post, {operation, cluster, start, BridgeID}, Config),
    {ok, 204, <<>>} = request(post, {operation, node, start, BridgeID}, Config),
    {ok, 204, <<>>} = request(post, {operation, cluster, restart, BridgeID}, Config),
    {ok, 204, <<>>} = request(post, {operation, node, restart, BridgeID}, Config),
    ok.

t_start_bridge_unknown_node(Config) ->
    {ok, 404, _} =
        request(
            post,
            uri(["nodes", "thisbetterbenotanatomyet", "bridges", "webhook:foo", start]),
            Config
        ),
    {ok, 404, _} =
        request(
            post,
            uri(["nodes", "undefined", "bridges", "webhook:foo", start]),
            Config
        ).

t_start_stop_bridges_node(Config) ->
    do_start_stop_bridges(node, Config).

t_start_stop_bridges_cluster(Config) ->
    do_start_stop_bridges(cluster, Config).

do_start_stop_bridges(Type, Config) ->
    %% assert we there's no bridges at first
    {ok, 200, []} = request_json(get, uri(["bridges"]), Config),

    Port = ?config(port, Config),
    URL1 = ?URL(Port, "abc"),
    Name = atom_to_binary(Type),
    ?assertMatch(
        {ok, 201, #{
            <<"type">> := ?BRIDGE_TYPE_HTTP,
            <<"name">> := Name,
            <<"enable">> := true,
            <<"status">> := <<"connected">>,
            <<"node_status">> := [_ | _],
            <<"url">> := URL1
        }},
        request_json(
            post,
            uri(["bridges"]),
            ?HTTP_BRIDGE(URL1, Name),
            Config
        )
    ),

    BridgeID = emqx_bridge_resource:bridge_id(?BRIDGE_TYPE_HTTP, Name),
    ExpectedStatus =
        case ?config(group, Config) of
            cluster when Type == node ->
                <<"inconsistent">>;
            _ ->
                <<"stopped">>
        end,

    %% stop it
    {ok, 204, <<>>} = request(post, {operation, Type, stop, BridgeID}, Config),
    ?assertMatch(
        {ok, 200, #{<<"status">> := ExpectedStatus}},
        request_json(get, uri(["bridges", BridgeID]), Config)
    ),
    %% start again
    {ok, 204, <<>>} = request(post, {operation, Type, start, BridgeID}, Config),
    ?assertMatch(
        {ok, 200, #{<<"status">> := <<"connected">>}},
        request_json(get, uri(["bridges", BridgeID]), Config)
    ),
    %% start a started bridge
    {ok, 204, <<>>} = request(post, {operation, Type, start, BridgeID}, Config),
    ?assertMatch(
        {ok, 200, #{<<"status">> := <<"connected">>}},
        request_json(get, uri(["bridges", BridgeID]), Config)
    ),
    %% restart an already started bridge
    {ok, 204, <<>>} = request(post, {operation, Type, restart, BridgeID}, Config),
    ?assertMatch(
        {ok, 200, #{<<"status">> := <<"connected">>}},
        request_json(get, uri(["bridges", BridgeID]), Config)
    ),
    %% stop it again
    {ok, 204, <<>>} = request(post, {operation, Type, stop, BridgeID}, Config),
    %% restart a stopped bridge
    {ok, 204, <<>>} = request(post, {operation, Type, restart, BridgeID}, Config),
    ?assertMatch(
        {ok, 200, #{<<"status">> := <<"connected">>}},
        request_json(get, uri(["bridges", BridgeID]), Config)
    ),

    {ok, 404, _} = request(post, {operation, Type, invalidop, BridgeID}, Config),

    %% delete the bridge
    {ok, 204, <<>>} = request(delete, uri(["bridges", BridgeID]), Config),
    {ok, 200, []} = request_json(get, uri(["bridges"]), Config),

    %% Fail parse-id check
    {ok, 404, _} = request(post, {operation, Type, start, <<"wreckbook_fugazi">>}, Config),
    %% Looks ok but doesn't exist
    {ok, 404, _} = request(post, {operation, Type, start, <<"webhook:cptn_hook">>}, Config),

    %% Create broken bridge
    {ListenPort, Sock} = listen_on_random_port(),
    %% Connecting to this endpoint should always timeout
    BadServer = iolist_to_binary(io_lib:format("localhost:~B", [ListenPort])),
    BadName = <<"bad_", (atom_to_binary(Type))/binary>>,
    ?assertMatch(
        {ok, 201, #{
            <<"type">> := ?BRIDGE_TYPE_MQTT,
            <<"name">> := BadName,
            <<"enable">> := true,
            <<"server">> := BadServer,
            <<"status">> := <<"connecting">>,
            <<"node_status">> := [_ | _]
        }},
        request_json(
            post,
            uri(["bridges"]),
            ?MQTT_BRIDGE(BadServer, BadName),
            Config
        )
    ),
    BadBridgeID = emqx_bridge_resource:bridge_id(?BRIDGE_TYPE_MQTT, BadName),
    ?assertMatch(
        {ok, SC, _} when SC == 500 orelse SC == 503,
        request(post, {operation, Type, start, BadBridgeID}, Config)
    ),
    ok = gen_tcp:close(Sock),
    ok.

t_start_stop_inconsistent_bridge_node(Config) ->
    start_stop_inconsistent_bridge(node, Config).

t_start_stop_inconsistent_bridge_cluster(Config) ->
    start_stop_inconsistent_bridge(cluster, Config).

start_stop_inconsistent_bridge(Type, Config) ->
    Port = ?config(port, Config),
    URL = ?URL(Port, "abc"),
    Node = ?config(api_node, Config),

    erpc:call(Node, fun() ->
        meck:new(emqx_bridge_resource, [passthrough, no_link]),
        meck:expect(
            emqx_bridge_resource,
            stop,
            fun
                (_, <<"bridge_not_found">>) -> {error, not_found};
                (BridgeType, Name) -> meck:passthrough([BridgeType, Name])
            end
        )
    end),

    emqx_common_test_helpers:on_exit(fun() ->
        erpc:call(Node, fun() ->
            meck:unload([emqx_bridge_resource])
        end)
    end),

    {ok, 201, _Bridge} = request(
        post,
        uri(["bridges"]),
        ?HTTP_BRIDGE(URL, <<"bridge_not_found">>),
        Config
    ),
    {ok, 503, _} = request(
        post, {operation, Type, stop, <<"webhook:bridge_not_found">>}, Config
    ).

t_enable_disable_bridges(Config) ->
    %% assert we there's no bridges at first
    {ok, 200, []} = request_json(get, uri(["bridges"]), Config),

    Name = ?BRIDGE_NAME,
    Port = ?config(port, Config),
    URL1 = ?URL(Port, "abc"),
    ?assertMatch(
        {ok, 201, #{
            <<"type">> := ?BRIDGE_TYPE_HTTP,
            <<"name">> := Name,
            <<"enable">> := true,
            <<"status">> := <<"connected">>,
            <<"node_status">> := [_ | _],
            <<"url">> := URL1
        }},
        request_json(
            post,
            uri(["bridges"]),
            ?HTTP_BRIDGE(URL1, Name),
            Config
        )
    ),
    BridgeID = emqx_bridge_resource:bridge_id(?BRIDGE_TYPE_HTTP, Name),
    %% disable it
    {ok, 204, <<>>} = request(put, enable_path(false, BridgeID), Config),
    ?assertMatch(
        {ok, 200, #{<<"status">> := <<"stopped">>}},
        request_json(get, uri(["bridges", BridgeID]), Config)
    ),
    %% enable again
    {ok, 204, <<>>} = request(put, enable_path(true, BridgeID), Config),
    ?assertMatch(
        {ok, 200, #{<<"status">> := <<"connected">>}},
        request_json(get, uri(["bridges", BridgeID]), Config)
    ),
    %% enable an already started bridge
    {ok, 204, <<>>} = request(put, enable_path(true, BridgeID), Config),
    ?assertMatch(
        {ok, 200, #{<<"status">> := <<"connected">>}},
        request_json(get, uri(["bridges", BridgeID]), Config)
    ),
    %% disable it again
    {ok, 204, <<>>} = request(put, enable_path(false, BridgeID), Config),

    %% bad param
    {ok, 404, _} = request(put, enable_path(foo, BridgeID), Config),
    {ok, 404, _} = request(put, enable_path(true, "foo"), Config),
    {ok, 404, _} = request(put, enable_path(true, "webhook:foo"), Config),

    {ok, 400, Res} = request(post, {operation, node, start, BridgeID}, <<>>, fun json/1, Config),
    ?assertEqual(
        #{
            <<"code">> => <<"BAD_REQUEST">>,
            <<"message">> => <<"Forbidden operation, bridge not enabled">>
        },
        Res
    ),
    {ok, 400, Res} = request(post, {operation, cluster, start, BridgeID}, <<>>, fun json/1, Config),

    %% enable a stopped bridge
    {ok, 204, <<>>} = request(put, enable_path(true, BridgeID), Config),
    ?assertMatch(
        {ok, 200, #{<<"status">> := <<"connected">>}},
        request_json(get, uri(["bridges", BridgeID]), Config)
    ),
    %% delete the bridge
    {ok, 204, <<>>} = request(delete, uri(["bridges", BridgeID]), Config),
    {ok, 200, []} = request_json(get, uri(["bridges"]), Config).

t_reset_bridges(Config) ->
    %% assert there's no bridges at first
    {ok, 200, []} = request_json(get, uri(["bridges"]), Config),

    Name = ?BRIDGE_NAME,
    Port = ?config(port, Config),
    URL1 = ?URL(Port, "abc"),
    ?assertMatch(
        {ok, 201, #{
            <<"type">> := ?BRIDGE_TYPE_HTTP,
            <<"name">> := Name,
            <<"enable">> := true,
            <<"status">> := <<"connected">>,
            <<"node_status">> := [_ | _],
            <<"url">> := URL1
        }},
        request_json(
            post,
            uri(["bridges"]),
            ?HTTP_BRIDGE(URL1, Name),
            Config
        )
    ),
    BridgeID = emqx_bridge_resource:bridge_id(?BRIDGE_TYPE_HTTP, Name),
    {ok, 204, <<>>} = request(put, uri(["bridges", BridgeID, "metrics/reset"]), Config),

    %% delete the bridge
    {ok, 204, <<>>} = request(delete, uri(["bridges", BridgeID]), Config),
    {ok, 200, []} = request_json(get, uri(["bridges"]), Config).

t_with_redact_update(Config) ->
    Name = <<"redact_update">>,
    Type = <<"mqtt">>,
    Password = <<"123456">>,
    Template = #{
        <<"type">> => Type,
        <<"name">> => Name,
        <<"server">> => <<"127.0.0.1:1883">>,
        <<"username">> => <<"test">>,
        <<"password">> => Password,
        <<"ingress">> =>
            #{<<"remote">> => #{<<"topic">> => <<"t/#">>}}
    },

    {ok, 201, _} = request(
        post,
        uri(["bridges"]),
        Template,
        Config
    ),

    %% update with redacted config
    BridgeConf = emqx_utils:redact(Template),
    BridgeID = emqx_bridge_resource:bridge_id(Type, Name),
    {ok, 200, _} = request(put, uri(["bridges", BridgeID]), BridgeConf, Config),
    ?assertEqual(
        Password,
        get_raw_config([bridges, Type, Name, password], Config)
    ),
    ok.

t_bridges_probe(Config) ->
    Port = ?config(port, Config),
    URL = ?URL(Port, "some_path"),

    {ok, 204, <<>>} = request(
        post,
        uri(["bridges_probe"]),
        ?HTTP_BRIDGE(URL),
        Config
    ),

    %% second time with same name is ok since no real bridge created
    {ok, 204, <<>>} = request(
        post,
        uri(["bridges_probe"]),
        ?HTTP_BRIDGE(URL),
        Config
    ),

    ?assertMatch(
        {ok, 400, #{
            <<"code">> := <<"TEST_FAILED">>,
            <<"message">> := _
        }},
        request_json(
            post,
            uri(["bridges_probe"]),
            ?HTTP_BRIDGE(<<"http://203.0.113.3:1234/foo">>),
            Config
        )
    ),

    %% Missing scheme in URL
    ?assertMatch(
        {ok, 400, #{
            <<"code">> := <<"TEST_FAILED">>,
            <<"message">> := _
        }},
        request_json(
            post,
            uri(["bridges_probe"]),
            ?HTTP_BRIDGE(<<"203.0.113.3:1234/foo">>),
            Config
        )
    ),

    %% Invalid port
    ?assertMatch(
        {ok, 400, #{
            <<"code">> := <<"TEST_FAILED">>,
            <<"message">> := _
        }},
        request_json(
            post,
            uri(["bridges_probe"]),
            ?HTTP_BRIDGE(<<"http://203.0.113.3:12341234/foo">>),
            Config
        )
    ),

    {ok, 204, _} = request(
        post,
        uri(["bridges_probe"]),
        ?MQTT_BRIDGE(<<"127.0.0.1:1883">>),
        Config
    ),

    ?assertMatch(
        {ok, 400, #{
            <<"code">> := <<"TEST_FAILED">>,
            <<"message">> := <<"Connection refused">>
        }},
        request_json(
            post,
            uri(["bridges_probe"]),
            ?MQTT_BRIDGE(<<"127.0.0.1:2883">>),
            Config
        )
    ),

    ?assertMatch(
        {ok, 400, #{
            <<"code">> := <<"TEST_FAILED">>,
            <<"message">> := <<"Could not resolve host">>
        }},
        request_json(
            post,
            uri(["bridges_probe"]),
            ?MQTT_BRIDGE(<<"nohost:2883">>),
            Config
        )
    ),

    AuthnConfig = #{
        <<"mechanism">> => <<"password_based">>,
        <<"backend">> => <<"built_in_database">>,
        <<"user_id_type">> => <<"username">>
    },
    Chain = 'mqtt:global',
    {ok, _} = update_config(
        [authentication],
        {create_authenticator, Chain, AuthnConfig},
        Config
    ),
    User = #{user_id => <<"u">>, password => <<"p">>},
    AuthenticatorID = <<"password_based:built_in_database">>,
    {ok, _} = add_user_auth(
        Chain,
        AuthenticatorID,
        User,
        Config
    ),

    emqx_common_test_helpers:on_exit(fun() ->
        delete_user_auth(Chain, AuthenticatorID, User, Config)
    end),

    ?assertMatch(
        {ok, 400, #{
            <<"code">> := <<"TEST_FAILED">>,
            <<"message">> := <<"Unauthorized client">>
        }},
        request_json(
            post,
            uri(["bridges_probe"]),
            ?MQTT_BRIDGE(<<"127.0.0.1:1883">>)#{<<"proto_ver">> => <<"v4">>},
            Config
        )
    ),

    ?assertMatch(
        {ok, 400, #{
            <<"code">> := <<"TEST_FAILED">>,
            <<"message">> := <<"Bad username or password">>
        }},
        request_json(
            post,
            uri(["bridges_probe"]),
            ?MQTT_BRIDGE(<<"127.0.0.1:1883">>)#{
                <<"proto_ver">> => <<"v4">>,
                <<"password">> => <<"mySecret">>,
                <<"username">> => <<"u">>
            },
            Config
        )
    ),

    ?assertMatch(
        {ok, 400, #{
            <<"code">> := <<"TEST_FAILED">>,
            <<"message">> := <<"Not authorized">>
        }},
        request_json(
            post,
            uri(["bridges_probe"]),
            ?MQTT_BRIDGE(<<"127.0.0.1:1883">>),
            Config
        )
    ),

    ?assertMatch(
        {ok, 400, #{<<"code">> := <<"BAD_REQUEST">>}},
        request_json(
            post,
            uri(["bridges_probe"]),
            ?BRIDGE(<<"bad_bridge">>, <<"unknown_type">>),
            Config
        )
    ),
    ok.

t_metrics(Config) ->
    Port = ?config(port, Config),
    %% assert we there's no bridges at first
    {ok, 200, []} = request_json(get, uri(["bridges"]), Config),

    %% then we add a webhook bridge, using POST
    %% POST /bridges/ will create a bridge
    URL1 = ?URL(Port, "path1"),
    Name = ?BRIDGE_NAME,
    ?assertMatch(
        {ok, 201,
            Bridge = #{
                <<"type">> := ?BRIDGE_TYPE_HTTP,
                <<"name">> := Name,
                <<"enable">> := true,
                <<"status">> := _,
                <<"node_status">> := [_ | _],
                <<"url">> := URL1
            }} when
            %% assert that the bridge return doesn't contain metrics anymore
            not is_map_key(<<"metrics">>, Bridge) andalso
                not is_map_key(<<"node_metrics">>, Bridge),
        request_json(
            post,
            uri(["bridges"]),
            ?HTTP_BRIDGE(URL1, Name),
            Config
        )
    ),

    BridgeID = emqx_bridge_resource:bridge_id(?BRIDGE_TYPE_HTTP, Name),

    %% check for empty bridge metrics
    ?assertMatch(
        {ok, 200, #{
            <<"metrics">> := #{<<"success">> := 0},
            <<"node_metrics">> := [_ | _]
        }},
        request_json(get, uri(["bridges", BridgeID, "metrics"]), Config)
    ),

    %% check that the bridge doesn't contain metrics anymore
    {ok, 200, Bridge} = request_json(get, uri(["bridges", BridgeID]), Config),
    ?assertNot(maps:is_key(<<"metrics">>, Bridge)),
    ?assertNot(maps:is_key(<<"node_metrics">>, Bridge)),

    %% send an message to emqx and the message should be forwarded to the HTTP server
    Body = <<"my msg">>,
    _ = publish_message(<<"emqx_webhook/1">>, Body, Config),
    ?assert(
        receive
            {http_server, received, #{
                method := <<"POST">>,
                path := <<"/path1">>,
                body := Body
            }} ->
                true;
            Msg ->
                ct:pal("error: http got unexpected request: ~p", [Msg]),
                false
        after 100 ->
            false
        end
    ),

    %% check for non-empty bridge metrics
    ?assertMatch(
        {ok, 200, #{
            <<"metrics">> := #{<<"success">> := _},
            <<"node_metrics">> := [_ | _]
        }},
        request_json(get, uri(["bridges", BridgeID, "metrics"]), Config)
    ),

    %% check for absence of metrics when listing all bridges
    {ok, 200, Bridges} = request_json(get, uri(["bridges"]), Config),
    ?assertNotMatch(
        [
            #{
                <<"metrics">> := #{},
                <<"node_metrics">> := [_ | _]
            }
        ],
        Bridges
    ),
    ok.

%% request_timeout in bridge root should match request_ttl in
%% resource_opts.
t_inconsistent_webhook_request_timeouts(Config) ->
    Port = ?config(port, Config),
    URL1 = ?URL(Port, "path1"),
    Name = ?BRIDGE_NAME,
    BadBridgeParams =
        emqx_utils_maps:deep_merge(
            ?HTTP_BRIDGE(URL1, Name),
            #{
                <<"request_timeout">> => <<"1s">>,
                <<"resource_opts">> => #{<<"request_ttl">> => <<"2s">>}
            }
        ),
    %% root request_timeout is deprecated for bridge.
    {ok, 201,
        #{
            <<"resource_opts">> := ResourceOpts
        } = Response} =
        request_json(
            post,
            uri(["bridges"]),
            BadBridgeParams,
            Config
        ),
    ?assertNot(maps:is_key(<<"request_timeout">>, Response)),
    ?assertMatch(#{<<"request_ttl">> := <<"2s">>}, ResourceOpts),
    validate_resource_request_ttl(proplists:get_value(group, Config), 2000, Name),
    ok.

t_cluster_later_join_metrics(Config) ->
    Port = ?config(port, Config),
    APINode = ?config(api_node, Config),
    ClusterNodes = ?config(cluster_nodes, Config),
    [OtherNode | _] = ClusterNodes -- [APINode],
    URL1 = ?URL(Port, "path1"),
    Name = ?BRIDGE_NAME,
    BridgeParams = ?HTTP_BRIDGE(URL1, Name),
    BridgeID = emqx_bridge_resource:bridge_id(?BRIDGE_TYPE_HTTP, Name),
    ?check_trace(
        begin
            %% Create a bridge on only one of the nodes.
            ?assertMatch({ok, 201, _}, request_json(post, uri(["bridges"]), BridgeParams, Config)),
            %% Pre-condition.
            ?assertMatch(
                {ok, 200, #{
                    <<"metrics">> := #{<<"success">> := _},
                    <<"node_metrics">> := [_ | _]
                }},
                request_json(get, uri(["bridges", BridgeID, "metrics"]), Config)
            ),
            %% Now join the other node join with the api node.
            ok = erpc:call(OtherNode, ekka, join, [APINode]),
            %% Check metrics; shouldn't crash even if the bridge is not
            %% ready on the node that just joined the cluster.
            ?assertMatch(
                {ok, 200, #{
                    <<"metrics">> := #{<<"success">> := _},
                    <<"node_metrics">> := [#{<<"metrics">> := #{}}, #{<<"metrics">> := #{}} | _]
                }},
                request_json(get, uri(["bridges", BridgeID, "metrics"]), Config)
            ),
            ok
        end,
        []
    ),
    ok.

validate_resource_request_ttl(single, Timeout, Name) ->
    SentData = #{payload => <<"Hello EMQX">>, timestamp => 1668602148000},
    BridgeID = emqx_bridge_resource:bridge_id(?BRIDGE_TYPE_HTTP, Name),
    ResId = emqx_bridge_resource:resource_id(<<"webhook">>, Name),
    ?check_trace(
        begin
            {ok, Res} =
                ?wait_async_action(
                    emqx_bridge:send_message(BridgeID, SentData),
                    #{?snk_kind := async_query},
                    1000
                ),
            ?assertMatch({ok, #{id := ResId, query_opts := #{timeout := Timeout}}}, Res)
        end,
        fun(Trace0) ->
            Trace = ?of_kind(async_query, Trace0),
            ?assertMatch([#{query_opts := #{timeout := Timeout}}], Trace),
            ok
        end
    );
validate_resource_request_ttl(_Cluster, _Timeout, _Name) ->
    ignore.

%%

request(Method, URL, Config) ->
    request(Method, URL, [], Config).

request(Method, {operation, Type, Op, BridgeID}, Body, Config) ->
    URL = operation_path(Type, Op, BridgeID, Config),
    request(Method, URL, Body, Config);
request(Method, URL, Body, Config) ->
    Opts = #{compatible_mode => true, httpc_req_opts => [{body_format, binary}]},
    emqx_mgmt_api_test_util:request_api(Method, URL, [], auth_header(Config), Body, Opts).

request(Method, URL, Body, Decoder, Config) ->
    case request(Method, URL, Body, Config) of
        {ok, Code, Response} ->
            {ok, Code, Decoder(Response)};
        Otherwise ->
            Otherwise
    end.

request_json(Method, URLLike, Config) ->
    request(Method, URLLike, [], fun json/1, Config).

request_json(Method, URLLike, Body, Config) ->
    request(Method, URLLike, Body, fun json/1, Config).

auth_header(Config) ->
    erpc:call(?config(api_node, Config), emqx_common_test_http, default_auth_header, []).

operation_path(node, Oper, BridgeID, Config) ->
    uri(["nodes", ?config(api_node, Config), "bridges", BridgeID, Oper]);
operation_path(cluster, Oper, BridgeID, _Config) ->
    uri(["bridges", BridgeID, Oper]).

enable_path(Enable, BridgeID) ->
    uri(["bridges", BridgeID, "enable", Enable]).

publish_message(Topic, Body, Config) ->
    Node = ?config(api_node, Config),
    erpc:call(Node, emqx, publish, [emqx_message:make(Topic, Body)]).

update_config(Path, Value, Config) ->
    Node = ?config(api_node, Config),
    erpc:call(Node, emqx, update_config, [Path, Value]).

get_raw_config(Path, Config) ->
    Node = ?config(api_node, Config),
    erpc:call(Node, emqx, get_raw_config, [Path]).

add_user_auth(Chain, AuthenticatorID, User, Config) ->
    Node = ?config(api_node, Config),
    erpc:call(Node, emqx_authentication, add_user, [Chain, AuthenticatorID, User]).

delete_user_auth(Chain, AuthenticatorID, User, Config) ->
    Node = ?config(api_node, Config),
    erpc:call(Node, emqx_authentication, delete_user, [Chain, AuthenticatorID, User]).

str(S) when is_list(S) -> S;
str(S) when is_binary(S) -> binary_to_list(S).

json(B) when is_binary(B) ->
    emqx_utils_json:decode(B, [return_maps]).
