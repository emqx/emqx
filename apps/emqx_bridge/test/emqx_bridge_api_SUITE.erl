%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-import(emqx_common_test_helpers, [on_exit/1]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/test_macros.hrl").

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

-define(APPSPECS, [
    emqx,
    emqx_conf,
    emqx_auth,
    emqx_auth_mnesia,
    emqx_management,
    emqx_connector,
    emqx_bridge_http,
    {emqx_bridge, "actions {}\n bridges {}"},
    {emqx_rule_engine, "rule_engine { rules {} }"}
]).

-define(APPSPEC_DASHBOARD,
    {emqx_dashboard, "dashboard.listeners.http { enable = true, bind = 18083 }"}
).

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
    [{timetrap, {seconds, 120}}].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(cluster = Name, Config) ->
    Nodes = [NodePrimary | _] = mk_cluster(Name, Config),
    init_api([{group, Name}, {cluster_nodes, Nodes}, {node, NodePrimary} | Config]);
init_per_group(cluster_later_join = Name, Config) ->
    Nodes = [NodePrimary | _] = mk_cluster(Name, Config, #{join_to => undefined}),
    init_api([{group, Name}, {cluster_nodes, Nodes}, {node, NodePrimary} | Config]);
init_per_group(_Name, Config) ->
    WorkDir = emqx_cth_suite:work_dir(Config),
    Apps = emqx_cth_suite:start(?APPSPECS ++ [?APPSPEC_DASHBOARD], #{work_dir => WorkDir}),
    init_api([{group, single}, {group_apps, Apps}, {node, node()} | Config]).

init_api(Config) ->
    APINode = ?config(node, Config),
    {ok, App} = erpc:call(APINode, emqx_common_test_http, create_default_app, []),
    [{api, App} | Config].

mk_cluster(Name, Config) ->
    mk_cluster(Name, Config, #{}).

mk_cluster(Name, Config, Opts) ->
    Node1Apps = ?APPSPECS ++ [?APPSPEC_DASHBOARD],
    Node2Apps = ?APPSPECS,
    emqx_cth_cluster:start(
        [
            {emqx_bridge_api_SUITE1, Opts#{role => core, apps => Node1Apps}},
            {emqx_bridge_api_SUITE2, Opts#{role => core, apps => Node2Apps}}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Name, Config)}
    ).

end_per_group(Group, Config) when
    Group =:= cluster;
    Group =:= cluster_later_join
->
    ok = emqx_cth_cluster:stop(?config(cluster_nodes, Config));
end_per_group(_, Config) ->
    emqx_cth_suite:stop(?config(group_apps, Config)),
    ok.

init_per_testcase(t_broken_bpapi_vsn, Config) ->
    meck:new(emqx_bpapi, [passthrough]),
    meck:expect(emqx_bpapi, supported_version, 2, -1),
    meck:new(emqx_bridge_api, [passthrough]),
    meck:expect(emqx_bridge_api, supported_versions, 1, []),
    init_per_testcase(common, Config);
init_per_testcase(t_old_bpapi_vsn, Config) ->
    meck:new(emqx_bpapi, [passthrough]),
    meck:expect(emqx_bpapi, supported_version, 1, 1),
    meck:expect(emqx_bpapi, supported_version, 2, 1),
    init_per_testcase(common, Config);
init_per_testcase(_, Config) ->
    {Port, Sock, Acceptor} = start_http_server(fun handle_fun_200_ok/2),
    [{port, Port}, {sock, Sock}, {acceptor, Acceptor} | Config].

end_per_testcase(t_broken_bpapi_vsn, Config) ->
    meck:unload(),
    end_per_testcase(common, Config);
end_per_testcase(t_old_bpapi_vsn, Config) ->
    meck:unload(),
    end_per_testcase(common, Config);
end_per_testcase(_, Config) ->
    Sock = ?config(sock, Config),
    Acceptor = ?config(acceptor, Config),
    Node = ?config(node, Config),
    ok = emqx_common_test_helpers:call_janitor(),
    ok = stop_http_server(Sock, Acceptor),
    ok = erpc:call(Node, fun clear_resources/0),
    ok.

clear_resources() ->
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    lists:foreach(
        fun(#{type := Type, name := Name}) ->
            ok = emqx_bridge:remove(Type, Name)
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
        #{<<"reason">> := <<"required_field">>},
        json(maps:get(<<"message">>, PutFail2))
    ),
    {ok, 400, _} = request_json(
        put,
        uri(["bridges", BridgeID]),
        ?HTTP_BRIDGE(<<"localhost:1234/foo">>, Name),
        Config
    ),
    {ok, 400, PutFail3} = request_json(
        put,
        uri(["bridges", BridgeID]),
        ?HTTP_BRIDGE(<<"htpp://localhost:12341234/foo">>, Name),
        Config
    ),
    ?assertMatch(
        #{<<"kind">> := <<"validation_error">>},
        json(maps:get(<<"message">>, PutFail3))
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
    BrokenURL = ?URL(Port + 1, "foo"),
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
    {ok, 400, Body} = request(
        delete, uri(["bridges", BridgeID]) ++ "?also_delete_dep_actions=false", Config
    ),
    ?assertMatch(#{<<"rules">> := [_ | _]}, emqx_utils_json:decode(Body, [return_maps])),
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
    CreateRes0 = request_json(
        post,
        uri(["bridges"]),
        ?MQTT_BRIDGE(BadServer, BadName),
        Config
    ),
    ?assertMatch(
        {ok, 201, #{
            <<"type">> := ?BRIDGE_TYPE_MQTT,
            <<"name">> := BadName,
            <<"enable">> := true,
            <<"server">> := BadServer
        }},
        CreateRes0
    ),
    {ok, 201, CreateRes1} = CreateRes0,
    case CreateRes1 of
        #{
            <<"node_status">> := [
                #{
                    <<"status">> := <<"disconnected">>,
                    <<"status_reason">> := <<"connack_timeout">>
                },
                #{<<"status">> := <<"connecting">>}
                | _
            ],
            %% `inconsistent': one node is `?status_disconnected' (because it has already
            %% timed out), the other node is `?status_connecting' (started later and
            %% haven't timed out yet)
            <<"status">> := <<"inconsistent">>,
            <<"status_reason">> := <<"connack_timeout">>
        } ->
            ok;
        #{
            <<"node_status">> := [_, _ | _],
            <<"status">> := <<"disconnected">>,
            <<"status_reason">> := <<"connack_timeout">>
        } ->
            ok;
        #{
            <<"node_status">> := [_],
            <<"status">> := <<"connecting">>
        } ->
            ok;
        _ ->
            error({unexpected_result, CreateRes1})
    end,
    BadBridgeID = emqx_bridge_resource:bridge_id(?BRIDGE_TYPE_MQTT, BadName),
    ?assertMatch(
        %% request from product: return 400 on such errors
        {ok, SC, _} when SC == 500 orelse SC == 400,
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
    Node = ?config(node, Config),

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

    on_exit(fun() ->
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
    ok = snabbkaffe:start_trace(),
    on_exit(fun() -> ok = snabbkaffe:stop() end),
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
    %% bridge is migrated after creation
    ConfigRootKey = connectors,
    ?assertEqual(
        Password,
        get_raw_config([ConfigRootKey, Type, Name, password], Config)
    ),

    %% probe with new password; should not be considered redacted
    {_, {ok, #{params := UsedParams}}} =
        ?wait_async_action(
            request(
                post,
                uri(["bridges_probe"]),
                Template#{<<"password">> := <<"newpassword">>},
                Config
            ),
            #{?snk_kind := bridge_v1_api_dry_run},
            1_000
        ),
    UsedPassword0 = maps:get(<<"password">>, UsedParams),
    %% the password field schema makes
    %% `emqx_dashboard_swagger:filter_check_request_and_translate_body' wrap the password.
    %% hack: this fails with `badfun' in CI only, due to cover compile, if not evaluated
    %% in the original node...
    PrimaryNode = ?config(node, Config),
    erpc:call(PrimaryNode, fun() -> ?assertEqual(<<"newpassword">>, UsedPassword0()) end),
    ok = snabbkaffe:stop(),

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
    %% with descriptions is ok.
    {ok, 204, <<>>} = request(
        post,
        uri(["bridges_probe"]),
        (?HTTP_BRIDGE(URL))#{<<"description">> => <<"Test Description">>},
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
            <<"message">> := <<"Connection refused", _/binary>>
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

    on_exit(fun() ->
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
    [PrimaryNode, OtherNode | _] = ?config(cluster_nodes, Config),
    URL1 = ?URL(Port, "path1"),
    Name = ?BRIDGE_NAME,
    BridgeParams = ?HTTP_BRIDGE(URL1, Name),
    BridgeID = emqx_bridge_resource:bridge_id(?BRIDGE_TYPE_HTTP, Name),

    ?check_trace(
        #{timetrap => 15_000},
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

            ct:print("node joining cluster"),
            %% Now join the other node join with the api node.
            ok = erpc:call(OtherNode, ekka, join, [PrimaryNode]),
            %% Hack / workaround for the fact that `emqx_machine_boot' doesn't restart the
            %% applications, in particular `emqx_conf' doesn't restart and synchronize the
            %% transaction id.  It's also unclear at the moment why the equivalent test in
            %% `emqx_bridge_v2_api_SUITE' doesn't need this hack.
            ok = erpc:call(OtherNode, application, stop, [emqx_conf]),
            ok = erpc:call(OtherNode, application, start, [emqx_conf]),
            ct:print("node joined cluster"),

            %% assert: wait for the bridge to be ready on the other node.
            {_, {ok, _}} =
                ?wait_async_action(
                    {emqx_cluster_rpc, OtherNode} ! wake_up,
                    #{?snk_kind := cluster_rpc_caught_up, ?snk_meta := #{node := OtherNode}},
                    10_000
                ),

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

t_create_with_bad_name(Config) ->
    Port = ?config(port, Config),
    URL1 = ?URL(Port, "path1"),
    Name = <<"test_哈哈"/utf8>>,
    BadBridgeParams =
        emqx_utils_maps:deep_merge(
            ?HTTP_BRIDGE(URL1, Name),
            #{
                <<"ssl">> =>
                    #{
                        <<"enable">> => true,
                        <<"certfile">> => cert_file("certfile")
                    }
            }
        ),
    {ok, 400, #{
        <<"code">> := <<"BAD_REQUEST">>,
        <<"message">> := Msg0
    }} =
        request_json(
            post,
            uri(["bridges"]),
            BadBridgeParams,
            Config
        ),
    Msg = emqx_utils_json:decode(Msg0, [return_maps]),
    ?assertMatch(
        #{
            <<"kind">> := <<"validation_error">>,
            <<"reason">> := <<"invalid_map_key">>
        },
        Msg
    ),
    ok.

validate_resource_request_ttl(single, Timeout, Name) ->
    SentData = #{payload => <<"Hello EMQX">>, timestamp => 1668602148000},
    _BridgeID = emqx_bridge_resource:bridge_id(?BRIDGE_TYPE_HTTP, Name),
    ?check_trace(
        begin
            {ok, Res} =
                ?wait_async_action(
                    do_send_message(?BRIDGE_TYPE_HTTP, Name, SentData),
                    #{?snk_kind := async_query},
                    1000
                ),
            ?assertMatch({ok, #{id := _ResId, query_opts := #{timeout := Timeout}}}, Res)
        end,
        fun(Trace0) ->
            Trace = ?of_kind(async_query, Trace0),
            ?assertMatch([#{query_opts := #{timeout := Timeout}}], Trace),
            ok
        end
    );
validate_resource_request_ttl(_Cluster, _Timeout, _Name) ->
    ignore.

do_send_message(BridgeV1Type, Name, Message) ->
    Type = emqx_bridge_v2:bridge_v1_type_to_bridge_v2_type(BridgeV1Type),
    emqx_bridge_v2:send_message(Type, Name, Message, #{}).

%%

request(Method, URL, Config) ->
    request(Method, URL, [], Config).

request(Method, {operation, Type, Op, BridgeID}, Body, Config) ->
    URL = operation_path(Type, Op, BridgeID, Config),
    request(Method, URL, Body, Config);
request(Method, URL, Body, Config) ->
    AuthHeader = emqx_common_test_http:auth_header(?config(api, Config)),
    Opts = #{compatible_mode => true, httpc_req_opts => [{body_format, binary}]},
    emqx_mgmt_api_test_util:request_api(Method, URL, [], AuthHeader, Body, Opts).

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

operation_path(node, Oper, BridgeID, Config) ->
    uri(["nodes", ?config(node, Config), "bridges", BridgeID, Oper]);
operation_path(cluster, Oper, BridgeID, _Config) ->
    uri(["bridges", BridgeID, Oper]).

enable_path(Enable, BridgeID) ->
    uri(["bridges", BridgeID, "enable", Enable]).

publish_message(Topic, Body, Config) ->
    Node = ?config(node, Config),
    erpc:call(Node, emqx, publish, [emqx_message:make(Topic, Body)]).

update_config(Path, Value, Config) ->
    Node = ?config(node, Config),
    erpc:call(Node, emqx, update_config, [Path, Value]).

get_raw_config(Path, Config) ->
    Node = ?config(node, Config),
    erpc:call(Node, emqx, get_raw_config, [Path]).

add_user_auth(Chain, AuthenticatorID, User, Config) ->
    Node = ?config(node, Config),
    erpc:call(Node, emqx_authn_chains, add_user, [Chain, AuthenticatorID, User]).

delete_user_auth(Chain, AuthenticatorID, User, Config) ->
    Node = ?config(node, Config),
    erpc:call(Node, emqx_authn_chains, delete_user, [Chain, AuthenticatorID, User]).

str(S) when is_list(S) -> S;
str(S) when is_binary(S) -> binary_to_list(S).

json(B) when is_binary(B) ->
    emqx_utils_json:decode(B, [return_maps]).

data_file(Name) ->
    Dir = code:lib_dir(emqx_bridge),
    {ok, Bin} = file:read_file(filename:join([Dir, "test", "data", Name])),
    Bin.

cert_file(Name) ->
    data_file(filename:join(["certs", Name])).
