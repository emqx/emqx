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

-import(emqx_mgmt_api_test_util, [request/3, uri/1]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-define(CONF_DEFAULT, <<"bridges: {}">>).
-define(BRIDGE_TYPE, <<"webhook">>).
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
    <<"proto_ver">> => <<"v5">>
}).
-define(MQTT_BRIDGE(SERVER), ?MQTT_BRIDGE(SERVER, <<"mqtt_egress_test_bridge">>)).

-define(HTTP_BRIDGE(URL, TYPE, NAME), ?BRIDGE(NAME, TYPE)#{
    <<"url">> => URL,
    <<"local_topic">> => <<"emqx_webhook/#">>,
    <<"method">> => <<"post">>,
    <<"body">> => <<"${payload}">>,
    <<"headers">> => #{
        <<"content-type">> => <<"application/json">>
    }
}).

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

suite() ->
    [{timetrap, {seconds, 60}}].

init_per_suite(Config) ->
    _ = application:load(emqx_conf),
    %% some testcases (may from other app) already get emqx_connector started
    _ = application:stop(emqx_resource),
    _ = application:stop(emqx_connector),
    ok = emqx_mgmt_api_test_util:init_suite(
        [emqx_rule_engine, emqx_bridge, emqx_authn]
    ),
    ok = emqx_common_test_helpers:load_config(
        emqx_rule_engine_schema,
        <<"rule_engine {rules {}}">>
    ),
    ok = emqx_common_test_helpers:load_config(emqx_bridge_schema, ?CONF_DEFAULT),
    Config.

end_per_suite(_Config) ->
    emqx_mgmt_api_test_util:end_suite([emqx_rule_engine, emqx_bridge, emqx_authn]),
    mria:clear_table(emqx_authn_mnesia),
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
    {ok, _} = emqx_cluster_rpc:start_link(node(), emqx_cluster_rpc, 1000),
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
    stop_http_server(Sock, Acceptor),
    clear_resources(),
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
    {ok, 200, <<"[]">>} = request(get, uri(["bridges"]), []),

    {ok, 404, _} = request(get, uri(["bridges", "foo"]), []),
    {ok, 404, _} = request(get, uri(["bridges", "webhook:foo"]), []),

    %% then we add a webhook bridge, using POST
    %% POST /bridges/ will create a bridge
    URL1 = ?URL(Port, "path1"),
    Name = ?BRIDGE_NAME,
    {ok, 201, Bridge} = request(
        post,
        uri(["bridges"]),
        ?HTTP_BRIDGE(URL1, ?BRIDGE_TYPE, Name)
    ),

    %ct:pal("---bridge: ~p", [Bridge]),
    #{
        <<"type">> := ?BRIDGE_TYPE,
        <<"name">> := Name,
        <<"enable">> := true,
        <<"status">> := _,
        <<"node_status">> := [_ | _],
        <<"url">> := URL1
    } = emqx_json:decode(Bridge, [return_maps]),

    BridgeID = emqx_bridge_resource:bridge_id(?BRIDGE_TYPE, Name),
    %% send an message to emqx and the message should be forwarded to the HTTP server
    Body = <<"my msg">>,
    emqx:publish(emqx_message:make(<<"emqx_webhook/1">>, Body)),
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
    {ok, 200, Bridge2} = request(
        put,
        uri(["bridges", BridgeID]),
        ?HTTP_BRIDGE(URL2, ?BRIDGE_TYPE, Name)
    ),
    ?assertMatch(
        #{
            <<"type">> := ?BRIDGE_TYPE,
            <<"name">> := Name,
            <<"enable">> := true,
            <<"status">> := _,
            <<"node_status">> := [_ | _],
            <<"url">> := URL2
        },
        emqx_json:decode(Bridge2, [return_maps])
    ),

    %% list all bridges again, assert Bridge2 is in it
    {ok, 200, Bridge2Str} = request(get, uri(["bridges"]), []),
    ?assertMatch(
        [
            #{
                <<"type">> := ?BRIDGE_TYPE,
                <<"name">> := Name,
                <<"enable">> := true,
                <<"status">> := _,
                <<"node_status">> := [_ | _],
                <<"url">> := URL2
            }
        ],
        emqx_json:decode(Bridge2Str, [return_maps])
    ),

    %% get the bridge by id
    {ok, 200, Bridge3Str} = request(get, uri(["bridges", BridgeID]), []),
    ?assertMatch(
        #{
            <<"type">> := ?BRIDGE_TYPE,
            <<"name">> := Name,
            <<"enable">> := true,
            <<"status">> := _,
            <<"node_status">> := [_ | _],
            <<"url">> := URL2
        },
        emqx_json:decode(Bridge3Str, [return_maps])
    ),

    %% send an message to emqx again, check the path has been changed
    emqx:publish(emqx_message:make(<<"emqx_webhook/1">>, Body)),
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

    %% delete the bridge
    {ok, 204, <<>>} = request(delete, uri(["bridges", BridgeID]), []),
    {ok, 200, <<"[]">>} = request(get, uri(["bridges"]), []),

    %% update a deleted bridge returns an error
    {ok, 404, ErrMsg2} = request(
        put,
        uri(["bridges", BridgeID]),
        ?HTTP_BRIDGE(URL2, ?BRIDGE_TYPE, Name)
    ),
    ?assertMatch(
        #{
            <<"code">> := <<"NOT_FOUND">>,
            <<"message">> := _
        },
        emqx_json:decode(ErrMsg2, [return_maps])
    ),

    %% try delete bad bridge id
    {ok, 404, BadId} = request(delete, uri(["bridges", "foo"]), []),
    ?assertMatch(
        #{
            <<"code">> := <<"NOT_FOUND">>,
            <<"message">> := <<"Invalid bridge ID", _/binary>>
        },
        emqx_json:decode(BadId, [return_maps])
    ),

    %% Deleting a non-existing bridge should result in an error
    {ok, 404, ErrMsg3} = request(delete, uri(["bridges", BridgeID]), []),
    ?assertMatch(
        #{
            <<"code">> := <<"NOT_FOUND">>,
            <<"message">> := _
        },
        emqx_json:decode(ErrMsg3, [return_maps])
    ),
    ok.

t_http_bridges_local_topic(Config) ->
    Port = ?config(port, Config),
    %% assert we there's no bridges at first
    {ok, 200, <<"[]">>} = request(get, uri(["bridges"]), []),

    %% then we add a webhook bridge, using POST
    %% POST /bridges/ will create a bridge
    URL1 = ?URL(Port, "path1"),
    Name1 = <<"t_http_bridges_with_local_topic1">>,
    Name2 = <<"t_http_bridges_without_local_topic1">>,
    %% create one http bridge with local_topic
    {ok, 201, _} = request(
        post,
        uri(["bridges"]),
        ?HTTP_BRIDGE(URL1, ?BRIDGE_TYPE, Name1)
    ),
    %% and we create another one without local_topic
    {ok, 201, _} = request(
        post,
        uri(["bridges"]),
        maps:remove(<<"local_topic">>, ?HTTP_BRIDGE(URL1, ?BRIDGE_TYPE, Name2))
    ),
    BridgeID1 = emqx_bridge_resource:bridge_id(?BRIDGE_TYPE, Name1),
    BridgeID2 = emqx_bridge_resource:bridge_id(?BRIDGE_TYPE, Name2),
    %% Send an message to emqx and the message should be forwarded to the HTTP server.
    %% This is to verify we can have 2 bridges with and without local_topic fields
    %% at the same time.
    Body = <<"my msg">>,
    emqx:publish(emqx_message:make(<<"emqx_webhook/1">>, Body)),
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
    {ok, 204, <<>>} = request(delete, uri(["bridges", BridgeID1]), []),
    {ok, 204, <<>>} = request(delete, uri(["bridges", BridgeID2]), []),
    ok.

t_check_dependent_actions_on_delete(Config) ->
    Port = ?config(port, Config),
    %% assert we there's no bridges at first
    {ok, 200, <<"[]">>} = request(get, uri(["bridges"]), []),

    %% then we add a webhook bridge, using POST
    %% POST /bridges/ will create a bridge
    URL1 = ?URL(Port, "path1"),
    Name = <<"t_http_crud_apis">>,
    BridgeID = emqx_bridge_resource:bridge_id(?BRIDGE_TYPE, Name),
    {ok, 201, _} = request(
        post,
        uri(["bridges"]),
        ?HTTP_BRIDGE(URL1, ?BRIDGE_TYPE, Name)
    ),
    {ok, 201, Rule} = request(
        post,
        uri(["rules"]),
        #{
            <<"name">> => <<"t_http_crud_apis">>,
            <<"enable">> => true,
            <<"actions">> => [BridgeID],
            <<"sql">> => <<"SELECT * from \"t\"">>
        }
    ),
    #{<<"id">> := RuleId} = emqx_json:decode(Rule, [return_maps]),
    %% deleting the bridge should fail because there is a rule that depends on it
    {ok, 400, _} = request(
        delete, uri(["bridges", BridgeID]) ++ "?also_delete_dep_actions=false", []
    ),
    %% delete the rule first
    {ok, 204, <<>>} = request(delete, uri(["rules", RuleId]), []),
    %% then delete the bridge is OK
    {ok, 204, <<>>} = request(delete, uri(["bridges", BridgeID]), []),
    {ok, 200, <<"[]">>} = request(get, uri(["bridges"]), []),

    ok.

t_cascade_delete_actions(Config) ->
    Port = ?config(port, Config),
    %% assert we there's no bridges at first
    {ok, 200, <<"[]">>} = request(get, uri(["bridges"]), []),

    %% then we add a webhook bridge, using POST
    %% POST /bridges/ will create a bridge
    URL1 = ?URL(Port, "path1"),
    Name = <<"t_http_crud_apis">>,
    BridgeID = emqx_bridge_resource:bridge_id(?BRIDGE_TYPE, Name),
    {ok, 201, _} = request(
        post,
        uri(["bridges"]),
        ?HTTP_BRIDGE(URL1, ?BRIDGE_TYPE, Name)
    ),
    {ok, 201, Rule} = request(
        post,
        uri(["rules"]),
        #{
            <<"name">> => <<"t_http_crud_apis">>,
            <<"enable">> => true,
            <<"actions">> => [BridgeID],
            <<"sql">> => <<"SELECT * from \"t\"">>
        }
    ),
    #{<<"id">> := RuleId} = emqx_json:decode(Rule, [return_maps]),
    %% delete the bridge will also delete the actions from the rules
    {ok, 204, _} = request(
        delete, uri(["bridges", BridgeID]) ++ "?also_delete_dep_actions=true", []
    ),
    {ok, 200, <<"[]">>} = request(get, uri(["bridges"]), []),
    {ok, 200, Rule1} = request(get, uri(["rules", RuleId]), []),
    ?assertMatch(
        #{
            <<"actions">> := []
        },
        emqx_json:decode(Rule1, [return_maps])
    ),
    {ok, 204, <<>>} = request(delete, uri(["rules", RuleId]), []),

    {ok, 201, _} = request(
        post,
        uri(["bridges"]),
        ?HTTP_BRIDGE(URL1, ?BRIDGE_TYPE, Name)
    ),
    {ok, 201, _} = request(
        post,
        uri(["rules"]),
        #{
            <<"name">> => <<"t_http_crud_apis">>,
            <<"enable">> => true,
            <<"actions">> => [BridgeID],
            <<"sql">> => <<"SELECT * from \"t\"">>
        }
    ),

    {ok, 204, _} = request(delete, uri(["bridges", BridgeID]) ++ "?also_delete_dep_actions", []),
    {ok, 200, <<"[]">>} = request(get, uri(["bridges"]), []),
    ok.

t_broken_bpapi_vsn(Config) ->
    Port = ?config(port, Config),
    URL1 = ?URL(Port, "abc"),
    Name = <<"t_bad_bpapi_vsn">>,
    {ok, 201, _Bridge} = request(
        post,
        uri(["bridges"]),
        ?HTTP_BRIDGE(URL1, ?BRIDGE_TYPE, Name)
    ),
    BridgeID = emqx_bridge_resource:bridge_id(?BRIDGE_TYPE, Name),
    %% still works since we redirect to 'restart'
    {ok, 501, <<>>} = request(post, operation_path(cluster, start, BridgeID), <<"">>),
    {ok, 501, <<>>} = request(post, operation_path(node, start, BridgeID), <<"">>),
    ok.

t_old_bpapi_vsn(Config) ->
    Port = ?config(port, Config),
    URL1 = ?URL(Port, "abc"),
    Name = <<"t_bad_bpapi_vsn">>,
    {ok, 201, _Bridge} = request(
        post,
        uri(["bridges"]),
        ?HTTP_BRIDGE(URL1, ?BRIDGE_TYPE, Name)
    ),
    BridgeID = emqx_bridge_resource:bridge_id(?BRIDGE_TYPE, Name),
    {ok, 204, <<>>} = request(post, operation_path(cluster, stop, BridgeID), <<"">>),
    {ok, 204, <<>>} = request(post, operation_path(node, stop, BridgeID), <<"">>),
    %% still works since we redirect to 'restart'
    {ok, 204, <<>>} = request(post, operation_path(cluster, start, BridgeID), <<"">>),
    {ok, 204, <<>>} = request(post, operation_path(node, start, BridgeID), <<"">>),
    {ok, 204, <<>>} = request(post, operation_path(cluster, restart, BridgeID), <<"">>),
    {ok, 204, <<>>} = request(post, operation_path(node, restart, BridgeID), <<"">>),
    ok.

t_start_stop_bridges_node(Config) ->
    {ok, 404, _} =
        request(
            post,
            uri(["nodes", "thisbetterbenotanatomyet", "bridges", "webhook:foo", start]),
            <<"">>
        ),
    {ok, 404, _} =
        request(
            post,
            uri(["nodes", "undefined", "bridges", "webhook:foo", start]),
            <<"">>
        ),
    do_start_stop_bridges(node, Config).

t_start_stop_bridges_cluster(Config) ->
    do_start_stop_bridges(cluster, Config).

do_start_stop_bridges(Type, Config) ->
    %% assert we there's no bridges at first
    {ok, 200, <<"[]">>} = request(get, uri(["bridges"]), []),

    Port = ?config(port, Config),
    URL1 = ?URL(Port, "abc"),
    Name = atom_to_binary(Type),
    {ok, 201, Bridge} = request(
        post,
        uri(["bridges"]),
        ?HTTP_BRIDGE(URL1, ?BRIDGE_TYPE, Name)
    ),
    %ct:pal("the bridge ==== ~p", [Bridge]),
    #{
        <<"type">> := ?BRIDGE_TYPE,
        <<"name">> := Name,
        <<"enable">> := true,
        <<"status">> := <<"connected">>,
        <<"node_status">> := [_ | _],
        <<"url">> := URL1
    } = emqx_json:decode(Bridge, [return_maps]),
    BridgeID = emqx_bridge_resource:bridge_id(?BRIDGE_TYPE, Name),
    %% stop it
    {ok, 204, <<>>} = request(post, operation_path(Type, stop, BridgeID), <<"">>),
    {ok, 200, Bridge2} = request(get, uri(["bridges", BridgeID]), []),
    ?assertMatch(#{<<"status">> := <<"stopped">>}, emqx_json:decode(Bridge2, [return_maps])),
    %% start again
    {ok, 204, <<>>} = request(post, operation_path(Type, start, BridgeID), <<"">>),
    {ok, 200, Bridge3} = request(get, uri(["bridges", BridgeID]), []),
    ?assertMatch(#{<<"status">> := <<"connected">>}, emqx_json:decode(Bridge3, [return_maps])),
    %% start a started bridge
    {ok, 204, <<>>} = request(post, operation_path(Type, start, BridgeID), <<"">>),
    {ok, 200, Bridge3_1} = request(get, uri(["bridges", BridgeID]), []),
    ?assertMatch(#{<<"status">> := <<"connected">>}, emqx_json:decode(Bridge3_1, [return_maps])),
    %% restart an already started bridge
    {ok, 204, <<>>} = request(post, operation_path(Type, restart, BridgeID), <<"">>),
    {ok, 200, Bridge3} = request(get, uri(["bridges", BridgeID]), []),
    ?assertMatch(#{<<"status">> := <<"connected">>}, emqx_json:decode(Bridge3, [return_maps])),
    %% stop it again
    {ok, 204, <<>>} = request(post, operation_path(Type, stop, BridgeID), <<"">>),
    %% restart a stopped bridge
    {ok, 204, <<>>} = request(post, operation_path(Type, restart, BridgeID), <<"">>),
    {ok, 200, Bridge4} = request(get, uri(["bridges", BridgeID]), []),
    ?assertMatch(#{<<"status">> := <<"connected">>}, emqx_json:decode(Bridge4, [return_maps])),

    {ok, 404, _} = request(post, operation_path(Type, invalidop, BridgeID), <<"">>),

    %% delete the bridge
    {ok, 204, <<>>} = request(delete, uri(["bridges", BridgeID]), []),
    {ok, 200, <<"[]">>} = request(get, uri(["bridges"]), []),

    %% Fail parse-id check
    {ok, 404, _} = request(post, operation_path(Type, start, <<"wreckbook_fugazi">>), <<"">>),
    %% Looks ok but doesn't exist
    {ok, 404, _} = request(post, operation_path(Type, start, <<"webhook:cptn_hook">>), <<"">>),

    %% Create broken bridge
    {ListenPort, Sock} = listen_on_random_port(),
    %% Connecting to this endpoint should always timeout
    BadServer = iolist_to_binary(io_lib:format("localhost:~B", [ListenPort])),
    BadName = <<"bad_", (atom_to_binary(Type))/binary>>,
    {ok, 201, BadBridge1} = request(
        post,
        uri(["bridges"]),
        ?MQTT_BRIDGE(BadServer, BadName)
    ),
    #{
        <<"type">> := ?BRIDGE_TYPE_MQTT,
        <<"name">> := BadName,
        <<"enable">> := true,
        <<"server">> := BadServer,
        <<"status">> := <<"connecting">>,
        <<"node_status">> := [_ | _]
    } = emqx_json:decode(BadBridge1, [return_maps]),
    BadBridgeID = emqx_bridge_resource:bridge_id(?BRIDGE_TYPE_MQTT, BadName),
    ?assertMatch(
        {ok, SC, _} when SC == 500 orelse SC == 503,
        request(post, operation_path(Type, start, BadBridgeID), <<"">>)
    ),
    ok = gen_tcp:close(Sock),
    ok.

t_enable_disable_bridges(Config) ->
    %% assert we there's no bridges at first
    {ok, 200, <<"[]">>} = request(get, uri(["bridges"]), []),

    Name = ?BRIDGE_NAME,
    Port = ?config(port, Config),
    URL1 = ?URL(Port, "abc"),
    {ok, 201, Bridge} = request(
        post,
        uri(["bridges"]),
        ?HTTP_BRIDGE(URL1, ?BRIDGE_TYPE, Name)
    ),
    %ct:pal("the bridge ==== ~p", [Bridge]),
    #{
        <<"type">> := ?BRIDGE_TYPE,
        <<"name">> := Name,
        <<"enable">> := true,
        <<"status">> := <<"connected">>,
        <<"node_status">> := [_ | _],
        <<"url">> := URL1
    } = emqx_json:decode(Bridge, [return_maps]),
    BridgeID = emqx_bridge_resource:bridge_id(?BRIDGE_TYPE, Name),
    %% disable it
    {ok, 204, <<>>} = request(put, enable_path(false, BridgeID), <<"">>),
    {ok, 200, Bridge2} = request(get, uri(["bridges", BridgeID]), []),
    ?assertMatch(#{<<"status">> := <<"stopped">>}, emqx_json:decode(Bridge2, [return_maps])),
    %% enable again
    {ok, 204, <<>>} = request(put, enable_path(true, BridgeID), <<"">>),
    {ok, 200, Bridge3} = request(get, uri(["bridges", BridgeID]), []),
    ?assertMatch(#{<<"status">> := <<"connected">>}, emqx_json:decode(Bridge3, [return_maps])),
    %% enable an already started bridge
    {ok, 204, <<>>} = request(put, enable_path(true, BridgeID), <<"">>),
    {ok, 200, Bridge3} = request(get, uri(["bridges", BridgeID]), []),
    ?assertMatch(#{<<"status">> := <<"connected">>}, emqx_json:decode(Bridge3, [return_maps])),
    %% disable it again
    {ok, 204, <<>>} = request(put, enable_path(false, BridgeID), <<"">>),

    %% bad param
    {ok, 404, _} = request(put, enable_path(foo, BridgeID), <<"">>),
    {ok, 404, _} = request(put, enable_path(true, "foo"), <<"">>),
    {ok, 404, _} = request(put, enable_path(true, "webhook:foo"), <<"">>),

    {ok, 400, Res} = request(post, operation_path(node, start, BridgeID), <<"">>),
    ?assertEqual(
        <<"{\"code\":\"BAD_REQUEST\",\"message\":\"Forbidden operation, bridge not enabled\"}">>,
        Res
    ),
    {ok, 400, Res} = request(post, operation_path(cluster, start, BridgeID), <<"">>),

    %% enable a stopped bridge
    {ok, 204, <<>>} = request(put, enable_path(true, BridgeID), <<"">>),
    {ok, 200, Bridge4} = request(get, uri(["bridges", BridgeID]), []),
    ?assertMatch(#{<<"status">> := <<"connected">>}, emqx_json:decode(Bridge4, [return_maps])),
    %% delete the bridge
    {ok, 204, <<>>} = request(delete, uri(["bridges", BridgeID]), []),
    {ok, 200, <<"[]">>} = request(get, uri(["bridges"]), []).

t_reset_bridges(Config) ->
    %% assert there's no bridges at first
    {ok, 200, <<"[]">>} = request(get, uri(["bridges"]), []),

    Name = ?BRIDGE_NAME,
    Port = ?config(port, Config),
    URL1 = ?URL(Port, "abc"),
    {ok, 201, Bridge} = request(
        post,
        uri(["bridges"]),
        ?HTTP_BRIDGE(URL1, ?BRIDGE_TYPE, Name)
    ),
    %ct:pal("the bridge ==== ~p", [Bridge]),
    #{
        <<"type">> := ?BRIDGE_TYPE,
        <<"name">> := Name,
        <<"enable">> := true,
        <<"status">> := <<"connected">>,
        <<"node_status">> := [_ | _],
        <<"url">> := URL1
    } = emqx_json:decode(Bridge, [return_maps]),
    BridgeID = emqx_bridge_resource:bridge_id(?BRIDGE_TYPE, Name),
    {ok, 204, <<>>} = request(put, uri(["bridges", BridgeID, "metrics/reset"]), []),

    %% delete the bridge
    {ok, 204, <<>>} = request(delete, uri(["bridges", BridgeID]), []),
    {ok, 200, <<"[]">>} = request(get, uri(["bridges"]), []).

t_with_redact_update(_Config) ->
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
        Template
    ),

    %% update with redacted config
    Conf = emqx_misc:redact(Template),
    BridgeID = emqx_bridge_resource:bridge_id(Type, Name),
    {ok, 200, _ResBin} = request(
        put,
        uri(["bridges", BridgeID]),
        Conf
    ),
    RawConf = emqx:get_raw_config([bridges, Type, Name]),
    Value = maps:get(<<"password">>, RawConf),
    ?assertEqual(Password, Value),
    ok.

t_bridges_probe(Config) ->
    Port = ?config(port, Config),
    URL = ?URL(Port, "some_path"),

    {ok, 204, <<>>} = request(
        post,
        uri(["bridges_probe"]),
        ?HTTP_BRIDGE(URL, ?BRIDGE_TYPE, ?BRIDGE_NAME)
    ),

    %% second time with same name is ok since no real bridge created
    {ok, 204, <<>>} = request(
        post,
        uri(["bridges_probe"]),
        ?HTTP_BRIDGE(URL, ?BRIDGE_TYPE, ?BRIDGE_NAME)
    ),

    {ok, 400, NxDomain} = request(
        post,
        uri(["bridges_probe"]),
        ?HTTP_BRIDGE(<<"http://203.0.113.3:1234/foo">>, ?BRIDGE_TYPE, ?BRIDGE_NAME)
    ),
    ?assertMatch(
        #{
            <<"code">> := <<"TEST_FAILED">>,
            <<"message">> := _
        },
        emqx_json:decode(NxDomain, [return_maps])
    ),

    {ok, 204, _} = request(
        post,
        uri(["bridges_probe"]),
        ?MQTT_BRIDGE(<<"127.0.0.1:1883">>)
    ),

    {ok, 400, ConnRefused} = request(
        post,
        uri(["bridges_probe"]),
        ?MQTT_BRIDGE(<<"127.0.0.1:2883">>)
    ),
    ?assertMatch(
        #{
            <<"code">> := <<"TEST_FAILED">>,
            <<"message">> := <<"Connection refused">>
        },
        emqx_json:decode(ConnRefused, [return_maps])
    ),

    {ok, 400, HostNotFound} = request(
        post,
        uri(["bridges_probe"]),
        ?MQTT_BRIDGE(<<"nohost:2883">>)
    ),
    ?assertMatch(
        #{
            <<"code">> := <<"TEST_FAILED">>,
            <<"message">> := <<"Host not found">>
        },
        emqx_json:decode(HostNotFound, [return_maps])
    ),

    AuthnConfig = #{
        <<"mechanism">> => <<"password_based">>,
        <<"backend">> => <<"built_in_database">>,
        <<"user_id_type">> => <<"username">>
    },
    Chain = 'mqtt:global',
    emqx:update_config(
        [authentication],
        {create_authenticator, Chain, AuthnConfig}
    ),
    User = #{user_id => <<"u">>, password => <<"p">>},
    AuthenticatorID = <<"password_based:built_in_database">>,
    {ok, _} = emqx_authentication:add_user(
        Chain,
        AuthenticatorID,
        User
    ),

    {ok, 400, Unauthorized} = request(
        post,
        uri(["bridges_probe"]),
        ?MQTT_BRIDGE(<<"127.0.0.1:1883">>)#{<<"proto_ver">> => <<"v4">>}
    ),
    ?assertMatch(
        #{
            <<"code">> := <<"TEST_FAILED">>,
            <<"message">> := <<"Unauthorized client">>
        },
        emqx_json:decode(Unauthorized, [return_maps])
    ),

    {ok, 400, Malformed} = request(
        post,
        uri(["bridges_probe"]),
        ?MQTT_BRIDGE(<<"127.0.0.1:1883">>)#{
            <<"proto_ver">> => <<"v4">>, <<"password">> => <<"mySecret">>, <<"username">> => <<"u">>
        }
    ),
    ?assertMatch(
        #{
            <<"code">> := <<"TEST_FAILED">>,
            <<"message">> := <<"Malformed username or password">>
        },
        emqx_json:decode(Malformed, [return_maps])
    ),

    {ok, 400, NotAuthorized} = request(
        post,
        uri(["bridges_probe"]),
        ?MQTT_BRIDGE(<<"127.0.0.1:1883">>)
    ),
    ?assertMatch(
        #{
            <<"code">> := <<"TEST_FAILED">>,
            <<"message">> := <<"Not authorized">>
        },
        emqx_json:decode(NotAuthorized, [return_maps])
    ),

    {ok, 400, BadReq} = request(
        post,
        uri(["bridges_probe"]),
        ?BRIDGE(<<"bad_bridge">>, <<"unknown_type">>)
    ),
    ?assertMatch(#{<<"code">> := <<"BAD_REQUEST">>}, emqx_json:decode(BadReq, [return_maps])),
    ok.

t_metrics(Config) ->
    Port = ?config(port, Config),
    %% assert we there's no bridges at first
    {ok, 200, <<"[]">>} = request(get, uri(["bridges"]), []),

    %% then we add a webhook bridge, using POST
    %% POST /bridges/ will create a bridge
    URL1 = ?URL(Port, "path1"),
    Name = ?BRIDGE_NAME,
    {ok, 201, Bridge} = request(
        post,
        uri(["bridges"]),
        ?HTTP_BRIDGE(URL1, ?BRIDGE_TYPE, Name)
    ),

    %ct:pal("---bridge: ~p", [Bridge]),
    Decoded = emqx_json:decode(Bridge, [return_maps]),
    #{
        <<"type">> := ?BRIDGE_TYPE,
        <<"name">> := Name,
        <<"enable">> := true,
        <<"status">> := _,
        <<"node_status">> := [_ | _],
        <<"url">> := URL1
    } = Decoded,

    %% assert that the bridge return doesn't contain metrics anymore
    ?assertNot(maps:is_key(<<"metrics">>, Decoded)),
    ?assertNot(maps:is_key(<<"node_metrics">>, Decoded)),

    BridgeID = emqx_bridge_resource:bridge_id(?BRIDGE_TYPE, Name),

    %% check for empty bridge metrics
    {ok, 200, Bridge1Str} = request(get, uri(["bridges", BridgeID, "metrics"]), []),
    ?assertMatch(
        #{
            <<"metrics">> := #{<<"success">> := 0},
            <<"node_metrics">> := [_ | _]
        },
        emqx_json:decode(Bridge1Str, [return_maps])
    ),

    %% check that the bridge doesn't contain metrics anymore
    {ok, 200, Bridge2Str} = request(get, uri(["bridges", BridgeID]), []),
    Decoded2 = emqx_json:decode(Bridge2Str, [return_maps]),
    ?assertNot(maps:is_key(<<"metrics">>, Decoded2)),
    ?assertNot(maps:is_key(<<"node_metrics">>, Decoded2)),

    %% send an message to emqx and the message should be forwarded to the HTTP server
    Body = <<"my msg">>,
    emqx:publish(emqx_message:make(<<"emqx_webhook/1">>, Body)),
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
    {ok, 200, Bridge3Str} = request(get, uri(["bridges", BridgeID, "metrics"]), []),
    ?assertMatch(
        #{
            <<"metrics">> := #{<<"success">> := _},
            <<"node_metrics">> := [_ | _]
        },
        emqx_json:decode(Bridge3Str, [return_maps])
    ),

    %% check that metrics isn't returned when listing all bridges
    {ok, 200, BridgesStr} = request(get, uri(["bridges"]), []),
    ?assert(
        lists:all(
            fun(E) -> not maps:is_key(<<"metrics">>, E) end,
            emqx_json:decode(BridgesStr, [return_maps])
        )
    ),
    ok.

%% request_timeout in bridge root should match request_timeout in
%% resource_opts.
t_inconsistent_webhook_request_timeouts(Config) ->
    Port = ?config(port, Config),
    URL1 = ?URL(Port, "path1"),
    Name = ?BRIDGE_NAME,
    BadBridgeParams =
        emqx_map_lib:deep_merge(
            ?HTTP_BRIDGE(URL1, ?BRIDGE_TYPE, Name),
            #{
                <<"request_timeout">> => <<"1s">>,
                <<"resource_opts">> => #{<<"request_timeout">> => <<"2s">>}
            }
        ),
    {ok, 201, RawResponse} = request(
        post,
        uri(["bridges"]),
        BadBridgeParams
    ),
    %% note: same value on both fields
    ?assertMatch(
        #{
            <<"request_timeout">> := <<"2s">>,
            <<"resource_opts">> := #{<<"request_timeout">> := <<"2s">>}
        },
        emqx_json:decode(RawResponse, [return_maps])
    ),
    ok.

operation_path(node, Oper, BridgeID) ->
    uri(["nodes", node(), "bridges", BridgeID, Oper]);
operation_path(cluster, Oper, BridgeID) ->
    uri(["bridges", BridgeID, Oper]).

enable_path(Enable, BridgeID) ->
    uri(["bridges", BridgeID, "enable", Enable]).

str(S) when is_list(S) -> S;
str(S) when is_binary(S) -> binary_to_list(S).
