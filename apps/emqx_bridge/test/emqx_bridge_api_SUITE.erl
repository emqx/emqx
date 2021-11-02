%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-define(CONF_DEFAULT, <<"bridges: {}">>).

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

suite() ->
	[{timetrap,{seconds,30}}].

init_per_suite(Config) ->
    ok = emqx_config:put([emqx_dashboard], #{
        default_username => <<"admin">>,
        default_password => <<"public">>,
        listeners => [#{
            protocol => http,
            port => 18083
        }]
    }),
    ok = emqx_common_test_helpers:start_apps([emqx_bridge, emqx_dashboard]),
    ok = emqx_config:init_load(emqx_bridge_schema, ?CONF_DEFAULT),
    Config.

end_per_suite(_Config) ->
    ok = ekka:stop(),
    emqx_common_test_helpers:stop_apps([emqx_bridge, emqx_dashboard]),
    ok.

init_per_testcase(_, Config) ->
    {ok, _} = emqx_cluster_rpc:start_link(node(), emqx_cluster_rpc, 1000),
    Config.
end_per_testcase(_, _Config) ->
    ok.

-define(URL1, <<"http://localhost:9901/path1">>).
-define(URL2, <<"http://localhost:9901/path2">>).
-define(HTTP_BRIDGE(URL),
#{
    <<"url">> => URL,
    <<"from_local_topic">> => <<"emqx_http/#">>,
    <<"method">> => <<"post">>,
    <<"ssl">> => #{<<"enable">> => false},
    <<"body">> => <<"${payload}">>,
    <<"headers">> => #{
        <<"content-type">> => <<"application/json">>
    }

}).

%%------------------------------------------------------------------------------
%% HTTP server for testing
%%------------------------------------------------------------------------------
start_http_server(Port, HandleFun) ->
    spawn_link(fun() ->
        {ok, Sock} = gen_tcp:listen(Port, [{active, false}]),
        loop(Sock, HandleFun)
    end).

loop(Sock, HandleFun) ->
    {ok, Conn} = gen_tcp:accept(Sock),
    Handler = spawn(fun () -> HandleFun(Conn) end),
    gen_tcp:controlling_process(Conn, Handler),
    loop(Sock, HandleFun).

make_response(CodeStr, Str) ->
    B = iolist_to_binary(Str),
    iolist_to_binary(
      io_lib:fwrite(
         "HTTP/1.0 ~s\nContent-Type: text/html\nContent-Length: ~p\n\n~s",
         [CodeStr, size(B), B])).

handle_fun_200_ok(Conn) ->
    case gen_tcp:recv(Conn, 0) of
        {ok, Request} ->
            gen_tcp:send(Conn, make_response("200 OK", "Request OK")),
            self() ! {http_server, received, Request},
            handle_fun_200_ok(Conn);
        {error, closed} ->
            gen_tcp:close(Conn)
    end.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_crud_apis(_) ->
    start_http_server(9901, fun handle_fun_200_ok/1),
    %% assert we there's no bridges at first
    {ok, 200, <<"[]">>} = request(get, uri(["bridges"]), []),

    %% then we add a http bridge now
    %% PUT /bridges/:id will create or update a bridge
    {ok, 200, Bridge} = request(put, uri(["bridges", "http:test_bridge"]), ?HTTP_BRIDGE(?URL1)),
    %ct:pal("---bridge: ~p", [Bridge]),
    ?assertMatch([ #{ <<"id">> := <<"http:test_bridge">>
                    , <<"bridge_type">> := <<"http">>
                    , <<"is_connected">> := _
                    , <<"node">> := _
                    , <<"url">> := ?URL1
                    }], jsx:decode(Bridge)),

    %% update the request-path of the bridge
    {ok, 200, Bridge2} = request(put, uri(["bridges", "http:test_bridge"]), ?HTTP_BRIDGE(?URL2)),
    ?assertMatch([ #{ <<"id">> := <<"http:test_bridge">>
                    , <<"bridge_type">> := <<"http">>
                    , <<"is_connected">> := _
                    , <<"node">> := _
                    , <<"url">> := ?URL2
                    }], jsx:decode(Bridge2)),

    %% list all bridges again, assert Bridge2 is in it
    {ok, 200, Bridge2Str} = request(get, uri(["bridges"]), []),
    ?assertMatch([ #{ <<"id">> := <<"http:test_bridge">>
                    , <<"bridge_type">> := <<"http">>
                    , <<"is_connected">> := _
                    , <<"node">> := _
                    , <<"url">> := ?URL2
                    }], jsx:decode(Bridge2Str)),

    %% get the bridge by id
    {ok, 200, Bridge3Str} = request(get, uri(["bridges", "http:test_bridge"]), []),
    ?assertMatch([#{ <<"id">> := <<"http:test_bridge">>
                    , <<"bridge_type">> := <<"http">>
                    , <<"is_connected">> := _
                    , <<"node">> := _
                    , <<"url">> := ?URL2
                    }], jsx:decode(Bridge3Str)),

    %% delete the bridge
    {ok,200,<<>>} = request(delete, uri(["bridges", "http:test_bridge"]), []),
    {ok, 200, <<"[]">>} = request(get, uri(["bridges"]), []),
    ok.

t_change_is_connnected_to_status() ->
    error(not_implimented).

t_start_stop_bridges(_) ->
    start_http_server(9901, fun handle_fun_200_ok/1),
    {ok, 200, Bridge} = request(put, uri(["bridges", "http:test_bridge"]), ?HTTP_BRIDGE(?URL1)),
    ?assertMatch(  #{ <<"id">> := <<"http:test_bridge">>
                    , <<"bridge_type">> := <<"http">>
                    , <<"is_connected">> := true
                    , <<"node">> := _
                    , <<"url">> := ?URL1
                    }, jsx:decode(Bridge)),
    {ok, 200, <<>>} = request(put,
        uri(["nodes", node(), "bridges", "http:test_bridge", "operation", "stop"]),
        ?HTTP_BRIDGE(?URL1)).

%%--------------------------------------------------------------------
%% HTTP Request
%%--------------------------------------------------------------------
-define(HOST, "http://127.0.0.1:18083/").
-define(API_VERSION, "v5").
-define(BASE_PATH, "api").

request(Method, Url, Body) ->
    Request = case Body of
        [] -> {Url, [auth_header_()]};
        _ -> {Url, [auth_header_()], "application/json", jsx:encode(Body)}
    end,
    ct:pal("Method: ~p, Request: ~p", [Method, Request]),
    case httpc:request(Method, Request, [], [{body_format, binary}]) of
        {error, socket_closed_remotely} ->
            {error, socket_closed_remotely};
        {ok, {{"HTTP/1.1", Code, _}, _Headers, Return} } ->
            {ok, Code, Return};
        {ok, {Reason, _, _}} ->
            {error, Reason}
    end.

uri() -> uri([]).
uri(Parts) when is_list(Parts) ->
    NParts = [E || E <- Parts],
    ?HOST ++ filename:join([?BASE_PATH, ?API_VERSION | NParts]).

auth_header_() ->
    Username = <<"admin">>,
    Password = <<"public">>,
    {ok, Token} = emqx_dashboard_admin:sign_token(Username, Password),
    {"Authorization", "Bearer " ++ binary_to_list(Token)}.

