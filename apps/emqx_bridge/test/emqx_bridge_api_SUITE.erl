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
    emqx_ct:all(?MODULE).

groups() ->
    [].

init_per_suite(Config) ->
    application:load(emqx_machine),
    ok = ekka:start(),
    ok = emqx_ct_helpers:start_apps([emqx_bridge]),
    ok = emqx_config:init_load(emqx_bridge_schema, ?CONF_DEFAULT),
    Config.

end_per_suite(_Config) ->
    ok = ekka:stop(),
    emqx_ct_helpers:stop_apps([emqx_bridge]),
    ok.

init_per_testcase(_, Config) ->
    {ok, _} = emqx_cluster_rpc:start_link(node(), emqx_cluster_rpc, 1000),
    Config.
end_per_testcase(_, _Config) ->
    ok.

-define(PATH1, <<"path1">>).
-define(PATH2, <<"path2">>).
-define(HTTP_BRIDGE(PATH),
#{
    <<"base_url">> => <<"http://localhost:9901">>,
    <<"egress">> => #{
        <<"a">> => #{
            <<"from_local_topic">> => <<"emqx_http/#">>,
            <<"method">> => <<"post">>,
            <<"path">> => PATH,
            <<"body">> => <<"${payload}">>,
            <<"headers">> => #{
                <<"content-type">> => <<"application/json">>
            }
        }
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
    %% assert we there's no bridges at first
    {200, []} = emqx_bridge_api:list_bridges(get, #{}),

    %% then we add a http bridge now
    {200, [Bridge]} = emqx_bridge_api:crud_bridges_cluster(put,
        #{ bindings => #{id => <<"http:test_bridge">>}
         , body => ?HTTP_BRIDGE(?PATH1)
         }),
    %ct:pal("---bridge: ~p", [Bridge]),
    ?assertMatch(#{ id := <<"http:test_bridge">>
                  , bridge_type := http
                  , is_connected := _
                  , node := _
                  , <<"egress">> := #{
                      <<"a">> := #{<<"path">> := ?PATH1}
                    }
                  }, Bridge),

    %% update the request-path of the bridge
    {200, [Bridge2]} = emqx_bridge_api:crud_bridges_cluster(put,
        #{ bindings => #{id => <<"http:test_bridge">>}
         , body => ?HTTP_BRIDGE(?PATH2)
         }),
    ?assertMatch(#{ id := <<"http:test_bridge">>
                  , bridge_type := http
                  , is_connected := _
                  , <<"egress">> := #{
                      <<"a">> := #{<<"path">> := ?PATH2}
                    }
                  }, Bridge2),

    %% list all bridges again, assert Bridge2 is in it
    {200, [Bridge2]} = emqx_bridge_api:list_bridges(get, #{}),

    %% delete teh bridge
    {200} = emqx_bridge_api:crud_bridges_cluster(delete,
        #{ bindings => #{id => <<"http:test_bridge">>}
         }),
    {200, []} = emqx_bridge_api:list_bridges(get, #{}),
    ok.

