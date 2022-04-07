%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------
-module(emqx_mgmt_api_listeners_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_mgmt_api_test_util:init_suite([emqx_conf]),
    Config.

end_per_suite(_) ->
    emqx_conf:remove([listeners, tcp, new], #{override_to => cluster}),
    emqx_conf:remove([listeners, tcp, new1], #{override_to => local}),
    emqx_mgmt_api_test_util:end_suite([emqx_conf]).

t_list_listeners(_) ->
    Path = emqx_mgmt_api_test_util:api_path(["listeners"]),
    Res = request(get, Path, [], []),
    Expect = emqx_mgmt_api_listeners:do_list_listeners(),
    ?assertEqual(emqx_json:encode([Expect]), emqx_json:encode(Res)),
    ok.

t_crud_listeners_by_id(_) ->
    TcpListenerId = <<"tcp:default">>,
    NewListenerId = <<"tcp:new">>,
    TcpPath = emqx_mgmt_api_test_util:api_path(["listeners", TcpListenerId]),
    NewPath = emqx_mgmt_api_test_util:api_path(["listeners", NewListenerId]),
    [#{<<"listeners">> := [TcpListener], <<"node">> := Node}] = request(get, TcpPath, [], []),
    ?assertEqual(atom_to_binary(node()), Node),

    %% create
    ?assertEqual({error, not_found}, is_running(NewListenerId)),
    ?assertMatch([#{<<"listeners">> := []}], request(get, NewPath, [], [])),
    NewConf = TcpListener#{
        <<"id">> => NewListenerId,
        <<"bind">> => <<"0.0.0.0:2883">>
    },
    [#{<<"listeners">> := [Create]}] = request(put, NewPath, [], NewConf),
    ?assertEqual(lists:sort(maps:keys(TcpListener)), lists:sort(maps:keys(Create))),
    [#{<<"listeners">> := [Get1]}] = request(get, NewPath, [], []),
    ?assertMatch(Create, Get1),
    ?assert(is_running(NewListenerId)),

    %% bad create(same port)
    BadId = <<"tcp:bad">>,
    BadPath = emqx_mgmt_api_test_util:api_path(["listeners", BadId]),
    BadConf = TcpListener#{
        <<"id">> => BadId,
        <<"bind">> => <<"0.0.0.0:2883">>
    },
    ?assertEqual({error, {"HTTP/1.1", 400, "Bad Request"}}, request(put, BadPath, [], BadConf)),

    %% update
    #{<<"acceptors">> := Acceptors} = Create,
    Acceptors1 = Acceptors + 10,
    [#{<<"listeners">> := [Update]}] =
        request(put, NewPath, [], Create#{<<"acceptors">> => Acceptors1}),
    ?assertMatch(#{<<"acceptors">> := Acceptors1}, Update),
    [#{<<"listeners">> := [Get2]}] = request(get, NewPath, [], []),
    ?assertMatch(#{<<"acceptors">> := Acceptors1}, Get2),

    %% delete
    ?assertEqual([], delete(NewPath)),
    ?assertEqual({error, not_found}, is_running(NewListenerId)),
    ?assertMatch([#{<<"listeners">> := []}], request(get, NewPath, [], [])),
    ?assertEqual([], delete(NewPath)),
    ok.

t_list_listeners_on_node(_) ->
    Node = atom_to_list(node()),
    Path = emqx_mgmt_api_test_util:api_path(["nodes", Node, "listeners"]),
    Listeners = request(get, Path, [], []),
    #{<<"listeners">> := Expect} = emqx_mgmt_api_listeners:do_list_listeners(),
    ?assertEqual(emqx_json:encode(Expect), emqx_json:encode(Listeners)),
    ok.

t_crud_listener_by_id_on_node(_) ->
    TcpListenerId = <<"tcp:default">>,
    NewListenerId = <<"tcp:new1">>,
    Node = atom_to_list(node()),
    TcpPath = emqx_mgmt_api_test_util:api_path(["nodes", Node, "listeners", TcpListenerId]),
    NewPath = emqx_mgmt_api_test_util:api_path(["nodes", Node, "listeners", NewListenerId]),
    TcpListener = request(get, TcpPath, [], []),

    %% create
    ?assertEqual({error, not_found}, is_running(NewListenerId)),
    ?assertMatch({error,{"HTTP/1.1", 404, "Not Found"}}, request(get, NewPath, [], [])),
    Create = request(put, NewPath, [], TcpListener#{
        <<"id">> => NewListenerId,
        <<"bind">> => <<"0.0.0.0:3883">>
    }),
    ?assertEqual(lists:sort(maps:keys(TcpListener)), lists:sort(maps:keys(Create))),
    Get1 = request(get, NewPath, [], []),
    ?assertMatch(Create, Get1),
    ?assert(is_running(NewListenerId)),

    %% update
    #{<<"acceptors">> := Acceptors} = Create,
    Acceptors1 = Acceptors + 10,
    Update = request(put, NewPath, [], Create#{<<"acceptors">> => Acceptors1}),
    ?assertMatch(#{<<"acceptors">> := Acceptors1}, Update),
    Get2 = request(get, NewPath, [], []),
    ?assertMatch(#{<<"acceptors">> := Acceptors1}, Get2),

    %% delete
    ?assertEqual([], delete(NewPath)),
    ?assertEqual({error, not_found}, is_running(NewListenerId)),
    ?assertMatch({error, {"HTTP/1.1", 404, "Not Found"}}, request(get, NewPath, [], [])),
    ?assertEqual([], delete(NewPath)),
    ok.

t_action_listeners(_) ->
    ID = "tcp:default",
    action_listener(ID, "stop", false),
    action_listener(ID, "start", true),
    action_listener(ID, "restart", true).

action_listener(ID, Action, Running) ->
    Path = emqx_mgmt_api_test_util:api_path(["listeners", ID, Action]),
    {ok, _} = emqx_mgmt_api_test_util:request_api(post, Path),
    timer:sleep(500),
    GetPath = emqx_mgmt_api_test_util:api_path(["listeners", ID]),
    [#{<<"listeners">> := Listeners}] = request(get, GetPath, [], []),
    [listener_stats(Listener, Running) || Listener <- Listeners].

request(Method, Url, QueryParams, Body) ->
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    case emqx_mgmt_api_test_util:request_api(Method, Url, QueryParams, AuthHeader, Body) of
        {ok, Res} -> emqx_json:decode(Res, [return_maps]);
        Error -> Error
    end.

delete(Url) ->
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    {ok, Res} = emqx_mgmt_api_test_util:request_api(delete, Url, AuthHeader),
    Res.

listener_stats(Listener, ExpectedStats) ->
    ?assertEqual(ExpectedStats, maps:get(<<"running">>, Listener)).

is_running(Id) ->
    emqx_listeners:is_running(binary_to_atom(Id)).
