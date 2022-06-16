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
    #{<<"listeners">> := Expect} = emqx_mgmt_api_listeners:do_list_listeners(),
    ?assertEqual(length(Expect), length(Res)),
    ok.

t_tcp_crud_listeners_by_id(_) ->
    ListenerId = <<"tcp:default">>,
    NewListenerId = <<"tcp:new">>,
    MinListenerId = <<"tcp:min">>,
    BadId = <<"tcp:bad">>,
    Type = <<"tcp">>,
    crud_listeners_by_id(ListenerId, NewListenerId, MinListenerId, BadId, Type).

t_ssl_crud_listeners_by_id(_) ->
    ListenerId = <<"ssl:default">>,
    NewListenerId = <<"ssl:new">>,
    MinListenerId = <<"ssl:min">>,
    BadId = <<"ssl:bad">>,
    Type = <<"ssl">>,
    crud_listeners_by_id(ListenerId, NewListenerId, MinListenerId, BadId, Type).

t_ws_crud_listeners_by_id(_) ->
    ListenerId = <<"ws:default">>,
    NewListenerId = <<"ws:new">>,
    MinListenerId = <<"ws:min">>,
    BadId = <<"ws:bad">>,
    Type = <<"ws">>,
    crud_listeners_by_id(ListenerId, NewListenerId, MinListenerId, BadId, Type).

t_wss_crud_listeners_by_id(_) ->
    ListenerId = <<"wss:default">>,
    NewListenerId = <<"wss:new">>,
    MinListenerId = <<"wss:min">>,
    BadId = <<"wss:bad">>,
    Type = <<"wss">>,
    crud_listeners_by_id(ListenerId, NewListenerId, MinListenerId, BadId, Type).

crud_listeners_by_id(ListenerId, NewListenerId, MinListenerId, BadId, Type) ->
    OriginPath = emqx_mgmt_api_test_util:api_path(["listeners", ListenerId]),
    NewPath = emqx_mgmt_api_test_util:api_path(["listeners", NewListenerId]),
    OriginListener = request(get, OriginPath, [], []),

    %% create with full options
    ?assertEqual({error, not_found}, is_running(NewListenerId)),
    ?assertMatch({error, {"HTTP/1.1", 404, _}}, request(get, NewPath, [], [])),
    NewConf = OriginListener#{
        <<"id">> => NewListenerId,
        <<"bind">> => <<"0.0.0.0:2883">>
    },
    Create = request(post, NewPath, [], NewConf),
    ?assertEqual(lists:sort(maps:keys(OriginListener)), lists:sort(maps:keys(Create))),
    Get1 = request(get, NewPath, [], []),
    ?assertMatch(Create, Get1),
    ?assert(is_running(NewListenerId)),

    %% create with required options
    MinPath = emqx_mgmt_api_test_util:api_path(["listeners", MinListenerId]),
    ?assertEqual({error, not_found}, is_running(MinListenerId)),
    ?assertMatch({error, {"HTTP/1.1", 404, _}}, request(get, MinPath, [], [])),
    MinConf =
        case OriginListener of
            #{
                <<"ssl_options">> :=
                    #{
                        <<"cacertfile">> := CaCertFile,
                        <<"certfile">> := CertFile,
                        <<"keyfile">> := KeyFile
                    }
            } ->
                #{
                    <<"id">> => MinListenerId,
                    <<"bind">> => <<"0.0.0.0:3883">>,
                    <<"type">> => Type,
                    <<"ssl_options">> => #{
                        <<"cacertfile">> => CaCertFile,
                        <<"certfile">> => CertFile,
                        <<"keyfile">> => KeyFile
                    }
                };
            _ ->
                #{
                    <<"id">> => MinListenerId,
                    <<"bind">> => <<"0.0.0.0:3883">>,
                    <<"type">> => Type
                }
        end,
    MinCreate = request(post, MinPath, [], MinConf),
    ?assertEqual(lists:sort(maps:keys(OriginListener)), lists:sort(maps:keys(MinCreate))),
    MinGet = request(get, MinPath, [], []),
    ?assertMatch(MinCreate, MinGet),
    ?assert(is_running(MinListenerId)),

    %% bad create(same port)
    BadPath = emqx_mgmt_api_test_util:api_path(["listeners", BadId]),
    BadConf = OriginListener#{
        <<"id">> => BadId,
        <<"bind">> => <<"0.0.0.0:2883">>
    },
    ?assertMatch({error, {"HTTP/1.1", 400, _}}, request(post, BadPath, [], BadConf)),

    %% update
    #{<<"acceptors">> := Acceptors} = Create,
    Acceptors1 = Acceptors + 10,
    Update =
        request(put, NewPath, [], Create#{<<"acceptors">> => Acceptors1}),
    ?assertMatch(#{<<"acceptors">> := Acceptors1}, Update),
    Get2 = request(get, NewPath, [], []),
    ?assertMatch(#{<<"acceptors">> := Acceptors1}, Get2),
    ?assert(is_running(NewListenerId)),

    %% update an stopped listener
    action_listener(NewListenerId, "stop", false),
    ?assertNot(is_running(NewListenerId)),
    %% update
    Get3 = request(get, NewPath, [], []),
    #{<<"acceptors">> := Acceptors3} = Get3,
    Acceptors4 = Acceptors3 + 1,
    Update1 =
        request(put, NewPath, [], Get3#{<<"acceptors">> => Acceptors4}),
    ?assertMatch(#{<<"acceptors">> := Acceptors4}, Update1),
    Get4 = request(get, NewPath, [], []),
    ?assertMatch(#{<<"acceptors">> := Acceptors4}, Get4),
    ?assertNot(is_running(NewListenerId)),

    %% delete
    ?assertEqual([], delete(NewPath)),
    ?assertEqual([], delete(MinPath)),
    ?assertEqual({error, not_found}, is_running(NewListenerId)),
    ?assertMatch({error, {"HTTP/1.1", 404, _}}, request(get, NewPath, [], [])),
    ?assertEqual([], delete(NewPath)),
    ok.

t_delete_nonexistent_listener(_) ->
    NonExist = emqx_mgmt_api_test_util:api_path(["listeners", "tcp:nonexistent"]),
    ?assertEqual([], delete(NonExist)),
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
    Listener = request(get, GetPath, [], []),
    listener_stats(Listener, Running).

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
