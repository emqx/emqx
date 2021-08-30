%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_mgmt_listeners_api_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() ->
    emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_mgmt_api_test_util:init_suite(),
    Config.

end_per_suite(_) ->
    emqx_mgmt_api_test_util:end_suite().

t_list_listeners(_) ->
    Path = emqx_mgmt_api_test_util:api_path(["listeners"]),
    get_api(Path).

t_list_node_listeners(_) ->
    Path = emqx_mgmt_api_test_util:api_path(["nodes", atom_to_binary(node(), utf8), "listeners"]),
    get_api(Path).

t_get_listeners(_) ->
    LocalListener = emqx_mgmt_api_listeners:format(hd(emqx_mgmt:list_listeners())),
    ID = maps:get(id, LocalListener),
    Path = emqx_mgmt_api_test_util:api_path(["listeners", atom_to_list(ID)]),
    get_api(Path).

t_get_node_listeners(_) ->
    LocalListener = emqx_mgmt_api_listeners:format(hd(emqx_mgmt:list_listeners())),
    ID = maps:get(id, LocalListener),
    Path = emqx_mgmt_api_test_util:api_path(
        ["nodes", atom_to_binary(node(), utf8), "listeners", atom_to_list(ID)]),
    get_api(Path).

t_manage_listener(_) ->
    ID = "tcp:default",
    manage_listener(ID, "stop", false),
    manage_listener(ID, "start", true),
    manage_listener(ID, "restart", true).

manage_listener(ID, Operation, Running) ->
    Path = emqx_mgmt_api_test_util:api_path(["listeners", ID, "operation", Operation]),
    {ok, _} = emqx_mgmt_api_test_util:request_api(post, Path),
    timer:sleep(500),
    GetPath = emqx_mgmt_api_test_util:api_path(["listeners", ID]),
    {ok, ListenersResponse} = emqx_mgmt_api_test_util:request_api(get, GetPath),
    Listeners = emqx_json:decode(ListenersResponse, [return_maps]),
    [listener_stats(Listener, Running) || Listener <- Listeners].

get_api(Path) ->
    {ok, ListenersData} = emqx_mgmt_api_test_util:request_api(get, Path),
    LocalListeners = emqx_mgmt_api_listeners:format(emqx_mgmt:list_listeners()),
    case emqx_json:decode(ListenersData, [return_maps]) of
        [Listener] ->
            ID = binary_to_atom(maps:get(<<"id">>, Listener), utf8),
            Filter =
                fun(Local) ->
                    maps:get(id, Local) =:= ID
                end,
            LocalListener = hd(lists:filter(Filter, LocalListeners)),
            comparison_listener(LocalListener, Listener);
        Listeners when is_list(Listeners) ->
            ?assertEqual(erlang:length(LocalListeners), erlang:length(Listeners)),
            Fun =
                fun(LocalListener) ->
                    ID = maps:get(id, LocalListener),
                    IDBinary = atom_to_binary(ID, utf8),
                    Filter =
                        fun(Listener) ->
                            maps:get(<<"id">>, Listener) =:= IDBinary
                        end,
                    Listener = hd(lists:filter(Filter, Listeners)),
                    comparison_listener(LocalListener, Listener)
                end,
            lists:foreach(Fun, LocalListeners);
        Listener when is_map(Listener) ->
            ID = binary_to_atom(maps:get(<<"id">>, Listener), utf8),
            Filter =
                fun(Local) ->
                    maps:get(id, Local) =:= ID
                end,
            LocalListener = hd(lists:filter(Filter, LocalListeners)),
            comparison_listener(LocalListener, Listener)
    end.

comparison_listener(Local, Response) ->
    ?assertEqual(maps:get(id, Local), binary_to_atom(maps:get(<<"id">>, Response))),
    ?assertEqual(maps:get(node, Local), binary_to_atom(maps:get(<<"node">>, Response))),
    ?assertEqual(maps:get(acceptors, Local), maps:get(<<"acceptors">>, Response)),
    ?assertEqual(maps:get(running, Local), maps:get(<<"running">>, Response)).


listener_stats(Listener, ExpectedStats) ->
    ?assertEqual(ExpectedStats, maps:get(<<"running">>, Listener)).
