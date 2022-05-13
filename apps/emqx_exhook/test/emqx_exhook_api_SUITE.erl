%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_exhook_api_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-define(HOST, "http://127.0.0.1:18083/").
-define(API_VERSION, "v5").
-define(BASE_PATH, "api").
-define(CLUSTER_RPC_SHARD, emqx_cluster_rpc_shard).

-define(DEFAULT_CLUSTER_NAME_ATOM, emqxcl).

-define(CONF_DEFAULT, <<
    "\n"
    "exhook {\n"
    "  servers =\n"
    "    [ { name = default,\n"
    "        url = \"http://127.0.0.1:9000\",\n"
    "        ssl = {\"enable\": false}"
    "      }\n"
    "    ]\n"
    "}\n"
>>).

all() ->
    [
        t_list,
        t_get,
        t_add,
        t_add_duplicate,
        t_move_front,
        t_move_rear,
        t_move_before,
        t_move_after,
        t_delete,
        t_hooks,
        t_update
    ].

init_per_suite(Config) ->
    application:load(emqx_conf),
    ok = ekka:start(),
    application:set_env(ekka, cluster_name, ?DEFAULT_CLUSTER_NAME_ATOM),
    ok = mria_rlog:wait_for_shards([?CLUSTER_RPC_SHARD], infinity),
    meck:new(emqx_alarm, [non_strict, passthrough, no_link]),
    meck:expect(emqx_alarm, activate, 3, ok),
    meck:expect(emqx_alarm, deactivate, 3, ok),

    _ = emqx_exhook_demo_svr:start(),
    load_cfg(?CONF_DEFAULT),
    emqx_mgmt_api_test_util:init_suite([emqx_exhook]),
    [Conf] = emqx:get_config([exhook, servers]),
    [{template, Conf} | Config].

end_per_suite(Config) ->
    application:set_env(ekka, cluster_name, ?DEFAULT_CLUSTER_NAME_ATOM),
    ekka:stop(),
    mria:stop(),
    mria_mnesia:delete_schema(),
    meck:unload(emqx_alarm),

    emqx_mgmt_api_test_util:end_suite([emqx_exhook]),
    emqx_exhook_demo_svr:stop(),
    emqx_exhook_demo_svr:stop(<<"test1">>),
    Config.

init_per_testcase(t_add, Config) ->
    {ok, _} = emqx_cluster_rpc:start_link(),
    _ = emqx_exhook_demo_svr:start(<<"test1">>, 9001),
    timer:sleep(200),
    Config;
init_per_testcase(_, Config) ->
    {ok, _} = emqx_cluster_rpc:start_link(),
    timer:sleep(200),
    Config.

end_per_testcase(_, Config) ->
    case erlang:whereis(node()) of
        undefined ->
            ok;
        P ->
            erlang:unlink(P),
            erlang:exit(P, kill)
    end,
    Config.

load_cfg(Cfg) ->
    ok = emqx_common_test_helpers:load_config(emqx_exhook_schema, Cfg).

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_list(_) ->
    {ok, Data} = request_api(
        get,
        api_path(["exhooks"]),
        "",
        auth_header_()
    ),

    List = decode_json(Data),
    ?assertEqual(1, length(List)),

    [Svr] = List,

    ?assertMatch(
        #{
            name := <<"default">>,
            metrics := _,
            node_metrics := _,
            node_status := _,
            hooks := _
        },
        Svr
    ).

t_get(_) ->
    {ok, Data} = request_api(
        get,
        api_path(["exhooks", "default"]),
        "",
        auth_header_()
    ),

    Svr = decode_json(Data),

    ?assertMatch(
        #{
            name := <<"default">>,
            metrics := _,
            node_metrics := _,
            node_status := _,
            hooks := _
        },
        Svr
    ).

t_add(Cfg) ->
    Template = proplists:get_value(template, Cfg),
    Instance = Template#{
        name => <<"test1">>,
        url => "http://127.0.0.1:9001"
    },
    {ok, Data} = request_api(
        post,
        api_path(["exhooks"]),
        "",
        auth_header_(),
        Instance
    ),

    Svr = decode_json(Data),

    ?assertMatch(
        #{
            name := <<"test1">>,
            metrics := _,
            node_metrics := _,
            node_status := _,
            hooks := _
        },
        Svr
    ),

    ?assertMatch([<<"default">>, <<"test1">>], emqx_exhook_mgr:running()).

t_add_duplicate(Cfg) ->
    Template = proplists:get_value(template, Cfg),
    Instance = Template#{
        name => <<"test1">>,
        url => "http://127.0.0.1:9001"
    },

    {error, _Reason} = request_api(
        post,
        api_path(["exhooks"]),
        "",
        auth_header_(),
        Instance
    ),

    ?assertMatch([<<"default">>, <<"test1">>], emqx_exhook_mgr:running()).

t_add_with_bad_name(Cfg) ->
    Template = proplists:get_value(template, Cfg),
    Instance = Template#{
        name => <<"🤔">>,
        url => "http://127.0.0.1:9001"
    },

    {error, _Reason} = request_api(
        post,
        api_path(["exhooks"]),
        "",
        auth_header_(),
        Instance
    ),

    ?assertMatch([<<"default">>, <<"test1">>], emqx_exhook_mgr:running()).

t_move_front(_) ->
    Result = request_api(
        post,
        api_path(["exhooks", "default", "move"]),
        "",
        auth_header_(),
        #{position => <<"front">>}
    ),

    ?assertMatch({ok, <<>>}, Result),
    ?assertMatch([<<"default">>, <<"test1">>], emqx_exhook_mgr:running()).

t_move_rear(_) ->
    Result = request_api(
        post,
        api_path(["exhooks", "default", "move"]),
        "",
        auth_header_(),
        #{position => <<"rear">>}
    ),

    ?assertMatch({ok, <<>>}, Result),
    ?assertMatch([<<"test1">>, <<"default">>], emqx_exhook_mgr:running()).

t_move_before(_) ->
    Result = request_api(
        post,
        api_path(["exhooks", "default", "move"]),
        "",
        auth_header_(),
        #{position => <<"before:test1">>}
    ),

    ?assertMatch({ok, <<>>}, Result),
    ?assertMatch([<<"default">>, <<"test1">>], emqx_exhook_mgr:running()).

t_move_after(_) ->
    Result = request_api(
        post,
        api_path(["exhooks", "default", "move"]),
        "",
        auth_header_(),
        #{position => <<"after:test1">>}
    ),

    ?assertMatch({ok, <<>>}, Result),
    ?assertMatch([<<"test1">>, <<"default">>], emqx_exhook_mgr:running()).

t_delete(_) ->
    Result = request_api(
        delete,
        api_path(["exhooks", "test1"]),
        "",
        auth_header_()
    ),

    ?assertMatch({ok, <<>>}, Result),
    ?assertMatch([<<"default">>], emqx_exhook_mgr:running()).

t_hooks(_Cfg) ->
    {ok, Data} = request_api(
        get,
        api_path(["exhooks", "default", "hooks"]),
        "",
        auth_header_()
    ),

    [Hook1 | _] = decode_json(Data),

    ?assertMatch(
        #{
            name := _,
            params := _,
            metrics := _,
            node_metrics := _
        },
        Hook1
    ).

t_update(Cfg) ->
    Template = proplists:get_value(template, Cfg),
    Instance = Template#{enable => false},
    {ok, <<"{\"", _/binary>>} = request_api(
        put,
        api_path(["exhooks", "default"]),
        "",
        auth_header_(),
        Instance
    ),

    ?assertMatch([], emqx_exhook_mgr:running()).

decode_json(Data) ->
    BinJosn = emqx_json:decode(Data, [return_maps]),
    emqx_map_lib:unsafe_atom_key_map(BinJosn).

request_api(Method, Url, Auth) ->
    request_api(Method, Url, [], Auth, []).

request_api(Method, Url, QueryParams, Auth) ->
    request_api(Method, Url, QueryParams, Auth, []).

request_api(Method, Url, QueryParams, Auth, []) ->
    NewUrl =
        case QueryParams of
            "" -> Url;
            _ -> Url ++ "?" ++ QueryParams
        end,
    do_request_api(Method, {NewUrl, [Auth]});
request_api(Method, Url, QueryParams, Auth, Body) ->
    NewUrl =
        case QueryParams of
            "" -> Url;
            _ -> Url ++ "?" ++ QueryParams
        end,
    do_request_api(Method, {NewUrl, [Auth], "application/json", emqx_json:encode(Body)}).

do_request_api(Method, Request) ->
    case httpc:request(Method, Request, [], [{body_format, binary}]) of
        {error, socket_closed_remotely} ->
            {error, socket_closed_remotely};
        {ok, {{"HTTP/1.1", Code, _}, _, Return}} when
            Code =:= 200 orelse Code =:= 204 orelse Code =:= 201
        ->
            {ok, Return};
        {ok, {Reason, _, _}} ->
            {error, Reason}
    end.

auth_header_() ->
    AppId = <<"admin">>,
    AppSecret = <<"public">>,
    auth_header_(binary_to_list(AppId), binary_to_list(AppSecret)).

auth_header_(User, Pass) ->
    Encoded = base64:encode_to_string(lists:append([User, ":", Pass])),
    {"Authorization", "Basic " ++ Encoded}.

api_path(Parts) ->
    ?HOST ++ filename:join([?BASE_PATH, ?API_VERSION] ++ Parts).
