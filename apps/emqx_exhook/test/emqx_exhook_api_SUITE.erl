%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include_lib("common_test/include/ct.hrl").

-define(HOST, "http://127.0.0.1:18083/").
-define(API_VERSION, "v5").
-define(BASE_PATH, "api").

-define(CONF_DEFAULT, <<
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
    _ = emqx_exhook_demo_svr:start(),
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_management,
            {emqx_exhook, ?CONF_DEFAULT},
            {emqx_dashboard, "dashboard.listeners.http { enable = true, bind = 18083 }"}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    {ok, _} = emqx_common_test_http:create_default_app(),
    [Conf] = emqx:get_raw_config([exhook, servers]),
    [{suite_apps, Apps}, {template, Conf} | Config].

end_per_suite(Config) ->
    emqx_exhook_demo_svr:stop(),
    emqx_exhook_demo_svr:stop(<<"test1">>),
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(t_add, Config) ->
    _ = emqx_exhook_demo_svr:start(<<"test1">>, 9001),
    Config;
init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, _Config) ->
    ok.

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
        <<"name">> => <<"test1">>,
        <<"url">> => "http://127.0.0.1:9001"
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
        <<"name">> => <<"test1">>,
        <<"url">> => "http://127.0.0.1:9001"
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
        <<"name">> => <<"🤔">>,
        <<"url">> => "http://127.0.0.1:9001"
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
    Instance = Template#{<<"enable">> => false},
    {ok, <<"{\"", _/binary>>} = request_api(
        put,
        api_path(["exhooks", "default"]),
        "",
        auth_header_(),
        Instance
    ),

    ?assertMatch([], emqx_exhook_mgr:running()).

decode_json(Data) ->
    BinJosn = emqx_utils_json:decode(Data, [return_maps]),
    emqx_utils_maps:unsafe_atom_key_map(BinJosn).

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
    do_request_api(Method, {NewUrl, [Auth], "application/json", emqx_utils_json:encode(Body)}).

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
    emqx_mgmt_api_test_util:auth_header_().

api_path(Parts) ->
    ?HOST ++ filename:join([?BASE_PATH, ?API_VERSION] ++ Parts).
