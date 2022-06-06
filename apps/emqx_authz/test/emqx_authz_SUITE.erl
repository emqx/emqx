%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authz_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("emqx_authz.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_placeholder.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_suite(Config) ->
    meck:new(emqx_resource, [non_strict, passthrough, no_history, no_link]),
    meck:expect(emqx_resource, create_local, fun(_, _, _, _) -> {ok, meck_data} end),
    meck:expect(emqx_resource, remove_local, fun(_) -> ok end),
    meck:expect(
        emqx_authz,
        acl_conf_file,
        fun() ->
            emqx_common_test_helpers:deps_path(emqx_authz, "etc/acl.conf")
        end
    ),

    ok = emqx_common_test_helpers:start_apps(
        [emqx_connector, emqx_conf, emqx_authz],
        fun set_special_configs/1
    ),
    Config.

end_per_suite(_Config) ->
    {ok, _} = emqx:update_config(
        [authorization],
        #{
            <<"no_match">> => <<"allow">>,
            <<"cache">> => #{<<"enable">> => <<"true">>},
            <<"sources">> => []
        }
    ),
    ok = stop_apps([emqx_resource]),
    emqx_common_test_helpers:stop_apps([emqx_connector, emqx_authz, emqx_conf]),
    meck:unload(emqx_resource),
    ok.

init_per_testcase(_, Config) ->
    {ok, _} = emqx_authz:update(?CMD_REPLACE, []),
    Config.

set_special_configs(emqx_authz) ->
    {ok, _} = emqx:update_config([authorization, cache, enable], false),
    {ok, _} = emqx:update_config([authorization, no_match], deny),
    {ok, _} = emqx:update_config([authorization, sources], []),
    ok;
set_special_configs(_App) ->
    ok.

-define(SOURCE1, #{
    <<"type">> => <<"http">>,
    <<"enable">> => true,
    <<"url">> => <<"https://example.com:443/a/b?c=d">>,
    <<"headers">> => #{},
    <<"ssl">> => #{<<"enable">> => true},
    <<"method">> => <<"get">>,
    <<"request_timeout">> => 5000
}).
-define(SOURCE2, #{
    <<"type">> => <<"mongodb">>,
    <<"enable">> => true,
    <<"mongo_type">> => <<"single">>,
    <<"server">> => <<"127.0.0.1:27017">>,
    <<"w_mode">> => <<"unsafe">>,
    <<"pool_size">> => 1,
    <<"database">> => <<"mqtt">>,
    <<"ssl">> => #{<<"enable">> => false},
    <<"collection">> => <<"authz">>,
    <<"filter">> => #{<<"a">> => <<"b">>}
}).
-define(SOURCE3, #{
    <<"type">> => <<"mysql">>,
    <<"enable">> => true,
    <<"server">> => <<"127.0.0.1:27017">>,
    <<"pool_size">> => 1,
    <<"database">> => <<"mqtt">>,
    <<"username">> => <<"xx">>,
    <<"password">> => <<"ee">>,
    <<"auto_reconnect">> => true,
    <<"ssl">> => #{<<"enable">> => false},
    <<"query">> => <<"abcb">>
}).
-define(SOURCE4, #{
    <<"type">> => <<"postgresql">>,
    <<"enable">> => true,
    <<"server">> => <<"127.0.0.1:27017">>,
    <<"pool_size">> => 1,
    <<"database">> => <<"mqtt">>,
    <<"username">> => <<"xx">>,
    <<"password">> => <<"ee">>,
    <<"auto_reconnect">> => true,
    <<"ssl">> => #{<<"enable">> => false},
    <<"query">> => <<"abcb">>
}).
-define(SOURCE5, #{
    <<"type">> => <<"redis">>,
    <<"redis_type">> => <<"single">>,
    <<"enable">> => true,
    <<"server">> => <<"127.0.0.1:27017">>,
    <<"pool_size">> => 1,
    <<"database">> => 0,
    <<"password">> => <<"ee">>,
    <<"auto_reconnect">> => true,
    <<"ssl">> => #{<<"enable">> => false},
    <<"cmd">> => <<"HGETALL mqtt_authz:", ?PH_USERNAME/binary>>
}).
-define(SOURCE6, #{
    <<"type">> => <<"file">>,
    <<"enable">> => true,
    <<"rules">> =>
        <<
            "{allow,{username,\"^dashboard?\"},subscribe,[\"$SYS/#\"]}."
            "\n{allow,{ipaddr,\"127.0.0.1\"},all,[\"$SYS/#\",\"#\"]}."
        >>
}).

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_update_source(_) ->
    %% replace all
    {ok, _} = emqx_authz:update(?CMD_REPLACE, [?SOURCE3]),
    {ok, _} = emqx_authz:update(?CMD_PREPEND, ?SOURCE2),
    {ok, _} = emqx_authz:update(?CMD_PREPEND, ?SOURCE1),
    {ok, _} = emqx_authz:update(?CMD_APPEND, ?SOURCE4),
    {ok, _} = emqx_authz:update(?CMD_APPEND, ?SOURCE5),
    {ok, _} = emqx_authz:update(?CMD_APPEND, ?SOURCE6),

    ?assertMatch(
        [
            #{type := http, enable := true},
            #{type := mongodb, enable := true},
            #{type := mysql, enable := true},
            #{type := postgresql, enable := true},
            #{type := redis, enable := true},
            #{type := file, enable := true}
        ],
        emqx_conf:get([authorization, sources], [])
    ),

    {ok, _} = emqx_authz:update({?CMD_REPLACE, http}, ?SOURCE1#{<<"enable">> := true}),
    {ok, _} = emqx_authz:update({?CMD_REPLACE, mongodb}, ?SOURCE2#{<<"enable">> := true}),
    {ok, _} = emqx_authz:update({?CMD_REPLACE, mysql}, ?SOURCE3#{<<"enable">> := true}),
    {ok, _} = emqx_authz:update({?CMD_REPLACE, postgresql}, ?SOURCE4#{<<"enable">> := true}),
    {ok, _} = emqx_authz:update({?CMD_REPLACE, redis}, ?SOURCE5#{<<"enable">> := true}),
    {ok, _} = emqx_authz:update({?CMD_REPLACE, file}, ?SOURCE6#{<<"enable">> := true}),

    {ok, _} = emqx_authz:update({?CMD_REPLACE, http}, ?SOURCE1#{<<"enable">> := false}),
    {ok, _} = emqx_authz:update({?CMD_REPLACE, mongodb}, ?SOURCE2#{<<"enable">> := false}),
    {ok, _} = emqx_authz:update({?CMD_REPLACE, mysql}, ?SOURCE3#{<<"enable">> := false}),
    {ok, _} = emqx_authz:update({?CMD_REPLACE, postgresql}, ?SOURCE4#{<<"enable">> := false}),
    {ok, _} = emqx_authz:update({?CMD_REPLACE, redis}, ?SOURCE5#{<<"enable">> := false}),
    {ok, _} = emqx_authz:update({?CMD_REPLACE, file}, ?SOURCE6#{<<"enable">> := false}),

    ?assertMatch(
        [
            #{type := http, enable := false},
            #{type := mongodb, enable := false},
            #{type := mysql, enable := false},
            #{type := postgresql, enable := false},
            #{type := redis, enable := false},
            #{type := file, enable := false}
        ],
        emqx_conf:get([authorization, sources], [])
    ),

    {ok, _} = emqx_authz:update(?CMD_REPLACE, []).

t_delete_source(_) ->
    {ok, _} = emqx_authz:update(?CMD_REPLACE, [?SOURCE1]),

    ?assertMatch([#{type := http, enable := true}], emqx_conf:get([authorization, sources], [])),

    {ok, _} = emqx_authz:update({?CMD_DELETE, http}, #{}),

    ?assertMatch([], emqx_conf:get([authorization, sources], [])).

t_move_source(_) ->
    {ok, _} = emqx_authz:update(
        ?CMD_REPLACE,
        [
            ?SOURCE1,
            ?SOURCE2,
            ?SOURCE3,
            ?SOURCE4,
            ?SOURCE5,
            ?SOURCE6
        ]
    ),
    ?assertMatch(
        [
            #{type := http},
            #{type := mongodb},
            #{type := mysql},
            #{type := postgresql},
            #{type := redis},
            #{type := file}
        ],
        emqx_authz:lookup()
    ),

    {ok, _} = emqx_authz:move(postgresql, ?CMD_MOVE_FRONT),
    ?assertMatch(
        [
            #{type := postgresql},
            #{type := http},
            #{type := mongodb},
            #{type := mysql},
            #{type := redis},
            #{type := file}
        ],
        emqx_authz:lookup()
    ),

    {ok, _} = emqx_authz:move(http, ?CMD_MOVE_REAR),
    ?assertMatch(
        [
            #{type := postgresql},
            #{type := mongodb},
            #{type := mysql},
            #{type := redis},
            #{type := file},
            #{type := http}
        ],
        emqx_authz:lookup()
    ),

    {ok, _} = emqx_authz:move(mysql, ?CMD_MOVE_BEFORE(postgresql)),
    ?assertMatch(
        [
            #{type := mysql},
            #{type := postgresql},
            #{type := mongodb},
            #{type := redis},
            #{type := file},
            #{type := http}
        ],
        emqx_authz:lookup()
    ),

    {ok, _} = emqx_authz:move(mongodb, ?CMD_MOVE_AFTER(http)),
    ?assertMatch(
        [
            #{type := mysql},
            #{type := postgresql},
            #{type := redis},
            #{type := file},
            #{type := http},
            #{type := mongodb}
        ],
        emqx_authz:lookup()
    ),

    ok.

t_get_enabled_authzs_none_enabled(_Config) ->
    ?assertEqual([], emqx_authz:get_enabled_authzs()).

t_get_enabled_authzs_some_enabled(_Config) ->
    {ok, _} = emqx_authz:update(?CMD_REPLACE, [?SOURCE4]),
    ?assertEqual([postgresql], emqx_authz:get_enabled_authzs()).

stop_apps(Apps) ->
    lists:foreach(fun application:stop/1, Apps).
