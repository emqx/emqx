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

-module(emqx_authz_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("emqx_authz.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(CONF_DEFAULT, <<"authorization_rules: {rules: []}">>).

all() ->
    emqx_ct:all(?MODULE).

groups() ->
    [].

init_per_suite(Config) ->
    meck:new(emqx_resource, [non_strict, passthrough, no_history, no_link]),
    meck:expect(emqx_resource, create, fun(_, _, _) -> {ok, meck_data} end),
    meck:expect(emqx_resource, remove, fun(_) -> ok end ),

    ok = emqx_config:init_load(emqx_authz_schema, ?CONF_DEFAULT),
    ok = emqx_ct_helpers:start_apps([emqx_authz]),
    {ok, _} = emqx:update_config([authorization, cache, enable], false),
    {ok, _} = emqx:update_config([authorization, no_match], deny),
    Config.

end_per_suite(_Config) ->
    {ok, _} = emqx_authz:update(replace, []),
    emqx_ct_helpers:stop_apps([emqx_authz, emqx_resource]),
    meck:unload(emqx_resource),
    ok.

init_per_testcase(_, Config) ->
    {ok, _} = emqx_authz:update(replace, []),
    Config.

-define(RULE1, #{<<"type">> => <<"http">>,
                 <<"config">> => #{
                    <<"url">> => <<"https://fake.com:443/">>,
                    <<"headers">> => #{},
                    <<"method">> => <<"get">>,
                    <<"request_timeout">> => 5000}
                }).
-define(RULE2, #{<<"type">> => <<"mongo">>,
                 <<"config">> => #{
                        <<"mongo_type">> => <<"single">>,
                        <<"server">> => <<"127.0.0.1:27017">>,
                        <<"pool_size">> => 1,
                        <<"database">> => <<"mqtt">>,
                        <<"ssl">> => #{<<"enable">> => false}},
                 <<"collection">> => <<"fake">>,
                 <<"find">> => #{<<"a">> => <<"b">>}
                }).
-define(RULE3, #{<<"type">> => <<"mysql">>,
                 <<"config">> => #{
                     <<"server">> => <<"127.0.0.1:27017">>,
                     <<"pool_size">> => 1,
                     <<"database">> => <<"mqtt">>,
                     <<"username">> => <<"xx">>,
                     <<"password">> => <<"ee">>,
                     <<"auto_reconnect">> => true,
                     <<"ssl">> => #{<<"enable">> => false}},
                 <<"sql">> => <<"abcb">>
                }).
-define(RULE4, #{<<"type">> => <<"pgsql">>,
                 <<"config">> => #{
                     <<"server">> => <<"127.0.0.1:27017">>,
                     <<"pool_size">> => 1,
                     <<"database">> => <<"mqtt">>,
                     <<"username">> => <<"xx">>,
                     <<"password">> => <<"ee">>,
                     <<"auto_reconnect">> => true,
                     <<"ssl">> => #{<<"enable">> => false}},
                 <<"sql">> => <<"abcb">>
                }).
-define(RULE5, #{<<"type">> => <<"redis">>,
                 <<"config">> => #{
                     <<"server">> => <<"127.0.0.1:27017">>,
                     <<"pool_size">> => 1,
                     <<"database">> => 0,
                     <<"password">> => <<"ee">>,
                     <<"auto_reconnect">> => true,
                     <<"ssl">> => #{<<"enable">> => false}},
                 <<"cmd">> => <<"HGETALL mqtt_authz:%u">>
                }).

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_update_rule(_) ->
    {ok, _} = emqx_authz:update(replace, [?RULE2]),
    {ok, _} = emqx_authz:update(head, [?RULE1]),
    {ok, _} = emqx_authz:update(tail, [?RULE3]),

    dbg:tracer(),dbg:p(all,c),
    dbg:tpl(hocon_schema, check, cx),
    Lists1 = emqx_authz:check_rules([?RULE1, ?RULE2, ?RULE3]),
    ?assertMatch(Lists1, emqx:get_config([authorization_rules, rules], [])),

    [#{annotations := #{id := Id1}, type := http},
     #{annotations := #{id := Id2}, type := mongo},
     #{annotations := #{id := Id3}, type := mysql}
    ] = emqx_authz:lookup(),

    {ok, _} = emqx_authz:update({replace_once, Id1}, ?RULE5),
    {ok, _} = emqx_authz:update({replace_once, Id3}, ?RULE4),
    Lists2 = emqx_authz:check_rules([?RULE1, ?RULE2, ?RULE4]),
    ?assertMatch(Lists2, emqx:get_config([authorization_rules, rules], [])),

    [#{annotations := #{id := Id1}, type := redis},
     #{annotations := #{id := Id2}, type := mongo},
     #{annotations := #{id := Id3}, type := pgsql}
    ] = emqx_authz:lookup(),

    {ok, _} = emqx_authz:update(replace, []).

t_move_rule(_) ->
    {ok, _} = emqx_authz:update(replace, [?RULE1, ?RULE2, ?RULE3, ?RULE4, ?RULE5]),
    [#{annotations := #{id := Id1}},
     #{annotations := #{id := Id2}},
     #{annotations := #{id := Id3}},
     #{annotations := #{id := Id4}},
     #{annotations := #{id := Id5}}
    ] = emqx_authz:lookup(),

    {ok, _} = emqx_authz:move(Id4, <<"top">>),
    ?assertMatch([#{annotations := #{id := Id4}},
                  #{annotations := #{id := Id1}},
                  #{annotations := #{id := Id2}},
                  #{annotations := #{id := Id3}},
                  #{annotations := #{id := Id5}}
                 ], emqx_authz:lookup()),

    {ok, _} = emqx_authz:move(Id1, <<"bottom">>),
    ?assertMatch([#{annotations := #{id := Id4}},
                  #{annotations := #{id := Id2}},
                  #{annotations := #{id := Id3}},
                  #{annotations := #{id := Id5}},
                  #{annotations := #{id := Id1}}
                 ], emqx_authz:lookup()),

    {ok, _} = emqx_authz:move(Id3, #{<<"before">> => Id4}),
    ?assertMatch([#{annotations := #{id := Id3}},
                  #{annotations := #{id := Id4}},
                  #{annotations := #{id := Id2}},
                  #{annotations := #{id := Id5}},
                  #{annotations := #{id := Id1}}
                 ], emqx_authz:lookup()),

    {ok, _} = emqx_authz:move(Id2, #{<<"after">> => Id1}),
    ?assertMatch([#{annotations := #{id := Id3}},
                  #{annotations := #{id := Id4}},
                  #{annotations := #{id := Id5}},
                  #{annotations := #{id := Id1}},
                  #{annotations := #{id := Id2}}
                 ], emqx_authz:lookup()),
    ok.
