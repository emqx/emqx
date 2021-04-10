%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_auth_mnesia_migration_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx_auth_mnesia/include/emqx_auth_mnesia.hrl").

-ifdef(EMQX_ENTERPRISE).

matrix() ->
    [ {username, "e4.2.9"}
    , {clientid, "e4.1.1"}
    , {username, "e4.1.1"}
    ].

all() ->
    [t_matrix].

-else. %% ! EMQX_ENTERPRISE

matrix() ->
    [{ImportAs, Version} || ImportAs <- [clientid, username]
                          , Version <- ["v4.2.9", "v4.1.5"]].

all() ->
    [t_matrix, t_import_4_0].

-endif. %% EMQX_ENTERPRISE

groups() ->
    [{username, [], cases()}, {clientid, [], cases()}].

cases() ->
    [t_import].

init_per_suite(Config) ->
    emqx_ct_helpers:start_apps([emqx_management, emqx_dashboard, emqx_auth_mnesia]),
    ekka_mnesia:start(),
    emqx_mgmt_auth:mnesia(boot),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([emqx_modules, emqx_management, emqx_dashboard, emqx_management, emqx_auth_mnesia]),
    ekka_mnesia:ensure_stopped().

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, _Config) ->
    mnesia:clear_table(emqx_acl),
    mnesia:clear_table(emqx_user),
    ok.

t_matrix(Config) ->
    [begin
         ct:pal("Testing import of ~p from ~p", [ImportAs, FromVersion]),
         do_import(Config, ImportAs, FromVersion),
         test_clientid_import(),
         ct:pal("ok")
     end
     || {ImportAs, FromVersion} <- matrix()].

%% This version is special, since it doesn't have mnesia ACL plugin
t_import_4_0(Config) ->
    mnesia:clear_table(emqx_acl),
    mnesia:clear_table(emqx_user),
    Filename = filename:join(proplists:get_value(data_dir, Config), "v4.0.7.json"),
    Overrides = emqx_json:encode(#{<<"auth.mnesia.as">> => atom_to_binary(clientid)}),
    ?assertMatch(ok, emqx_mgmt_data_backup:import(Filename, Overrides)),
    timer:sleep(100),
    test_clientid_import().

do_import(Config, Type, V) ->
    File = V ++ ".json",
    mnesia:clear_table(emqx_acl),
    mnesia:clear_table(emqx_user),
    Filename = filename:join(proplists:get_value(data_dir, Config), File),
    Overrides = emqx_json:encode(#{<<"auth.mnesia.as">> => atom_to_binary(Type)}),
    ?assertMatch(ok, emqx_mgmt_data_backup:import(Filename, Overrides)),
    Records = lists:sort(ets:tab2list(emqx_acl)),
    %% Check importing of records related to emqx_auth_mnesia
    ?assertMatch([#emqx_acl{
                     filter = {{Type,<<"emqx_c">>}, <<"Topic/A">>},
                     action = pub,
                     access = allow
                    },
                  #emqx_acl{
                     filter = {{Type,<<"emqx_c">>}, <<"Topic/A">>},
                     action = sub,
                     access = allow
                    }],
                 lists:sort(Records)),
    ?assertMatch([_, _], ets:tab2list(emqx_user)),
    ?assertMatch([_], ets:lookup(emqx_user, {Type, <<"emqx_c">>})),
    Req = #{clientid => <<"blah">>}
          #{Type => <<"emqx_c">>,
            password => <<"emqx_p">>
           },
    ?assertMatch({stop, #{auth_result := success}},
                 emqx_auth_mnesia:check(Req, #{}, #{hash_type => sha256})).

test_clientid_import() ->
    [#emqx_user{password = _Pass}] = ets:lookup(emqx_user, {clientid, <<"emqx_clientid">>}),
    %% Req = #{clientid => <<"emqx_clientid">>,
    %%         password => <<"emqx_p">>
    %%        },
    %% ?assertMatch({stop, #{auth_result := success}},
    %%              emqx_auth_mnesia:check(Req, #{}, #{hash_type => sha256})),
    ok.
