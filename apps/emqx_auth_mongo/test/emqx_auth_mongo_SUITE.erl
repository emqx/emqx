%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_auth_mongo_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(APP, emqx_auth_mongo).

-define(POOL(App),  ecpool_worker:client(gproc_pool:pick_worker({ecpool, App}))).

-define(MONGO_CL_ACL, <<"mqtt_acl">>).
-define(MONGO_CL_USER, <<"mqtt_user">>).

-define(INIT_ACL, [{<<"username">>, <<"testuser">>, <<"clientid">>, <<"null">>, <<"subscribe">>, [<<"#">>]},
                   {<<"username">>, <<"dashboard">>, <<"clientid">>, <<"null">>, <<"pubsub">>, [<<"$SYS/#">>]},
                   {<<"username">>, <<"user3">>, <<"clientid">>, <<"null">>, <<"publish">>, [<<"a/b/c">>]}]).

-define(INIT_AUTH, [{<<"username">>, <<"plain">>, <<"password">>, <<"plain">>, <<"salt">>, <<"salt">>, <<"is_superuser">>, true},
                    {<<"username">>, <<"md5">>, <<"password">>, <<"1bc29b36f623ba82aaf6724fd3b16718">>, <<"salt">>, <<"salt">>, <<"is_superuser">>, false},
                    {<<"username">>, <<"sha">>, <<"password">>, <<"d8f4590320e1343a915b6394170650a8f35d6926">>, <<"salt">>, <<"salt">>, <<"is_superuser">>, false},
                    {<<"username">>, <<"sha256">>, <<"password">>, <<"5d5b09f6dcb2d53a5fffc60c4ac0d55fabdf556069d6631545f42aa6e3500f2e">>, <<"salt">>, <<"salt">>, <<"is_superuser">>, false},
                    {<<"username">>, <<"pbkdf2_password">>, <<"password">>, <<"cdedb5281bb2f801565a1122b2563515">>, <<"salt">>, <<"ATHENA.MIT.EDUraeburn">>, <<"is_superuser">>, false},
                    {<<"username">>, <<"bcrypt_foo">>, <<"password">>, <<"$2a$12$sSS8Eg.ovVzaHzi1nUHYK.HbUIOdlQI0iS22Q5rd5z.JVVYH6sfm6">>, <<"salt">>, <<"$2a$12$sSS8Eg.ovVzaHzi1nUHYK.">>, <<"is_superuser">>, false}
                    ]).

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() ->
    emqx_ct:all(?MODULE).

init_per_suite(Cfg) ->
    emqx_ct_helpers:start_apps([emqx_auth_mongo], fun set_special_confs/1),
    emqx_modules:load_module(emqx_mod_acl_internal, false),
    init_mongo_data(),
    Cfg.

end_per_suite(_Cfg) ->
    deinit_mongo_data(),
    emqx_ct_helpers:stop_apps([emqx_auth_mongo]).

set_special_confs(emqx) ->
    application:set_env(emqx, acl_nomatch, deny),
    application:set_env(emqx, acl_file,
                        emqx_ct_helpers:deps_path(emqx, "test/emqx_SUITE_data/acl.conf")),
    application:set_env(emqx, allow_anonymous, false),
    application:set_env(emqx, enable_acl_cache, false),
    application:set_env(emqx, plugins_loaded_file,
                        emqx_ct_helpers:deps_path(emqx, "test/emqx_SUITE_data/loaded_plugins"));
set_special_confs(_App) ->
    ok.

init_mongo_data() ->
    %% Users
    {ok, Connection} = ?POOL(?APP),
    mongo_api:delete(Connection, ?MONGO_CL_USER, {}),
    ?assertMatch({{true, _}, _}, mongo_api:insert(Connection, ?MONGO_CL_USER, ?INIT_AUTH)),
    %% ACLs
    mongo_api:delete(Connection, ?MONGO_CL_ACL, {}),
    ?assertMatch({{true, _}, _}, mongo_api:insert(Connection, ?MONGO_CL_ACL, ?INIT_ACL)).

deinit_mongo_data() ->
    {ok, Connection} = ?POOL(?APP),
    mongo_api:delete(Connection, ?MONGO_CL_USER, {}),
    mongo_api:delete(Connection, ?MONGO_CL_ACL, {}).

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_check_auth(_) ->
    Plain = #{zone => external, clientid => <<"client1">>, username => <<"plain">>},
    Plain1 = #{zone => external, clientid => <<"client1">>, username => <<"plain2">>},
    Md5 = #{zone => external, clientid => <<"md5">>, username => <<"md5">>},
    Sha = #{zone => external, clientid => <<"sha">>, username => <<"sha">>},
    Sha256 = #{zone => external, clientid => <<"sha256">>, username => <<"sha256">>},
    Pbkdf2 = #{zone => external, clientid => <<"pbkdf2_password">>, username => <<"pbkdf2_password">>},
    Bcrypt = #{zone => external, clientid => <<"bcrypt_foo">>, username => <<"bcrypt_foo">>},
    User1 = #{zone => external, clientid => <<"bcrypt_foo">>, username => <<"user">>},
    reload({auth_query, [{password_hash, plain}]}),
    %% With exactly username/password, connection success
    {ok, #{is_superuser := true}} = emqx_access_control:authenticate(Plain#{password => <<"plain">>}),
    %% With exactly username and wrong password, connection fail
    {error, _} = emqx_access_control:authenticate(Plain#{password => <<"error_pwd">>}),
    %% With wrong username and wrong password, emqx_auth_mongo auth fail, then allow anonymous authentication
    {error, _} = emqx_access_control:authenticate(Plain1#{password => <<"error_pwd">>}),
    %% With wrong username and exactly password, emqx_auth_mongo auth fail, then allow anonymous authentication
    {error, _} = emqx_access_control:authenticate(Plain1#{password => <<"plain">>}),
    reload({auth_query, [{password_hash, md5}]}),
    {ok, #{is_superuser := false}} = emqx_access_control:authenticate(Md5#{password => <<"md5">>}),
    reload({auth_query, [{password_hash, sha}]}),
    {ok, #{is_superuser := false}} = emqx_access_control:authenticate(Sha#{password => <<"sha">>}),
    reload({auth_query, [{password_hash, sha256}]}),
    {ok, #{is_superuser := false}} = emqx_access_control:authenticate(Sha256#{password => <<"sha256">>}),
    %%pbkdf2 sha
    reload({auth_query, [{password_hash, {pbkdf2, sha, 1, 16}}, {password_field, [<<"password">>, <<"salt">>]}]}),
    {ok, #{is_superuser := false}} = emqx_access_control:authenticate(Pbkdf2#{password => <<"password">>}),
    reload({auth_query, [{password_hash, {salt, bcrypt}}]}),
    {ok, #{is_superuser := false}} = emqx_access_control:authenticate(Bcrypt#{password => <<"foo">>}),
    {error, _} = emqx_access_control:authenticate(User1#{password => <<"foo">>}).

t_check_acl(_) ->
    {ok, Connection} = ?POOL(?APP),
    User1 = #{zone => external, clientid => <<"client1">>, username => <<"testuser">>},
    User2 = #{zone => external, clientid => <<"client2">>, username => <<"dashboard">>},
    User3 = #{zone => external, clientid => <<"client2">>, username => <<"user3">>},
    User4 = #{zone => external, clientid => <<"$$client2">>, username => <<"$$user3">>},
    3 = mongo_api:count(Connection, ?MONGO_CL_ACL, {}, 17),
    %% ct log output
    allow = emqx_access_control:check_acl(User1, subscribe, <<"users/testuser/1">>),
    deny = emqx_access_control:check_acl(User1, subscribe, <<"$SYS/testuser/1">>),
    deny = emqx_access_control:check_acl(User2, subscribe, <<"a/b/c">>),
    allow = emqx_access_control:check_acl(User2, subscribe, <<"$SYS/testuser/1">>),
    allow = emqx_access_control:check_acl(User3, publish, <<"a/b/c">>),
    deny = emqx_access_control:check_acl(User3, publish, <<"c">>),
    allow = emqx_access_control:check_acl(User4, publish, <<"a/b/c">>).

t_acl_super(_) ->
    reload({auth_query, [{password_hash, plain}, {password_field, [<<"password">>]}]}),
    {ok, C} = emqtt:start_link([{clientid, <<"simpleClient">>},
                                {username, <<"plain">>},
                                {password, <<"plain">>}]),
    {ok, _} = emqtt:connect(C),
    timer:sleep(10),
    emqtt:subscribe(C, <<"TopicA">>, qos2),
    timer:sleep(1000),
    emqtt:publish(C, <<"TopicA">>, <<"Payload">>, qos2),
    timer:sleep(1000),
    receive
        {publish, #{payload := Payload}} ->
        ?assertEqual(<<"Payload">>, Payload)
    after
        1000 ->
        ct:fail({receive_timeout, <<"Payload">>}),
        ok
    end,
    emqtt:disconnect(C).

%%--------------------------------------------------------------------
%% Utils
%%--------------------------------------------------------------------

reload({Par, Vals}) when is_list(Vals) ->
    application:stop(?APP),
    {ok, TupleVals} = application:get_env(?APP, Par),
    NewVals =
    lists:filtermap(fun({K, V}) ->
        case lists:keymember(K, 1, Vals) of
        false ->{true, {K, V}};
        _ -> false
        end
    end, TupleVals),
    application:set_env(?APP, Par, lists:append(NewVals, Vals)),
    application:start(?APP).
