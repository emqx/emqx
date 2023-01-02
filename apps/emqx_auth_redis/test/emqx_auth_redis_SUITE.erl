%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_auth_redis_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").

-include_lib("common_test/include/ct.hrl").

-include_lib("eunit/include/eunit.hrl").

-define(APP, emqx_auth_redis).

-define(POOL(App), ecpool_worker:client(gproc_pool:pick_worker({ecpool, App}))).

-define(INIT_ACL, [{"mqtt_acl:test1", "topic1", "2"},
                   {"mqtt_acl:test2", "topic2", "1"},
                   {"mqtt_acl:test3", "topic3", "3"}]).

-define(INIT_AUTH, [{"mqtt_user:plain", ["password", "plain", "salt", "salt", "is_superuser", "1"]},
                    {"mqtt_user:special&symbol", ["password", "plain", "salt", "salt", "is_superuser", "0"]},
                    {"mqtt_user:md5", ["password", "1bc29b36f623ba82aaf6724fd3b16718", "salt", "salt", "is_superuser", "0"]},
                    {"mqtt_user:sha", ["password", "d8f4590320e1343a915b6394170650a8f35d6926", "salt", "salt", "is_superuser", "0"]},
                    {"mqtt_user:sha256", ["password", "5d5b09f6dcb2d53a5fffc60c4ac0d55fabdf556069d6631545f42aa6e3500f2e", "salt", "salt", "is_superuser", "0"]},
                    {"mqtt_user:pbkdf2_password", ["password", "cdedb5281bb2f801565a1122b2563515", "salt", "ATHENA.MIT.EDUraeburn", "is_superuser", "0"]},
                    {"mqtt_user:bcrypt_foo", ["password", "$2a$12$sSS8Eg.ovVzaHzi1nUHYK.HbUIOdlQI0iS22Q5rd5z.JVVYH6sfm6", "salt", "$2a$12$sSS8Eg.ovVzaHzi1nUHYK.", "is_superuser", "0"]},
                    {"mqtt_user:bcrypt", ["password", "$2y$16$rEVsDarhgHYB0TGnDFJzyu5f.T.Ha9iXMTk9J36NCMWWM7O16qyaK", "salt", "salt", "is_superuser", "0"]}]).

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() ->
    emqx_ct:all(?MODULE).

init_per_suite(Cfg) ->
    emqx_ct_helpers:start_apps([emqx_auth_redis], fun set_special_configs/1),
    init_redis_rows(),
    Cfg.

end_per_suite(_Cfg) ->
    deinit_redis_rows(),
    emqx_ct_helpers:stop_apps([emqx_auth_redis]).

set_special_configs(emqx) ->
    application:set_env(emqx, allow_anonymous, false),
    application:set_env(emqx, acl_nomatch, deny),
    application:set_env(emqx, enable_acl_cache, false);
set_special_configs(_App) ->
    ok.

init_redis_rows() ->
    %% Users
    [q(["HMSET", Key|FiledValue]) || {Key, FiledValue} <- ?INIT_AUTH],
    %% ACLs
    Result = [q(["HSET", Key, Filed, Value]) || {Key, Filed, Value} <- ?INIT_ACL],
    ct:pal("redis init result: ~p~n", [Result]).

deinit_redis_rows() ->
    AuthKeys = [Key || {Key, _Filed, _Value} <- ?INIT_AUTH],
    AclKeys = [Key || {Key, _Value} <- ?INIT_ACL],
    q(["DEL" | AuthKeys]),
    q(["DEL" | AclKeys]).

%%--------------------------------------------------------------------
%% Cases
%%--------------------------------------------------------------------

t_check_auth(_) ->
    Plain = #{clientid => <<"client1">>, username => <<"plain">>, zone => external},
    SpecialSymbol = #{clientid => <<"special_symbol">>, username => <<"special&symbol">>, zone => external},
    Md5 = #{clientid => <<"md5">>, username => <<"md5">>, zone => external},
    Sha = #{clientid => <<"sha">>, username => <<"sha">>, zone => external},
    Sha256 = #{clientid => <<"sha256">>, username => <<"sha256">>, zone => external},
    Pbkdf2 = #{clientid => <<"pbkdf2_password">>, username => <<"pbkdf2_password">>, zone => external},
    BcryptFoo = #{clientid => <<"bcrypt_foo">>, username => <<"bcrypt_foo">>, zone => external},
    User1 = #{clientid => <<"bcrypt_foo">>, username => <<"user">>, zone => external},
    User3 = #{clientid => <<"client3">>, zone => external},
    Bcrypt = #{clientid => <<"bcrypt">>, username => <<"bcrypt">>, zone => external},
    {error, _} = emqx_access_control:authenticate(User3#{password => <<>>}),
    reload([{password_hash, plain}]),
    {ok, #{is_superuser := true}} = emqx_access_control:authenticate(Plain#{password => <<"plain">>}),
    {ok, #{is_superuser := false}} = emqx_access_control:authenticate(SpecialSymbol#{password => <<"plain">>}),
    reload([{password_hash, md5}]),
    {ok, #{is_superuser := false}} = emqx_access_control:authenticate(Md5#{password => <<"md5">>}),
    reload([{password_hash, sha}]),
    {ok, #{is_superuser := false}} = emqx_access_control:authenticate(Sha#{password => <<"sha">>}),
    reload([{password_hash, sha256}]),
    {ok, #{is_superuser := false}} = emqx_access_control:authenticate(Sha256#{password => <<"sha256">>}),
    reload([{password_hash, bcrypt}]),
    {ok, #{is_superuser := false}} = emqx_access_control:authenticate(Bcrypt#{password => <<"password">>}),
    %%pbkdf2 sha
    reload([{password_hash, {pbkdf2, sha, 1, 16}}, {auth_cmd, "HMGET mqtt_user:%u password salt"}]),
    {ok, #{is_superuser := false}} = emqx_access_control:authenticate(Pbkdf2#{password => <<"password">>}),
    reload([{password_hash, {salt, bcrypt}}]),
    {ok, #{is_superuser := false}} = emqx_access_control:authenticate(BcryptFoo#{password => <<"foo">>}),
    {error,_} = emqx_access_control:authenticate(User1#{password => <<"foo">>}),
    {error, _} = emqx_access_control:authenticate(Bcrypt#{password => <<"password">>}).

t_check_auth_hget(_) ->
    q(["HSET", "mqtt_user:hset", "password", "hset"]),
    q(["HSET", "mqtt_user:hset", "is_superuser", "1"]),
    reload([{password_hash, plain}, {auth_cmd, "HGET mqtt_user:%u password"}]),
    Hset = #{clientid => <<"hset">>, username => <<"hset">>, zone => external},
    {ok, #{is_superuser := true}} = emqx_access_control:authenticate(Hset#{password => <<"hset">>}).

t_check_acl(_) ->
    User1 = #{zone => external, clientid => <<"client1">>, username => <<"test1">>},
    User2 = #{zone => external, clientid => <<"client2">>, username => <<"test2">>},
    User3 = #{zone => external, clientid => <<"client3">>, username => <<"test3">>},
    User4 = #{zone => external, clientid => <<"client4">>, username => <<"$$user4">>},
    deny = emqx_access_control:check_acl(User1, subscribe, <<"topic1">>),
    allow = emqx_access_control:check_acl(User1, publish, <<"topic1">>),

    deny = emqx_access_control:check_acl(User2, publish, <<"topic2">>),
    allow = emqx_access_control:check_acl(User2, subscribe, <<"topic2">>),
    allow = emqx_access_control:check_acl(User3, publish, <<"topic3">>),
    allow = emqx_access_control:check_acl(User3, subscribe, <<"topic3">>),
    deny = emqx_access_control:check_acl(User4, publish, <<"a/b/c">>).

t_acl_super(_) ->
    reload([{password_hash, plain}]),
    {ok, C} = emqtt:start_link([{host,      "localhost"},
                                {clientid, <<"simpleClient">>},
                                {username,  <<"plain">>},
                                {password,  <<"plain">>}]),
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

t_check_cluster_connection(_) ->
    ?assertMatch({error, _Reason}, reload([{server, [{type,cluster},
                                           {pool_size,8},
                                           {auto_reconnect,1},
                                           {database,0},
                                           {password,[]},
                                           {sentinel,[]},
                                           {servers,[{"wrong",6379},{"wrong",6380},{"wrong",6381}]}]}])).


%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

reload(Config) when is_list(Config) ->
    application:stop(?APP),
    [application:set_env(?APP, K, V) || {K, V} <- Config],
    application:start(?APP).

q(Cmd) ->
    {ok, Server} = application:get_env(?APP, server),
    case proplists:get_value(type, Server) of
        cluster ->
            eredis_cluster:q(emqx_auth_redis, Cmd);
        _ ->
            {ok, Connection} = ?POOL(?APP),
            eredis:q(Connection, Cmd)
    end.
