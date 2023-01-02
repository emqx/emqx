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

-module(emqx_auth_mysql_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-define(APP, emqx_auth_mysql).

-include_lib("emqx/include/emqx.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(DROP_ACL_TABLE, <<"DROP TABLE IF EXISTS mqtt_acl">>).

-define(CREATE_ACL_TABLE, <<"CREATE TABLE mqtt_acl ("
                            "   id int(11) unsigned NOT NULL AUTO_INCREMENT,"
                            "   allow int(1) DEFAULT NULL COMMENT '0: deny, 1: allow',"
                            "   ipaddr varchar(60) DEFAULT NULL COMMENT 'IpAddress',"
                            "   username varchar(100) DEFAULT NULL COMMENT 'Username',"
                            "   clientid varchar(100) DEFAULT NULL COMMENT 'ClientId',"
                            "   access int(2) NOT NULL COMMENT '1: subscribe, 2: publish, 3: pubsub',"
                            "   topic varchar(100) NOT NULL DEFAULT '' COMMENT 'Topic Filter',"
                            "   PRIMARY KEY (`id`)"
                            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4">>).

-define(INIT_ACL, <<"INSERT INTO mqtt_acl (id, allow, ipaddr, username, clientid, access, topic)"
                    "VALUES (1,1,'127.0.0.1','u1','c1',1,'t1'),"
                           "(2,0,'127.0.0.1','u2','c2',1,'t1'),"
                           "(3,1,'10.10.0.110','u1','c1',1,'t1'),"
                           "(4,1,'127.0.0.1','u3','c3',3,'t1')">>).

-define(DROP_AUTH_TABLE, <<"DROP TABLE IF EXISTS `mqtt_user`">>).

-define(CREATE_AUTH_TABLE, <<"CREATE TABLE `mqtt_user` ("
                             "`id` int(11) unsigned NOT NULL AUTO_INCREMENT,"
                             "`username` varchar(100) DEFAULT NULL,"
                             "`password` varchar(100) DEFAULT NULL,"
                             "`salt` varchar(100) DEFAULT NULL,"
                             "`is_superuser` tinyint(1) DEFAULT 0,"
                             "`created` datetime DEFAULT NULL,"
                             "PRIMARY KEY (`id`),"
                             "UNIQUE KEY `mqtt_username` (`username`)"
                             ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4">>).

-define(INIT_AUTH, <<"INSERT INTO mqtt_user (id, is_superuser, username, password, salt)"
                     "VALUES (1, 1, 'plain', 'plain', 'salt'),"
                            "(2, 0, 'md5', '1bc29b36f623ba82aaf6724fd3b16718', 'salt'),"
                            "(3, 0, 'sha', 'd8f4590320e1343a915b6394170650a8f35d6926', 'salt'),"
                            "(4, 0, 'sha256', '5d5b09f6dcb2d53a5fffc60c4ac0d55fabdf556069d6631545f42aa6e3500f2e', 'salt'),"
                            "(5, 0, 'pbkdf2_password', 'cdedb5281bb2f801565a1122b2563515', 'ATHENA.MIT.EDUraeburn'),"
                            "(6, 0, 'bcrypt_foo', '$2a$12$sSS8Eg.ovVzaHzi1nUHYK.HbUIOdlQI0iS22Q5rd5z.JVVYH6sfm6', '$2a$12$sSS8Eg.ovVzaHzi1nUHYK.'),"
                            "(7, 0, 'bcrypt', '$2y$16$rEVsDarhgHYB0TGnDFJzyu5f.T.Ha9iXMTk9J36NCMWWM7O16qyaK', 'salt'),"
                            "(8, 0, 'bcrypt_wrong', '$2y$16$rEVsDarhgHYB0TGnDFJzyu', 'salt')">>).

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() ->
    emqx_ct:all(?MODULE).

init_per_suite(Cfg) ->
    emqx_ct_helpers:start_apps([emqx_auth_mysql], fun set_special_configs/1),
    init_mysql_data(),
    Cfg.

end_per_suite(_) ->
    deinit_mysql_data(),
    emqx_ct_helpers:stop_apps([emqx_auth_mysql]),
    ok.

set_special_configs(emqx) ->
    application:set_env(emqx, allow_anonymous, false),
    application:set_env(emqx, enable_acl_cache, false),
    application:set_env(emqx, plugins_loaded_file,
                        emqx_ct_helpers:deps_path(emqx, "test/emqx_SUITE_data/loaded_plugins"));

set_special_configs(_App) ->
    ok.

init_mysql_data() ->
    {ok, Pid} = ecpool_worker:client(gproc_pool:pick_worker({ecpool, ?APP})),
    %% Users
    ok = mysql:query(Pid, ?DROP_AUTH_TABLE),
    ok = mysql:query(Pid, ?CREATE_AUTH_TABLE),
    ok = mysql:query(Pid, ?INIT_AUTH),

    %% ACLs
    ok = mysql:query(Pid, ?DROP_ACL_TABLE),
    ok = mysql:query(Pid, ?CREATE_ACL_TABLE),
    ok = mysql:query(Pid, ?INIT_ACL).

deinit_mysql_data() ->
    {ok, Pid} = ecpool_worker:client(gproc_pool:pick_worker({ecpool, ?APP})),
    ok = mysql:query(Pid, ?DROP_AUTH_TABLE),
    ok = mysql:query(Pid, ?DROP_ACL_TABLE).

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_check_acl(_) ->
    User0 = #{zone => external,peerhost => {127,0,0,1}},
    allow = emqx_access_control:check_acl(User0, subscribe, <<"t1">>),
    User1 = #{zone => external, clientid => <<"c1">>, username => <<"u1">>, peerhost => {127,0,0,1}},
    User2 = #{zone => external, clientid => <<"c2">>, username => <<"u2">>, peerhost => {127,0,0,1}},
    allow = emqx_access_control:check_acl(User1, subscribe, <<"t1">>),
    deny = emqx_access_control:check_acl(User2, subscribe, <<"t1">>),

    User3 = #{zone => external, peerhost => {10,10,0,110}, clientid => <<"c1">>, username => <<"u1">>},
    User4 = #{zone => external, peerhost => {10,10,10,110}, clientid => <<"c1">>, username => <<"u1">>},
    allow = emqx_access_control:check_acl(User3, subscribe, <<"t1">>),
    allow = emqx_access_control:check_acl(User3, subscribe, <<"t1">>),
    allow = emqx_access_control:check_acl(User3, subscribe, <<"t2">>),%% nomatch -> ignore -> emqx acl
    allow = emqx_access_control:check_acl(User4, subscribe, <<"t1">>),%% nomatch -> ignore -> emqx acl
    User5 = #{zone => external, peerhost => {127,0,0,1}, clientid => <<"c3">>, username => <<"u3">>},
    allow = emqx_access_control:check_acl(User5, subscribe, <<"t1">>),
    allow = emqx_access_control:check_acl(User5, publish, <<"t1">>).

t_acl_super(_Config) ->
    reload([{password_hash, plain},
            {auth_query, "select password from mqtt_user where username = '%u' limit 1"}]),
    {ok, C} = emqtt:start_link([{host, "localhost"},
                                {clientid, <<"simpleClient">>},
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

t_check_auth(_) ->
    Plain = #{clientid => <<"client1">>, username => <<"plain">>, zone => external},
    Md5 = #{clientid => <<"md5">>, username => <<"md5">>, zone => external},
    Sha = #{clientid => <<"sha">>, username => <<"sha">>, zone => external},
    Sha256 = #{clientid => <<"sha256">>, username => <<"sha256">>, zone => external},
    Pbkdf2 = #{clientid => <<"pbkdf2_password">>, username => <<"pbkdf2_password">>, zone => external},
    BcryptFoo = #{clientid => <<"bcrypt_foo">>, username => <<"bcrypt_foo">>, zone => external},
    User1 = #{clientid => <<"bcrypt_foo">>, username => <<"user">>, zone => external},
    Bcrypt = #{clientid => <<"bcrypt">>, username => <<"bcrypt">>, zone => external},
    BcryptWrong = #{clientid => <<"bcrypt_wrong">>, username => <<"bcrypt_wrong">>, zone => external},
    reload([{password_hash, plain}]),
    {ok,#{is_superuser := true}} =
        emqx_access_control:authenticate(Plain#{password => <<"plain">>}),
    reload([{password_hash, md5}]),
    {ok,#{is_superuser := false}} =
        emqx_access_control:authenticate(Md5#{password => <<"md5">>}),
    reload([{password_hash, sha}]),
    {ok,#{is_superuser := false}} =
        emqx_access_control:authenticate(Sha#{password => <<"sha">>}),
    reload([{password_hash, sha256}]),
    {ok,#{is_superuser := false}} =
        emqx_access_control:authenticate(Sha256#{password => <<"sha256">>}),
    reload([{password_hash, bcrypt}]),
    {ok,#{is_superuser := false}} =
        emqx_access_control:authenticate(Bcrypt#{password => <<"password">>}),
    {error, not_authorized} =
        emqx_access_control:authenticate(BcryptWrong#{password => <<"password">>}),
    %%pbkdf2 sha
    reload([{password_hash, {pbkdf2, sha, 1, 16}},
            {auth_query, "select password, salt from mqtt_user where username = '%u' limit 1"}]),
    {ok,#{is_superuser := false}} =
        emqx_access_control:authenticate(Pbkdf2#{password => <<"password">>}),
    reload([{password_hash, {salt, bcrypt}}]),
    {ok,#{is_superuser := false}} =
        emqx_access_control:authenticate(BcryptFoo#{password => <<"foo">>}),
    {error, _} = emqx_access_control:authenticate(User1#{password => <<"foo">>}),
    {error, not_authorized} = emqx_access_control:authenticate(Bcrypt#{password => <<"password">>}).

t_comment_config(_) ->
    application:stop(?APP),
    [application:unset_env(?APP, Par) || Par <- [acl_query, auth_query]],
    application:start(?APP).

t_placeholders(_) ->
    ClientA = #{username => <<"plain">>, clientid => <<"plain">>, zone => external},

    reload([{password_hash, plain},
            {auth_query, "select password from mqtt_user where username = '%u' and 'a_cn_val' = '%C' limit 1"}]),
    {error, not_authorized} =
        emqx_access_control:authenticate(ClientA#{password => <<"plain">>}),
    {error, not_authorized} =
        emqx_access_control:authenticate(ClientA#{password => <<"plain">>, cn => undefined}),
    {ok, _} =
        emqx_access_control:authenticate(ClientA#{password => <<"plain">>, cn => <<"a_cn_val">>}),

    reload([{auth_query, "select password from mqtt_user where username = '%c' and 'a_dn_val' = '%d' limit 1"}]),
    {error, not_authorized} =
        emqx_access_control:authenticate(ClientA#{password => <<"plain">>}),
    {error, not_authorized} =
        emqx_access_control:authenticate(ClientA#{password => <<"plain">>, dn => undefined}),
    {ok, _} =
        emqx_access_control:authenticate(ClientA#{password => <<"plain">>, dn => <<"a_dn_val">>}),

    reload([{auth_query, "select password from mqtt_user where username = '%u' and '192.168.1.5' = '%a' limit 1"}]),
    {error, not_authorized} =
        emqx_access_control:authenticate(ClientA#{password => <<"plain">>}),
    {ok, _} =
        emqx_access_control:authenticate(ClientA#{password => <<"plain">>, peerhost => {192,168,1,5}}).

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

reload(Config) when is_list(Config) ->
    application:stop(?APP),
    [application:set_env(?APP, K, V) || {K, V} <- Config],
    application:start(?APP).
