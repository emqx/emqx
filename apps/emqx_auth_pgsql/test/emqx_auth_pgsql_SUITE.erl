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

-module(emqx_auth_pgsql_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-define(POOL, emqx_auth_pgsql_test_suite).

-define(APP, emqx_auth_pgsql).

-include_lib("emqx/include/emqx.hrl").

-include_lib("eunit/include/eunit.hrl").

-include_lib("common_test/include/ct.hrl").

%%setp1 init table
-define(DROP_ACL_TABLE, "DROP TABLE IF EXISTS mqtt_acl").

-define(CREATE_ACL_TABLE, "CREATE TABLE mqtt_acl (
                           id SERIAL primary key,
                           allow integer,
                           ipaddr character varying(60),
                           username character varying(100),
                           clientid character varying(100),
                           access  integer,
                           topic character varying(100))").

-define(INIT_ACL, "INSERT INTO mqtt_acl (id, allow, ipaddr, username, clientid, access, topic)
                   VALUES
                   (1,1,'127.0.0.1','u1','c1',1,'t1'),
                   (2,0,'127.0.0.1','u2','c2',1,'t1'),
                   (3,1,'10.10.0.110','u1','c1',1,'t1'),
                   (4,1,'127.0.0.1','u3','c3',3,'t1')").

-define(DROP_AUTH_TABLE, "DROP TABLE IF EXISTS mqtt_user").

-define(CREATE_AUTH_TABLE, "CREATE TABLE mqtt_user (
                            id SERIAL primary key,
                            is_superuser boolean,
                            username character varying(100),
                            password character varying(100),
                            salt character varying(40))").

-define(INIT_AUTH, "INSERT INTO mqtt_user (id, is_superuser, username, password, salt)
                     VALUES
                     (1, true, 'plain', 'plain', 'salt'),
                     (2, false, 'md5', '1bc29b36f623ba82aaf6724fd3b16718', 'salt'),
                     (3, false, 'sha', 'd8f4590320e1343a915b6394170650a8f35d6926', 'salt'),
                     (4, false, 'sha256', '5d5b09f6dcb2d53a5fffc60c4ac0d55fabdf556069d6631545f42aa6e3500f2e', 'salt'),
                     (5, false, 'pbkdf2_password', 'cdedb5281bb2f801565a1122b2563515', 'ATHENA.MIT.EDUraeburn'),
                     (6, false, 'bcrypt_foo', '$2a$12$sSS8Eg.ovVzaHzi1nUHYK.HbUIOdlQI0iS22Q5rd5z.JVVYH6sfm6', '$2a$12$sSS8Eg.ovVzaHzi1nUHYK.'),
                     (7, false, 'bcrypt', '$2y$16$rEVsDarhgHYB0TGnDFJzyu5f.T.Ha9iXMTk9J36NCMWWM7O16qyaK', 'salt')").

-define(OS_ENVS(SERVER, USER, PASSWD, DB), [SERVER, USER, PASSWD, DB]).
-define(CMD,"EMQX_AUTH__PGSQL__SERVER=127.0.0.1:5432 "
            "EMQX_AUTH__PGSQL__USERNAME=root "
            "EMQX_AUTH__PGSQL__PASSWORD=public "
            "EMQX_AUTH__PGSQL__DATABASE=mqtt "
            "./rebar3 ct --name 'test@127.0.0.1' -v --suite apps/emqx_auth_pgsql/test/emqx_auth_pgsql_SUITE").
all() ->
    emqx_ct:all(?MODULE).

suite() ->
    [{timetrap, {seconds, 120}}].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(ecpool),
    case get_params_from_os_env(?OS_ENVS(
            "EMQX_AUTH__PGSQL__SERVER",
            "EMQX_AUTH__PGSQL__USERNAME",
            "EMQX_AUTH__PGSQL__PASSWORD",
            "EMQX_AUTH__PGSQL__DATABASE")) of
        {error, Reason} ->
            ct:print("You could start your test as: "?CMD),
            {skip, Reason};
        {ok, ?OS_ENVS(Server, Username, Password, Database)} ->
            do_init_per_suite(Server, Username, Password, Database, Config)
    end.

get_params_from_os_env(Keys) ->
    get_params_from_os_env(Keys, []).

get_params_from_os_env([], Acc) ->
    {ok, lists:reverse(Acc)};
get_params_from_os_env([Key | Keys], Acc) ->
    case os:getenv(Key) of
        false -> {error, iolist_to_binary(io_lib:format("OS Env ~s is undefined", [Key]))};
        Val -> get_params_from_os_env(Keys, [Val | Acc])
    end.

do_init_per_suite(Server, Username, Password, Database, Config) ->
    {HostS, Port} = case string:split(Server, ":", trailing) of
        [Domain] -> {string:trim(Domain), 5432};
        [Domain, PortS] -> {string:trim(Domain), list_to_integer(string:trim(PortS))}
    end,
    Host = case inet:parse_address(HostS) of
        {ok, IpAddr} -> IpAddr;
        _ -> HostS
    end,
    Opts = [{host, Host},
            {port, Port},
            {username, Username},
            {password, Password},
            {database, Database},
            {timeout, 5000},
            {auto_reconnect, 15},
            {encoding, utf8},
            {pool_type, random},
            {pool_size, 8}],
    case ecpool:start_sup_pool(?POOL, ?MODULE, Opts) of
        {ok, _} -> ok;
        {error, Reason} ->
            {error, Reason}
    end,
    drop_acl(),
    drop_auth(),
    init_auth(),
    init_acl(),
    emqx_ct_helpers:start_apps([emqx_auth_pgsql]),
    set_special_configs(),
    Config.

connect(Opts) ->
    Host = proplists:get_value(host, Opts),
    Username = proplists:get_value(username, Opts),
    Password = proplists:get_value(password, Opts),
    case epgsql:connect(Host, Username, Password, conn_opts(Opts)) of
        {ok, C} -> {ok, C};
        {error, Reason} -> {error, Reason}
    end.

end_per_suite(Config) ->
    emqx_ct_helpers:stop_apps([emqx_auth_pgsql]),
    Config.

set_special_configs() ->
    application:set_env(emqx, acl_nomatch, deny),
    application:set_env(emqx, allow_anonymous, false),
    application:set_env(emqx, enable_acl_cache, false).

t_comment_config(_) ->
    AuthCount = length(emqx_hooks:lookup('client.authenticate')),
    AclCount = length(emqx_hooks:lookup('client.check_acl')),
    application:stop(?APP),
    [application:unset_env(?APP, Par) || Par <- [acl_query, auth_query]],
    application:start(?APP),
    ?assertEqual([], emqx_hooks:lookup('client.authenticate')),
    ?assertEqual(AuthCount - 1, length(emqx_hooks:lookup('client.authenticate'))),
    ?assertEqual(AclCount - 1, length(emqx_hooks:lookup('client.check_acl'))).

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

t_check_acl(_) ->
    User1 = #{zone => external, peerhost => {127,0,0,1}, clientid => <<"c1">>, username => <<"u1">>},
    User2 = #{zone => external, peerhost => {127,0,0,1}, clientid => <<"c2">>, username => <<"u2">>},
    allow = emqx_access_control:check_acl(User1, subscribe, <<"t1">>),
    deny = emqx_access_control:check_acl(User2, subscribe, <<"t1">>),
    User3 = #{zone => external, peerhost => {10,10,0,110}, clientid => <<"c1">>, username => <<"u1">>},
    User4 = #{zone => external, peerhost => {10,10,10,110}, clientid => <<"c1">>, username => <<"u1">>},
    allow = emqx_access_control:check_acl(User3, subscribe, <<"t1">>),
    allow = emqx_access_control:check_acl(User3, subscribe, <<"t1">>),
    deny = emqx_access_control:check_acl(User3, subscribe, <<"t2">>),%% nomatch -> ignore -> emqx acl
    deny = emqx_access_control:check_acl(User4, subscribe, <<"t1">>),%% nomatch -> ignore -> emqx acl
    User5 = #{zone => external, peerhost => {127,0,0,1}, clientid => <<"c3">>, username => <<"u3">>},
    allow = emqx_access_control:check_acl(User5, subscribe, <<"t1">>),
    allow = emqx_access_control:check_acl(User5, publish, <<"t1">>).

t_acl_super(_) ->
    reload([{password_hash, plain}, {auth_query, "select password from mqtt_user where username = '%u' limit 1"}]),
    {ok, C} = emqtt:start_link([{host, "localhost"}, {clientid, <<"simpleClient">>},
                                {username, <<"plain">>}, {password, <<"plain">>}]),
    {ok, _} = emqtt:connect(C),
    timer:sleep(10),
    emqtt:subscribe(C, <<"TopicA">>, qos2),
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

reload(Config) when is_list(Config) ->
    application:stop(?APP),
    [application:set_env(?APP, K, V) || {K, V} <- Config],
    application:start(?APP).

init_acl() ->
    ecpool:with_client(?POOL, fun(Pid) ->
        {ok, [], []} = epgsql:squery(Pid, ?DROP_ACL_TABLE),
        {ok, [], []} = epgsql:squery(Pid, ?CREATE_ACL_TABLE),
        {ok, _} = epgsql:equery(Pid, ?INIT_ACL)
    end).

drop_acl() ->
    ecpool:with_client(?POOL, fun(Pid) ->
        {ok, [], []}= epgsql:squery(Pid, ?DROP_ACL_TABLE)
    end).

init_auth() ->
    ecpool:with_client(?POOL, fun(Pid) ->
        {ok, [], []} = epgsql:squery(Pid, ?DROP_AUTH_TABLE),
        {ok, [], []} = epgsql:squery(Pid, ?CREATE_AUTH_TABLE),
        {ok, _} = epgsql:equery(Pid, ?INIT_AUTH)
    end).

drop_auth() ->
    ecpool:with_client(?POOL, fun(Pid) ->
        {ok, [], []} = epgsql:squery(Pid, ?DROP_AUTH_TABLE)
    end).

conn_opts(Opts) ->
    conn_opts(Opts, []).
conn_opts([], Acc) ->
    Acc;
conn_opts([Opt = {database, _}|Opts], Acc) ->
    conn_opts(Opts, [Opt|Acc]);
conn_opts([Opt = {ssl, _}|Opts], Acc) ->
    conn_opts(Opts, [Opt|Acc]);
conn_opts([Opt = {port, _}|Opts], Acc) ->
    conn_opts(Opts, [Opt|Acc]);
conn_opts([Opt = {timeout, _}|Opts], Acc) ->
    conn_opts(Opts, [Opt|Acc]);
conn_opts([Opt = {ssl_opts, _}|Opts], Acc) ->
    conn_opts(Opts, [Opt|Acc]);
conn_opts([_Opt|Opts], Acc) ->
    conn_opts(Opts, Acc).
