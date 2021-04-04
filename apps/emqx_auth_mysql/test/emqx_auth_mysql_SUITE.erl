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

-module(emqx_auth_mysql_SUITE).

-compile(export_all).
-compile(nowarn_export_all).
-compile([{parse_transform, emqx_ct_jobs_suite_transform}]).

-define(APP, emqx_auth_mysql).
-define(JOBS_NODE_PREFIX, <<"job-matrix-test-node-">>).

-include_lib("emqx_ct_helpers/include/emqx_ct_job.hrl").
-include_lib("emqx_ct_helpers/include/emqx_ct_jobs_transform.hrl").
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

-define(JOB_MYSQLV8, mysqlV8).
-define(JOB_MYSQLV5_7, mysqlV5_7).
-define(JOB_TCP, tcp).
-define(JOB_TLS, tls).
-define(JOB_IPV4, ipv4).
-define(JOB_IPV6, ipv6).

%% -------------------------------------------------------------------------------------------------
%%  Jobs Setup
%% -------------------------------------------------------------------------------------------------

%% ( Multiple configurations concurrently in unique nodes )
job_matrix() ->
    [[?JOB_MYSQLV5_7, ?JOB_MYSQLV8],
     [?JOB_TCP, ?JOB_TLS],
     [?JOB_IPV4, ?JOB_IPV6]].

init_per_job(Job) ->
    proc_lib:spawn(fun() -> start_mysql_dep_container(Job) end ).

start_mysql_dep_container(#ct_job{name=Name, vectors=[VSN,Connection,IPv], index=Index} = Job) ->
    DEnv = docker_env(Name, VSN, Index),
    File = docker_compose_file(Connection),
    JobName = binary_to_list(Name),
    server_env(JobName, IPv, Index),
    JobName = binary_to_list(Name),
    DockerRes = emqx_ct_helpers_docker:compose(File, JobName, "mysql_server", "", DEnv),
    io:format("~n Job :~p DockerRes : ~p ", [Job, DockerRes]).

end_per_job(#ct_job{ name = Name }, _Config) ->
    JobName = binary_to_list(Name),
    emqx_ct_helpers_docker:force_remove(JobName, true).

job_options(#ct_job{ vectors = [_VSN, tls, _IPv], index = Index}) ->
    AllJobsEnv = all_jobs_env(Index),
    {ok, CaPath} = data_file("ca.pem"),
    {ok, CertPath} = data_file("client-cert.pem"),
    {ok, KeyPath} = data_file("client-key.pem"),
    TlsEnvs = [{"EMQX_AUTH__MYSQL__SSL", "on"},
               {"EMQX_AUTH__MYSQL__SSL__CACERTFILE", CaPath},
               {"EMQX_AUTH__MYSQL__SSL__CERTFILE", CertPath},
               {"EMQX_AUTH__MYSQL__SSL__KEYFILE", KeyPath},
               {"EMQX_AUTH__MYSQL__SSL__VERIFY", "true"},
               {"EMQX_AUTH__MYSQL__SSL__SERVER_NAME_INDICATION", "disable"}
              ],
    Envs = AllJobsEnv ++ TlsEnvs,
    io:format("~n TLS Envs : ~p ", [ Envs ] ),
    [{env, Envs}];
job_options(#ct_job{ vectors = [_VSN, tcp, _IPv], index = Index}) ->
    AllJobsEnv = all_jobs_env(Index),
    TcpEnvs = [{"EMQX_AUTH__MYSQL__SSL", "off"}],
    Envs = AllJobsEnv ++ TcpEnvs,
    io:format("~n TCP Envs : ~p ", [ Envs ] ),
    [{env, Envs}].

%% -------------------------------------------------------------------------------------------------
%%  Suite Setups
%% -------------------------------------------------------------------------------------------------

all() ->
    emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    JobsConfig = proplists:get_value(?JOBS_MATRIX_CONFIG, Config),
    Job = proplists:get_value(job, JobsConfig),
    emqx_ct_helpers:start_apps([emqx_auth_mysql], set_special_configs(Job)),
    init_mysql_data(),
    Config.

end_per_suite(_) ->
    deinit_mysql_data(),
    emqx_ct_helpers:stop_apps([emqx_auth_mysql]),
    ok.

set_special_configs(Job) ->
    fun(App) ->
        set_special_configs(Job, App)
    end.

set_special_configs(#ct_job{ index = Index }, emqx) ->
    application:load(gen_rpc),
    application:set_env(gen_rpc, port_discovery, manual),
    application:set_env(gen_rpc, tcp_server_port, ensure_port(Index, 5369)),
    application:set_env(gen_rpc, ssl_server_port, false),
    application:set_env(gen_rpc, tcp_client_port, ensure_port(Index, 5369)),

    PluginPath = emqx_ct_helpers:deps_path(emqx, "test/emqx_SUITE_data/loaded_plugins"),

    application:set_env(emqx, allow_anonymous, false),
    application:set_env(emqx, enable_acl_cache, false),
    application:set_env(emqx, plugins_loaded_file, PluginPath);
set_special_configs(_Job, _App) ->
    ok.

init_mysql_data() ->
    init_mysql_data(120000).

init_mysql_data(0) ->
    exit(" Unable to connect to MYSQL");
init_mysql_data(WaitFor) ->
    case gproc_pool:pick_worker({ecpool, ?APP}) of
        false ->
            timer:sleep(2000),
            init_mysql_data(WaitFor-2000);
        Worker ->
            io:format("~n Worker : ~p ", [ Worker ] ),
            {ok, Pid} = ecpool_worker:client(Worker),
            %% Users
            ok = mysql:query(Pid, ?DROP_AUTH_TABLE),
            ok = mysql:query(Pid, ?CREATE_AUTH_TABLE),
            ok = mysql:query(Pid, ?INIT_AUTH),

            %% ACLs
            ok = mysql:query(Pid, ?DROP_ACL_TABLE),
            ok = mysql:query(Pid, ?CREATE_ACL_TABLE),
            ok = mysql:query(Pid, ?INIT_ACL)
    end.

deinit_mysql_data() ->
    {ok, Pid} = ecpool_worker:client(gproc_pool:pick_worker({ecpool, ?APP})),
    ok = mysql:query(Pid, ?DROP_AUTH_TABLE),
    ok = mysql:query(Pid, ?DROP_ACL_TABLE).

%% -------------------------------------------------------------------------------------------------
%% Test cases
%% -------------------------------------------------------------------------------------------------

t_check_acl(_) ->
    User0 = #{zone => external, peerhost => {127, 0, 0, 1}},
    allow = emqx_access_control:check_acl(User0, subscribe, <<"t1">>),
    User1 = #{zone => external, clientid => <<"c1">>, username => <<"u1">>, peerhost => {127, 0, 0, 1}},
    User2 = #{zone => external, clientid => <<"c2">>, username => <<"u2">>, peerhost => {127, 0, 0, 1}},
    allow = emqx_access_control:check_acl(User1, subscribe, <<"t1">>),
    deny = emqx_access_control:check_acl(User2, subscribe, <<"t1">>),

    User3 = #{zone => external, peerhost => {10, 10, 0, 110}, clientid => <<"c1">>, username => <<"u1">>},
    User4 = #{zone => external, peerhost => {10, 10, 10, 110}, clientid => <<"c1">>, username => <<"u1">>},
    allow = emqx_access_control:check_acl(User3, subscribe, <<"t1">>),
    allow = emqx_access_control:check_acl(User3, subscribe, <<"t1">>),
    allow = emqx_access_control:check_acl(User3, subscribe, <<"t2">>),%% nomatch -> ignore -> emqx acl
    allow = emqx_access_control:check_acl(User4, subscribe, <<"t1">>),%% nomatch -> ignore -> emqx acl
    User5 = #{zone => external, peerhost => {127, 0, 0, 1}, clientid => <<"c3">>, username => <<"u3">>},
    allow = emqx_access_control:check_acl(User5, subscribe, <<"t1">>),
    allow = emqx_access_control:check_acl(User5, publish, <<"t1">>).

t_acl_super(Config) ->
    reload([{password_hash, plain},
            {auth_query, "select password from mqtt_user where username = '%u' limit 1"}]),

    JobsConfig = proplists:get_value(?JOBS_MATRIX_CONFIG, Config),
    #ct_job{ index = Index } = proplists:get_value(job, JobsConfig),

    {ok, C} = emqtt:start_link([{host, "localhost"},
                                {port, ensure_port(Index, 1983)},
                                {clientid, <<"simpleClient">>},
                                {username, <<"plain">>},
                                {password, <<"plain">>}]),

    try emqtt:connect(C) of
        {ok, _} ->
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
            emqtt:disconnect(C)
    catch
        Type:Exception:Stack ->
            io:format("~n Type : ~p ", [ Type ] ),
            io:format("~n Exception : ~p ", [ Exception ] ),
            io:format("~n Stack : ~p ", [ Stack ] )
    end.

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
    {ok, #{is_superuser := true}} =
        emqx_access_control:authenticate(Plain#{password => <<"plain">>}),
    reload([{password_hash, md5}]),
    {ok, #{is_superuser := false}} =
        emqx_access_control:authenticate(Md5#{password => <<"md5">>}),
    reload([{password_hash, sha}]),
    {ok, #{is_superuser := false}} =
        emqx_access_control:authenticate(Sha#{password => <<"sha">>}),
    reload([{password_hash, sha256}]),
    {ok, #{is_superuser := false}} =
        emqx_access_control:authenticate(Sha256#{password => <<"sha256">>}),
    reload([{password_hash, bcrypt}]),
    {ok, #{is_superuser := false}} =
        emqx_access_control:authenticate(Bcrypt#{password => <<"password">>}),
    {error, not_authorized} =
        emqx_access_control:authenticate(BcryptWrong#{password => <<"password">>}),
    %%pbkdf2 sha
    reload([{password_hash, {pbkdf2, sha, 1, 16}},
            {auth_query, "select password, salt from mqtt_user where username = '%u' limit 1"}]),
    {ok, #{is_superuser := false}} =
        emqx_access_control:authenticate(Pbkdf2#{password => <<"password">>}),
    reload([{password_hash, {salt, bcrypt}}]),
    {ok, #{is_superuser := false}} =
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
        emqx_access_control:authenticate(ClientA#{password => <<"plain">>, peerhost => {192, 168, 1, 5}}).

%% -------------------------------------------------------------------------------------------------
%% Internal funcs
%% -------------------------------------------------------------------------------------------------

reload(Config) when is_list(Config) ->
    application:stop(?APP),
    [application:set_env(?APP, K, V) || {K, V} <- Config],
    application:start(?APP).


%% -------------------------------------------------------------------------------------------------
%%  JOB SETUP HELPERS
%% =================================================================================================

server_env(ContainerName, IPv, Index) ->
    {ok, SERVER_IP} = emqx_ct_helpers_jobs:container_ip(ContainerName, IPv),
    Port = ensure_port(Index, 3306),
    PortStr = integer_to_list(Port),
    Server = SERVER_IP ++ ":" ++ PortStr,
    os:set_env_var( "EMQX_AUTH__MYSQL__SERVER", Server ).

all_jobs_env(Index) ->
    [{"EMQX_LISTENER__TCP__INTERNAL", port(Index, 1983)},
     {"EMQX_LISTENER__TCP__EXTERNAL", port(Index, 1883)},
     {"EMQX_LISTENER__SSL__EXTERNAL", port(Index, 4883)},
     {"EMQX_LISTENER__WS__EXTERNAL",  port(Index, 4083)},
     {"EMQX_LISTENER__WSS__EXTERNAL", port(Index, 4184)},
     {"EMQX_AUTH__MYSQL__USERNAME", "emqx"},
     {"EMQX_AUTH__MYSQL__PASSWORD", "public"},
     {"EMQX_AUTH__MYSQL__DATABASE", "mqtt"},
     {"EMQX_AUTH__MYSQL__START_UP_ATTEMPTS", "120"},
     {"EMQX_AUTH__MYSQL__AUTO_RECONNECT", "1"},
     {"CUTTLEFISH_ENV_OVERRIDE_PREFIX", "EMQX_"}].

mysql_vsn(?JOB_MYSQLV8)   -> "8";
mysql_vsn(?JOB_MYSQLV5_7) -> "5.7".

docker_env(JobName, VSN, Index) ->
    Port = ensure_port(Index, 3306),
    PortStr = integer_to_list(Port),
    MYSQL_TAG = mysql_vsn(VSN),
    JobNameStr = binary_to_list(JobName),
    "MYSQL_CONTAINER_NAME=" ++ JobNameStr ++
                               "\nMYSQL_TAG=" ++ MYSQL_TAG ++
                               "\nMAP_PORT=" ++ PortStr.

docker_compose_file(Connection) ->
    {ok, RepoRoot} = emqx_ct_helpers_jobs:repo_root(),
    ComposeFile = case Connection of
                      tls -> "docker-compose-mysql-tls.yaml";
                      tcp -> "docker-compose-mysql-tcp.yaml"
                  end,
    File = filename:join([RepoRoot, ".ci", "docker-compose-file", ComposeFile]),
    io:format("~n Docker Compose File : ~p ", [ File ] ),
    File.

port(Index, PortStart) ->
    Port = ensure_port(Index, PortStart),
    integer_to_list(Port).

ensure_port(Index, Port) ->
    {ok, Platform} = emqx_ct_helpers_jobs:test_platform_host(),
    case Platform of
        local -> Port * 10 + Index;
        _Other -> Port
    end.

get_job() ->
    JobName = list_to_binary(get_name()),
    Jobs = emqx_ct_helpers_jobs:matrix_jobs(?JOB_MATRIX()),
    Job = lists:keyfind(JobName, 2, Jobs),
    Job.

get_name() ->
    Node = atom_to_list(node()),
    [Name, _Host] = string:tokens(Node, "@"),
    case string:prefix(Name, binary_to_list(?JOBS_NODE_PREFIX)) of
        nomatch -> Name;
        NameCleaned -> NameCleaned
    end.

data_file(Filename) ->
    {ok, Dir} = get_data_dir(),
    Path = filename:join([Dir, Filename]),
    {ok, Path}.


get_data_dir() ->
    BeamFile = lists:concat([?MODULE, ".beam"]),
    Path = filename:dirname(code:where_is_file(BeamFile)),
    Dir = filename:join([Path, "emqx_auth_mysql_SUITE_data"]),
    {ok, Dir}.
