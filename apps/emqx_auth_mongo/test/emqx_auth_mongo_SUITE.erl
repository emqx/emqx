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

-module(emqx_auth_mongo_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_auth_mongo.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(POOL(App),  ecpool_worker:client(gproc_pool:pick_worker({ecpool, App}))).

-define(MONGO_CL_ACL, <<"mqtt_acl">>).
-define(MONGO_CL_USER, <<"mqtt_user">>).

-define(INIT_ACL, [ { <<"username">>, <<"testuser">>
                    , <<"clientid">>, <<"null">>
                    , <<"subscribe">>, [<<"#">>]
                    }
                  , { <<"username">>, <<"dashboard">>
                    , <<"clientid">>, <<"null">>
                    , <<"pubsub">>, [<<"$SYS/#">>]
                    }
                  , { <<"username">>, <<"user3">>
                    , <<"clientid">>, <<"null">>
                    , <<"publish">>, [<<"a/b/c">>]
                    }
                  ]).

-define(INIT_AUTH, [ { <<"username">>, <<"plain">>
                     , <<"password">>, <<"plain">>
                     , <<"salt">>, <<"salt">>
                     , <<"is_superuser">>, true
                     }
                   , { <<"username">>, <<"md5">>
                     , <<"password">>, <<"1bc29b36f623ba82aaf6724fd3b16718">>
                     , <<"salt">>, <<"salt">>
                     , <<"is_superuser">>, false
                     }
                   , { <<"username">>, <<"sha">>
                     , <<"password">>, <<"d8f4590320e1343a915b6394170650a8f35d6926">>
                     , <<"salt">>, <<"salt">>
                     , <<"is_superuser">>, false
                     }
                   , { <<"username">>, <<"sha256">>
                     , <<"password">>, <<"5d5b09f6dcb2d53a5fffc60c4ac0d55fabdf556069d6631545f42aa6e3500f2e">>
                     , <<"salt">>, <<"salt">>
                     , <<"is_superuser">>, false
                     }
                   , { <<"username">>, <<"pbkdf2_password">>
                     , <<"password">>, <<"cdedb5281bb2f801565a1122b2563515">>
                     , <<"salt">>, <<"ATHENA.MIT.EDUraeburn">>
                     , <<"is_superuser">>, false
                     }
                   , { <<"username">>, <<"bcrypt_foo">>
                     , <<"password">>, <<"$2a$12$sSS8Eg.ovVzaHzi1nUHYK.HbUIOdlQI0iS22Q5rd5z.JVVYH6sfm6">>
                     , <<"salt">>, <<"$2a$12$sSS8Eg.ovVzaHzi1nUHYK.">>
                     , <<"is_superuser">>, false
                     }
                   , { <<"username">>, <<"user_full">>
                     , <<"clientid">>, <<"client_full">>
                     , <<"common_name">>, <<"cn_full">>
                     , <<"distinguished_name">>, <<"dn_full">>
                     , <<"password">>, <<"plain">>
                     , <<"salt">>, <<"salt">>
                     , <<"is_superuser">>, false
                     }
                   ]).

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() ->
    OtherTCs = emqx_ct:all(?MODULE) -- resilience_tests(),
    [ {group, resilience}
    | OtherTCs].

resilience_tests() ->
    [ t_acl_superuser_timeout
    , t_available_acl_query_no_connection
    , t_available_acl_query_timeout
    , t_available_authn_query_timeout
    , t_authn_timeout
    , t_available
    ].

groups() ->
    [ {resilience, resilience_tests()}
    ].

init_per_suite(Config) ->
    emqx_ct_helpers:start_apps([emqx_auth_mongo], fun set_special_confs/1),
    %% avoid inter-suite flakiness
    ok = emqx_mod_acl_internal:unload([]),
    Config.

end_per_suite(_Cfg) ->
    deinit_mongo_data(),
    %% avoid inter-suite flakiness
    emqx_mod_acl_internal:load([]),
    emqx_ct_helpers:stop_apps([emqx_auth_mongo]).

set_special_confs(emqx) ->
    application:set_env(emqx, acl_nomatch, deny),
    application:set_env(emqx, allow_anonymous, false),
    application:set_env(emqx, enable_acl_cache, false);
set_special_confs(_App) ->
    ok.

init_per_group(resilience, Config) ->
    ProxyHost = os:getenv("PROXY_HOST", "toxiproxy"),
    ProxyPortStr = os:getenv("PROXY_PORT", "8474"),
    ProxyPort = list_to_integer(ProxyPortStr),
    reset_proxy(ProxyHost, ProxyPort),
    ProxyServer = ProxyHost ++ ":27017",
    {ok, OriginalServer} = application:get_env(emqx_auth_mongo, server),
    OriginalServerMap = maps:from_list(OriginalServer),
    NewServerMap = OriginalServerMap#{hosts => [ProxyServer]},
    NewServer = maps:to_list(NewServerMap),
    emqx_ct_helpers:stop_apps([emqx_auth_mongo]),
    Handler =
        fun(App = emqx_auth_mongo) ->
             application:set_env(emqx_auth_mongo, server, NewServer),
             set_special_confs(App);
           (App)->
             set_special_confs(App)
        end,
    emqx_ct_helpers:start_apps([emqx_auth_mongo], Handler),
    [ {original_server, OriginalServer}
    , {proxy_host, ProxyHost}
    , {proxy_port, ProxyPort}
    | Config];
init_per_group(_Group, Config) ->
    Config.

end_per_group(resilience, Config) ->
    OriginalServer = ?config(original_server, Config),
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    application:set_env(emqx_auth_mongo, server, OriginalServer),
    emqx_ct_helpers:stop_apps([emqx_auth_mongo]),
    reset_proxy(ProxyHost, ProxyPort),
    emqx_ct_helpers:start_apps([emqx_auth_mongo], fun set_special_confs/1),
    ok;
end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(t_authn_full_selector_variables, Config) ->
    {ok, AuthQuery} = application:get_env(emqx_auth_mongo, auth_query),
    OriginalSelector = proplists:get_value(selector, AuthQuery),
    Selector = [ {<<"username">>, <<"%u">>}
               , {<<"clientid">>, <<"%c">>}
               , {<<"common_name">>, <<"%C">>}
               , {<<"distinguished_name">>, <<"%d">>}
               ],
    reload({auth_query, [{selector, Selector}]}),
    init_mongo_data(),
    [ {original_selector, OriginalSelector}
    , {selector, Selector}
    | Config];
init_per_testcase(_TestCase, Config) ->
    init_mongo_data(),
    Config.

end_per_testcase(t_authn_full_selector_variables, Config) ->
    OriginalSelector = ?config(original_selector, Config),
    reload({auth_query, [{selector, OriginalSelector}]}),
    deinit_mongo_data(),
    ok;
end_per_testcase(TestCase, Config)
 when TestCase =:= t_available_acl_query_timeout;
      TestCase =:= t_acl_superuser_timeout;
      TestCase =:= t_authn_no_connection;
      TestCase =:= t_available_authn_query_timeout;
      TestCase =:= t_authn_timeout;
      TestCase =:= t_available_acl_query_no_connection ->
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    reset_proxy(ProxyHost, ProxyPort),
    %% force restart of clients because CI tends to get stuck...
    application:stop(emqx_auth_mongo),
    application:start(emqx_auth_mongo),
    wait_for_stabilization(#{attempts => 10, interval_ms => 500}),
    deinit_mongo_data(),
    ok;
end_per_testcase(_TestCase, _Config) ->
    deinit_mongo_data(),
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

%% for full coverage ;-)
t_authn_description(_Config) ->
    ?assert(is_list(emqx_auth_mongo:description())).

%% for full coverage ;-)
t_acl_description(_Config) ->
    ?assert(is_list(emqx_acl_mongo:description())).

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
    {error, _} = emqx_access_control:authenticate(User1#{password => <<"foo">>}),
    %% bad field config
    reload({auth_query, [{password_field, [<<"bad_field">>]}]}),
    ?assertEqual({error, password_error},
                 emqx_access_control:authenticate(Plain#{password => <<"plain">>})),
    %% unknown username
    Unknown = #{zone => unknown, clientid => <<"?">>, username => <<"?">>, password => <<"">>},
    ?assertEqual({error, not_authorized}, emqx_access_control:authenticate(Unknown)),
    ok.

t_authn_full_selector_variables(Config) ->
    Selector = ?config(selector, Config),
    ClientInfo = #{ zone => external
                  , clientid => <<"client_full">>
                  , username => <<"user_full">>
                  , cn => <<"cn_full">>
                  , dn => <<"dn_full">>
                  , password => <<"plain">>
                  },
    ?assertMatch({ok, _}, emqx_access_control:authenticate(ClientInfo)),
    EnvFields = [ clientid
                , username
                , cn
                , dn
                ],
    lists:foreach(
     fun(Field) ->
       UnauthorizedClientInfo = ClientInfo#{Field => <<"wrong">>},
       ?assertEqual({error, not_authorized},
                    emqx_access_control:authenticate(UnauthorizedClientInfo),
                    #{ field => Field
                     , client_info => UnauthorizedClientInfo
                     , selector => Selector
                     })
     end,
     EnvFields),
    ok.

t_authn_interpolation_no_info(_Config) ->
    Valid = #{zone => external, clientid => <<"client1">>,
              username => <<"plain">>, password => <<"plain">>},
    ?assertMatch({ok, _}, emqx_access_control:authenticate(Valid)),
    try
        %% has values that are equal to placeholders
        InterpolationUser = #{ <<"username">> => <<"%u">>
                             , <<"password">> => <<"plain">>
                             , <<"salt">> => <<"salt">>
                             , <<"is_superuser">> => true
                             },
        {ok, Conn} = ?POOL(?APP),
        {{true, _}, _} = mongo_api:insert(Conn, ?MONGO_CL_USER, InterpolationUser),
        Invalid = maps:without([username], Valid),
        ?assertMatch({error, not_authorized}, emqx_access_control:authenticate(Invalid))
    after
        deinit_mongo_data(),
        init_mongo_data()
    end.

%% authenticates, but superquery returns no documents
t_authn_empty_is_superuser_collection(_Config) ->
    {ok, SuperQuery} = application:get_env(emqx_auth_mongo, super_query),
    Collection = list_to_binary(proplists:get_value(collection, SuperQuery)),
    reload({auth_query, [{password_hash, plain}]}),
    Plain = #{zone => external, clientid => <<"client1">>,
              username => <<"plain">>, password => <<"plain">>},
    ok = snabbkaffe:start_trace(),
    ?force_ordering(
      #{?snk_kind := emqx_auth_mongo_superuser_check_authn_ok},
      #{?snk_kind := truncate_coll_enter}),
    ?force_ordering(
      #{?snk_kind := truncate_coll_done},
      #{?snk_kind := emqx_auth_mongo_superuser_query_enter}),
    try
        spawn_link(fun() ->
          ?tp(truncate_coll_enter, #{}),
          {ok, Conn} = ?POOL(?APP),
          {true, _} = mongo_api:delete(Conn, Collection, _Selector = #{}),
          ?tp(truncate_coll_done, #{})
        end),
        ?assertMatch({ok, #{is_superuser := false}}, emqx_access_control:authenticate(Plain)),
        ok = snabbkaffe:stop(),
        ok
    after
        init_mongo_data()
    end.

t_available(Config) ->
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    Pool = ?APP,
    SuperQuery = #superquery{collection = SuperCollection} = superquery(),
    %% success;
    ?assertEqual(ok, emqx_auth_mongo:available(Pool, SuperQuery)),
    %% error with code;
    EmptySelector = #{},
    ?assertEqual(
       {error, {mongo_error, 2}},
       emqx_auth_mongo:available(Pool, SuperCollection, EmptySelector, fun error_code_query/3)),
    %% exception (in query)
    ?assertMatch(
       {error, _},
       with_failure(down, ProxyHost, ProxyPort,
         fun() ->
           Collection = <<"mqtt_user">>,
           Selector = #{},
           emqx_auth_mongo:available(Pool, Collection, Selector)
         end)),
    %% exception (arbitrary function)
    ?assertMatch(
       {error, _},
       with_failure(down, ProxyHost, ProxyPort,
         fun() ->
           Collection = <<"mqtt_user">>,
           Selector = #{},
           RaisingFun = fun(_, _, _) -> error(some_error) end,
           emqx_auth_mongo:available(Pool, Collection, Selector, RaisingFun)
         end)),
    ok.

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
    deny = emqx_access_control:check_acl(User4, publish, <<"a/b/c">>),
    %% undefined value to interpolate
    User1Undef = User1#{clientid => undefined},
    allow = emqx_access_control:check_acl(User1Undef, subscribe, <<"users/testuser/1">>),
    ok.

t_acl_empty_results(_Config) ->
    #aclquery{selector = Selector} = aclquery(),
    User1 = #{zone => external, clientid => <<"client1">>, username => <<"testuser">>},
    try
        reload({acl_query, [{selector, []}]}),
        ?assertEqual(deny, emqx_access_control:check_acl(User1, subscribe, <<"users/testuser/1">>)),
        ok
    after
        reload({acl_query, [{selector, Selector}]})
    end,
    ok.

t_acl_exception(_Config) ->
    %% FIXME: is there a more authentic way to produce an exception in
    %% `match'???
    User1 = #{zone => external, clientid => not_a_binary, username => <<"testuser">>},
    ?assertEqual(deny, emqx_access_control:check_acl(User1, subscribe, <<"users/testuser/1">>)),
    ok.

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

%% apparently, if the config is undefined in `emqx_auth_mongo_app:r',
%% this is allowed...
t_is_superuser_undefined(_Config) ->
    Pool = ClientInfo = unused_in_this_case,
    SuperQuery = undefined,
    ?assertNot(emqx_auth_mongo:is_superuser(Pool, SuperQuery, ClientInfo)),
    ok.

t_authn_timeout(Config) ->
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    FailureType = timeout,
    {ok, C} = emqtt:start_link([{clientid, <<"simpleClient">>},
                                {username, <<"plain">>},
                                {password, <<"plain">>}]),
    unlink(C),

    ?check_trace(
       try
           enable_failure(FailureType, ProxyHost, ProxyPort),
           {error, {unauthorized_client, _}} = emqtt:connect(C),
           ok
       after
           heal_failure(FailureType, ProxyHost, ProxyPort)
       end,
       fun(Trace) ->
           %% fails with `{exit,{{{badmatch,{{error,closed},...'
           ?assertMatch([_], ?of_kind(emqx_auth_mongo_check_authn_error, Trace)),
           ok
       end),

    ok.

%% tests query timeout failure
t_available_authn_query_timeout(Config) ->
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    FailureType = timeout,
    SuperQuery = superquery(),

    ?check_trace(
       #{timetrap => timer:seconds(60)},
       try
           enable_failure(FailureType, ProxyHost, ProxyPort),
           Pool = ?APP,
           %% query_multi returns an empty list even on failures.
           ?assertEqual({error, timeout}, emqx_auth_mongo:available(Pool, SuperQuery)),
           ok
       after
           heal_failure(FailureType, ProxyHost, ProxyPort)
       end,
       fun(Trace) ->
         ?assertMatch(
            [#{?snk_kind := emqx_auth_mongo_available_error , error := _}],
            ?of_kind(emqx_auth_mongo_available_error, Trace))
       end),

    ok.

%% tests query_multi failure
t_available_acl_query_no_connection(Config) ->
    test_acl_query_failure(down, Config).

%% ensure query_multi has a timeout
t_available_acl_query_timeout(Config) ->
    ct:timetrap(90000),
    test_acl_query_failure(timeout, Config).

%% checks that `with_timeout' lets unknown errors pass through
t_query_multi_unknown_exception(_Config) ->
    ok = meck:new(ecpool, [no_link, no_history, non_strict, passthrough]),
    ok = meck:expect(ecpool, with_client, fun(_, _) -> throw(some_error) end),
    Pool = ?APP,
    Collection = ?MONGO_CL_ACL,
    SelectorList = [#{<<"username">> => <<"user">>}],
    try
        ?assertThrow(some_error, emqx_auth_mongo:query_multi(Pool, Collection, SelectorList))
    after
        meck:unload(ecpool)
    end.

t_acl_superuser_timeout(Config) ->
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    FailureType = timeout,
    reload({auth_query, [{password_hash, plain}, {password_field, [<<"password">>]}]}),
    {ok, C} = emqtt:start_link([{clientid, <<"simpleClient">>},
                                {username, <<"plain">>},
                                {password, <<"plain">>}]),
    unlink(C),

    ?check_trace(
       try
           ?force_ordering(
              #{?snk_kind := emqx_auth_mongo_superuser_check_authn_ok},
              #{?snk_kind := connection_will_cut}
             ),
           ?force_ordering(
              #{?snk_kind := connection_cut},
              #{?snk_kind := emqx_auth_mongo_superuser_query_enter}
             ),
           spawn(fun() ->
                   ?tp(connection_will_cut, #{}),
                   enable_failure(FailureType, ProxyHost, ProxyPort),
                   ?tp(connection_cut, #{})
                 end),

           {ok, _} = emqtt:connect(C),
           ok = emqtt:disconnect(C),
           ok
       after
           heal_failure(FailureType, ProxyHost, ProxyPort)
       end,
       fun(Trace) ->
         ?assertMatch(
            [ #{ ?snk_kind := emqx_auth_mongo_superuser_query_error
               , error := _
               }
            , #{ ?snk_kind := emqx_auth_mongo_superuser_query_result
               , is_superuser := false
               }
            ],
            ?of_kind([ emqx_auth_mongo_superuser_query_error
                     , emqx_auth_mongo_superuser_query_result
                     ], Trace))
       end),

    ok.

%%--------------------------------------------------------------------
%% Utils
%%--------------------------------------------------------------------

test_acl_query_failure(FailureType, Config) ->
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    ACLQuery = aclquery(),

    ?check_trace(
       #{timetrap => timer:seconds(60)},
       try
           ?force_ordering(
              #{?snk_kind := emqx_auth_mongo_query_multi_enter},
              #{?snk_kind := connection_will_cut}
             ),
           ?force_ordering(
              #{?snk_kind := connection_cut},
              #{?snk_kind := emqx_auth_mongo_query_multi_find_selector}
             ),
           spawn(fun() ->
                   ?tp(connection_will_cut, #{}),
                   enable_failure(FailureType, ProxyHost, ProxyPort),
                   ?tp(connection_cut, #{})
                 end),
           Pool = ?APP,
           %% query_multi returns an empty list even on failures.
           ?assertMatch(ok, emqx_auth_mongo:available(Pool, ACLQuery)),
           ok
       after
           heal_failure(FailureType, ProxyHost, ProxyPort)
       end,
       fun(Trace) ->
         ?assertMatch(
            [#{?snk_kind := emqx_auth_mongo_query_multi_error , error := _}],
            ?of_kind(emqx_auth_mongo_query_multi_error, Trace))
       end),

    ok.

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

superquery() ->
    emqx_auth_mongo_app:with_env(super_query, fun(SQ) -> SQ end).

aclquery() ->
    emqx_auth_mongo_app:with_env(acl_query, fun(SQ) -> SQ end).

%% TODO: any easier way to make mongo return a map with an error code???
error_code_query(Pool, Collection, Selector) ->
    %% should be a query; this is to provoke an error return from
    %% mongo.
    WrongLimit = {},
    ecpool:with_client(
      Pool,
      fun(Conn) ->
        mongoc:transaction_query(
          Conn,
          fun(Conf = #{pool := Worker}) ->
            Query = mongoc:count_query(Conf, Collection, Selector, WrongLimit),
            {_, Res} = mc_worker_api:command(Worker, Query),
            Res
          end)
      end).

wait_for_stabilization(#{attempts := Attempts, interval_ms := IntervalMS})
  when Attempts > 0 ->
    try
        {ok, Conn} = ?POOL(?APP),
        #{} = mongo_api:find_one(Conn, ?MONGO_CL_USER, #{}, #{}),
        ok
    catch
        _:_ ->
            ct:pal("mongodb connection still stabilizing... sleeping for ~b ms", [IntervalMS]),
            ct:sleep(IntervalMS),
            wait_for_stabilization(#{attempts => Attempts - 1, interval_ms => IntervalMS})
    end;
wait_for_stabilization(_) ->
    error(mongo_connection_did_not_stabilize).

%% TODO: move to ct helpers???
reset_proxy(ProxyHost, ProxyPort) ->
    Url = "http://" ++ ProxyHost ++ ":" ++ integer_to_list(ProxyPort) ++ "/reset",
    Body = <<>>,
    {ok, {{_, 204, _}, _, _}} = httpc:request(post, {Url, [], "application/json", Body}, [],
                                              [{body_format, binary}]).

with_failure(FailureType, ProxyHost, ProxyPort, Fun) ->
    enable_failure(FailureType, ProxyHost, ProxyPort),
    try
        Fun()
    after
        heal_failure(FailureType, ProxyHost, ProxyPort)
    end.

enable_failure(FailureType, ProxyHost, ProxyPort) ->
    case FailureType of
        down -> switch_proxy(off, ProxyHost, ProxyPort);
        timeout -> timeout_proxy(on, ProxyHost, ProxyPort);
        latency_up -> latency_up_proxy(on, ProxyHost, ProxyPort)
    end.

heal_failure(FailureType, ProxyHost, ProxyPort) ->
    case FailureType of
        down -> switch_proxy(on, ProxyHost, ProxyPort);
        timeout -> timeout_proxy(off, ProxyHost, ProxyPort);
        latency_up -> latency_up_proxy(off, ProxyHost, ProxyPort)
    end.

switch_proxy(Switch, ProxyHost, ProxyPort) ->
    Url = "http://" ++ ProxyHost ++ ":" ++ integer_to_list(ProxyPort) ++ "/proxies/mongo_single",
    Body = case Switch of
               off -> <<"{\"enabled\":false}">>;
               on  -> <<"{\"enabled\":true}">>
           end,
    {ok, {{_, 200, _}, _, _}} = httpc:request(post, {Url, [], "application/json", Body}, [],
                                              [{body_format, binary}]).

timeout_proxy(on, ProxyHost, ProxyPort) ->
    Url = "http://" ++ ProxyHost ++ ":" ++ integer_to_list(ProxyPort) ++ "/proxies/mongo_single/toxics",
    Body = <<"{\"name\":\"timeout\",\"type\":\"timeout\","
             "\"stream\":\"upstream\",\"toxicity\":1.0,"
             "\"attributes\":{\"timeout\":0}}">>,
    {ok, {{_, 200, _}, _, _}} = httpc:request(post, {Url, [], "application/json", Body}, [],
                                              [{body_format, binary}]);
timeout_proxy(off, ProxyHost, ProxyPort) ->
    Url = "http://" ++ ProxyHost ++ ":" ++ integer_to_list(ProxyPort) ++ "/proxies/mongo_single/toxics/timeout",
    Body = <<>>,
    {ok, {{_, 204, _}, _, _}} = httpc:request(delete, {Url, [], "application/json", Body}, [],
                                              [{body_format, binary}]).

latency_up_proxy(on, ProxyHost, ProxyPort) ->
    Url = "http://" ++ ProxyHost ++ ":" ++ integer_to_list(ProxyPort) ++ "/proxies/mongo_single/toxics",
    Body = <<"{\"name\":\"latency_up\",\"type\":\"latency\","
             "\"stream\":\"upstream\",\"toxicity\":1.0,"
             "\"attributes\":{\"latency\":20000,\"jitter\":3000}}">>,
    {ok, {{_, 200, _}, _, _}} = httpc:request(post, {Url, [], "application/json", Body}, [],
                                              [{body_format, binary}]);
latency_up_proxy(off, ProxyHost, ProxyPort) ->
    Url = "http://" ++ ProxyHost ++ ":" ++ integer_to_list(ProxyPort) ++ "/proxies/mongo_single/toxics/latency_up",
    Body = <<>>,
    {ok, {{_, 204, _}, _, _}} = httpc:request(delete, {Url, [], "application/json", Body}, [],
                                              [{body_format, binary}]).
