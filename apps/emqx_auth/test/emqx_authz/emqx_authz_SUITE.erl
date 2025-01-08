%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_placeholder.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            {emqx_conf,
                "authorization { cache { enable = false }, no_match = deny, sources = [] }"},
            emqx_auth
        ],
        #{
            work_dir => filename:join(?config(priv_dir, Config), ?MODULE)
        }
    ),
    ok = emqx_authz_test_lib:register_fake_sources([http, redis, mongodb, mysql, postgresql]),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    {ok, _} = emqx:update_config(
        [authorization],
        #{
            <<"no_match">> => <<"allow">>,
            <<"cache">> => #{<<"enable">> => <<"true">>},
            <<"sources">> => []
        }
    ),
    ok = emqx_authz_test_lib:deregister_sources(),
    emqx_cth_suite:stop(?config(suite_apps, Config)),
    ok.

init_per_testcase(TestCase, Config) when
    TestCase =:= t_subscribe_deny_disconnect_publishes_last_will_testament;
    TestCase =:= t_publish_last_will_testament_banned_client_connecting;
    TestCase =:= t_publish_deny_disconnect_publishes_last_will_testament
->
    {ok, _} = emqx_authz:update(?CMD_REPLACE, []),
    {ok, _} = emqx:update_config([authorization, deny_action], disconnect),
    Config;
init_per_testcase(_TestCase, Config) ->
    _ = file:delete(emqx_authz_file:acl_conf_file()),
    {ok, _} = emqx_authz:update(?CMD_REPLACE, []),
    Config.

end_per_testcase(TestCase, _Config) when
    TestCase =:= t_subscribe_deny_disconnect_publishes_last_will_testament;
    TestCase =:= t_publish_last_will_testament_banned_client_connecting;
    TestCase =:= t_publish_deny_disconnect_publishes_last_will_testament
->
    {ok, _} = emqx:update_config([authorization, deny_action], ignore),
    {ok, _} = emqx_authz:update(?CMD_REPLACE, []),
    emqx_common_test_helpers:call_janitor(),
    ok;
end_per_testcase(_TestCase, _Config) ->
    emqx_common_test_helpers:call_janitor(),
    ok.

-define(SOURCE_HTTP, #{
    <<"type">> => <<"http">>,
    <<"enable">> => true,
    <<"url">> => <<"https://example.com:443/a/b?c=d">>,
    <<"headers">> => #{},
    <<"ssl">> => #{<<"enable">> => true},
    <<"method">> => <<"get">>,
    <<"request_timeout">> => <<"5s">>
}).
-define(SOURCE_MONGODB, #{
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
-define(SOURCE_MYSQL, #{
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
-define(SOURCE_POSTGRESQL, #{
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
-define(SOURCE_REDIS, #{
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

-define(SOURCE_FILE(Rules), #{
    <<"type">> => <<"file">>,
    <<"enable">> => true,
    <<"rules">> => Rules
}).

-define(SOURCE_FILE1,
    ?SOURCE_FILE(
        <<
            "{allow,{username,\"^dashboard?\"},subscribe,[\"$SYS/#\"]}."
            "\n{allow,{ipaddr,\"127.0.0.1\"},all,[\"$SYS/#\",\"#\"]}."
        >>
    )
).
-define(SOURCE_FILE2,
    ?SOURCE_FILE(
        <<
            "{allow,{username,\"some_client\"},publish,[\"some_client/lwt\"]}.\n"
            "{deny, all}."
        >>
    )
).

%% Allow all clients to publish or subscribe to topics with their alias as prefix.
-define(SOURCE_FILE_CLIENT_ATTR,
    ?SOURCE_FILE(
        <<
            "{allow,all,all,[\"${client_attrs.alias}/#\",\"client_attrs_backup\"]}.\n"
            "{deny, all}."
        >>
    )
).

-define(SOURCE_FILE_CLIENT_NO_SUCH_ATTR,
    ?SOURCE_FILE(
        <<
            "{allow,all,all,[\"${client_attrs.nonexist}/#\",\"client_attrs_backup\"]}.\n"
            "{deny, all}."
        >>
    )
).

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

-define(UPDATE_ERROR(Err), {error, {pre_config_update, emqx_authz, Err}}).

t_bad_file_source(_) ->
    BadContent = ?SOURCE_FILE(<<"{allow,{username,\"bar\"}, publish, [\"test\"]}">>),
    BadContentErr = {bad_acl_file_content, {1, erl_parse, ["syntax error before: ", []]}},
    BadRule = ?SOURCE_FILE(<<"{allow,{username,\"bar\"},publish}.">>),
    BadRuleErr = #{
        reason => invalid_authorization_rule, value => {allow, {username, "bar"}, publish}
    },
    BadPermission = ?SOURCE_FILE(<<"{not_allow,{username,\"bar\"},publish,[\"test\"]}.">>),
    BadPermissionErr = #{reason => invalid_authorization_permission, value => not_allow},
    BadAction = ?SOURCE_FILE(<<"{allow,{username,\"bar\"},pubsub,[\"test\"]}.">>),
    BadActionErr = #{reason => invalid_authorization_action, value => pubsub},
    lists:foreach(
        fun({Source, Error}) ->
            File = emqx_authz_file:acl_conf_file(),
            ?assertEqual({error, enoent}, file:read_file(File)),
            ?assertEqual(?UPDATE_ERROR(Error), emqx_authz:update(?CMD_REPLACE, [Source])),
            ?assertEqual(?UPDATE_ERROR(Error), emqx_authz:update(?CMD_PREPEND, Source)),
            ?assertEqual(?UPDATE_ERROR(Error), emqx_authz:update(?CMD_APPEND, Source)),
            %% Check file is not created if update failed;
            ?assertEqual({error, enoent}, file:read_file(File))
        end,
        [
            {BadContent, BadContentErr},
            {BadRule, BadRuleErr},
            {BadPermission, BadPermissionErr},
            {BadAction, BadActionErr}
        ]
    ),
    ?assertMatch(
        [],
        emqx_conf:get([authorization, sources], [])
    ).

t_good_file_source(_) ->
    RuleBin = <<"{allow,{username,\"bar\"}, publish, [\"test\"]}.">>,
    GoodFileSource = ?SOURCE_FILE(RuleBin),
    File = emqx_authz_file:acl_conf_file(),
    lists:foreach(
        fun({Command, Argument}) ->
            _ = file:delete(File),
            ?assertMatch({ok, _}, emqx_authz:update(Command, Argument)),
            ?assertEqual({ok, RuleBin}, file:read_file(File)),
            {ok, _} = emqx_authz:update(?CMD_REPLACE, [])
        end,
        [
            {?CMD_REPLACE, [GoodFileSource]},
            {?CMD_PREPEND, GoodFileSource},
            {?CMD_APPEND, GoodFileSource}
        ]
    ).

t_update_source(_) ->
    %% replace all
    {ok, _} = emqx_authz:update(?CMD_REPLACE, [?SOURCE_MYSQL]),
    {ok, _} = emqx_authz:update(?CMD_PREPEND, ?SOURCE_MONGODB),
    {ok, _} = emqx_authz:update(?CMD_PREPEND, ?SOURCE_HTTP),
    {ok, _} = emqx_authz:update(?CMD_APPEND, ?SOURCE_POSTGRESQL),
    {ok, _} = emqx_authz:update(?CMD_APPEND, ?SOURCE_REDIS),
    {ok, _} = emqx_authz:update(?CMD_APPEND, ?SOURCE_FILE1),

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

    {ok, _} = emqx_authz:update({?CMD_REPLACE, http}, ?SOURCE_HTTP#{<<"enable">> := true}),
    {ok, _} = emqx_authz:update({?CMD_REPLACE, mongodb}, ?SOURCE_MONGODB#{<<"enable">> := true}),
    {ok, _} = emqx_authz:update({?CMD_REPLACE, mysql}, ?SOURCE_MYSQL#{<<"enable">> := true}),
    {ok, _} = emqx_authz:update({?CMD_REPLACE, postgresql}, ?SOURCE_POSTGRESQL#{
        <<"enable">> := true
    }),
    {ok, _} = emqx_authz:update({?CMD_REPLACE, redis}, ?SOURCE_REDIS#{<<"enable">> := true}),
    {ok, _} = emqx_authz:update({?CMD_REPLACE, file}, ?SOURCE_FILE1#{<<"enable">> := true}),

    {ok, _} = emqx_authz:update({?CMD_REPLACE, http}, ?SOURCE_HTTP#{<<"enable">> := false}),
    {ok, _} = emqx_authz:update({?CMD_REPLACE, mongodb}, ?SOURCE_MONGODB#{<<"enable">> := false}),
    {ok, _} = emqx_authz:update({?CMD_REPLACE, mysql}, ?SOURCE_MYSQL#{<<"enable">> := false}),
    {ok, _} = emqx_authz:update({?CMD_REPLACE, postgresql}, ?SOURCE_POSTGRESQL#{
        <<"enable">> := false
    }),
    {ok, _} = emqx_authz:update({?CMD_REPLACE, redis}, ?SOURCE_REDIS#{<<"enable">> := false}),
    {ok, _} = emqx_authz:update({?CMD_REPLACE, file}, ?SOURCE_FILE1#{<<"enable">> := false}),

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
    ?assertMatch(
        [
            #{type := http, enable := false},
            #{type := mongodb, enable := false},
            #{type := mysql, enable := false},
            #{type := postgresql, enable := false},
            #{type := redis, enable := false},
            #{type := file, enable := false}
        ],
        emqx_authz:lookup()
    ),

    {ok, _} = emqx_authz:update(?CMD_REPLACE, []).

t_replace_all(_) ->
    RootKey = [<<"authorization">>],
    Conf = emqx:get_raw_config(RootKey),
    ?assertMatch(
        {ok, _},
        emqx_authz_utils:update_config(RootKey, Conf#{
            <<"sources">> => [
                ?SOURCE_FILE1,
                ?SOURCE_REDIS,
                ?SOURCE_POSTGRESQL,
                ?SOURCE_MYSQL,
                ?SOURCE_MONGODB,
                ?SOURCE_HTTP
            ]
        })
    ),
    %% config
    ?assertMatch(
        [
            #{type := file, enable := true},
            #{type := redis, enable := true},
            #{type := postgresql, enable := true},
            #{type := mysql, enable := true},
            #{type := mongodb, enable := true},
            #{type := http, enable := true}
        ],
        emqx_conf:get([authorization, sources], [])
    ),
    %% hooks status
    ?assertMatch(
        [
            #{type := file, enable := true},
            #{type := redis, enable := true},
            #{type := postgresql, enable := true},
            #{type := mysql, enable := true},
            #{type := mongodb, enable := true},
            #{type := http, enable := true}
        ],
        emqx_authz:lookup()
    ),
    Ids = [http, mongodb, mysql, postgresql, redis, file],
    %% metrics
    lists:foreach(
        fun(Id) ->
            ?assert(emqx_metrics_worker:has_metrics(authz_metrics, Id), Id)
        end,
        Ids
    ),

    ?assertMatch(
        {ok, _},
        emqx_authz_utils:update_config(
            RootKey,
            Conf#{<<"sources">> => [?SOURCE_HTTP#{<<"enable">> => false}]}
        )
    ),
    %% hooks status
    ?assertMatch([#{type := http, enable := false}], emqx_authz:lookup()),
    %% metrics
    ?assert(emqx_metrics_worker:has_metrics(authz_metrics, http)),
    lists:foreach(
        fun(Id) ->
            ?assertNot(emqx_metrics_worker:has_metrics(authz_metrics, Id), Id)
        end,
        Ids -- [http]
    ),
    ok.

t_delete_source(_) ->
    {ok, _} = emqx_authz:update(?CMD_REPLACE, [?SOURCE_HTTP]),

    ?assertMatch([#{type := http, enable := true}], emqx_conf:get([authorization, sources], [])),

    {ok, _} = emqx_authz:update({?CMD_DELETE, http}, #{}),

    ?assertMatch([], emqx_conf:get([authorization, sources], [])).

t_move_source(_) ->
    {ok, _} = emqx_authz:update(
        ?CMD_REPLACE,
        [
            ?SOURCE_HTTP,
            ?SOURCE_MONGODB,
            ?SOURCE_MYSQL,
            ?SOURCE_POSTGRESQL,
            ?SOURCE_REDIS,
            ?SOURCE_FILE1
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

t_pre_config_update_crash(_) ->
    ok = meck:new(emqx_authz_fake_source, [non_strict, passthrough, no_history]),
    ok = meck:expect(emqx_authz_fake_source, write_files, fun(_) -> meck:exception(error, oops) end),
    ?assertEqual(
        {error, {pre_config_update, emqx_authz, oops}},
        emqx_authz:update(?CMD_APPEND, ?SOURCE_HTTP)
    ),
    ok = meck:unload(emqx_authz_fake_source).

t_get_enabled_authzs_none_enabled(_Config) ->
    ?assertEqual([], emqx_authz:get_enabled_authzs()).

t_get_enabled_authzs_some_enabled(_Config) ->
    {ok, _} = emqx_authz:update(?CMD_REPLACE, [
        ?SOURCE_POSTGRESQL, ?SOURCE_REDIS#{<<"enable">> := false}
    ]),
    ?assertEqual([postgresql], emqx_authz:get_enabled_authzs()).

t_subscribe_deny_disconnect_publishes_last_will_testament(_Config) ->
    {ok, _} = emqx_authz:update(?CMD_REPLACE, [?SOURCE_FILE2]),
    {ok, C} = emqtt:start_link([
        {username, <<"some_client">>},
        {will_topic, <<"some_client/lwt">>},
        {will_payload, <<"should be published">>}
    ]),
    {ok, _} = emqtt:connect(C),
    ok = emqx:subscribe(<<"some_client/lwt">>),
    process_flag(trap_exit, true),

    try
        emqtt:subscribe(C, <<"unauthorized">>),
        error(should_have_disconnected)
    catch
        exit:{{shutdown, tcp_closed}, _} ->
            ok
    end,

    receive
        {deliver, <<"some_client/lwt">>, #message{payload = <<"should be published">>}} ->
            ok
    after 2_000 ->
        error(lwt_not_published)
    end,

    ok.

t_publish_deny_disconnect_publishes_last_will_testament(_Config) ->
    {ok, _} = emqx_authz:update(?CMD_REPLACE, [?SOURCE_FILE2]),
    {ok, C} = emqtt:start_link([
        {username, <<"some_client">>},
        {will_topic, <<"some_client/lwt">>},
        {will_payload, <<"should be published">>}
    ]),
    {ok, _} = emqtt:connect(C),
    ok = emqx:subscribe(<<"some_client/lwt">>),
    process_flag(trap_exit, true),

    %% disconnect is async
    Ref = monitor(process, C),
    emqtt:publish(C, <<"some/topic">>, <<"unauthorized">>),
    receive
        {'DOWN', Ref, process, C, _} ->
            ok
    after 1_000 ->
        error(client_should_have_been_disconnected)
    end,
    receive
        {deliver, <<"some_client/lwt">>, #message{payload = <<"should be published">>}} ->
            ok
    after 2_000 ->
        error(lwt_not_published)
    end,

    ok.

t_publish_last_will_testament_denied_topic(_Config) ->
    {ok, C} = emqtt:start_link([
        {will_topic, <<"$SYS/lwt">>},
        {will_payload, <<"should not be published">>}
    ]),
    {ok, _} = emqtt:connect(C),
    ok = emqx:subscribe(<<"$SYS/lwt">>),
    unlink(C),
    ok = snabbkaffe:start_trace(),
    {true, {ok, _}} = ?wait_async_action(
        exit(C, kill),
        #{?snk_kind := last_will_testament_publish_denied},
        1_000
    ),
    ok = snabbkaffe:stop(),

    receive
        {deliver, <<"$SYS/lwt">>, #message{payload = <<"should not be published">>}} ->
            error(lwt_should_not_be_published_to_forbidden_topic)
    after 1_000 ->
        ok
    end,

    ok.

t_alias_prefix(_Config) ->
    {ok, _} = emqx_authz:update(?CMD_REPLACE, [?SOURCE_FILE_CLIENT_ATTR]),
    %% '^.*-(.*)$': extract the suffix after the last '-'
    {ok, Compiled} = emqx_variform:compile("concat(regex_extract(clientid,'^.*-(.*)$'))"),
    emqx_config:put_zone_conf(default, [mqtt, client_attrs_init], [
        #{
            expression => Compiled,
            set_as_attr => <<"alias">>
        }
    ]),
    ClientId = <<"org1-name2">>,
    SubTopic = <<"name2/#">>,
    SubTopicNotAllowed = <<"name3/#">>,
    {ok, C} = emqtt:start_link([{clientid, ClientId}, {proto_ver, v5}]),
    ?assertMatch({ok, _}, emqtt:connect(C)),
    ?assertMatch({ok, _, [?RC_SUCCESS]}, emqtt:subscribe(C, SubTopic)),
    ?assertMatch({ok, _, [?RC_NOT_AUTHORIZED]}, emqtt:subscribe(C, SubTopicNotAllowed)),
    ?assertMatch({ok, _, [?RC_NOT_AUTHORIZED]}, emqtt:subscribe(C, <<"/#">>)),
    unlink(C),
    emqtt:stop(C),
    NonMatching = <<"clientid_which_has_no_dash">>,
    {ok, C2} = emqtt:start_link([{clientid, NonMatching}, {proto_ver, v5}]),
    ?assertMatch({ok, _}, emqtt:connect(C2)),
    ?assertMatch({ok, _, [?RC_SUCCESS]}, emqtt:subscribe(C2, <<"client_attrs_backup">>)),
    %% assert '${client_attrs.alias}/#' is not rendered as '/#'
    ?assertMatch({ok, _, [?RC_NOT_AUTHORIZED]}, emqtt:subscribe(C2, <<"/#">>)),
    unlink(C2),
    emqtt:stop(C2),
    emqx_config:put_zone_conf(default, [mqtt, client_attrs_init], []),
    ok.

t_non_existing_attr(_Config) ->
    {ok, _} = emqx_authz:update(?CMD_REPLACE, [?SOURCE_FILE_CLIENT_NO_SUCH_ATTR]),
    %% '^.*-(.*)$': extract the suffix after the last '-'
    {ok, Compiled} = emqx_variform:compile("concat(regex_extract(clientid,'^.*-(.*)$'))"),
    emqx_config:put_zone_conf(default, [mqtt, client_attrs_init], [
        #{
            expression => Compiled,
            %% this is intended to be different from 'nonexist'
            set_as_attr => <<"existing">>
        }
    ]),
    ClientId = <<"org1-name3">>,
    {ok, C} = emqtt:start_link([{clientid, ClientId}, {proto_ver, v5}]),
    ?assertMatch({ok, _}, emqtt:connect(C)),
    ?assertMatch({ok, _, [?RC_SUCCESS]}, emqtt:subscribe(C, <<"client_attrs_backup">>)),
    %% assert '${client_attrs.nonexist}/#' is not rendered as '/#'
    ?assertMatch({ok, _, [?RC_NOT_AUTHORIZED]}, emqtt:subscribe(C, <<"/#">>)),
    unlink(C),
    emqtt:stop(C),
    ok.

%% client is allowed by ACL to publish to its LWT topic, is connected,
%% and then gets banned and kicked out while connected.  Should not
%% publish LWT.
t_publish_last_will_testament_banned_client_connecting(_Config) ->
    {ok, _} = emqx_authz:update(?CMD_REPLACE, [?SOURCE_FILE2]),
    Username = <<"some_client">>,
    ClientId = <<"some_clientid">>,
    LWTPayload = <<"should not be published">>,
    LWTTopic = <<"some_client/lwt">>,
    ok = emqx:subscribe(<<"some_client/lwt">>),
    {ok, C} = emqtt:start_link([
        {clientid, ClientId},
        {username, Username},
        {will_topic, LWTTopic},
        {will_payload, LWTPayload}
    ]),
    ?assertMatch({ok, _}, emqtt:connect(C)),

    %% Now we ban the client while it is connected.
    Now = erlang:system_time(second),
    Who = emqx_banned:who(username, Username),
    emqx_banned:create(#{
        who => Who,
        by => <<"test">>,
        reason => <<"test">>,
        at => Now,
        until => Now + 120
    }),
    on_exit(fun() -> emqx_banned:delete(Who) end),
    %% Now kick it as we do in the ban API.
    process_flag(trap_exit, true),
    ?check_trace(
        begin
            ok = emqx_cm:kick_session(ClientId),
            receive
                {deliver, LWTTopic, #message{payload = LWTPayload}} ->
                    error(lwt_should_not_be_published_to_forbidden_topic)
            after 2_000 -> ok
            end,
            ok
        end,
        fun(Trace) ->
            ?assertMatch(
                [
                    #{
                        client_banned := true,
                        publishing_disallowed := false
                    }
                ],
                ?of_kind(last_will_testament_publish_denied, Trace)
            ),
            ok
        end
    ),
    ok = snabbkaffe:stop(),

    ok.

t_sikpped_as_superuser(_Config) ->
    ClientInfo = #{
        clientid => <<"clientid">>,
        username => <<"username">>,
        peerhost => {127, 0, 0, 1},
        zone => default,
        listener => {tcp, default},
        is_superuser => true
    },
    ?check_trace(
        begin
            ?assertEqual(
                allow,
                emqx_access_control:authorize(ClientInfo, ?AUTHZ_PUBLISH(?QOS_0), <<"p/t/0">>)
            ),
            ?assertEqual(
                allow,
                emqx_access_control:authorize(ClientInfo, ?AUTHZ_PUBLISH(?QOS_1), <<"p/t/1">>)
            ),
            ?assertEqual(
                allow,
                emqx_access_control:authorize(ClientInfo, ?AUTHZ_PUBLISH(?QOS_2), <<"p/t/2">>)
            ),
            ?assertEqual(
                allow,
                emqx_access_control:authorize(ClientInfo, ?AUTHZ_SUBSCRIBE(?QOS_0), <<"s/t/0">>)
            ),
            ?assertEqual(
                allow,
                emqx_access_control:authorize(ClientInfo, ?AUTHZ_SUBSCRIBE(?QOS_1), <<"s/t/1">>)
            ),
            ?assertEqual(
                allow,
                emqx_access_control:authorize(ClientInfo, ?AUTHZ_SUBSCRIBE(?QOS_2), <<"s/t/2">>)
            )
        end,
        fun(Trace) ->
            ?assertMatch(
                [
                    #{
                        reason := client_is_superuser,
                        action := #{qos := ?QOS_0, action_type := publish}
                    },
                    #{
                        reason := client_is_superuser,
                        action := #{qos := ?QOS_1, action_type := publish}
                    },
                    #{
                        reason := client_is_superuser,
                        action := #{qos := ?QOS_2, action_type := publish}
                    },
                    #{
                        reason := client_is_superuser,
                        action := #{qos := ?QOS_0, action_type := subscribe}
                    },
                    #{
                        reason := client_is_superuser,
                        action := #{qos := ?QOS_1, action_type := subscribe}
                    },
                    #{
                        reason := client_is_superuser,
                        action := #{qos := ?QOS_2, action_type := subscribe}
                    }
                ],
                ?of_kind(authz_skipped, Trace)
            ),
            ok
        end
    ),

    ok = snabbkaffe:stop().

stop_apps(Apps) ->
    lists:foreach(fun application:stop/1, Apps).
