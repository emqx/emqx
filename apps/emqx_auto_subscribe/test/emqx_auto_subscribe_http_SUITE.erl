%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_auto_subscribe_http_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx_auth/include/emqx_authn.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

all() ->
    [{group, legacy}, {group, hardened}].

groups() ->
    Cases = emqx_common_test_helpers:all(?MODULE),
    [{legacy, [], Cases}, {hardened, [], Cases}].

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            cowboy,
            {emqx_conf, emqx_authn_test_lib:emqx_appspec()},
            emqx_auth,
            emqx_auth_http,
            emqx_auto_subscribe,
            emqx_retainer,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_group(Profile, Config) ->
    ok = emqx_common_test_helpers:set_security_profile(Profile),
    [{profile, Profile} | Config].

end_per_group(_, _) ->
    emqx_common_test_helpers:clear_security_profile().

init_per_testcase(_, Config) ->
    Port = emqx_common_test_helpers:select_free_port(tcp),
    {ok, _} = emqx_utils_http_test_server:start_link(Port, "/auth/[...]"),
    [{http_port, Port} | Config].

end_per_testcase(_, _) ->
    emqx_common_test_helpers:call_janitor(),
    emqx_authn_test_lib:delete_authenticators([authentication], ?GLOBAL),
    {ok, _} = emqx_auto_subscribe:update([]),
    _ = emqx_auth_cache:reset(?AUTHN_CACHE),
    ok = emqx_authn_test_lib:enable_node_cache(false),
    ok = emqx_utils_http_test_server:stop().

-doc "HTTP subscriptions deliver messages and override overlapping global subscriptions once.".
t_delivery(Config) ->
    {ok, _} = emqx_auto_subscribe:update([
        #{<<"topic">> => <<"auto/overlap">>, <<"qos">> => 0},
        #{<<"topic">> => <<"$share/g/auto/shared">>, <<"qos">> => 0},
        #{<<"topic">> => <<"auto/static">>}
    ]),
    Entries = [
        #{topic => <<"auto/overlap">>, qos => 0},
        #{topic => <<"auto/overlap">>, qos => 1},
        #{topic => <<"auto/wild/+">>, qos => 2},
        #{topic => <<"$share/g/auto/shared">>, qos => 1},
        #{topic => <<"auto/default">>}
    ],
    configure(Config, #{auto_subscribe => Entries}),
    lists:foreach(
        fun(Proto) ->
            ClientId = atom_to_binary(Proto),
            C = connect(ClientId, #{proto_ver => Proto}),
            assert_subscriptions(ClientId, [
                {<<"auto/overlap">>, 1},
                {<<"auto/static">>, 0},
                {<<"auto/wild/+">>, 2},
                {#share{group = <<"g">>, topic = <<"auto/shared">>}, 1},
                {<<"auto/default">>, 0}
            ]),
            assert_delivery(<<"auto/overlap">>, <<"auto/overlap">>, 1),
            assert_delivery(<<"auto/wild/1">>, <<"auto/wild/1">>, 2),
            assert_delivery(<<"auto/shared">>, <<"auto/shared">>, 1),
            ?assertNotReceive({publish, _}, 100),
            ok = emqtt:disconnect(C)
        end,
        [v4, v5]
    ).

-doc "Connected hooks see HTTP topics, but channel and CM state release them after connection.".
t_client_api(Config) ->
    ClientId = <<"api-client">>,
    Topic = <<"$share/g/auto/api">>,
    configure(Config, #{auto_subscribe => [#{topic => Topic, qos => 1}]}),
    ok = emqx_hooks:add('client.connected', {?MODULE, on_connected, [self()]}, ?HP_LOWEST),
    on_exit(fun() -> emqx_hooks:del('client.connected', {?MODULE, on_connected}) end),
    _ = connect(ClientId, #{}),
    ?assertReceive(
        {connected_info, #{clientid := ClientId, auto_subscribe := [{Topic, #{qos := 1}}]}}
    ),
    assert_subscriptions(ClientId, [{#share{group = <<"g">>, topic = <<"auto/api">>}, 1}]),
    [Pid] = emqx_cm:lookup_channels(ClientId),
    lists:foreach(
        fun(#{clientinfo := ClientInfo}) ->
            ?assertNot(maps:is_key(auto_subscribe, ClientInfo))
        end,
        [emqx_connection:info(Pid), emqx_cm:get_chan_info(ClientId)]
    ),
    assert_delivery(<<"auto/api">>, <<"auto/api">>, 1),
    Auth = emqx_mgmt_api_test_util:auth_header_(),
    lists:foreach(
        fun(Parts) ->
            Path = emqx_mgmt_api_test_util:api_path(Parts),
            {ok, Response} = emqx_mgmt_api_test_util:request_api(get, Path, "", Auth),
            Body = emqx_utils_json:decode(Response),
            Rows =
                case Body of
                    #{<<"data">> := Data} -> Data;
                    _ -> [Body]
                end,
            ?assertMatch([#{<<"clientid">> := ClientId}], Rows),
            [Row] = Rows,
            ?assertNot(maps:is_key(<<"auto_subscribe">>, Row))
        end,
        [["clients", ClientId], ["clients"], ["clients_v2"]]
    ),
    SubPath = emqx_mgmt_api_test_util:api_path(["clients", ClientId, "subscriptions"]),
    {ok, SubResponse} = emqx_mgmt_api_test_util:request_api(get, SubPath, "", Auth),
    ?assertMatch([#{<<"topic">> := Topic, <<"qos">> := 1}], emqx_utils_json:decode(SubResponse)).

-doc "Overlapping global and HTTP entries deliver a retained message once with the dynamic QoS.".
t_retained(Config) ->
    Topic = <<"auto/retained">>,
    {ok, _} = emqx_auto_subscribe:update([#{<<"topic">> => Topic, <<"qos">> => 0}]),
    configure(Config, #{auto_subscribe => [#{topic => Topic}, #{topic => Topic, qos => 1}]}),
    on_exit(fun() -> emqx_retainer:delete(Topic) end),
    Msg = emqx_message:set_flag(retain, emqx_message:make(test, 1, Topic, <<"retained">>)),
    _ = emqx:publish(Msg),
    ?retry(20, 100, ?assertMatch({ok, [_]}, emqx_retainer:read_message(Topic))),
    _ = connect(<<"retained">>, #{}),
    assert_subscriptions(<<"retained">>, [{Topic, 1}]),
    ?assertReceive(
        {publish, #{topic := Topic, qos := 1, retain := true, payload := <<"retained">>}}
    ),
    ?assertNotReceive({publish, _}, 300).

-doc "HTTP and global subscriptions follow the same security-profile authorization policy.".
t_authorization(Config) ->
    {ok, _} = emqx_auto_subscribe:update([
        #{<<"topic">> => <<"auto/static">>},
        #{<<"topic">> => <<"auto/allowed-global">>}
    ]),
    configure(Config, #{
        auto_subscribe => [#{topic => <<"auto/dynamic">>}, #{topic => <<"auto/allowed-dynamic">>}],
        acl => [
            #{
                permission => allow,
                action => subscribe,
                topics => [<<"auto/allowed-global">>, <<"auto/allowed-dynamic">>]
            },
            #{permission => deny, action => all, topics => [<<"#">>]}
        ]
    }),
    _ = connect(<<"authorization">>, #{}),
    Allowed = [{<<"auto/allowed-global">>, 0}, {<<"auto/allowed-dynamic">>, 0}],
    Expected =
        case ?config(profile, Config) of
            legacy -> Allowed ++ [{<<"auto/static">>, 0}, {<<"auto/dynamic">>, 0}];
            hardened -> Allowed
        end,
    assert_subscriptions(<<"authorization">>, Expected),
    assert_delivery(<<"auto/allowed-global">>, <<"auto/allowed-global">>, 0),
    assert_delivery(<<"auto/allowed-dynamic">>, <<"auto/allowed-dynamic">>, 0),
    case ?config(profile, Config) of
        legacy ->
            assert_delivery(<<"auto/static">>, <<"auto/static">>, 0),
            assert_delivery(<<"auto/dynamic">>, <<"auto/dynamic">>, 0);
        hardened ->
            _ = emqx:publish(emqx_message:make(test, 0, <<"auto/static">>, <<"denied">>)),
            _ = emqx:publish(emqx_message:make(test, 0, <<"auto/dynamic">>, <<"denied">>)),
            ?assertNotReceive({publish, _}, 100)
    end.

-doc "Malformed entries are skipped without preventing valid subscriptions or disconnecting the client.".
t_invalid_entries(Config) ->
    configure(Config, #{
        auto_subscribe => [
            null,
            <<"auto/string">>,
            #{},
            #{topic => 42},
            #{topic => <<>>},
            #{topic => <<"a/#/b">>},
            #{topic => <<"$share/g/$share/h/t">>},
            #{topic => <<"$queue/$share/g/t">>},
            #{topic => <<"$exclusive/">>},
            #{topic => <<"auto/qos">>, qos => 3},
            #{topic => <<"auto/qos">>, qos => -1},
            #{topic => <<"auto/qos">>, qos => true},
            #{topic => <<"auto/qos">>, qos => 1.0},
            #{topic => <<"auto/valid">>, qos => 1}
        ]
    }),
    _ = connect(<<"invalid">>, #{}),
    assert_subscriptions(<<"invalid">>, [{<<"auto/valid">>, 1}]),
    assert_delivery(<<"auto/valid">>, <<"auto/valid">>, 1).

-doc "An omitted, empty or malformed list adds no subscriptions and leaves global rules active.".
t_empty(Config) ->
    {ok, _} = emqx_auto_subscribe:update([#{<<"topic">> => <<"auto/static">>}]),
    configure(Config, #{}),
    lists:foreach(
        fun({I, Body}) ->
            set_response(Body),
            ClientId = integer_to_binary(I),
            C = connect(ClientId, #{}),
            assert_subscriptions(ClientId, [{<<"auto/static">>, 0}]),
            ok = emqtt:disconnect(C)
        end,
        [{1, #{}}, {2, #{auto_subscribe => []}}, {3, #{auto_subscribe => #{topic => <<"bad">>}}}]
    ).

-doc "Each response processes at most 200 entries independently of the global 20-topic limit.".
t_limit(Config) ->
    configure(Config, #{}),
    lists:foreach(
        fun(N) ->
            Entries = [
                #{topic => <<"limit/", (integer_to_binary(I))/binary>>}
             || I <- lists:seq(1, N)
            ],
            set_response(#{auto_subscribe => Entries}),
            ClientId = <<"limit-", (integer_to_binary(N))/binary>>,
            C = connect(ClientId, #{}),
            Expected = [{maps:get(topic, E), 0} || E <- lists:sublist(Entries, 200)],
            assert_subscriptions(ClientId, Expected),
            ok = emqtt:disconnect(C)
        end,
        [20, 200, 201]
    ).

-doc "Cached HTTP responses create subscriptions on reconnect and cache keys isolate different clients.".
t_cache_reconnect(Config) ->
    configure(Config, #{auto_subscribe => [#{topic => <<"auto/cached">>, qos => 1}]}),
    ok = emqx_authn_test_lib:enable_node_cache(true),
    lists:foreach(
        fun(ClientId) ->
            C = connect(ClientId, #{}),
            assert_subscriptions(ClientId, [{<<"auto/cached">>, 1}]),
            assert_delivery(<<"auto/cached">>, <<"auto/cached">>, 1),
            ok = emqtt:disconnect(C),
            %% lookup_channels/1 hides dead PIDs before asynchronous cleanup finishes.
            ?retry(20, 100, ?assertEqual([], emqx_cm_registry:lookup_all_channels(ClientId)))
        end,
        [<<"cache1">>, <<"cache1">>, <<"cache2">>]
    ),
    ?assertMatch(
        #{hits := #{value := 1}, misses := #{value := 2}},
        emqx_auth_cache:metrics(?AUTHN_CACHE)
    ).

-doc "Resuming a session keeps subscriptions that are absent from the next authentication response.".
t_session_resume(Config) ->
    configure(Config, #{auto_subscribe => [#{topic => <<"auto/old">>}]}),
    ClientId = <<"resume">>,
    Opts = #{clean_start => false, properties => #{'Session-Expiry-Interval' => 60}},
    C1 = connect(ClientId, Opts),
    assert_subscriptions(ClientId, [{<<"auto/old">>, 0}]),
    {ok, _, _} = emqtt:subscribe(C1, <<"auto/manual">>, 1),
    ok = emqtt:disconnect(C1),
    ok = set_response(#{auto_subscribe => [#{topic => <<"auto/new">>, qos => 1}]}),
    C2 = connect(ClientId, Opts),
    ?assertEqual(1, proplists:get_value(session_present, emqtt:info(C2))),
    assert_subscriptions(ClientId, [
        {<<"auto/old">>, 0}, {<<"auto/manual">>, 1}, {<<"auto/new">>, 1}
    ]),
    assert_delivery(<<"auto/old">>, <<"auto/old">>, 0),
    assert_delivery(<<"auto/new">>, <<"auto/new">>, 1),
    ok = emqtt:disconnect(C2, 0, #{'Session-Expiry-Interval' => 0}).

-doc "Dynamic subscriptions use the listener mountpoint and clients receive unmounted topic names.".
t_mountpoint(Config) ->
    Path = [listeners, tcp, default, mountpoint],
    Old = emqx:get_raw_config(Path),
    {ok, _} = emqx_conf:update(Path, <<"tenant/">>, #{}),
    on_exit(fun() -> emqx_conf:update(Path, Old, #{}) end),
    configure(Config, #{auto_subscribe => [#{topic => <<"auto/mounted">>, qos => 1}]}),
    _ = connect(<<"mounted">>, #{}),
    assert_subscriptions(<<"mounted">>, [{<<"tenant/auto/mounted">>, 1}]),
    assert_delivery(<<"tenant/auto/mounted">>, <<"auto/mounted">>, 1).

-doc "Filtered exact and wildcard subscriptions override global rules and reject nonmatching messages.".
t_message_filter(Config) ->
    set_mqtt_conf(subscription_message_filter, enable),
    Topics = [
        {<<"auto/filtered">>, <<"auto/filtered">>},
        {<<"auto/hash/#">>, <<"auto/hash/value">>},
        {<<"auto/plus/+">>, <<"auto/plus/value">>}
    ],
    {ok, _} = emqx_auto_subscribe:update([
        #{<<"topic">> => Filter, <<"qos">> => 0}
     || {Filter, _} <- Topics
    ]),
    configure(Config, #{
        auto_subscribe =>
            [#{topic => <<"auto/bad?invalid">>}, #{topic => <<"auto/valid">>}] ++
            [
                #{topic => <<Filter/binary, "?location=roomA">>, qos => 1}
             || {Filter, _} <- Topics
            ]
    }),
    _ = connect(<<"message-filter">>, #{}),
    assert_subscriptions(
        <<"message-filter">>, [{<<"auto/valid">>, 0} | [{Filter, 1} || {Filter, _} <- Topics]]
    ),
    assert_delivery(<<"auto/valid">>, <<"auto/valid">>, 0),
    lists:foreach(
        fun({_, Topic}) ->
            Excluded = emqx_message:set_header(
                properties,
                #{'User-Property' => [{<<"location">>, <<"roomB">>}]},
                emqx_message:make(test, 1, Topic, <<"excluded">>)
            ),
            _ = emqx:publish(Excluded),
            ?assertNotReceive({publish, #{payload := <<"excluded">>}}, 100),
            Included = emqx_message:set_header(
                properties,
                #{'User-Property' => [{<<"location">>, <<"roomA">>}]},
                emqx_message:make(test, 1, Topic, <<"included">>)
            ),
            _ = emqx:publish(Included),
            ?assertReceive({publish, #{topic := Topic, qos := 1, payload := <<"included">>}}),
            ?assertNotReceive({publish, #{payload := <<"included">>}}, 100)
        end,
        Topics
    ).

-doc "Unsupported filtered queue and stream entries are skipped without dropping valid subscriptions.".
t_message_filter_unsupported_topics(Config) ->
    set_mqtt_conf(subscription_message_filter, enable),
    Invalid = [
        #{topic => <<Prefix/binary, "auto/unsupported?location=roomA">>}
     || Prefix <- [<<"$stream/">>, <<"$s/">>, <<"$queue/">>, <<"$q/">>]
    ],
    configure(Config, #{auto_subscribe => Invalid ++ [#{topic => <<"auto/valid">>}]}),
    process_flag(trap_exit, true),
    _ = connect(<<"unsupported-filters">>, #{}),
    assert_subscriptions(<<"unsupported-filters">>, [{<<"auto/valid">>, 0}]),
    assert_delivery(<<"auto/valid">>, <<"auto/valid">>, 0).

-doc "HTTP exclusive subscriptions use the same profile-dependent locking as global rules.".
t_exclusive(Config) ->
    set_mqtt_conf(exclusive_subscription, true),
    on_exit(fun emqx_exclusive_subscription:clear/0),
    Topic = <<"auto/exclusive">>,
    configure(Config, #{auto_subscribe => [#{topic => <<"$exclusive/auto/exclusive">>}]}),
    C1 = connect(<<"exclusive1">>, #{}),
    assert_subscriptions(<<"exclusive1">>, [{Topic, 0}]),
    C2 = connect(<<"exclusive2">>, #{}),
    case ?config(profile, Config) of
        legacy ->
            assert_subscriptions(<<"exclusive2">>, [{Topic, 0}]),
            ?assertEqual(undefined, emqx_exclusive_subscription:dirty_lookup_clientid(Topic)),
            assert_delivery(Topic, Topic, 0),
            ?assertReceive({publish, #{topic := Topic}}),
            ?assertNotReceive({publish, _}, 100);
        hardened ->
            assert_subscriptions(<<"exclusive2">>, []),
            assert_delivery(Topic, Topic, 0),
            ?assertNotReceive({publish, _}, 100),
            {ok, _, _} = emqtt:unsubscribe(C1, <<"$exclusive/auto/exclusive">>),
            ok = emqtt:disconnect(C2),
            ?retry(
                20, 100, ?assertEqual([], emqx_cm_registry:lookup_all_channels(<<"exclusive2">>))
            ),
            _ = connect(<<"exclusive2">>, #{}),
            assert_subscriptions(<<"exclusive2">>, [{Topic, 0}]),
            assert_delivery(Topic, Topic, 0)
    end.

-doc "HTTP-only and mixed subscriptions honor disconnect-on-deny without acquiring locks.".
t_authorization_disconnect(Config) ->
    set_mqtt_conf(exclusive_subscription, true),
    on_exit(fun emqx_exclusive_subscription:clear/0),
    Path = [authorization, deny_action],
    Old = emqx:get_config(Path),
    {ok, _} = emqx:update_config(Path, disconnect),
    on_exit(fun() -> emqx:update_config(Path, Old) end),
    configure(Config, #{
        auto_subscribe => [#{topic => <<"$exclusive/auto/queued">>}],
        acl => [#{permission => deny, action => all, topics => [<<"#">>]}]
    }),
    process_flag(trap_exit, true),
    lists:foreach(
        fun({ClientId, GlobalTopics}) ->
            {ok, _} = emqx_auto_subscribe:update(GlobalTopics),
            C = connect(ClientId, #{}),
            case ?config(profile, Config) of
                legacy ->
                    Expected = [{maps:get(<<"topic">>, T), 0} || T <- GlobalTopics],
                    assert_subscriptions(ClientId, [{<<"auto/queued">>, 0} | Expected]),
                    ok = emqtt:disconnect(C);
                hardened ->
                    ?assertReceive(
                        {'EXIT', C, {shutdown, {disconnected, ?RC_NOT_AUTHORIZED, #{}}}}
                    ),
                    ?retry(20, 100, ?assertEqual([], emqx_cm:lookup_channels(ClientId)))
            end,
            ?assertEqual(
                undefined, emqx_exclusive_subscription:dirty_lookup_clientid(<<"auto/queued">>)
            )
        end,
        [{<<"deny-http-only">>, []}, {<<"deny-mixed">>, [#{<<"topic">> => <<"auto/denied">>}]}]
    ).

-doc "HTTP subscriptions follow the existing profile policy for capabilities and granted QoS.".
t_capabilities(Config) ->
    set_mqtt_conf(exclusive_subscription, false),
    set_mqtt_conf(wildcard_subscription, false),
    set_mqtt_conf(max_qos_allowed, 0),
    configure(Config, #{
        auto_subscribe => [
            #{topic => <<"$exclusive/auto/disabled">>},
            #{topic => <<"auto/wild/+">>},
            #{topic => <<"auto/capped">>, qos => 2}
        ]
    }),
    _ = connect(<<"capabilities">>, #{}),
    case ?config(profile, Config) of
        legacy ->
            assert_subscriptions(<<"capabilities">>, [
                {<<"auto/disabled">>, 0}, {<<"auto/wild/+">>, 0}, {<<"auto/capped">>, 2}
            ]),
            assert_delivery(<<"auto/capped">>, <<"auto/capped">>, 2),
            ?assertNotReceive({publish, _}, 100);
        hardened ->
            assert_subscriptions(<<"capabilities">>, [{<<"auto/capped">>, 0}]),
            assert_delivery(<<"auto/capped">>, <<"auto/capped">>, 0)
    end.

-doc "Disabled shared subscriptions keep the existing profile-specific disconnect behavior.".
t_shared_capability(Config) ->
    set_mqtt_conf(shared_subscription, false),
    configure(Config, #{auto_subscribe => [#{topic => <<"$share/g/auto/shared-disabled">>}]}),
    process_flag(trap_exit, true),
    ClientId = <<"shared-disabled">>,
    C = connect(ClientId, #{}),
    case ?config(profile, Config) of
        legacy ->
            assert_subscriptions(ClientId, [
                {#share{group = <<"g">>, topic = <<"auto/shared-disabled">>}, 0}
            ]),
            assert_delivery(<<"auto/shared-disabled">>, <<"auto/shared-disabled">>, 0);
        hardened ->
            ?assertReceive(
                {'EXIT', C, {shutdown, {disconnected, ?RC_SHARED_SUBSCRIPTIONS_NOT_SUPPORTED, #{}}}}
            ),
            ?retry(20, 100, ?assertEqual([], emqx_cm:lookup_channels(ClientId)))
    end.

-doc "Only one exclusive prefix is stripped and overlap uses the resulting filter.".
t_parse_once(Config) ->
    set_mqtt_conf(exclusive_subscription, true),
    on_exit(fun emqx_exclusive_subscription:clear/0),
    Topic = <<"$exclusive/auto/nested">>,
    configure(Config, #{auto_subscribe => [#{topic => <<"$exclusive/$exclusive/auto/nested">>}]}),
    _ = connect(<<"parse-once">>, #{}),
    assert_subscriptions(<<"parse-once">>, [{Topic, 0}]),
    assert_delivery(Topic, Topic, 0),
    _ = emqx:publish(emqx_message:make(test, 0, <<"auto/nested">>, <<"wrong-topic">>)),
    ?assertNotReceive({publish, #{payload := <<"wrong-topic">>}}, 100).

set_mqtt_conf(Key, Value) ->
    Old = emqx_config:get_zone_conf(default, [mqtt, Key]),
    emqx_config:put_zone_conf(default, [mqtt, Key], Value),
    on_exit(fun() -> emqx_config:put_zone_conf(default, [mqtt, Key], Old) end).

on_connected(ClientInfo, _ConnInfo, TestPid) ->
    TestPid ! {connected_info, ClientInfo},
    ok.

configure(Config, Response) ->
    ok = set_response(Response),
    Port = integer_to_binary(?config(http_port, Config)),
    AuthConfig = #{
        <<"mechanism">> => <<"password_based">>,
        <<"backend">> => <<"http">>,
        <<"method">> => <<"get">>,
        <<"url">> => <<"http://127.0.0.1:", Port/binary, "/auth">>,
        <<"body">> => #{<<"clientid">> => <<"${clientid}">>}
    },
    {ok, _} = emqx:update_config([authentication], {create_authenticator, ?GLOBAL, AuthConfig}),
    ok.

set_response(Response) ->
    emqx_utils_http_test_server:set_handler(fun(Req0, State) ->
        Body = emqx_utils_json:encode(maps:merge(#{result => allow}, Response)),
        Req = cowboy_req:reply(200, #{<<"content-type">> => <<"application/json">>}, Body, Req0),
        {ok, Req, State}
    end).

connect(ClientId, Extra) ->
    {ok, C} = emqtt:start_link(
        maps:merge(
            #{
                clientid => ClientId,
                username => <<"user">>,
                password => <<"pass">>,
                proto_ver => v5,
                clean_start => true
            },
            Extra
        )
    ),
    on_exit(fun() -> catch emqtt:stop(C) end),
    {ok, _} = emqtt:connect(C),
    C.

assert_subscriptions(ClientId, Expected) ->
    [Pid] = emqx_cm:lookup_channels(ClientId),
    %% Option rows can appear before the channel finishes installing subscriptions.
    _ = emqx_connection:info(Pid),
    ?retry(
        20,
        100,
        ?assertEqual(
            lists:sort(Expected),
            lists:sort([{T, maps:get(qos, O)} || {T, O} <- emqx_broker:subscriptions(ClientId)])
        )
    ).

assert_delivery(PublishTopic, ReceivedTopic, QoS) ->
    _ = emqx:publish(emqx_message:make(test, 2, PublishTopic, <<"hello">>)),
    ?assertReceive({publish, #{topic := ReceivedTopic, qos := QoS, payload := <<"hello">>}}, 2000).
