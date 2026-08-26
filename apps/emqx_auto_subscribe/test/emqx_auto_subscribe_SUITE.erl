%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_auto_subscribe_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-import(emqx_config_SUITE, [prepare_conf_file/3]).
-import(emqx_common_test_helpers, [on_exit/1]).

-define(TOPIC_C, <<"/c/${clientid}">>).
-define(TOPIC_U, <<"/u/${username}">>).
-define(TOPIC_H, <<"/h/${host}">>).
-define(TOPIC_P, <<"/p/${port}">>).
-define(TOPIC_A, <<"/client/${clientid}/username/${username}/host/${host}/port/${port}">>).
-define(TOPIC_S, <<"/topic/simple">>).

-define(TOPICS, [?TOPIC_C, ?TOPIC_U, ?TOPIC_H, ?TOPIC_P, ?TOPIC_A, ?TOPIC_S]).

-define(ENSURE_TOPICS, [
    <<"/c/auto_sub_c">>,
    <<"/u/auto_sub_u">>,
    ?TOPIC_S
]).

-define(CLIENT_ID, <<"auto_sub_c">>).
-define(CLIENT_USERNAME, <<"auto_sub_u">>).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    meck:new(emqx_schema, [non_strict, passthrough, no_history, no_link]),
    meck:expect(emqx_schema, fields, fun
        ("auto_subscribe") ->
            meck:passthrough(["auto_subscribe"]) ++
                emqx_auto_subscribe_schema:fields("auto_subscribe");
        (F) ->
            meck:passthrough([F])
    end),

    meck:new(emqx_resource, [non_strict, passthrough, no_history, no_link]),
    meck:expect(emqx_resource, create, fun(_, _, _) -> {ok, meck_data} end),
    meck:expect(emqx_resource, update, fun(_, _, _, _) -> {ok, meck_data} end),
    meck:expect(emqx_resource, remove, fun(_) -> ok end),

    ASCfg =
        ~b"""
    auto_subscribe {
        topics = [
            {
                topic = "/c/${clientid}"
            },
            {
                topic = "/u/${username}"
            },
            {
                topic = "/h/${host}"
            },
            {
                topic = "/p/${port}"
            },
            {
                topic = "/client/${clientid}/username/${username}/host/${host}/port/${port}"
            },
            {
                topic = "/topic/simple"
                qos   = 1
                rh    = 0
                rap   = 0
                nl    = 0
            }
        ]
    }
    """,
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            {emqx_auto_subscribe, ASCfg},
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

init_per_testcase(t_get_basic_usage_info, Config) ->
    {ok, _} = emqx_auto_subscribe:update([]),
    Config;
init_per_testcase(t_auto_subscribe_reload_from_file, Config) ->
    {ok, _} = emqx_auto_subscribe:update([]),
    Config;
init_per_testcase(TestCase, Config) when
    TestCase =:= t_auto_subscribe_respects_authorization;
    TestCase =:= t_auto_subscribe_shared_topic
->
    emqx_common_test_helpers:set_security_profile("hardened"),
    Config;
init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(t_get_basic_usage_info, _Config) ->
    {ok, _} = emqx_auto_subscribe:update([]),
    emqx_common_test_helpers:call_janitor(),
    ok;
end_per_testcase(t_auto_subscribe_reload_from_file, _Config) ->
    {ok, _} = emqx_auto_subscribe:update([]),
    emqx_common_test_helpers:call_janitor(),
    ok;
end_per_testcase(TestCase, _Config) when
    TestCase =:= t_auto_subscribe_respects_authorization;
    TestCase =:= t_auto_subscribe_shared_topic
->
    emqx_common_test_helpers:clear_security_profile(),
    emqx_common_test_helpers:call_janitor(),
    ok;
end_per_testcase(_TestCase, _Config) ->
    emqx_common_test_helpers:call_janitor(),
    ok.

topic_config(T) ->
    #{
        topic => T,
        qos => 0,
        rh => 0,
        rap => 0,
        nl => 0
    }.

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

t_auto_subscribe(_) ->
    emqx_auto_subscribe:update([#{<<"topic">> => Topic} || Topic <- ?TOPICS]),
    {ok, Client} = emqtt:start_link(#{username => ?CLIENT_USERNAME, clientid => ?CLIENT_ID}),
    {ok, _} = emqtt:connect(Client),
    timer:sleep(200),
    ?assertEqual(check_subs(length(?TOPICS)), ok),
    emqtt:disconnect(Client),
    ok.

t_auto_subscribe_respects_authorization(_) ->
    TestPid = self(),
    TopicTemplate = <<"/denied/${clientid}">>,
    DeniedTopic = <<"/denied/auto_sub_c">>,
    {ok, _} = emqx_auto_subscribe:update([#{<<"topic">> => TopicTemplate}]),
    on_exit(fun() -> {ok, _} = emqx_auto_subscribe:update([]) end),
    ok = meck:new(emqx_access_control, [passthrough, no_history, no_link]),
    ok = meck:expect(emqx_access_control, authorize, fun
        (_ClientInfo, #{action_type := subscribe}, Topic) when Topic =:= DeniedTopic ->
            TestPid ! authorization_checked,
            deny;
        (ClientInfo, Action, Topic) ->
            meck:passthrough([ClientInfo, Action, Topic])
    end),
    on_exit(fun() -> meck:unload(emqx_access_control) end),
    {ok, Client} = emqtt:start_link(#{username => ?CLIENT_USERNAME, clientid => ?CLIENT_ID}),
    {ok, _} = emqtt:connect(Client),
    ?assertReceive(authorization_checked, 1_000),
    snabbkaffe_diff:assert_lists_eq([], client_subscriptions(?CLIENT_ID)),
    emqtt:disconnect(Client).

t_auto_subscribe_shared_topic(_) ->
    Topic = <<"$share/group/auto/${clientid}">>,
    RenderedTopic = <<"auto/auto_sub_c">>,
    {ok, _} = emqx_auto_subscribe:update([#{<<"topic">> => Topic}]),
    on_exit(fun() -> {ok, _} = emqx_auto_subscribe:update([]) end),
    {ok, Client} = emqtt:start_link(#{username => ?CLIENT_USERNAME, clientid => ?CLIENT_ID}),
    {ok, _} = emqtt:connect(Client),
    snabbkaffe_diff:assert_lists_eq(
        [#share{group = <<"group">>, topic = RenderedTopic}],
        client_subscriptions(?CLIENT_ID)
    ),
    emqtt:disconnect(Client).

t_auto_subscribe_reload_from_file(Config) ->
    ConfBin = hocon_pp:do(
        #{<<"auto_subscribe">> => #{<<"topics">> => [#{<<"topic">> => Topic} || Topic <- ?TOPICS]}},
        #{}
    ),
    ConfFile = prepare_conf_file(?FUNCTION_NAME, ConfBin, Config),
    ok = emqx_conf_cli:conf(["load", "--replace", ConfFile]),
    {ok, Client} = emqtt:start_link(#{username => ?CLIENT_USERNAME, clientid => ?CLIENT_ID}),
    {ok, _} = emqtt:connect(Client),
    timer:sleep(200),
    ?assertEqual(check_subs(length(?TOPICS)), ok),
    emqtt:disconnect(Client),
    ok.

t_update(_) ->
    Path = emqx_mgmt_api_test_util:api_path(["mqtt", "auto_subscribe"]),
    Auth = emqx_mgmt_api_test_util:auth_header_(),
    Body = [#{topic => ?TOPIC_S}],
    {ok, Response} = emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, Body),
    ResponseMap = emqx_utils_json:decode(Response),
    ?assertEqual(1, erlang:length(ResponseMap)),

    BadBody1 = #{topic => ?TOPIC_S},
    ?assertMatch(
        {error, {"HTTP/1.1", 400, "Bad Request"}},
        emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, BadBody1)
    ),
    BadBody2 = [#{topic => ?TOPIC_S, qos => 3}],
    ?assertMatch(
        {error, {"HTTP/1.1", 400, "Bad Request"}},
        emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, BadBody2)
    ),
    BadBody3 = [#{topic => ?TOPIC_S, rh => 10}],
    ?assertMatch(
        {error, {"HTTP/1.1", 400, "Bad Request"}},
        emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, BadBody3)
    ),
    BadBody4 = [#{topic => ?TOPIC_S, rap => -1}],
    ?assertMatch(
        {error, {"HTTP/1.1", 400, "Bad Request"}},
        emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, BadBody4)
    ),
    BadBody5 = [#{topic => ?TOPIC_S, nl => -1}],
    ?assertMatch(
        {error, {"HTTP/1.1", 400, "Bad Request"}},
        emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, BadBody5)
    ),

    {ok, Client} = emqtt:start_link(#{username => ?CLIENT_USERNAME, clientid => ?CLIENT_ID}),
    {ok, _} = emqtt:connect(Client),
    timer:sleep(100),
    ?assertEqual(check_subs(ets:tab2list(emqx_suboption), [?TOPIC_S]), ok),
    emqtt:disconnect(Client),

    {ok, GETResponse} = emqx_mgmt_api_test_util:request_api(get, Path),
    GETResponseMap = emqx_utils_json:decode(GETResponse),
    ?assertEqual(1, erlang:length(GETResponseMap)),
    ok.

-doc """
Regression: `${clientid}`, `${username}`, `${host}` and `${port}` render exactly the same
topics as before the move to `emqx_template`. `${host}`/`${port}` are the legacy
auto-subscribe names and come from the connection peername.
""".
t_placeholder_compat(_) ->
    Topics = [?TOPIC_C, ?TOPIC_U, ?TOPIC_H, ?TOPIC_P, ?TOPIC_A],
    {ok, _} = emqx_auto_subscribe:update([#{<<"topic">> => Topic} || Topic <- Topics]),
    on_exit(fun() -> {ok, _} = emqx_auto_subscribe:update([]) end),
    {ok, Client} = emqtt:start_link(#{username => ?CLIENT_USERNAME, clientid => ?CLIENT_ID}),
    {ok, _} = emqtt:connect(Client),
    #{conninfo := #{peername := {Host, Port}}} = emqx_cm:get_chan_info(?CLIENT_ID),
    HostBin = list_to_binary(inet:ntoa(Host)),
    PortBin = integer_to_binary(Port),
    Expected = lists:sort([
        <<"/c/", (?CLIENT_ID)/binary>>,
        <<"/u/", (?CLIENT_USERNAME)/binary>>,
        <<"/h/", HostBin/binary>>,
        <<"/p/", PortBin/binary>>,
        <<"/client/", (?CLIENT_ID)/binary, "/username/", (?CLIENT_USERNAME)/binary, "/host/",
            HostBin/binary, "/port/", PortBin/binary>>
    ]),
    ?retry(
        100,
        20,
        snabbkaffe_diff:assert_lists_eq(Expected, lists:sort(client_subscriptions(?CLIENT_ID)))
    ),
    emqtt:disconnect(Client).

-doc """
`${client_attrs.<key>}` renders to the client attribute value set by
`mqtt.client_attrs_init`.
""".
t_client_attrs_placeholder(_) ->
    {ok, Compiled} = emqx_variform:compile("substr(clientid,0,4)"),
    OldInit = emqx_config:get_zone_conf(default, [mqtt, client_attrs_init]),
    emqx_config:put_zone_conf(default, [mqtt, client_attrs_init], [
        #{expression => Compiled, set_as_attr => <<"tenant">>}
    ]),
    on_exit(fun() ->
        emqx_config:put_zone_conf(default, [mqtt, client_attrs_init], OldInit)
    end),
    {ok, _} = emqx_auto_subscribe:update([
        #{<<"topic">> => <<"/t/${client_attrs.tenant}/c/${clientid}">>}
    ]),
    on_exit(fun() -> {ok, _} = emqx_auto_subscribe:update([]) end),
    {ok, Client} = emqtt:start_link(#{username => ?CLIENT_USERNAME, clientid => ?CLIENT_ID}),
    {ok, _} = emqtt:connect(Client),
    Expected = [<<"/t/auto/c/", (?CLIENT_ID)/binary>>],
    ?retry(
        100,
        20,
        snabbkaffe_diff:assert_lists_eq(Expected, client_subscriptions(?CLIENT_ID))
    ),
    emqtt:disconnect(Client).

-doc """
A `${client_attrs.<key>}` placeholder referencing an attribute the client does not have
skips that subscription. The topic filter must not contain the literal placeholder text.
""".
t_client_attrs_missing_key(_) ->
    {ok, _} = emqx_auto_subscribe:update([
        #{<<"topic">> => <<"/t/${client_attrs.tenant}">>},
        #{<<"topic">> => ?TOPIC_C}
    ]),
    on_exit(fun() -> {ok, _} = emqx_auto_subscribe:update([]) end),
    {ok, Client} = emqtt:start_link(#{username => ?CLIENT_USERNAME, clientid => ?CLIENT_ID}),
    {ok, _} = emqtt:connect(Client),
    Expected = [<<"/c/", (?CLIENT_ID)/binary>>],
    ?retry(
        100,
        20,
        snabbkaffe_diff:assert_lists_eq(Expected, client_subscriptions(?CLIENT_ID))
    ),
    emqtt:disconnect(Client).

-doc """
An unknown placeholder such as `${nope}` is kept as literal topic text.
A warning is logged when the config is loaded.
""".
t_unknown_placeholder(_) ->
    {ok, _} = emqx_auto_subscribe:update([#{<<"topic">> => <<"/n/${nope}">>}]),
    on_exit(fun() -> {ok, _} = emqx_auto_subscribe:update([]) end),
    {ok, Client} = emqtt:start_link(#{username => ?CLIENT_USERNAME, clientid => ?CLIENT_ID}),
    {ok, _} = emqtt:connect(Client),
    ?retry(
        100,
        20,
        snabbkaffe_diff:assert_lists_eq([<<"/n/${nope}">>], client_subscriptions(?CLIENT_ID))
    ),
    emqtt:disconnect(Client).

-doc """
A client with no username skips `${username}` topics and still gets the other
auto-subscriptions.
""".
t_username_undefined_skips(_) ->
    {ok, _} = emqx_auto_subscribe:update([
        #{<<"topic">> => ?TOPIC_U},
        #{<<"topic">> => ?TOPIC_C}
    ]),
    on_exit(fun() -> {ok, _} = emqx_auto_subscribe:update([]) end),
    {ok, Client} = emqtt:start_link(#{clientid => ?CLIENT_ID}),
    {ok, _} = emqtt:connect(Client),
    Expected = [<<"/c/", (?CLIENT_ID)/binary>>],
    ?retry(
        100,
        20,
        snabbkaffe_diff:assert_lists_eq(Expected, client_subscriptions(?CLIENT_ID))
    ),
    emqtt:disconnect(Client).

t_get_basic_usage_info(_Config) ->
    ?assertEqual(#{auto_subscribe_count => 0}, emqx_auto_subscribe:get_basic_usage_info()),
    AutoSubscribeTopics =
        lists:map(
            fun(N) ->
                Num = integer_to_binary(N),
                Topic = <<"auto/", Num/binary>>,
                #{<<"topic">> => Topic}
            end,
            lists:seq(1, 3)
        ),
    {ok, _} = emqx_auto_subscribe:update(AutoSubscribeTopics),
    ?assertEqual(#{auto_subscribe_count => 3}, emqx_auto_subscribe:get_basic_usage_info()),
    ok.

client_subscriptions(ClientId) ->
    [ChannelPid] = emqx_cm:lookup_channels(ClientId),
    #{session := #{subscriptions := Subscriptions}} = emqx_connection:info(ChannelPid),
    maps:keys(Subscriptions).

check_subs(Count) ->
    Subs = ets:tab2list(emqx_suboption),
    ct:pal("--->  ~p ~p ~n", [Subs, Count]),
    ?assert(length(Subs) >= Count),
    check_subs((Subs), ?ENSURE_TOPICS).

check_subs([], []) ->
    ok;
check_subs([{{Topic, _}, #{subid := ?CLIENT_ID}} | Subs], List) ->
    check_subs(Subs, lists:delete(Topic, List));
check_subs([_ | Subs], List) ->
    check_subs(Subs, List).
