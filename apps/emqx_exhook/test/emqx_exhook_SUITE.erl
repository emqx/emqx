%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_exhook_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx_access_control.hrl").

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(OTHER_CLUSTER_NAME_STRING, "test_emqx_cluster").

-define(CONF_DEFAULT, <<"""
    exhook {
      servers = [
        { name = default,
          url = "http://127.0.0.1:9000"
        },
        { name = enable,
          enable = false,
          url = "http://127.0.0.1:9000"
        },
        { name = error,
          url = "http://127.0.0.1:9001"
        },
        { name = not_reconnect,
          auto_reconnect = false,
          url = "http://127.0.0.1:9001"
        }
      ]
    }
""">>).

-define(CONF_TWO_SERVERS, <<"""
    exhook {
      servers = [
        { name = default,
          url = "http://127.0.0.1:9000"
        },
        { name = test1,
          url = "http://127.0.0.1:9001"
        }
      ]
    }
""">>).

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() ->
    ProfileCases = profile_cases(),
    (emqx_common_test_helpers:all(?MODULE) -- ProfileCases) ++
        [{group, legacy}, {group, hardened}].

groups() ->
    ProfileCases = profile_cases(),
    [{legacy, [], ProfileCases}, {hardened, [], ProfileCases}].

init_per_suite(Config) ->
    {ok, Apps} = application:ensure_all_started(grpc),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_group(Profile, Config) when Profile =:= legacy; Profile =:= hardened ->
    ok = emqx_common_test_helpers:set_security_profile(Profile),
    [{security_profile, Profile} | Config].

end_per_group(Profile, _Config) when Profile =:= legacy; Profile =:= hardened ->
    emqx_common_test_helpers:clear_security_profile().

init_per_testcase(t_health_check = TC, Config) ->
    common_init(TC, Config);
init_per_testcase(TC, Config) when TC == t_handler_tcp; TC == t_handler_ws ->
    _ = emqx_exhook_demo_svr:start(),
    Config1 = common_init(TC, Config),
    emqx_common_test_helpers:listeners_enable_authn_scoped(),
    Config1;
init_per_testcase(t_update_order_with_unhealthy_server = TC, Config) ->
    _ = emqx_exhook_demo_svr:start(),
    _ = emqx_exhook_demo_svr:start(<<"test1">>, 9001),
    common_init(TC, Config);
init_per_testcase(TC, Config) when
    TC =:= t_subscribe_rewrite;
    TC =:= t_subscribe_rewrite_rejects_count_mismatch;
    TC =:= t_subscribe_rewrite_rejects_invalid_subopts;
    TC =:= t_subscribe_rewrite_rejects_beyond_caps;
    TC =:= t_subscribe_rewrite_keeps_shared_subscription
->
    ok = emqx_exhook_demo_svr:enable_rewrite_hook(),
    _ = emqx_exhook_demo_svr:start(),
    common_init(TC, Config);
init_per_testcase(TC, Config) ->
    _ = emqx_exhook_demo_svr:start(),
    common_init(TC, Config).

end_per_testcase(t_health_check = TC, Config) ->
    common_stop(TC, Config);
end_per_testcase(t_update_order_with_unhealthy_server = TC, Config) ->
    common_stop(TC, Config),
    ok = emqx_exhook_demo_svr:stop(<<"test1">>),
    ok = emqx_exhook_demo_svr:stop();
end_per_testcase(TC, Config) ->
    common_stop(TC, Config),
    ok = emqx_exhook_demo_svr:stop().

emqx_conf(t_cluster_name) ->
    io_lib:format("cluster.name = ~p", [?OTHER_CLUSTER_NAME_STRING]);
emqx_conf(t_subscribe_rewrite_rejects_beyond_caps) ->
    "mqtt.wildcard_subscription = false";
emqx_conf(t_access_failed_if_no_server_running) ->
    "authorization.sources = []";
emqx_conf(_) ->
    #{}.

common_init(TC, Config) ->
    ExHookConf =
        case TC of
            t_update_order_with_unhealthy_server -> ?CONF_TWO_SERVERS;
            _ -> ?CONF_DEFAULT
        end,
    Apps = emqx_cth_suite:start(
        [
            emqx,
            {emqx_conf, emqx_conf(TC)}
        ] ++
            [emqx_auth || TC =:= t_access_failed_if_no_server_running] ++
            [
                {emqx_exhook, ExHookConf}
            ],
        #{work_dir => emqx_cth_suite:work_dir(TC, Config)}
    ),
    emqx_common_test_helpers:init_per_testcase(?MODULE, TC, [{tc_apps, Apps} | Config]).

common_stop(TC, Config) ->
    ok = emqx_exhook_demo_svr:reset_rewrite_hook(),
    emqx_common_test_helpers:end_per_testcase(?MODULE, TC, Config),
    emqx_common_test_helpers:call_janitor(),
    ok = emqx_cth_suite:stop(?config(tc_apps, Config)).

profile_cases() ->
    [
        t_access_failed_if_no_server_running,
        t_no_running_server_honors_failed_action,
        t_handler_tcp,
        t_handler_ws
    ].

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

%% A server that does not advertise the hookpoint answers `ignore'. That is
%% not a failure, so it must not be routed through `failed_action': under
%% the default `deny' that stops the chain and every server behind it stops
%% seeing the event, silently.
t_call_fold_skips_servers_without_the_hookpoint(_Config) ->
    ok = meck:new(emqx_exhook_mgr, [passthrough, no_history]),
    ok = meck:new(emqx_exhook_server, [passthrough, no_history]),
    Tester = self(),
    try
        meck:expect(emqx_exhook_mgr, running, fun() -> [<<"a">>, <<"b">>] end),
        meck:expect(emqx_exhook_mgr, service, fun(Name) -> #{name => Name} end),
        meck:expect(emqx_exhook_server, call, fun
            (_Hookpoint, _Req, #{name := <<"a">>}) ->
                ignore;
            (_Hookpoint, Req, #{name := <<"b">>}) ->
                Tester ! reached_second_server,
                {ok, Req}
        end),
        meck:expect(emqx_exhook_server, failed_action, fun(_Server) -> deny end),
        Req = #{topic_filters => []},
        ?assertMatch(
            {ok, _},
            emqx_exhook:call_fold('client.subscribe', Req, fun(_Req, Resp) -> {ok, Resp} end)
        ),
        receive
            reached_second_server -> ok
        after 0 ->
            ct:fail("the server behind the one without the hookpoint was never called")
        end
    after
        meck:unload(emqx_exhook_server),
        meck:unload(emqx_exhook_mgr)
    end.

t_access_failed_if_no_server_running(Config) ->
    ClientInfo = #{
        clientid => <<"user-id-1">>,
        username => <<"usera">>,
        peername => {{127, 0, 0, 1}, 3456},
        sockport => 1883,
        protocol => mqtt,
        mountpoint => undefined
    },
    ?assertMatch(
        allow,
        emqx_access_control:authorize(
            emqx_authz_context:make(ClientInfo#{username => <<"gooduser">>}),
            ?AUTHZ_PUBLISH,
            <<"acl/1">>
        )
    ),

    ?assertMatch(
        deny,
        emqx_access_control:authorize(
            emqx_authz_context:make(ClientInfo#{username => <<"baduser">>}),
            ?AUTHZ_PUBLISH,
            <<"acl/2">>
        )
    ),

    emqx_exhook_mgr:disable(<<"default">>),
    ?assertMatch(
        {stop, {error, not_authorized}},
        emqx_exhook_handler:on_client_authenticate(ClientInfo, #{auth_result => success})
    ),

    ?assertMatch(
        {stop, #{result := deny, from := exhook}},
        emqx_exhook_handler:on_client_authorize(ClientInfo, ?AUTHZ_PUBLISH, <<"t/1">>, #{
            result => allow, from => exhook
        })
    ),

    Message = emqx_message:make(<<"t/1">>, <<"abc">>),
    ?assertEqual(
        {stop, Message},
        emqx_common_test_helpers:with_security_profile("legacy", fun() ->
            emqx_exhook_handler:on_message_publish(Message)
        end)
    ),
    {stop, DeniedMessage} = emqx_common_test_helpers:with_security_profile("hardened", fun() ->
        emqx_exhook_handler:on_message_publish(Message)
    end),
    ?assertEqual(false, emqx_message:get_header(allow_publish, DeniedMessage)),
    ?assertMatch(
        {stop, {error, not_authorized}},
        emqx_exhook_handler:on_message_ingress(
            #{authz_ctx => emqx_authz_context:make(ClientInfo)}, Message
        )
    ),
    emqx_exhook_mgr:enable(<<"default">>),
    assert_get_basic_usage_info(Config).

t_no_running_server_honors_failed_action(_) ->
    ClientInfo = #{
        clientid => <<"user-id-1">>,
        username => <<"usera">>,
        peername => {{127, 0, 0, 1}, 3456},
        peerhost => {127, 0, 0, 1},
        sockport => 1883,
        protocol => mqtt,
        mountpoint => undefined
    },
    ok = disable_server(<<"default">>),
    ok = disable_server(<<"not_reconnect">>),
    ok = disable_server(<<"error">>),
    ?assertEqual([], emqx_exhook_mgr:running()),
    ?assertEqual(
        ignore,
        emqx_common_test_helpers:with_security_profile("legacy", fun() ->
            emqx_exhook_handler:on_client_authenticate(ClientInfo, #{auth_result => success})
        end)
    ),
    ?assertEqual(
        {stop, {error, not_authorized}},
        emqx_common_test_helpers:with_security_profile("hardened", fun() ->
            emqx_exhook_handler:on_client_authenticate(ClientInfo, #{auth_result => success})
        end)
    ),
    ok = update_failed_action(<<"error">>, <<"ignore">>),
    ?assertEqual([], emqx_exhook_mgr:running()),
    ?assertEqual(
        ignore,
        emqx_common_test_helpers:with_security_profile("legacy", fun() ->
            emqx_exhook_handler:on_client_authenticate(ClientInfo, #{auth_result => success})
        end)
    ),
    ?assertEqual(
        {stop, {error, not_authorized}},
        emqx_common_test_helpers:with_security_profile("hardened", fun() ->
            emqx_exhook_handler:on_client_authenticate(ClientInfo, #{auth_result => success})
        end)
    ),
    ?assertEqual(
        ignore,
        emqx_common_test_helpers:with_security_profile("legacy", fun() ->
            emqx_exhook_handler:on_client_authorize(
                ClientInfo,
                ?AUTHZ_PUBLISH,
                <<"t/1">>,
                #{result => allow, from => exhook}
            )
        end)
    ),
    Message = emqx_message:make(<<"t/1">>, <<"abc">>),
    ?assertEqual(
        ignore,
        emqx_common_test_helpers:with_security_profile("legacy", fun() ->
            emqx_exhook_handler:on_message_ingress(
                #{authz_ctx => emqx_authz_context:make(ClientInfo)},
                Message
            )
        end)
    ),
    ?assertEqual(
        ignore,
        emqx_common_test_helpers:with_security_profile("legacy", fun() ->
            emqx_exhook_handler:on_message_publish(Message)
        end)
    ),
    ok = update_failed_action(<<"not_reconnect">>, <<"deny">>),
    ?assertEqual([], emqx_exhook_mgr:running()),
    ?assertEqual(
        {stop, {error, not_authorized}},
        emqx_common_test_helpers:with_security_profile("legacy", fun() ->
            emqx_exhook_handler:on_client_authenticate(ClientInfo, #{auth_result => success})
        end)
    ).

disable_server(Name) ->
    {ok, _} = emqx_exhook_mgr:disable(Name),
    ok.

update_failed_action(Name, FailedAction) ->
    {ok, _} = emqx_exhook_mgr:update_config(
        [exhook, servers],
        {update, Name, #{
            <<"name">> => Name,
            <<"url">> => <<"http://127.0.0.1:9001">>,
            <<"failed_action">> => FailedAction
        }}
    ),
    ok.

t_message_ingress(_) ->
    _ = emqx_exhook_demo_svr:flush(),
    AuthzContext = #{
        clientid => <<"ingress-client">>,
        username => <<"ingress-user">>,
        peername => {{127, 0, 0, 1}, 3456},
        sockport => 1883,
        protocol => mqtt,
        mountpoint => undefined
    },
    Message = emqx_message:make(<<"ingress-client">>, 0, <<"/ingress">>, <<"payload">>),
    {ok, IngressedMessage} = emqx_message_ingress:ingress(AuthzContext, Message),
    ?assertEqual(<<"/ingressed">>, emqx_message:topic(IngressedMessage)),
    ?assertMatch(
        {on_message_ingress, #{
            clientinfo := #{clientid := <<"ingress-client">>, password := <<>>},
            message := #{topic := <<"/ingress">>}
        }},
        emqx_exhook_demo_svr:take()
    ),
    MessageMap = emqx_message:to_map(Message),
    DeniedMessage = emqx_message:from_map(MessageMap#{topic => <<"/denied-ingress">>}),
    ?assertEqual(
        {error, not_authorized},
        emqx_message_ingress:ingress(AuthzContext, DeniedMessage)
    ),
    InvalidMessage = emqx_message:from_map(MessageMap#{topic => <<"/invalid-ingress">>}),
    ?assertMatch(
        {error, {invalid_exhook_response, _}},
        emqx_message_ingress:ingress(AuthzContext, InvalidMessage)
    ).

t_lookup(_) ->
    Result = emqx_exhook_mgr:lookup(<<"default">>),
    ?assertMatch(#{name := <<"default">>, status := _}, Result),
    not_found = emqx_exhook_mgr:lookup(<<"not_found">>).

t_list(_) ->
    [H | _] = emqx_exhook_mgr:list(),
    ?assertMatch(
        #{
            name := _,
            status := _,
            hooks := _
        },
        H
    ).

t_unexpected(_) ->
    ok = gen_server:cast(emqx_exhook_mgr, unexpected),
    unexpected = erlang:send(erlang:whereis(emqx_exhook_mgr), unexpected),
    Result = gen_server:call(emqx_exhook_mgr, unexpected),
    ?assertEqual(Result, ok).

t_timer(_) ->
    Pid = erlang:whereis(emqx_exhook_mgr),
    refresh_tick = erlang:send(Pid, refresh_tick),
    _ = erlang:send(Pid, {timeout, undefined, {reload, <<"default">>}}),
    _ = erlang:send(Pid, {timeout, undefined, {reload, <<"not_found">>}}),
    _ = erlang:send(Pid, {timeout, undefined, {reload, <<"error">>}}),
    ok.

t_running_deduplicated_after_successful_reload(_) ->
    Name = <<"default">>,
    Pid = erlang:whereis(emqx_exhook_mgr),

    ?assertEqual([Name], emqx_exhook_mgr:running()),

    ok = emqx_exhook_demo_svr:stop(),
    refresh_tick = erlang:send(Pid, refresh_tick),

    #{status := connecting, timer := Timer} = emqx_exhook_mgr:lookup(Name),
    ?assertNotEqual(undefined, Timer),

    {ok, _} = emqx_exhook_demo_svr:start(),
    _ = erlang:send(Pid, {timeout, undefined, {reload, Name}}),
    timer:sleep(200),

    ?assertMatch(#{status := connected}, emqx_exhook_mgr:lookup(Name)),
    ?assertEqual([Name], emqx_exhook_mgr:running()).

t_update_order_with_unhealthy_server(_) ->
    Name = <<"test1">>,
    MgrPid = erlang:whereis(emqx_exhook_mgr),

    ?assertEqual([<<"default">>, Name], emqx_exhook_mgr:running()),

    ok = emqx_exhook_demo_svr:stop(Name),
    refresh_tick = erlang:send(MgrPid, refresh_tick),
    ?retry(
        100,
        20,
        begin
            #{status := Status} = emqx_exhook_mgr:lookup(Name),
            ?assert(lists:member(Status, [connecting, disconnected]))
        end
    ),

    ?assertEqual(
        {ok, ok},
        emqx_exhook_mgr:update_config([exhook, servers], {move, Name, front})
    ),
    ?assertEqual(MgrPid, erlang:whereis(emqx_exhook_mgr)),
    ?assertEqual([Name, <<"default">>], emqx_exhook_mgr:running()).

t_health_check('init', Config) ->
    %% health_check and auto_reconnect logic:
    %% assume auto_reconnect = 7.5s
    %% It takes too much time to perform health checks manually.
    %%
    %% Status:
    %% C: Connected
    %% R: (re)Connecting
    %% D: Disconnected
    %%
    %% Time (s) 0.0   2.5   5.0   7.5  10.0  12.5  15.0  17.5  20.0  22.5  25.0
    %%          |-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|
    %%      Stop Server  ^                      Start Server ^
    %% Health Check timer:
    %%          |           |           |           |           |           |
    %%          C1          C2         C3          C4          C5          C6
    %%          |---------->|---------->|---------->|---------->|---------->|
    %%          |           |           |           |           |           |
    %% Status:  |---- C ----|---- R ----|- D -|- R -|---- D ----|---- C ----|
    %%          |           |           |           |           |           |
    %% Reconnect Interval:  |           |           |           |           |
    %%                      |---- 7.5s ------>|---- 7.5s ------>| Cancaled

    emqx_exhook_mgr:disable(<<"default">>),
    _ = emqx_exhook_demo_svr:start(),
    Config;
t_health_check('end', _Config) ->
    ok.

t_health_check(_Config) ->
    %% before time C1
    Name = <<"default">>,
    ?assertMatch(#{status := disabled}, emqx_exhook_mgr:lookup(Name)),

    emqx_exhook_mgr:enable(Name),

    timer:sleep(200),
    %% will be `connecting` and then changed to `connected` very soon
    %% disabled -> connected
    ?assertMatch(#{status := connected}, emqx_exhook_mgr:lookup(Name)),

    %% stop server
    _ = emqx_exhook_demo_svr:stop(),

    %% manually perform health_check
    %% health_check found it unhealthy and then start a reload timer,
    %% the reload timer marked it `connecting`
    %% connected -> connecting
    erlang:send(erlang:whereis(emqx_exhook_mgr), refresh_tick),
    ?assertMatch(#{status := connecting}, emqx_exhook_mgr:lookup(Name)),

    %% still unhealthy after next health_check, then mark `disconnected` (reload timer still working)
    %% connecting -> disconnected
    erlang:send(erlang:whereis(emqx_exhook_mgr), refresh_tick),
    ?assertMatch(#{status := disconnected}, emqx_exhook_mgr:lookup(Name)),

    emqx_exhook_mgr:disable(Name),
    emqx_exhook_mgr:enable(Name),

    %% re-enable, will be `connecting` if server is not started
    ?assertMatch(#{status := connecting}, emqx_exhook_mgr:lookup(Name)),

    %% still unhealthy after next health_check
    %% connecting -> disconnected
    erlang:send(erlang:whereis(emqx_exhook_mgr), refresh_tick),
    ?assertMatch(#{status := disconnected}, emqx_exhook_mgr:lookup(Name)),

    ok.

t_error_update_conf(_) ->
    Path = [exhook, servers],
    Name = <<"error_update">>,
    ErrorCfg = #{<<"name">> => Name},
    {error, not_found} = emqx_exhook_mgr:update_config(Path, {update, Name, ErrorCfg}),
    {error, not_found} = emqx_exhook_mgr:update_config(Path, {move, Name, top}),
    {error, not_found} = emqx_exhook_mgr:update_config(Path, {enable, Name, true}),

    ErrorAnd = #{<<"name">> => Name, <<"url">> => <<"http://127.0.0.1:9001">>},
    {ok, _} = emqx_exhook_mgr:update_config(Path, {add, ErrorAnd}),

    DisableAnd = #{
        <<"name">> => Name,
        <<"url">> => <<"http://127.0.0.1:9001">>,
        <<"enable">> => false
    },
    {ok, _} = emqx_exhook_mgr:update_config(Path, {update, Name, DisableAnd}),

    {ok, _} = emqx_exhook_mgr:update_config(Path, {delete, Name}),
    {error, not_found} = emqx_exhook_mgr:update_config(Path, {delete, Name}),
    ok.

t_update_conf(_Config) ->
    Path = [exhook],
    Conf = #{<<"servers">> := Servers} = emqx_config:get_raw(Path),
    ?assert(length(Servers) > 1),
    Servers1 = shuffle(Servers),
    ReOrderedConf = Conf#{<<"servers">> => Servers1},
    validate_servers(Path, ReOrderedConf, Servers1),
    [_ | Servers2] = Servers,
    DeletedConf = Conf#{<<"servers">> => Servers2},
    validate_servers(Path, DeletedConf, Servers2),
    [L1, L2 | Servers3] = Servers,
    UpdateL2 = L2#{<<"pool_size">> => 1, <<"request_timeout">> => <<"1s">>},
    UpdatedServers = [L1, UpdateL2 | Servers3],
    UpdatedConf = Conf#{<<"servers">> => UpdatedServers},
    validate_servers(Path, UpdatedConf, UpdatedServers),
    %% reset
    validate_servers(Path, Conf, Servers),
    ok.

validate_servers(Path, ReOrderConf, Servers1) ->
    {ok, _} = emqx_exhook_mgr:update_config(Path, ReOrderConf),
    ?assertEqual(ReOrderConf, emqx_config:get_raw(Path)),
    List = emqx_exhook_mgr:list(),
    ExpectL = lists:map(fun(#{<<"name">> := Name}) -> Name end, Servers1),
    L1 = lists:map(fun(#{name := Name}) -> Name end, List),
    ?assertEqual(ExpectL, L1).

t_error_server_info(_) ->
    not_found = emqx_exhook_mgr:server_info(<<"not_exists">>),
    ok.

t_metrics(_) ->
    ok = emqx_exhook_metrics:succeed(<<"default">>, 'client.connect'),
    ok = emqx_exhook_metrics:failed(<<"default">>, 'client.connect'),
    true = emqx_exhook_metrics:update(1000),
    timer:sleep(100),
    SvrMetrics = emqx_exhook_metrics:server_metrics(<<"default">>),
    ?assertMatch(#{succeed := _, failed := _, rate := _, max_rate := _}, SvrMetrics),

    SvrsMetrics = emqx_exhook_metrics:servers_metrics(),
    ?assertMatch(#{<<"default">> := #{succeed := _}}, SvrsMetrics),

    HooksMetrics = emqx_exhook_metrics:hooks_metrics(<<"default">>),
    ?assertMatch(#{'client.connect' := #{succeed := _}}, HooksMetrics),
    ok.

t_handler_tcp(_) ->
    t_handler(fun emqtt:connect/1, 1883, <<"exhook_tcp">>).

t_handler_ws(_) ->
    t_handler(fun emqtt:ws_connect/1, 8083, <<"exhook_ws">>).

t_handler(ConnFun, Port, CId) ->
    ?assertMatch(
        [{on_provider_loaded, #{broker := _Broker}}],
        emqx_exhook_demo_svr:flush()
    ),

    %% connect
    {ok, C} = emqtt:start_link([
        {host, "localhost"},
        {port, Port},
        {username, <<"gooduser">>},
        {clientid, CId}
    ]),
    {ok, _} = ConnFun(C),

    ?assertMatch(
        [
            {on_client_connect, #{conninfo := #{sockport := Port}}},
            {on_client_authenticate, #{
                clientinfo := #{sockport := Port, peerport := _, clientid := CId}
            }},
            {on_session_created, #{clientinfo := #{clientid := CId}}},
            {on_client_connected, #{clientinfo := #{clientid := CId}}},
            {on_client_connack, #{conninfo := #{}}}
        ],
        emqx_exhook_demo_svr:flush()
    ),

    %% pub/sub
    {ok, _, _} = emqtt:subscribe(C, <<"/exhook">>, qos0),
    timer:sleep(100),
    ok = emqtt:publish(C, <<"/exhook">>, <<>>, qos0),
    ok = emqtt:publish(C, <<"/ignore">>, <<>>, qos0),
    timer:sleep(100),
    {ok, _, _} = emqtt:unsubscribe(C, <<"/exhook">>),

    Events1 = emqx_exhook_demo_svr:flush(),
    ?assertMatch(
        [
            {on_client_authorize, #{type := 'SUBSCRIBE', clientinfo := #{clientid := CId}}},
            {on_client_subscribe, #{
                topic_filters := [#{name := <<"/exhook">>}],
                clientinfo := #{clientid := CId}
            }},
            {on_session_subscribed, #{
                topic := <<"/exhook">>,
                clientinfo := #{clientid := CId}
            }},
            {on_message_ingress, #{message := #{topic := <<"/exhook">>, qos := 0}}},
            {on_client_authorize, #{
                type := 'PUBLISH',
                topic := <<"/exhook">>,
                clientinfo := #{clientid := CId}
            }},
            {on_message_publish, #{message := #{topic := <<"/exhook">>, qos := 0}}},
            {on_message_ingress, #{message := #{topic := <<"/ignore">>, qos := 0}}},
            {on_client_authorize, #{
                type := 'PUBLISH',
                topic := <<"/ignore">>,
                clientinfo := #{clientid := CId}
            }},
            {on_message_publish, #{message := #{topic := <<"/ignore">>, qos := 0}}},
            {on_client_unsubscribe, #{
                topic_filters := [#{name := <<"/exhook">>}],
                clientinfo := #{clientid := CId}
            }},
            {on_session_unsubscribed, #{
                topic := <<"/exhook">>,
                clientinfo := #{clientid := CId}
            }}
        ],
        [E || {ET, _} = E <- Events1, ET /= on_message_dropped, ET /= on_message_delivered]
    ),
    ?assertMatch(
        [{on_message_delivered, #{message := #{topic := <<"/exhook">>, qos := 0}}}],
        [E || {ET, _} = E <- Events1, ET == on_message_delivered]
    ),
    ?assertMatch(
        [{on_message_dropped, #{message := #{topic := <<"/ignore">>, qos := 0}}}],
        [E || {ET, _} = E <- Events1, ET == on_message_dropped]
    ),

    %% sys pub/sub
    ok = emqtt:publish(C, <<"$SYS">>, <<>>, qos0),
    {ok, _, _} = emqtt:subscribe(C, <<"$SYS/systest">>, qos1),
    timer:sleep(100),
    {ok, _} = emqtt:publish(C, <<"$SYS/systest">>, <<>>, qos1),
    ok = emqtt:publish(C, <<"$SYS/ignore">>, <<>>, qos0),
    timer:sleep(100),
    {ok, _, _} = emqtt:unsubscribe(C, <<"$SYS/systest">>),

    ?assertMatch(
        [
            {on_message_ingress, #{message := #{topic := <<"$SYS">>, qos := 0}}},
            {on_client_authorize, #{
                type := 'PUBLISH',
                topic := <<"$SYS">>,
                clientinfo := #{clientid := CId}
            }},
            {on_message_publish, #{message := #{topic := <<"$SYS">>, qos := 0}}},
            {on_message_dropped, #{message := #{topic := <<"$SYS">>, qos := 0}}},
            {on_client_authorize, #{type := 'SUBSCRIBE', clientinfo := #{clientid := CId}}},
            {on_client_subscribe, #{
                topic_filters := [#{name := <<"$SYS/systest">>}],
                clientinfo := #{clientid := CId}
            }},
            {on_session_subscribed, #{
                topic := <<"$SYS/systest">>,
                clientinfo := #{clientid := CId}
            }},
            {on_client_authorize, #{
                type := 'PUBLISH',
                topic := <<"$SYS/systest">>,
                clientinfo := #{clientid := CId}
            }},
            {on_client_authorize, #{
                type := 'PUBLISH',
                topic := <<"$SYS/ignore">>,
                clientinfo := #{clientid := CId}
            }},
            %% No publishes.
            {on_client_unsubscribe, #{
                topic_filters := [#{name := <<"$SYS/systest">>}],
                clientinfo := #{clientid := CId}
            }},
            {on_session_unsubscribed, #{
                topic := <<"$SYS/systest">>,
                clientinfo := #{clientid := CId}
            }}
        ],
        emqx_exhook_demo_svr:flush()
    ),

    %% ack
    {ok, _, _} = emqtt:subscribe(C, <<"/exhook1">>, qos1),
    timer:sleep(100),
    {ok, _} = emqtt:publish(C, <<"/exhook1">>, <<>>, qos1),
    timer:sleep(100),
    emqtt:stop(C),
    timer:sleep(100),

    ?assertMatch(
        [
            {on_client_authorize, #{type := 'SUBSCRIBE', clientinfo := #{clientid := CId}}},
            {on_client_subscribe, #{
                topic_filters := [#{name := <<"/exhook1">>}],
                clientinfo := #{clientid := CId}
            }},
            {on_session_subscribed, #{
                topic := <<"/exhook1">>,
                clientinfo := #{clientid := CId}
            }},
            {on_message_ingress, #{message := #{topic := <<"/exhook1">>, qos := 1}}},
            {on_client_authorize, #{
                type := 'PUBLISH',
                topic := <<"/exhook1">>,
                clientinfo := #{clientid := CId}
            }},
            {on_message_publish, #{message := #{topic := <<"/exhook1">>, qos := 1}}},
            {on_message_delivered, #{message := #{topic := <<"/exhook1">>, qos := 1}}},
            {on_message_acked, #{
                message := #{topic := <<"/exhook1">>, qos := 1},
                clientinfo := #{clientid := CId}
            }},
            {on_client_disconnected, #{clientinfo := #{clientid := CId}}},
            {on_session_terminated, #{clientinfo := #{clientid := CId}}}
        ],
        emqx_exhook_demo_svr:flush()
    ).

t_simulated_handler(_) ->
    ClientInfo = #{
        clientid => <<"user-id-1">>,
        username => <<"usera">>,
        peername => {{127, 0, 0, 1}, 3456},
        peerhost => {127, 0, 0, 1},
        sockport => 1883,
        protocol => mqtt,
        mountpoint => undefined
    },
    %% resume/takeover
    ok = emqx_exhook_handler:on_session_resumed(ClientInfo, undefined),
    ok = emqx_exhook_handler:on_session_discarded(ClientInfo, undefined),
    ok = emqx_exhook_handler:on_session_takenover(ClientInfo, undefined),
    ok.

t_misc_test(_) ->
    "5.0.0" = emqx_exhook_proto_v1:introduced_in(),
    <<"test">> = emqx_exhook_server:name(#{name => <<"test">>}),
    _ = emqx_exhook_server:format(#{name => <<"test">>, hookspec => #{}}),
    ok.

t_cluster_name(_) ->
    ?assertEqual(?OTHER_CLUSTER_NAME_STRING, emqx_sys:cluster_name()),

    emqx_exhook_mgr:disable(<<"default">>),
    emqx_exhook_mgr:enable(<<"default">>),
    %% See emqx_exhook_demo_svr:on_provider_loaded/2
    ?assertEqual([], emqx_hooks:lookup('session.created')),
    ?assertEqual([], emqx_hooks:lookup('message_publish')),
    ?assertEqual(
        true,
        erlang:length(emqx_hooks:lookup('client.connected')) > 1
    ),
    emqx_exhook_mgr:disable(<<"default">>).

t_stop_timeout('init', Config) ->
    ok = snabbkaffe:start_trace(),
    ok = meck:new(emqx_exhook_demo_svr, [passthrough, no_history]),
    Config;
t_stop_timeout('end', _Config) ->
    %% ensure started for other tests
    {ok, _} = application:ensure_all_started(emqx_exhook),
    ok = snabbkaffe:stop(),
    ok = meck:unload(emqx_exhook_demo_svr).

t_stop_timeout(_) ->
    TestPid = self(),
    Ref = make_ref(),
    meck:expect(
        emqx_exhook_demo_svr,
        on_provider_unloaded,
        fun(Req, Md) ->
            TestPid ! {Ref, self()},
            receive
                Ref -> ok
            end,
            meck:passthrough([Req, Md])
        end
    ),

    %% stop application
    ok = application:stop(emqx_exhook),
    ?block_until(#{?snk_kind := exhook_mgr_terminated}, 20000),
    receive
        {Ref, CallbackPid} -> CallbackPid ! Ref
    after 1000 ->
        ct:fail("Provider unload callback was not called")
    end,

    %% all exhook hooked point should be unloaded
    Hooks = lists:flatmap(
        fun emqx_hooks:lookup/1,
        maps:keys(emqx_hookpoints:registered_hookpoints())
    ),
    ?assertEqual(
        [],
        [H || H = {callback, {emqx_exhook_handler, _, _}, _, _} <- Hooks]
    ).

t_ssl_clear(_) ->
    SvrName = <<"ssl_test">>,
    SSLConf = #{
        <<"enable">> => true,
        <<"cacertfile">> => cert_file("cafile"),
        <<"certfile">> => cert_file("certfile"),
        <<"keyfile">> => cert_file("keyfile"),
        <<"verify">> => <<"verify_peer">>
    },
    AddConf = #{
        <<"auto_reconnect">> => <<"60s">>,
        <<"enable">> => false,
        <<"failed_action">> => <<"deny">>,
        <<"name">> => <<"ssl_test">>,
        <<"pool_size">> => 16,
        <<"request_timeout">> => <<"5s">>,
        <<"ssl">> => SSLConf,
        <<"url">> => <<"http://127.0.0.1:9000">>
    },
    emqx_exhook_mgr:update_config([exhook, servers], {add, AddConf}),
    ListResult1 = list_pem_dir(SvrName),
    ?assertMatch({ok, [_, _, _]}, ListResult1),
    {ok, ResultList1} = ListResult1,

    UpdateConf = AddConf#{<<"ssl">> => SSLConf#{<<"keyfile">> => cert_file("keyfile2")}},
    emqx_exhook_mgr:update_config([exhook, servers], {update, SvrName, UpdateConf}),
    {ok, _} = emqx_tls_certfile_gc:force(),
    ListResult2 = list_pem_dir(SvrName),
    ?assertMatch({ok, [_, _, _]}, ListResult2),
    {ok, ResultList2} = ListResult2,

    FindKeyFile = fun(List) ->
        case lists:search(fun(E) -> lists:prefix("key", E) end, List) of
            {value, Value} ->
                Value;
            _ ->
                ?assert(false, "Can't find keyfile")
        end
    end,

    ?assertNotEqual(FindKeyFile(ResultList1), FindKeyFile(ResultList2)),

    emqx_exhook_mgr:update_config([exhook, servers], {delete, SvrName}),
    {ok, _} = emqx_tls_certfile_gc:force(),
    ?assertMatch({error, enoent}, list_pem_dir(SvrName)),
    ok.

%%--------------------------------------------------------------------
%% `client.subscribe.rewrite' topic filter rewrite
%%--------------------------------------------------------------------

t_subscribe_rewrite(_) ->
    ok = emqx_exhook_demo_svr:set_rewrite_fun(
        fun(TopicFilters) ->
            [Tf#{name => <<"rewritten/", Name/binary>>} || Tf = #{name := Name} <- TopicFilters]
        end
    ),
    ClientId = <<"exhook_sub_rw">>,
    C = connect(ClientId),
    ?assertMatch({ok, _, [0]}, emqtt:subscribe(C, <<"t/1">>, qos0)),
    %% the client is subscribed to what the hook asked for, and only to that
    ?assertEqual([<<"rewritten/t/1">>], subscribed_topics(ClientId)),
    ok = emqtt:stop(C).

%% Returning the filters unchanged is how a server says "I have looked at this
%% and no later hook should touch it". `emqx_hooks' continues the chain on any
%% term other than `stop'/`{stop, Acc}', so answering `ignore' here would
%% silently downgrade STOP_AND_RETURN to CONTINUE.
t_subscribe_unchanged_filters_still_stop(_) ->
    ok = meck:new(emqx_exhook, [passthrough, no_history]),
    try
        TopicFilters = [{<<"t/1">>, #{qos => 0, rh => 0, rap => 0, nl => 0}}],
        meck:expect(emqx_exhook, call_fold, fun('client.subscribe', Req, _Fun) ->
            {stop, #{topic_filters => maps:get(topic_filters, Req)}}
        end),
        ?assertEqual(
            {stop, TopicFilters},
            emqx_exhook_handler:on_client_subscribe(clientinfo_for_test(), #{}, TopicFilters)
        ),
        %% A server that answered CONTINUE leaves the accumulator alone, so
        %% the rest of the chain runs.
        meck:expect(emqx_exhook, call_fold, fun('client.subscribe', Req, _Fun) ->
            {ok, #{topic_filters => maps:get(topic_filters, Req)}}
        end),
        ?assertEqual(
            ignore,
            emqx_exhook_handler:on_client_subscribe(clientinfo_for_test(), #{}, TopicFilters)
        )
    after
        meck:unload(emqx_exhook)
    end.

clientinfo_for_test() ->
    #{
        clientid => <<"exhook_stop_test">>,
        username => <<"u">>,
        peername => {{127, 0, 0, 1}, 54321},
        sockport => 1883,
        protocol => mqtt,
        mountpoint => undefined,
        is_superuser => false,
        anonymous => false,
        cn => undefined,
        dn => undefined
    }.

%% Both names resolve to the `client.subscribe' hookpoint. A server that
%% registers the two -- the plausible shape while migrating -- gets the
%% `OnClientSubscribeRewrite' RPC, in either HookSpec order.
t_duplicate_subscribe_registration_prefers_rewrite(_) ->
    Plain = #{name => <<"client.subscribe">>},
    Rewrite = #{name => <<"client.subscribe.rewrite">>},
    ?assertEqual(
        #{'client.subscribe' => #{valued => true}},
        emqx_exhook_server:resolve_hookspec([Plain, Rewrite])
    ),
    ?assertEqual(
        #{'client.subscribe' => #{valued => true}},
        emqx_exhook_server:resolve_hookspec([Rewrite, Plain])
    ),
    %% A single registration of either name is unaffected.
    ?assertEqual(
        #{'client.subscribe' => #{}},
        emqx_exhook_server:resolve_hookspec([Plain])
    ),
    ?assertEqual(
        #{'client.subscribe' => #{valued => true}},
        emqx_exhook_server:resolve_hookspec([Rewrite])
    ).

t_subscribe_rewrite_rejects_count_mismatch(_) ->
    %% SUBACK owes the client exactly one reason code per requested topic filter,
    %% ordered as requested (MQTT-3.9.3-1). A hook handing back a different number
    %% of filters is refused as a whole rather than applied in part.
    ok = emqx_exhook_demo_svr:set_rewrite_fun(
        fun(TopicFilters) ->
            TopicFilters ++ [#{name => <<"extra/topic">>, subopts => #{qos => 0}}]
        end
    ),
    ClientId = <<"exhook_sub_rw_count">>,
    C = connect(ClientId),
    ?assertMatch({ok, _, [0]}, emqtt:subscribe(C, <<"t/1">>, qos0)),
    ?assertEqual([<<"t/1">>], subscribed_topics(ClientId)),
    ok = emqtt:stop(C).

t_subscribe_rewrite_rejects_invalid_subopts(_) ->
    %% `qos' is what the SUBACK reports as the granted QoS: 200 is not a reason
    %% code the client could act on, and on MQTT 3.1.1 it would not serialize.
    ok = emqx_exhook_demo_svr:set_rewrite_fun(
        fun(TopicFilters) -> [Tf#{subopts => #{qos => 200}} || Tf <- TopicFilters] end
    ),
    ClientId = <<"exhook_sub_rw_qos">>,
    C = connect(ClientId),
    ?assertMatch({ok, _, [1]}, emqtt:subscribe(C, <<"t/1">>, qos1)),
    ?assertEqual([<<"t/1">>], subscribed_topics(ClientId)),
    ok = emqtt:stop(C).

t_subscribe_rewrite_rejects_beyond_caps(_) ->
    %% The hook runs after `check_sub_caps', so a rewritten filter is re-checked
    %% where the rewrite happens; otherwise it would defeat the zone's own limits.
    ok = emqx_exhook_demo_svr:set_rewrite_fun(
        fun(TopicFilters) -> [Tf#{name => <<"t/#">>} || Tf <- TopicFilters] end
    ),
    ClientId = <<"exhook_sub_rw_caps">>,
    C = connect(ClientId),
    ?assertMatch({ok, _, [0]}, emqtt:subscribe(C, <<"t/1">>, qos0)),
    ?assertEqual([<<"t/1">>], subscribed_topics(ClientId)),
    ok = emqtt:stop(C).

t_subscribe_rewrite_keeps_shared_subscription(_) ->
    %% Filters echoed back unchanged must stay exactly as they were: session
    %% subscriptions are keyed by the `#share{}' record, so one that came back
    %% flattened to its `$share/...' wire form would no longer match on unsubscribe.
    ok = emqx_exhook_demo_svr:set_rewrite_fun(fun(TopicFilters) -> TopicFilters end),
    ClientId = <<"exhook_sub_rw_share">>,
    C = connect(ClientId),
    ?assertMatch({ok, _, [0]}, emqtt:subscribe(C, <<"$share/g/t/1">>, qos0)),
    ?assertEqual([<<"$share/g/t/1">>], subscribed_topics(ClientId)),
    ?assertMatch({ok, _, [0]}, emqtt:unsubscribe(C, <<"$share/g/t/1">>)),
    ?assertEqual([], subscribed_topics(ClientId)),
    ok = emqtt:stop(C).

connect(ClientId) ->
    {ok, C} = emqtt:start_link([
        {host, "localhost"},
        {port, 1883},
        {clientid, ClientId},
        {proto_ver, v5}
    ]),
    {ok, _} = emqtt:connect(C),
    C.

subscribed_topics(ClientId) ->
    [emqx_topic:maybe_format_share(T) || {T, _SubOpts} <- emqx_broker:subscriptions(ClientId)].

t_format_props(_) ->
    ?assertMatch(
        [{on_provider_loaded, #{broker := _Broker}}],
        emqx_exhook_demo_svr:flush()
    ),

    %% connect
    ClientId = <<"exhook_format_props">>,
    {ok, C} = emqtt:start_link([
        {host, "localhost"},
        {port, 1883},
        {username, <<"gooduser">>},
        {clientid, ClientId},
        {proto_ver, v5},
        {properties, #{
            'User-Property' => [{<<"k1">>, <<"v1">>}],
            'Session-Expiry-Interval' => 100
        }}
    ]),
    %% assert the connect/connack props
    {ok, _} = emqtt:connect(C),
    Events1 = get_props_from_events(
        [on_client_connect, on_client_connack],
        emqx_exhook_demo_svr:flush()
    ),
    ?assertMatch(
        [
            %% assert the requested props
            {on_client_connect, #{
                props := [#{name := <<"Session-Expiry-Interval">>, value := <<"100">>}],
                user_props := [#{name := <<"k1">>, value := <<"v1">>}]
            }},
            %% broker will modify/add some props
            {on_client_connack, #{props := _, user_props := []}}
        ],
        Events1
    ),
    %% assert the subscribe props
    SubReqProps = #{
        'Subscription-Identifier' => 1,
        'User-Property' => [{<<"k2">>, <<"v2">>}]
    },
    {ok, _, _} = emqtt:subscribe(C, SubReqProps, <<"t/a">>, qos0),
    Events2 = get_props_from_events(
        [on_client_subscribe],
        emqx_exhook_demo_svr:flush()
    ),
    ?assertMatch(
        [
            {on_client_subscribe, #{
                props := [#{name := <<"Subscription-Identifier">>, value := <<"1">>}],
                user_props := [#{name := <<"k2">>, value := <<"v2">>}]
            }}
        ],
        Events2
    ),
    %% assert the unsubscribe props
    UnsubReqProps = #{
        'Subscription-Identifier' => 1,
        'User-Property' => [{<<"k3">>, <<"v3">>}]
    },
    {ok, _, _} = emqtt:unsubscribe(C, UnsubReqProps, <<"t/a">>),
    Events3 = get_props_from_events(
        [on_client_unsubscribe],
        emqx_exhook_demo_svr:flush()
    ),
    ?assertMatch(
        [
            {on_client_unsubscribe, #{
                props := [#{name := <<"Subscription-Identifier">>, value := <<"1">>}],
                user_props := [#{name := <<"k3">>, value := <<"v3">>}]
            }}
        ],
        Events3
    ),
    %% assert the publish props
    PubReqProps = #{
        'Message-Expiry-Interval' => 300,
        'User-Property' => [{<<"k4">>, <<"v4">>}]
    },
    ok = emqtt:publish(C, <<"t/a">>, PubReqProps, <<"payload">>, []),
    timer:sleep(500),
    Events4 = get_props_from_events(
        [on_message_publish],
        emqx_exhook_demo_svr:flush()
    ),
    ?assertMatch(
        [
            {on_message_publish, #{
                props := [#{name := <<"Message-Expiry-Interval">>, value := <<"300">>}],
                user_props := [#{name := <<"k4">>, value := <<"v4">>}]
            }}
        ],
        Events4
    ),
    timer:sleep(100),
    emqtt:stop(C),
    timer:sleep(100),
    ok.

%%--------------------------------------------------------------------
%% Cases Helpers
%%--------------------------------------------------------------------

assert_get_basic_usage_info(_Config) ->
    #{
        num_servers := NumServers,
        servers := Servers
    } = emqx_exhook:get_basic_usage_info(),
    ?assertEqual(1, NumServers),
    ?assertMatch([_], Servers),
    [#{driver := Driver, hooks := Hooks}] = Servers,
    ?assertEqual(grpc, Driver),
    ?assertEqual(
        [
            'client.authenticate',
            'client.authorize',
            'client.connack',
            'client.connect',
            'client.connected',
            'client.disconnected',
            'client.subscribe',
            'client.unsubscribe',
            'message.acked',
            'message.delivered',
            'message.dropped',
            'message.ingress',
            'message.publish',
            'session.created',
            'session.discarded',
            'session.resumed',
            'session.subscribed',
            'session.takenover',
            'session.terminated',
            'session.unsubscribed'
        ],
        lists:sort(Hooks)
    ).

%%--------------------------------------------------------------------
%% Utils
%%--------------------------------------------------------------------

list_pem_dir(Name) ->
    Dir = filename:join([emqx:mutable_certs_dir(), "exhook", Name]),
    file:list_dir(Dir).

data_file(Name) ->
    Dir = code:lib_dir(emqx_exhook),
    {ok, Bin} = file:read_file(filename:join([Dir, "test", "data", Name])),
    Bin.

cert_file(Name) ->
    data_file(filename:join(["certs", Name])).

shuffle(List) ->
    Sorted = lists:sort(lists:map(fun(L) -> {rand:uniform(), L} end, List)),
    lists:map(fun({_, L}) -> L end, Sorted).

get_props_from_events(Names, Events) ->
    lists:filtermap(
        fun({Name, Req}) ->
            case lists:member(Name, Names) of
                true ->
                    {true, {Name, maps:with([props, user_props], Req)}};
                false ->
                    false
            end
        end,
        Events
    ).
