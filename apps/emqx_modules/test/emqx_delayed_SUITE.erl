%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_delayed_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-record(delayed_message, {key, delayed, msg}).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_access_control.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------
-define(BASE_CONF, #{
    <<"dealyed">> => <<"true">>,
    <<"max_delayed_messages">> => <<"0">>
}).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            {emqx_modules, #{config => ?BASE_CONF}}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(t_load_case, Config) ->
    emqx_common_test_helpers:set_security_profile("legacy"),
    Config;
init_per_testcase(Case, Config) ->
    emqx_common_test_helpers:set_security_profile(test_security_profile(Case)),
    {atomic, ok} = mria:clear_table(emqx_delayed),
    ok = emqx_delayed:load(),
    Config.

end_per_testcase(_Case, _Config) ->
    persistent_term:erase({?MODULE, replay_authz_result}),
    persistent_term:erase({?MODULE, replay_authz_context}),
    persistent_term:erase({?MODULE, replay_authz_topic}),
    emqx_hooks:del('client.authorize', {?MODULE, replay_authz}),
    emqx_hooks:del('message.publish', {?MODULE, capture_publish}),
    emqx_common_test_helpers:clear_security_profile(),
    {atomic, ok} = mria:clear_table(emqx_delayed),
    ok = emqx_delayed:unload().

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_enable_disable_case(_) ->
    emqx_delayed:unload(),
    timer:sleep(100),
    Hooks = emqx_hooks:lookup('message.publish'),
    MFA = {emqx_delayed, on_message_publish, []},
    ?assertEqual(false, lists:keyfind(MFA, 2, Hooks)),

    ok = emqx_delayed:load(),
    Hooks1 = emqx_hooks:lookup('message.publish'),
    ?assertNotEqual(false, lists:keyfind(MFA, 2, Hooks1)),

    Ts0 = integer_to_binary(erlang:system_time(second) + 10),
    DelayedMsg0 = emqx_message:make(
        ?MODULE, 1, <<"$delayed/", Ts0/binary, "/publish">>, <<"delayed_abs">>
    ),
    _ = on_message_publish(DelayedMsg0),
    ?assertMatch(#{data := Datas} when Datas =/= [], emqx_delayed:list(#{})),

    emqx_delayed:unload(),
    timer:sleep(100),
    ?assertEqual(false, lists:keyfind(MFA, 2, Hooks)),
    ?assertMatch(#{data := []}, emqx_delayed:list(#{})),
    ok.

t_delayed_message(_) ->
    DelayedMsg = emqx_message:make(?MODULE, 1, <<"$delayed/1/publish">>, <<"delayed_m">>),
    ?assertMatch(
        {stop, #message{
            topic = <<"publish">>,
            headers = #{
                allow_publish := false,
                delayed := #{delay := {interval, 1}}
            }
        }},
        on_message_publish(DelayedMsg)
    ),

    Msg = emqx_message:make(?MODULE, 1, <<"no_delayed_msg">>, <<"no_delayed">>),
    ?assertEqual({ok, Msg}, on_message_publish(Msg)),

    [#delayed_message{msg = #message{payload = Payload}}] = ets:tab2list(emqx_delayed),
    ?assertEqual(<<"delayed_m">>, Payload),
    ct:sleep(2500),

    EmptyKey = mnesia:dirty_all_keys(emqx_delayed),
    ?assertEqual([], EmptyKey).

t_delayed_message_abs_time(_) ->
    Ts0 = integer_to_binary(erlang:system_time(second) + 1),
    DelayedMsg0 = emqx_message:make(
        ?MODULE, 1, <<"$delayed/", Ts0/binary, "/publish">>, <<"delayed_abs">>
    ),
    _ = on_message_publish(DelayedMsg0),

    ?assertMatch(
        [#delayed_message{msg = #message{payload = <<"delayed_abs">>}}],
        ets:tab2list(emqx_delayed)
    ),

    ct:sleep(2000),

    ?assertMatch(
        [],
        ets:tab2list(emqx_delayed)
    ),

    %% later than max allowed interval
    Ts1 = integer_to_binary(erlang:system_time(second) + 42949670 + 100),
    DelayedMsg1 = emqx_message:make(
        ?MODULE, 1, <<"$delayed/", Ts1/binary, "/publish">>, <<"delayed_abs">>
    ),

    ?assertEqual({error, invalid_topic_name}, on_message_publish(DelayedMsg1)).

t_list(_) ->
    Ts0 = integer_to_binary(erlang:system_time(second) + 1),
    DelayedMsg0 = emqx_message:make(
        ?MODULE, 1, <<"$delayed/", Ts0/binary, "/publish">>, <<"delayed_abs">>
    ),
    _ = on_message_publish(DelayedMsg0),

    ?assertMatch(
        #{data := [#{topic := <<"publish">>}]},
        emqx_delayed:list(#{})
    ).

t_max(_) ->
    emqx:update_config([delayed, max_delayed_messages], 1),

    DelayedMsg0 = emqx_message:make(?MODULE, 1, <<"$delayed/10/t0">>, <<"delayed0">>),
    DelayedMsg1 = emqx_message:make(?MODULE, 1, <<"$delayed/10/t1">>, <<"delayed1">>),
    _ = on_message_publish(DelayedMsg0),
    _ = on_message_publish(DelayedMsg1),

    ?assertMatch(
        #{data := [#{topic := <<"t0">>}]},
        emqx_delayed:list(#{})
    ).

t_cluster(_) ->
    DelayedMsg = emqx_message:make(?MODULE, 1, <<"$delayed/1/publish">>, <<"delayed">>),
    Id = emqx_message:id(DelayedMsg),
    _ = on_message_publish(DelayedMsg),

    ?assertMatch(
        {ok, _},
        emqx_delayed_proto_v2:get_delayed_message(node(), Id)
    ),

    %% The 'local' and the 'fake-remote' values should be the same,
    %% however there is a race condition, so we are just assert that they are both 'ok' tuples
    ?assertMatch({ok, _}, emqx_delayed:get_delayed_message(Id)),
    ?assertMatch({ok, _}, emqx_delayed_proto_v2:get_delayed_message(node(), Id)),

    ok = emqx_delayed_proto_v2:delete_delayed_message(node(), Id),

    ?assertMatch(
        {error, _},
        emqx_delayed:get_delayed_message(Id)
    ).

t_unknown_messages(_) ->
    OldPid = whereis(emqx_delayed),
    OldPid ! unknown,
    ok = gen_server:cast(OldPid, unknown),
    ?assertEqual(
        ignored,
        gen_server:call(OldPid, unknown)
    ).

t_get_basic_usage_info(_Config) ->
    emqx:update_config([delayed, max_delayed_messages], 10000),
    ?assertEqual(#{delayed_message_count => 0}, emqx_delayed:get_basic_usage_info()),
    lists:foreach(
        fun(N) ->
            Num = integer_to_binary(N),
            Message = emqx_message:make(<<"$delayed/", Num/binary, "/delayed">>, <<"payload">>),
            {stop, _} = on_message_publish(Message)
        end,
        lists:seq(1, 4)
    ),
    ?assertEqual(#{delayed_message_count => 4}, emqx_delayed:get_basic_usage_info()),
    ok.

t_delayed_precision(_) ->
    MaxSpan = 1250,
    FutureDiff = subscribe_proc(),
    DelayedMsg0 = emqx_message:make(
        ?MODULE, 1, <<"$delayed/1/delayed/test">>, <<"delayed/test">>
    ),
    _ = on_message_publish(DelayedMsg0),
    ?assert(FutureDiff() =< MaxSpan).

t_reauthorize_delayed_message(_) ->
    ClientInfo = #{
        zone => default,
        listener => 'tcp:default',
        protocol => mqtt,
        peerhost => {127, 0, 0, 1},
        peername => {{127, 0, 0, 1}, 1883},
        sockport => 1883,
        clientid => <<"reauthorize-client">>,
        username => <<"reauthorize-user">>,
        is_bridge => false,
        is_superuser => false,
        mountpoint => undefined
    },
    Topic = <<"reauthorize/target">>,
    DelayedTopic = <<"$delayed/1/", Topic/binary>>,
    Action = ?AUTHZ_PUBLISH(?QOS_1, true),
    Msg0 = emqx_message:make(
        maps:get(clientid, ClientInfo),
        ?QOS_1,
        DelayedTopic,
        <<"payload">>,
        #{retain => true},
        #{}
    ),
    {ok, Msg} = prepare_delayed_message(ClientInfo, Msg0),
    ok = emqx_hooks:put('client.authorize', {?MODULE, replay_authz, []}, ?HP_HIGHEST),
    persistent_term:put({?MODULE, replay_authz_result}, allow),
    AuthzContext = emqx_authz_context:make(ClientInfo),
    ?assertEqual(allow, emqx_access_control:authorize(AuthzContext, Action, Topic)),

    snabbkaffe:start_trace(),
    {ok, SubRef} = snabbkaffe:subscribe(
        ?match_event(#{
            ?snk_kind := ignore_delayed_message_publish,
            reason := "authorization denied"
        }),
        1,
        5000,
        0
    ),
    {stop, _} = on_message_publish(Msg),
    persistent_term:put({?MODULE, replay_authz_result}, deny),
    {ok, [_]} = snabbkaffe:receive_events(SubRef),
    snabbkaffe:stop(),
    ?assertEqual([], mnesia:dirty_all_keys(emqx_delayed)).

t_authorized_delayed_message(_) ->
    ClientInfo = #{
        zone => default,
        listener => 'tcp:default',
        protocol => mqtt,
        peerhost => {127, 0, 0, 1},
        peername => {{127, 0, 0, 1}, 1883},
        sockport => 1883,
        clientid => <<"authorized-client">>,
        username => <<"authorized-user">>,
        is_bridge => false,
        is_superuser => false,
        mountpoint => undefined
    },
    Topic = <<"authorized/target">>,
    Msg0 = emqx_message:make(
        maps:get(clientid, ClientInfo),
        ?QOS_1,
        <<"$delayed/1/", Topic/binary>>,
        <<"payload">>
    ),
    {ok, Msg} = prepare_delayed_message(ClientInfo, Msg0),
    ok = emqx_hooks:put('client.authorize', {?MODULE, replay_authz, []}, ?HP_HIGHEST),
    persistent_term:put({?MODULE, replay_authz_result}, allow),
    ok = emqx_broker:subscribe(Topic),
    try
        {stop, _} = on_message_publish(Msg),
        receive
            {deliver, Topic, Delivered} ->
                ?assertEqual(undefined, emqx_message:get_header(delayed, Delivered, undefined)),
                ?assertMatch(
                    #{peerport := 1883},
                    persistent_term:get({?MODULE, replay_authz_context})
                )
        after 5000 ->
            ct:fail(delayed_message_not_delivered)
        end
    after
        emqx_broker:unsubscribe(Topic)
    end.

t_reauthorize_legacy_delayed_message(_) ->
    Topic = <<"legacy/target">>,
    ok = emqx_broker:subscribe(Topic),
    Msg = emqx_message:make(
        <<"legacy-client">>, ?QOS_1, <<"$delayed/1/", Topic/binary>>, <<"payload">>
    ),
    try
        {stop, _} = on_message_publish(Msg),
        ok = emqx_hooks:put('client.authorize', {?MODULE, replay_authz, []}, ?HP_HIGHEST),
        persistent_term:put({?MODULE, replay_authz_result}, deny),
        receive
            {deliver, Topic, _Delivered} -> ok
        after 5000 ->
            ct:fail(legacy_delayed_message_not_delivered)
        end
    after
        emqx_broker:unsubscribe(Topic)
    end.

t_hardened_missing_authz_context(_) ->
    ClientId = <<"missing-context-client">>,
    Topic = <<"missing/context">>,
    snabbkaffe:start_trace(),
    {ok, SubRef} = snabbkaffe:subscribe(
        ?match_event(#{
            ?snk_kind := ignore_delayed_message_publish,
            reason := "authorization context missing"
        }),
        1,
        5000,
        0
    ),
    Msg0 = emqx_message:make(ClientId, ?QOS_1, Topic, <<"payload">>),
    Msg = emqx_message:set_header(
        delayed, #{delay => {interval, 1}}, Msg0
    ),
    {stop, _} = emqx_delayed:on_message_publish(Msg),
    {ok, [_]} = snabbkaffe:receive_events(SubRef),
    snabbkaffe:stop(),
    ?assertEqual([], mnesia:dirty_all_keys(emqx_delayed)).

t_hardened_mountpoint_replay(_) ->
    ClientInfo0 = test_clientinfo(<<"mountpoint-client">>),
    ClientInfo = ClientInfo0#{mountpoint => <<"mp/">>},
    Topic = <<"mountpoint/target">>,
    MountedTopic = <<"mp/", Topic/binary>>,
    Msg0 = emqx_message:make(
        maps:get(clientid, ClientInfo), ?QOS_1, <<"$delayed/1/", Topic/binary>>, <<"payload">>
    ),
    {ok, Prepared0} = prepare_delayed_message(ClientInfo, Msg0),
    Prepared = emqx_mountpoint:mount(maps:get(mountpoint, ClientInfo), Prepared0),
    OldIncludeMountpoint = emqx:get_config([authorization, include_mountpoint], false),
    ok = emqx_config:put([authorization, include_mountpoint], true),
    ok = emqx_hooks:put('client.authorize', {?MODULE, replay_authz, []}, ?HP_HIGHEST),
    persistent_term:put({?MODULE, replay_authz_result}, allow),
    ok = emqx_broker:subscribe(MountedTopic),
    try
        {stop, _} = emqx_delayed:on_message_publish(Prepared),
        receive
            {deliver, MountedTopic, _Delivered} ->
                ?assertEqual(
                    MountedTopic,
                    persistent_term:get({?MODULE, replay_authz_topic})
                )
        after 5000 ->
            ct:fail(delayed_message_not_delivered)
        end
    after
        ok = emqx_config:put([authorization, include_mountpoint], OldIncludeMountpoint),
        emqx_broker:unsubscribe(MountedTopic)
    end.

t_publish_hooks_run_on_replay(_) ->
    Topic = <<"hooks/target">>,
    Msg0 = emqx_message:make(
        <<"hooks-client">>, ?QOS_1, <<"$delayed/1/", Topic/binary>>, <<"payload">>
    ),
    {ok, Prepared} = prepare_delayed_message(test_clientinfo(<<"hooks-client">>), Msg0),
    ok = emqx_hooks:put(
        'message.publish', {?MODULE, capture_publish, [self()]}, ?HP_RULE_ENGINE
    ),
    _ = emqx_broker:publish(Prepared),
    receive
        {publish_hook_called, _} -> ct:fail(publish_hook_called_while_scheduling)
    after 200 ->
        ok
    end,
    receive
        {publish_hook_called, #message{topic = Topic}} -> ok
    after 5000 ->
        ct:fail(publish_hook_not_called_on_replay)
    end,
    receive
        {publish_hook_called, _} -> ct:fail(publish_hook_called_more_than_once)
    after 200 ->
        ok
    end.

t_delayed_will(_) ->
    ClientInfo = test_clientinfo(<<"will-client">>),
    Topic = <<"will/target">>,
    Will0 = emqx_message:make(
        maps:get(clientid, ClientInfo), ?QOS_1, <<"$delayed/1/", Topic/binary>>, <<"will">>
    ),
    ok = emqx_hooks:put('client.authorize', {?MODULE, replay_authz, []}, ?HP_HIGHEST),
    persistent_term:put({?MODULE, replay_authz_result}, allow),
    {ok, Prepared} = emqx_channel:prepare_will_message_for_publishing(ClientInfo, Will0),
    ?assertMatch(
        #message{
            topic = Topic,
            headers = #{
                delayed := #{
                    delay := {interval, 1},
                    authz_context := #{}
                }
            }
        },
        Prepared
    ),
    ok = emqx_broker:subscribe(Topic),
    try
        _ = emqx_broker:publish(Prepared),
        receive
            {deliver, Topic, #message{payload = <<"will">>}} -> ok
        after 5000 ->
            ct:fail(delayed_will_not_delivered)
        end
    after
        emqx_broker:unsubscribe(Topic)
    end.

t_banned_delayed(_) ->
    emqx:update_config([delayed, max_delayed_messages], 10000),
    ClientId1 = <<"bc1">>,
    ClientId2 = <<"bc2">>,
    ClientId3 = <<"bc3">>,

    Now = erlang:system_time(second),

    Who = emqx_banned:who(clientid, ClientId2),
    emqx_banned:create(#{
        who => Who,
        by => <<"test">>,
        reason => <<"test">>,
        at => Now,
        until => Now + 120
    }),
    WhoRE = emqx_banned:who(clientid_re, <<"c3">>),
    emqx_banned:create(#{
        who => WhoRE,
        by => <<"test">>,
        reason => <<"test">>,
        at => Now,
        until => Now + 120
    }),

    snabbkaffe:start_trace(),
    {ok, SubRef} =
        snabbkaffe:subscribe(
            ?match_event(#{?snk_kind := ignore_delayed_message_publish}),
            _NEvents = 4,
            _Timeout = 10000,
            0
        ),

    lists:foreach(
        fun(ClientId) ->
            Msg = emqx_message:make(ClientId, <<"$delayed/1/bc">>, <<"payload">>),
            on_message_publish(Msg)
        end,
        [ClientId1, ClientId1, ClientId1, ClientId2, ClientId2, ClientId3, ClientId3]
    ),

    {ok, Trace} = snabbkaffe:receive_events(SubRef),
    snabbkaffe:stop(),
    emqx_banned:delete(Who),
    emqx_banned:delete(WhoRE),
    mnesia:clear_table(emqx_delayed),

    ?assertEqual(4, length(?of_kind(ignore_delayed_message_publish, Trace))).

subscribe_proc() ->
    Self = self(),
    Ref = erlang:make_ref(),
    erlang:spawn(fun() ->
        Topic = <<"delayed/+">>,
        emqx_broker:subscribe(Topic),
        Self !
            {Ref,
                receive
                    {deliver, Topic, Msg} ->
                        erlang:system_time(milli_seconds) - Msg#message.timestamp
                after 2000 ->
                    2000
                end},
        emqx_broker:unsubscribe(Topic)
    end),
    fun() ->
        receive
            {Ref, Diff} ->
                Diff
        after 2000 ->
            2000
        end
    end.

t_delayed_load_unload(_Config) ->
    Conf = emqx:get_raw_config([delayed]),
    Conf1 = Conf#{<<"max_delayed_messages">> => 1234},
    ?assertMatch({ok, _}, emqx:update_config([delayed], Conf1#{<<"enable">> := true})),
    ?assert(is_hooks_exist()),
    ?assertEqual(1234, emqx:get_config([delayed, max_delayed_messages])),
    ?assertMatch({ok, _}, emqx:update_config([delayed], Conf1#{<<"enable">> := false})),
    ?assertNot(is_hooks_exist()),
    ?assertMatch({ok, _}, emqx:update_config([delayed], Conf)),
    ok.

is_hooks_exist() ->
    PublishHooks = emqx_hooks:lookup('message.publish'),
    IngressHooks = emqx_hooks:lookup('message.ingress'),
    false =/= lists:keyfind({emqx_delayed, on_message_publish, []}, 2, PublishHooks) andalso
        false =/=
            lists:keyfind(
                {emqx_delayed, on_message_ingress, []}, 2, IngressHooks
            ).

on_message_publish(Msg) ->
    ClientInfo = test_clientinfo(emqx_message:from(Msg)),
    case prepare_delayed_message(ClientInfo, Msg) of
        {ok, PreparedMsg} -> emqx_delayed:on_message_publish(PreparedMsg);
        {error, _} = Error -> Error
    end.

prepare_delayed_message(ClientInfo, Msg) ->
    case emqx_message_ingress:authorize(ClientInfo, Msg) of
        {allow, PreparedMsg} ->
            {ok, PreparedMsg};
        deny ->
            {error, deny};
        {error, _} = Error ->
            Error
    end.

test_clientinfo(ClientId) ->
    #{
        zone => default,
        listener => 'tcp:default',
        protocol => mqtt,
        peerhost => {127, 0, 0, 1},
        peername => {{127, 0, 0, 1}, 1883},
        sockport => 1883,
        clientid => ClientId,
        username => <<"username">>,
        is_bridge => false,
        is_superuser => false,
        mountpoint => undefined
    }.

test_security_profile(Case) ->
    case
        lists:member(Case, [
            t_reauthorize_delayed_message,
            t_authorized_delayed_message,
            t_hardened_missing_authz_context,
            t_hardened_mountpoint_replay,
            t_delayed_will
        ])
    of
        true -> "hardened";
        false -> "legacy"
    end.

replay_authz(ClientInfo, _Action, _Topic, _Default) ->
    persistent_term:put({?MODULE, replay_authz_context}, ClientInfo),
    persistent_term:put({?MODULE, replay_authz_topic}, _Topic),
    Result = persistent_term:get({?MODULE, replay_authz_result}),
    {stop, #{result => Result, from => test}}.

capture_publish(Msg, TestPid) ->
    TestPid ! {publish_hook_called, Msg},
    {ok, Msg}.
