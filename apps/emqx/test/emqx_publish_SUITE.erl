%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_publish_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [emqx], #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    ok = emqx_hookpoints:register_hookpoints(),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(proplists:get_value(apps, Config)).

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, _Config) ->
    emqx_hooks:del('message.ingress', {?MODULE, override_publish}),
    emqx_hooks:del('message.ingress', {?MODULE, reject_publish}),
    emqx_hooks:del('message.ingress', {?MODULE, crash_publish}),
    emqx_hooks:del('message.ingress', {?MODULE, override_publish_action}),
    emqx_hooks:del('client.authorize', {?MODULE, capture_authorize}),
    ok.

t_no_overrides(_) ->
    Msg = message(<<"topic">>),
    ?assertEqual(
        {allow, Msg},
        emqx_message_ingress:authorize(clientinfo(), Msg)
    ).

t_overrides(_) ->
    emqx_hooks:put(
        'message.ingress', {?MODULE, override_publish, []}, ?HP_HIGHEST
    ),
    {allow, Msg} = emqx_message_ingress:authorize(clientinfo(), message(<<"source">>)),
    ?assertEqual(<<"target">>, Msg#message.topic),
    ?assertEqual(value, emqx_message:get_header(internal, Msg)).

t_reject(_) ->
    emqx_hooks:put('message.ingress', {?MODULE, reject_publish, []}, ?HP_HIGHEST),
    ?assertEqual(
        {error, invalid_topic_name},
        emqx_message_ingress:authorize(clientinfo(), message(<<"source">>))
    ).

t_crash(_) ->
    emqx_hooks:put('message.ingress', {?MODULE, crash_publish, []}, ?HP_HIGHEST),
    ?assertMatch(
        {error, {message_ingress_hook_failed, _}},
        emqx_message_ingress:authorize(clientinfo(), message(<<"source">>))
    ).

t_action_uses_folded_message(_) ->
    emqx_hooks:put(
        'message.ingress',
        {?MODULE, override_publish_action, []},
        ?HP_HIGHEST
    ),
    emqx_hooks:put(
        'client.authorize',
        {?MODULE, capture_authorize, [self()]},
        ?HP_HIGHEST
    ),
    {allow, Msg} = emqx_message_ingress:authorize(clientinfo(), message(<<"source">>)),
    ?assertEqual(2, Msg#message.qos),
    ?assertEqual(true, emqx_message:get_flag(retain, Msg)),
    receive
        {authorize, Action} ->
            ?assertEqual(#{action_type => publish, qos => 2, retain => true}, Action)
    after 1000 ->
        ct:fail(authorize_hook_not_called)
    end.

t_finalize_mounts_message(_) ->
    Msg = message(<<"topic">>),
    MountedMsg = emqx_message_ingress:finalize(
        #{mountpoint => <<"gateway/client/">>}, Msg
    ),
    ?assertEqual(<<"gateway/client/topic">>, MountedMsg#message.topic).

override_publish(_AuthzContext, Msg) ->
    {ok, emqx_message:set_header(internal, value, Msg#message{topic = <<"target">>})}.

override_publish_action(_AuthzContext, Msg) ->
    {ok, emqx_message:set_flag(retain, true, Msg#message{qos = 2})}.

capture_authorize(_AuthzContext, Action, _Topic, _DefaultResult, TestPid) ->
    TestPid ! {authorize, Action},
    {stop, #{result => allow, from => test}}.

reject_publish(_AuthzContext, _Msg) ->
    {stop, {error, invalid_topic_name}}.

crash_publish(_AuthzContext, _Msg) ->
    error(expected_crash).

message(Topic) ->
    emqx_message:make(<<"client">>, 1, Topic, <<"payload">>).

clientinfo() ->
    #{
        zone => default,
        protocol => mqtt,
        peerhost => {127, 0, 0, 1},
        sockport => 1883,
        clientid => <<"client">>,
        is_bridge => false,
        is_superuser => false,
        mountpoint => undefined
    }.
