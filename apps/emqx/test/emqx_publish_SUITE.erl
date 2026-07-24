%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_publish_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
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
    emqx_hooks:del('client.publish_pre_authz', {?MODULE, override_publish}),
    emqx_hooks:del('client.publish_pre_authz', {?MODULE, reject_publish}),
    emqx_hooks:del('client.publish_pre_authz', {?MODULE, crash_publish}),
    ok.

t_no_overrides(_) ->
    Action = action(),
    ?assertEqual(
        {ok, #{action => Action, topic => <<"topic">>, headers => #{}}},
        emqx_publish:run_pre_authz_hook(packet, #{}, Action, <<"topic">>)
    ).

t_overrides(_) ->
    emqx_hooks:put(
        'client.publish_pre_authz', {?MODULE, override_publish, []}, ?HP_HIGHEST
    ),
    ?assertEqual(
        {ok, #{
            action => #{action_type => publish, qos => 1, retain => true},
            topic => <<"target">>,
            headers => #{internal => value}
        }},
        emqx_publish:run_pre_authz_hook(packet, #{}, action(), <<"source">>)
    ).

t_reject(_) ->
    emqx_hooks:put('client.publish_pre_authz', {?MODULE, reject_publish, []}, ?HP_HIGHEST),
    ?assertEqual(
        {error, invalid_topic_name},
        emqx_publish:run_pre_authz_hook(packet, #{}, action(), <<"source">>)
    ).

t_crash(_) ->
    emqx_hooks:put('client.publish_pre_authz', {?MODULE, crash_publish, []}, ?HP_HIGHEST),
    ?assertMatch(
        {error, {publish_pre_authz_hook_failed, _}},
        emqx_publish:run_pre_authz_hook(packet, #{}, action(), <<"source">>)
    ).

t_ignore_unsupported_override(_) ->
    emqx_hooks:put(
        'client.publish_pre_authz',
        {?MODULE, override_publish, [#{qos => 2}]},
        ?HP_HIGHEST
    ),
    ?assertEqual(
        {ok, #{
            action => #{action_type => publish, qos => 1, retain => true},
            topic => <<"target">>,
            headers => #{internal => value}
        }},
        emqx_publish:run_pre_authz_hook(packet, #{}, action(), <<"source">>)
    ).

t_accept_topic_override(_) ->
    emqx_hooks:put(
        'client.publish_pre_authz',
        {?MODULE, override_publish, [#{topic => <<"invalid/#">>}]},
        ?HP_HIGHEST
    ),
    ?assertEqual(
        {ok, #{
            action => #{action_type => publish, qos => 1, retain => true},
            topic => <<"invalid/#">>,
            headers => #{internal => value}
        }},
        emqx_publish:run_pre_authz_hook(packet, #{}, action(), <<"source">>)
    ).

override_publish(_Packet, _Context, {ok, Overrides}) ->
    override_publish(_Packet, _Context, {ok, Overrides}, #{}).

override_publish(_Packet, _Context, {ok, Overrides}, Extra) ->
    {ok,
        {ok,
            maps:merge(
                Overrides#{
                    topic => <<"target">>,
                    retain => true,
                    headers => #{internal => value}
                },
                Extra
            )}}.

reject_publish(_Packet, _Context, _Acc) ->
    {stop, {error, invalid_topic_name}}.

crash_publish(_Packet, _Context, _Acc) ->
    error(expected_crash).

action() ->
    #{action_type => publish, qos => 1, retain => false}.
