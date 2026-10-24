%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_sys_msg_dispatch_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include("emqx_hooks.hrl").

-define(HOOK, 'message.publish').

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_testcase(_TestCase, Config) ->
    {ok, _} = emqx_hook_manager:start_link(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    catch emqx_hook_manager:stop().

%% A `$` topic goes to the handler of its prefix. Other topics pass through.
t_dispatch_by_prefix(_) ->
    ok = reg(<<"$A/">>, a),
    ok = reg(<<"$B/x/">>, b),
    ?assertEqual(a, publish(<<"$A/1">>)),
    ?assertEqual(b, publish(<<"$B/x/1">>)),
    assert_pass_through(<<"$B/y">>),
    assert_pass_through(<<"$C/1">>),
    assert_pass_through(<<"t/$A/1">>),
    assert_pass_through(<<"A/1">>).

%% The callback is in the hook list only while a handler is registered,
%% and there is never more than one.
t_hook_follows_handlers(_) ->
    ?assertEqual([], dispatch_priorities()),
    ok = reg(<<"$A/">>, a),
    ?assertEqual([?HP_SYS_MSGS], dispatch_priorities()),
    ok = reg(<<"$B/">>, b),
    ?assertEqual([?HP_SYS_MSGS], dispatch_priorities()),
    ok = emqx_sys_msg_dispatch:unregister(<<"$A/">>),
    ?assertEqual([?HP_SYS_MSGS], dispatch_priorities()),
    assert_pass_through(<<"$A/1">>),
    ?assertEqual(b, publish(<<"$B/1">>)),
    ok = emqx_sys_msg_dispatch:unregister(<<"$B/">>),
    ?assertEqual([], dispatch_priorities()),
    ?assertEqual([], emqx_sys_msg_dispatch:handlers()),
    ok = emqx_sys_msg_dispatch:unregister(<<"$B/">>).

%% Registering a prefix again replaces its handler and keeps the callback
%% order against the other callbacks.
t_register_again_keeps_order(_) ->
    ok = emqx_hooks:add(?HOOK, {?MODULE, h_noop, []}, ?HP_SYS_MSGS + 1),
    ok = emqx_hooks:add(?HOOK, {?MODULE, h_noop2, []}, ?HP_SYS_MSGS - 1),
    ok = reg(<<"$A/">>, a),
    Order = hook_order(),
    ok = reg(<<"$A/">>, a2),
    ?assertEqual(Order, hook_order()),
    ?assertEqual(
        [{?MODULE, h_noop}, {emqx_sys_msg_dispatch, on_message_publish}, {?MODULE, h_noop2}],
        Order
    ),
    ?assertEqual(a2, publish(<<"$A/1">>)).

%% Overlapping prefixes are refused, so a topic never matches two handlers.
t_prefix_conflict(_) ->
    ok = reg(<<"$A/x/">>, a),
    ?assertEqual({error, {prefix_conflict, <<"$A/x/">>}}, reg(<<"$A/">>, b)),
    ?assertEqual({error, {prefix_conflict, <<"$A/x/">>}}, reg(<<"$A/x/y/">>, b)),
    ?assertEqual(a, publish(<<"$A/x/y/1">>)),
    ?assertEqual([<<"$A/x/">>], [P || {P, _} <- emqx_sys_msg_dispatch:handlers()]).

%% A prefix must start with `$`.
t_prefix_must_be_sys(_) ->
    ?assertError(function_clause, reg(<<"A/">>, a)),
    ?assertEqual([], dispatch_priorities()).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

reg(Prefix, Tag) ->
    emqx_sys_msg_dispatch:register(Prefix, {?MODULE, h_tag, [Tag]}).

publish(Topic) ->
    emqx_hooks:'message.publish'(emqx_message:make(<<"c">>, Topic, <<"p">>)).

assert_pass_through(Topic) ->
    Msg = emqx_message:make(<<"c">>, Topic, <<"p">>),
    ?assertEqual(Msg, emqx_hooks:'message.publish'(Msg)).

dispatch_priorities() ->
    [
        emqx_hooks:callback_priority(CB)
     || CB <- emqx_hooks:lookup(?HOOK),
        element(1, emqx_hooks:callback_action(CB)) =:= emqx_sys_msg_dispatch
    ].

hook_order() ->
    [
        begin
            {M, F, _} = emqx_hooks:callback_action(CB),
            {M, F}
        end
     || CB <- emqx_hooks:lookup(?HOOK)
    ].

h_tag(_Msg, Tag) -> {stop, Tag}.

h_noop(_Msg) -> ok.

h_noop2(_Msg) -> ok.
