%%--------------------------------------------------------------------
%% Copyright (c) 2018-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_hooks_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_testcase(_TestCase, Config) ->
    {ok, _} = emqx_hook_manager:start_link(),
    TestHookpoints = [
        test_hook,
        emqx_hook,
        foldl_hook,
        foldl_hook2,
        foreach_hook,
        foreach_stop_hook,
        foreach_filter1_hook,
        foldl_filter2_hook,
        foldl_strict_hook,
        foldl_crash_hook,
        foldl_strict_crash_hook,
        foldl_strict_context_hook
    ],
    ok = emqx_hookpoints:register_hookpoints(
        maps:merge(emqx_hookpoints:default_hookpoints(), maps:from_keys(TestHookpoints, valid))
    ),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok = emqx_hookpoints:register_hookpoints(),
    catch emqx_hook_manager:stop().

t_add_hook_order(_) ->
    ?assert(
        proper:quickcheck(
            add_hook_order_prop(),
            [
                {numtests, 1000}
            ]
        )
    ).

add_hook_order_prop() ->
    %% Note: order is inversed, since higher prio hooks run first:
    Comparator = fun({Prio1, M1, F1}, {Prio2, M2, F2}) ->
        Prio1 > Prio2 orelse
            (Prio1 =:= Prio2 andalso {M1, F1} =< {M2, F2})
    end,
    ?FORALL(
        Hooks,
        hooks(),
        try
            _ = emqx_hook_manager:start_link(),
            ok = emqx_hookpoints:register_hookpoints([prop_hook]),
            [ok = emqx_hooks:add(prop_hook, {M, F, []}, Prio) || {Prio, M, F} <- Hooks],
            Callbacks = emqx_hooks:lookup(prop_hook),
            Order = [{Prio, M, F} || {callback, {M, F, _}, _Filter, Prio} <- Callbacks],
            ?assertEqual(
                lists:sort(Comparator, Hooks),
                Order
            ),
            true
        after
            emqx_hook_manager:stop()
        end
    ).

hooks() ->
    ?SUCHTHAT(
        L0,
        list({range(-1, 5), atom(), atom()}),
        begin
            %% Duplicate callbacks are ignored, so check that
            %% all callbacks are unique:
            L = [{M, F} || {_Prio, M, F} <- L0],
            length(lists:usort(L)) =:= length(L0)
        end
    ).

t_add_put_del_hook(_) ->
    ok = emqx_hooks:add(test_hook, {?MODULE, hook_fun1, []}, 0),
    ok = emqx_hooks:add(test_hook, {?MODULE, hook_fun2, []}, 0),
    ?assertEqual(
        {error, already_exists},
        emqx_hooks:add(test_hook, {?MODULE, hook_fun2, []}, 0)
    ),
    Callbacks0 = [
        {callback, {?MODULE, hook_fun1, []}, undefined, 0},
        {callback, {?MODULE, hook_fun2, []}, undefined, 0}
    ],
    ?assertEqual(Callbacks0, emqx_hooks:lookup(test_hook)),

    ok = emqx_hooks:put(test_hook, {?MODULE, hook_fun1, [test]}, 0),
    ok = emqx_hooks:put(test_hook, {?MODULE, hook_fun2, [test]}, 0),
    Callbacks1 = [
        {callback, {?MODULE, hook_fun1, [test]}, undefined, 0},
        {callback, {?MODULE, hook_fun2, [test]}, undefined, 0}
    ],
    ?assertEqual(Callbacks1, emqx_hooks:lookup(test_hook)),

    ok = emqx_hooks:del(test_hook, {?MODULE, hook_fun1}),
    ok = emqx_hooks:del(test_hook, {?MODULE, hook_fun2}),
    timer:sleep(200),
    ?assertEqual([], emqx_hooks:lookup(test_hook)),

    ok = emqx_hooks:add(emqx_hook, {?MODULE, hook_fun8, []}, 8),
    ok = emqx_hooks:add(emqx_hook, {?MODULE, hook_fun2, []}, 2),
    ok = emqx_hooks:add(emqx_hook, {?MODULE, hook_fun10, []}, 10),
    ok = emqx_hooks:add(emqx_hook, {?MODULE, hook_fun9, []}, 9),
    Callbacks2 = [
        {callback, {?MODULE, hook_fun10, []}, undefined, 10},
        {callback, {?MODULE, hook_fun9, []}, undefined, 9},
        {callback, {?MODULE, hook_fun8, []}, undefined, 8},
        {callback, {?MODULE, hook_fun2, []}, undefined, 2}
    ],
    ?assertEqual(Callbacks2, emqx_hooks:lookup(emqx_hook)),

    ok = emqx_hooks:put(emqx_hook, {?MODULE, hook_fun8, [test]}, 3),
    ok = emqx_hooks:put(emqx_hook, {?MODULE, hook_fun2, [test]}, 4),
    ok = emqx_hooks:put(emqx_hook, {?MODULE, hook_fun10, [test]}, 1),
    ok = emqx_hooks:put(emqx_hook, {?MODULE, hook_fun9, [test]}, 2),
    Callbacks3 = [
        {callback, {?MODULE, hook_fun2, [test]}, undefined, 4},
        {callback, {?MODULE, hook_fun8, [test]}, undefined, 3},
        {callback, {?MODULE, hook_fun9, [test]}, undefined, 2},
        {callback, {?MODULE, hook_fun10, [test]}, undefined, 1}
    ],
    ?assertEqual(Callbacks3, emqx_hooks:lookup(emqx_hook)),

    ok = emqx_hooks:del(emqx_hook, {?MODULE, hook_fun2, [test]}),
    ok = emqx_hooks:del(emqx_hook, {?MODULE, hook_fun8, [test]}),
    ok = emqx_hooks:del(emqx_hook, {?MODULE, hook_fun9, [test]}),
    ok = emqx_hooks:del(emqx_hook, {?MODULE, hook_fun10, [test]}),
    timer:sleep(200),
    ?assertEqual([], emqx_hooks:lookup(emqx_hook)).

t_run_hooks(_) ->
    ok = emqx_hooks:add(foldl_hook, {?MODULE, hook_fun3, [init]}, 0),
    ok = emqx_hooks:add(foldl_hook, {?MODULE, hook_fun4, [init]}, 0),
    ok = emqx_hooks:add(foldl_hook, {?MODULE, hook_fun5, [init]}, 0),
    [r5, r4] = emqx_hooks:run_fold(foldl_hook, [arg1, arg2], []),

    ?assertException(
        error,
        {invalid_hookpoint, unknown_hook},
        emqx_hooks:run_fold(unknown_hook, [], [])
    ),
    ?assertException(
        error,
        {invalid_hookpoint, unknown_hook},
        emqx_hooks:add(unknown_hook, {?MODULE, hook_fun5, [init]}, 0)
    ),

    ok = emqx_hooks:add(foldl_hook2, {?MODULE, hook_fun9, []}, 0),
    ok = emqx_hooks:add(foldl_hook2, {?MODULE, hook_fun10, []}, 0),
    %% Note: 10 is _less_ than 9 per lexicographic order
    [r10] = emqx_hooks:run_fold(foldl_hook2, [arg], []),

    ok = emqx_hooks:add(foreach_hook, {?MODULE, hook_fun6, [initArg]}, 0),
    {error, already_exists} = emqx_hooks:add(foreach_hook, {?MODULE, hook_fun6, [initArg]}, 0),
    ok = emqx_hooks:add(foreach_hook, {?MODULE, hook_fun7, [initArg]}, 0),
    ok = emqx_hooks:add(foreach_hook, {?MODULE, hook_fun8, [initArg]}, 0),
    ok = emqx_hooks:run(foreach_hook, [arg]),

    ok = emqx_hooks:add(
        foreach_filter1_hook, {?MODULE, hook_fun1, []}, 0, {?MODULE, hook_filter1, []}
    ),
    %% filter passed
    ?assertEqual(ok, emqx_hooks:run(foreach_filter1_hook, [arg])),
    %% filter failed
    ?assertEqual(ok, emqx_hooks:run(foreach_filter1_hook, [arg1])),

    ok = emqx_hooks:add(
        foldl_filter2_hook, {?MODULE, hook_fun2, []}, 0, {?MODULE, hook_filter2, [init_arg]}
    ),
    ok = emqx_hooks:add(
        foldl_filter2_hook, {?MODULE, hook_fun2_1, []}, 0, {?MODULE, hook_filter2_1, [init_arg]}
    ),
    ?assertEqual(3, emqx_hooks:run_fold(foldl_filter2_hook, [arg], 1)),
    ?assertEqual(2, emqx_hooks:run_fold(foldl_filter2_hook, [arg1], 1)).

t_run_stops_on_stop(_) ->
    _ = erlang:erase(hook_mark_run),
    ok = emqx_hooks:add(foreach_stop_hook, {?MODULE, hook_stop_run, []}, 1),
    ok = emqx_hooks:add(foreach_stop_hook, {?MODULE, hook_mark_run, []}, 0),
    ?assertEqual(ok, emqx_hooks:run(foreach_stop_hook, [arg])),
    ?assertEqual(undefined, erlang:get(hook_mark_run)).

t_run_fold_strict(_) ->
    ok = emqx_hooks:add(foldl_strict_hook, {?MODULE, hook_fun2, []}, 0),
    ok = emqx_hooks:add(foldl_strict_hook, {?MODULE, hook_fun2_1, []}, 0),
    ?assertEqual({ok, 3}, emqx_hooks:run_fold_strict(foldl_strict_hook, [arg], 1)),

    ok = emqx_hooks:put(foldl_strict_hook, {?MODULE, hook_stop_new_acc, []}, 1),
    ?assertEqual({ok, 11}, emqx_hooks:run_fold_strict(foldl_strict_hook, [arg], 1)),

    ok = emqx_hooks:add(foldl_strict_crash_hook, {?MODULE, hook_crash, []}, 1),
    ok = emqx_hooks:add(foldl_strict_crash_hook, {?MODULE, hook_fun2, []}, 0),
    ?assertEqual(
        {error, strict_fold_crashed},
        emqx_hooks:run_fold_strict(foldl_strict_crash_hook, [arg], 1)
    ).

t_run_fold_ignores_crashed_callbacks(_) ->
    ok = emqx_hooks:add(foldl_crash_hook, {?MODULE, hook_crash, []}, 1),
    ok = emqx_hooks:add(foldl_crash_hook, {?MODULE, hook_fun2, []}, 0),
    ?assertEqual(2, emqx_hooks:run_fold(foldl_crash_hook, [arg], 1)).

t_run_fold_strict_with_context(_) ->
    ok = emqx_hooks:add(
        foldl_strict_context_hook,
        {?MODULE, hook_context_fold, [foldl_strict_context_hook]},
        0
    ),
    ?assertEqual(
        {ok, [#{ctx => value}]},
        emqx_hooks:run_fold_strict(foldl_strict_context_hook, #{ctx => value}, [arg], [])
    ),
    ?assertEqual(undefined, emqx_hooks:context(foldl_strict_context_hook)).

t_named_notification(_) ->
    ok = emqx_hooks:add('client.check_authn_complete', {?MODULE, authn_complete, [self()]}, 0),
    ClientInfo = #{clientid => <<"client">>},
    Result = #{reason_code => success},
    ok = emqx_hookpoints:register_hookpoints([]),
    ?assertError(
        {invalid_hookpoint, 'client.check_authn_complete'},
        emqx_hooks:run('client.check_authn_complete', [ClientInfo, Result])
    ),
    ?assertEqual(ok, emqx_hooks:'client.check_authn_complete'(ClientInfo, Result)),
    receive
        {authn_complete, ClientInfo, Result} -> ok
    after 1000 ->
        ct:fail(missing_notification)
    end.

t_named_folds(_) ->
    ok = emqx_hooks:add('client.ping', {?MODULE, client_ping, []}, 0),
    ?assertEqual(
        ping_seen,
        emqx_hooks:'client.ping'(#{clientid => <<"client">>}, #{}, ok)
    ),
    ok = emqx_hooks:add('api_actor.pre_create', {?MODULE, actor_pre_create, []}, 0),
    ?assertEqual(
        {error, unknown_namespace},
        emqx_hooks:'api_actor.pre_create'(#{namespace => <<"missing">>}, ok)
    ),
    ok = emqx_hooks:add('namespace.resource_pre_create', {?MODULE, resource_pre_create, []}, 0),
    ?assertEqual(
        #{exists => true},
        emqx_hooks:'namespace.resource_pre_create'(#{namespace => <<"known">>}, #{exists => false})
    ),
    Message = emqx_message:make(<<"client">>, <<"topic">>, <<"payload">>),
    ?assertEqual(Message, emqx_hooks:'message.publish'(Message)),
    ok = emqx_hooks:add('message.publish', {?MODULE, publish_many, []}, 0),
    ?assertEqual([Message, Message], emqx_hooks:'message.publish'(Message)).

t_named_strict_folds(_) ->
    Message = emqx_message:make(<<"client">>, <<"topic">>, <<"payload">>),
    ?assertEqual({ok, Message}, emqx_hooks:'message.ingress'(#{}, Message)),
    ok = emqx_hooks:add('message.ingress', {?MODULE, ingress_reject, []}, 0),
    ?assertEqual({ok, {error, rejected}}, emqx_hooks:'message.ingress'(#{}, Message)),
    ok = emqx_hooks:add('message.ingress', {?MODULE, hook_crash, []}, 1),
    ?assertEqual({error, strict_fold_crashed}, emqx_hooks:'message.ingress'(#{}, Message)),
    ?assertEqual({ok, ignore}, emqx_hooks:'client.authenticate_strict'(#{}, ignore)),
    ok = emqx_hooks:add('client.authenticate', {?MODULE, hook_crash, []}, 0),
    ?assertEqual(ignore, emqx_hooks:'client.authenticate'(#{}, ignore)),
    ?assertEqual(
        {error, strict_fold_crashed},
        emqx_hooks:'client.authenticate_strict'(#{}, ignore)
    ),
    Default = #{result => deny, from => default},
    ?assertEqual(
        {ok, Default}, emqx_hooks:'client.authorize_strict'(#{}, publish, <<"t">>, Default)
    ),
    ok = emqx_hooks:add('client.authorize', {?MODULE, authorize_crash, []}, 0),
    ?assertEqual(
        Default, emqx_hooks:'client.authorize'(#{}, publish, <<"t">>, Default)
    ),
    ?assertEqual(
        {error, strict_fold_crashed},
        emqx_hooks:'client.authorize_strict'(#{}, publish, <<"t">>, Default)
    ).

t_named_context(_) ->
    ok = emqx_hooks:add('client.timeout', {?MODULE, timeout_context, []}, 0),
    Context = #{chan_info_fn => fun(clientid) -> <<"client">> end},
    TRef = make_ref(),
    ?assertEqual([{event, updated}], emqx_hooks:'client.timeout'(Context, TRef, timeout, [])),
    ?assertEqual(undefined, emqx_hooks:context('client.timeout')),
    ?assertEqual([], emqx_hooks:'client.timeout'(TRef, timeout, [])).

t_uncovered_func(_) ->
    Pid = erlang:whereis(emqx_hook_manager),
    gen_server:call(Pid, test),
    gen_server:cast(Pid, test),
    Pid ! test,
    ok = emqx_hook_manager:stop().

%%--------------------------------------------------------------------
%% Hook fun
%%--------------------------------------------------------------------

client_ping(#{clientid := <<"client">>}, #{}, ok) ->
    {ok, ping_seen}.

authn_complete(ClientInfo, Result, Pid) ->
    Pid ! {authn_complete, ClientInfo, Result}.

actor_pre_create(#{namespace := <<"missing">>}, ok) ->
    {stop, {error, unknown_namespace}}.

resource_pre_create(#{namespace := <<"known">>}, Result) ->
    {stop, Result#{exists := true}}.

publish_many(Message) ->
    {stop, [Message, Message]}.

ingress_reject(_Context, _Message) ->
    {stop, {error, rejected}}.

authorize_crash(_Context, _Action, _Topic, _Result) ->
    error(strict_fold_crashed).

timeout_context(TRef, timeout, Replies) when is_reference(TRef) ->
    case emqx_hooks:context('client.timeout') of
        #{chan_info_fn := InfoFn} ->
            ?assertEqual(<<"client">>, InfoFn(clientid)),
            {ok, [{event, updated} | Replies]};
        undefined ->
            {ok, Replies}
    end.

hook_fun1(arg) -> ok;
hook_fun1(_) -> error.

hook_fun2(arg) -> ok;
hook_fun2(_) -> error.

hook_fun2(_, Acc) -> {ok, Acc + 1}.
hook_fun2_1(_, Acc) -> {ok, Acc + 1}.

hook_fun3(arg1, arg2, _Acc, init) -> ok.
hook_fun4(arg1, arg2, Acc, init) -> {ok, [r4 | Acc]}.
hook_fun5(arg1, arg2, Acc, init) -> {ok, [r5 | Acc]}.

hook_fun6(arg, initArg) -> ok.
hook_fun7(arg, initArg) -> ok.
hook_fun8(arg, initArg) -> ok.

hook_fun9(arg, Acc) -> {stop, [r9 | Acc]}.
hook_fun10(arg, Acc) -> {stop, [r10 | Acc]}.

hook_stop_run(_Arg) -> stop.

hook_mark_run(_Arg) ->
    _ = erlang:put(hook_mark_run, true),
    ok.

hook_stop_new_acc(_Arg, Acc) -> {stop, Acc + 10}.

hook_crash(_Arg, _Acc) -> erlang:error(strict_fold_crashed).

hook_context_fold(arg, Acc, HookPoint) -> {ok, [emqx_hooks:context(HookPoint) | Acc]}.

hook_filter1(arg) -> true;
hook_filter1(_) -> false.

hook_filter2(arg, _Acc, init_arg) -> true;
hook_filter2(_, _Acc, _IntArg) -> false.

hook_filter2_1(arg, _Acc, init_arg) -> true;
hook_filter2_1(arg1, _Acc, init_arg) -> true;
hook_filter2_1(_, _Acc, _IntArg) -> false.
