%%--------------------------------------------------------------------
%% Copyright (c) 2018-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_hooks_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_testcase(_, Config) ->
    {ok, _} = emqx_hooks:start_link(),
    ok = emqx_hookpoints:register_hookpoints(
        [
            test_hook,
            emqx_hook,
            foldl_hook,
            foldl_hook2,
            foreach_hook,
            foreach_filter1_hook,
            foldl_filter2_hook
        ]
    ),
    Config.

end_per_testcase(_) ->
    ok = emqx_hookpoints:register_hookpoints(),
    catch emqx_hooks:stop().

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
            _ = emqx_hooks:start_link(),
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
            emqx_hooks:stop()
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

t_uncovered_func(_) ->
    Pid = erlang:whereis(emqx_hooks),
    gen_server:call(Pid, test),
    gen_server:cast(Pid, test),
    Pid ! test,
    ok = emqx_hooks:stop().

%%--------------------------------------------------------------------
%% Hook fun
%%--------------------------------------------------------------------

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

hook_filter1(arg) -> true;
hook_filter1(_) -> false.

hook_filter2(arg, _Acc, init_arg) -> true;
hook_filter2(_, _Acc, _IntArg) -> false.

hook_filter2_1(arg, _Acc, init_arg) -> true;
hook_filter2_1(arg1, _Acc, init_arg) -> true;
hook_filter2_1(_, _Acc, _IntArg) -> false.
