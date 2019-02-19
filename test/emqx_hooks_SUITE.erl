%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_hooks_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    [add_delete_hook, run_hooks].

add_delete_hook(_) ->
    {ok, _} = emqx_hooks:start_link(),
    ok = emqx:hook(test_hook, fun ?MODULE:hook_fun1/1, []),
    ok = emqx:hook(test_hook, fun ?MODULE:hook_fun2/1, []),
    ?assertEqual({error, already_exists},
                 emqx:hook(test_hook, fun ?MODULE:hook_fun2/1, [])),
    Callbacks = [{callback, {fun ?MODULE:hook_fun1/1, []}, undefined, 0},
                 {callback, {fun ?MODULE:hook_fun2/1, []}, undefined, 0}],
    ?assertEqual(Callbacks, emqx_hooks:lookup(test_hook)),
    ok = emqx:unhook(test_hook, fun ?MODULE:hook_fun1/1),
    ok = emqx:unhook(test_hook, fun ?MODULE:hook_fun2/1),
    timer:sleep(1000),
    ?assertEqual([], emqx_hooks:lookup(test_hook)),

    ok = emqx:hook(emqx_hook, {?MODULE, hook_fun2, []}, 8),
    ok = emqx:hook(emqx_hook, {?MODULE, hook_fun1, []}, 9),
    Callbacks2 = [{callback, {?MODULE, hook_fun1, []}, undefined, 9},
                  {callback, {?MODULE, hook_fun2, []}, undefined, 8}],
    ?assertEqual(Callbacks2, emqx_hooks:lookup(emqx_hook)),
    ok = emqx:unhook(emqx_hook, {?MODULE, hook_fun1, []}),
    ok = emqx:unhook(emqx_hook, {?MODULE, hook_fun2}),
    timer:sleep(1000),
    ?assertEqual([], emqx_hooks:lookup(emqx_hook)),
    ok = emqx_hooks:stop().

run_hooks(_) ->
    {ok, _} = emqx_hooks:start_link(),
    ok = emqx:hook(foldl_hook, fun ?MODULE:hook_fun3/4, [init]),
    ok = emqx:hook(foldl_hook, {?MODULE, hook_fun3, [init]}),
    ok = emqx:hook(foldl_hook, fun ?MODULE:hook_fun4/4, [init]),
    ok = emqx:hook(foldl_hook, fun ?MODULE:hook_fun5/4, [init]),
    {stop, [r3, r2]} = emqx:run_hooks(foldl_hook, [arg1, arg2], []),
    {ok, []} = emqx:run_hooks(unknown_hook, [], []),

    ok = emqx:hook(foreach_hook, fun ?MODULE:hook_fun6/2, [initArg]),
    {error, already_exists} = emqx:hook(foreach_hook, fun ?MODULE:hook_fun6/2, [initArg]),
    ok = emqx:hook(foreach_hook, fun ?MODULE:hook_fun7/2, [initArg]),
    ok = emqx:hook(foreach_hook, fun ?MODULE:hook_fun8/2, [initArg]),
    stop = emqx:run_hooks(foreach_hook, [arg]),

    ok = emqx:hook(foldl_hook2, fun ?MODULE:hook_fun9/2),
    ok = emqx:hook(foldl_hook2, {?MODULE, hook_fun10, []}),
    {stop, []} = emqx:run_hooks(foldl_hook2, [arg], []),

    ok = emqx:hook(filter1_hook, {?MODULE, hook_fun1, []}, {?MODULE, hook_filter1, []}, 0),
    ok = emqx:run_hooks(filter1_hook, [arg]),

    ok = emqx:hook(filter2_hook, {?MODULE, hook_fun2, []}, {?MODULE, hook_filter2, []}),
    {ok, []} = emqx:run_hooks(filter2_hook, [arg], []),

    ok = emqx_hooks:stop().

hook_fun1([]) -> ok.
hook_fun2([]) -> {ok, []}.

hook_fun3(arg1, arg2, _Acc, init) -> ok.
hook_fun4(arg1, arg2, Acc, init)  -> {ok, [r2 | Acc]}.
hook_fun5(arg1, arg2, Acc, init)  -> {stop, [r3 | Acc]}.

hook_fun6(arg, initArg) -> ok.
hook_fun7(arg, initArg) -> any.
hook_fun8(arg, initArg) -> stop.

hook_fun9(arg, _Acc)  -> any.
hook_fun10(arg, _Acc)  -> stop.

hook_filter1(arg) -> true.
hook_filter2(arg, _Acc) -> true.

