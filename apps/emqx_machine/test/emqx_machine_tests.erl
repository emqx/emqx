%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_machine_tests).

-include_lib("eunit/include/eunit.hrl").

sorted_reboot_apps_test_() ->
    Apps1 = [
        {1, [2, 3, 4]},
        {2, [3, 4]}
    ],
    Apps2 = [
        {1, [2, 3, 4]},
        {2, [3, 4]},
        {5, [4, 3, 2, 1, 1]}
    ],
    [
        fun() -> check_order(Apps1) end,
        fun() -> check_order(Apps2) end
    ].

sorted_reboot_apps_cycle_test() ->
    Apps = [{1, [2]}, {2, [1, 3]}],
    ?assertError(
        {circular_application_dependency, [[1, 2, 1], [2, 1, 2]]},
        check_order(Apps)
    ).

check_order(Apps) ->
    AllApps = lists:usort(lists:append([[A | Deps] || {A, Deps} <- Apps])),
    [emqx_conf | Sorted] = emqx_machine_boot:sorted_reboot_apps(Apps),
    case length(AllApps) =:= length(Sorted) of
        true -> ok;
        false -> error({AllApps, Sorted})
    end,
    {_, SortedWithIndex} =
        lists:foldr(fun(A, {I, Acc}) -> {I + 1, [{A, I} | Acc]} end, {1, []}, Sorted),
    do_check_order(Apps, SortedWithIndex).

do_check_order([], _) ->
    ok;
do_check_order([{A, Deps} | Rest], Sorted) ->
    case lists:filter(fun(Dep) -> is_sorted_before(Dep, A, Sorted) end, Deps) of
        [] -> do_check_order(Rest, Sorted);
        Bad -> throw({A, Bad})
    end.

is_sorted_before(A, B, Sorted) ->
    {A, IndexA} = lists:keyfind(A, 1, Sorted),
    {B, IndexB} = lists:keyfind(B, 1, Sorted),
    IndexA < IndexB.
