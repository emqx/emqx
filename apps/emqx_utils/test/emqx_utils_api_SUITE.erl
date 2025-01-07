%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_utils_api_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx_utils/include/emqx_utils_api.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(DUMMY, dummy_module).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx], #{work_dir => emqx_cth_suite:work_dir(Config)}),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(proplists:get_value(apps, Config)).

init_per_testcase(_Case, Config) ->
    meck:new(?DUMMY, [non_strict]),
    meck:expect(?DUMMY, expect_not_called, 1, fun(Node) -> throw({blow_this_up, Node}) end),
    meck:expect(?DUMMY, expect_success, 1, {ok, success}),
    meck:expect(?DUMMY, expect_error, 1, {error, error}),
    Config.

end_per_testcase(_Case, _Config) ->
    meck:unload(?DUMMY).

t_with_node(_) ->
    test_with(fun emqx_utils_api:with_node/2, [<<"all">>]).

t_with_node_or_cluster(_) ->
    test_with(fun emqx_utils_api:with_node_or_cluster/2, []),
    meck:reset(?DUMMY),
    ?assertEqual(
        ?OK(success),
        emqx_utils_api:with_node_or_cluster(
            <<"all">>,
            fun ?DUMMY:expect_success/1
        )
    ),
    ?assertMatch([{_, {?DUMMY, expect_success, [all]}, {ok, success}}], meck:history(?DUMMY)).

%% helpers
test_with(TestFun, ExtraBadNodes) ->
    % make sure this is an atom
    'unknownnode@unknownnohost',
    BadNodes =
        [
            <<"undefined">>,
            <<"this_should_not_be_an_atom">>,
            <<"unknownnode@unknownnohost">>
        ] ++ ExtraBadNodes,
    [ensure_not_found(TestFun(N, fun ?DUMMY:expect_not_called/1)) || N <- BadNodes],
    ensure_not_called(?DUMMY, expect_not_called),
    ensure_not_existing_atom(<<"this_should_not_be_an_atom">>),

    GoodNode = node(),

    ?assertEqual(
        ?OK(success),
        TestFun(GoodNode, fun ?DUMMY:expect_success/1)
    ),

    ?assertEqual(
        ?BAD_REQUEST(error),
        TestFun(GoodNode, fun ?DUMMY:expect_error/1)
    ),
    ok.

ensure_not_found(Result) ->
    ?assertMatch({404, _}, Result).

ensure_not_called(Mod, Fun) ->
    ?assert(not meck:called(Mod, Fun, '_')).

ensure_not_existing_atom(Bin) ->
    try binary_to_existing_atom(Bin) of
        _ -> throw(is_atom)
    catch
        error:badarg ->
            ok
    end.
