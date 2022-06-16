%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_resource_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include("emqx_resource.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-define(TEST_RESOURCE, emqx_test_resource).
-define(ID, <<"id">>).
-define(DEFAULT_RESOURCE_GROUP, <<"default">>).

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_testcase(_, Config) ->
    Config.

init_per_suite(Config) ->
    code:ensure_loaded(?TEST_RESOURCE),
    ok = emqx_common_test_helpers:start_apps([emqx_conf]),
    {ok, _} = application:ensure_all_started(emqx_resource),
    Config.

end_per_suite(_Config) ->
    ok = emqx_common_test_helpers:stop_apps([emqx_resource, emqx_conf]).

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

t_list_types(_) ->
    ?assert(lists:member(?TEST_RESOURCE, emqx_resource:list_types())).

t_check_config(_) ->
    {ok, #{}} = emqx_resource:check_config(?TEST_RESOURCE, bin_config()),
    {ok, #{}} = emqx_resource:check_config(?TEST_RESOURCE, config()),

    {error, _} = emqx_resource:check_config(?TEST_RESOURCE, <<"not a config">>),
    {error, _} = emqx_resource:check_config(?TEST_RESOURCE, #{invalid => config}).

t_create_remove(_) ->
    {error, _} = emqx_resource:check_and_create_local(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{unknown => test_resource}
    ),

    {ok, _} = emqx_resource:create(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource}
    ),

    {ok, _} = emqx_resource:recreate(
        ?ID,
        ?TEST_RESOURCE,
        #{name => test_resource},
        #{}
    ),
    #{pid := Pid} = emqx_resource:query(?ID, get_state),

    ?assert(is_process_alive(Pid)),

    ok = emqx_resource:remove(?ID),
    {error, _} = emqx_resource:remove(?ID),

    ?assertNot(is_process_alive(Pid)).

t_create_remove_local(_) ->
    {error, _} = emqx_resource:check_and_create_local(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{unknown => test_resource}
    ),

    {ok, _} = emqx_resource:create_local(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource}
    ),

    emqx_resource:recreate_local(
        ?ID,
        ?TEST_RESOURCE,
        #{name => test_resource},
        #{}
    ),
    #{pid := Pid} = emqx_resource:query(?ID, get_state),

    ?assert(is_process_alive(Pid)),

    emqx_resource:set_resource_status_connecting(?ID),

    emqx_resource:recreate_local(
        ?ID,
        ?TEST_RESOURCE,
        #{name => test_resource},
        #{}
    ),

    ok = emqx_resource:remove_local(?ID),
    {error, _} = emqx_resource:remove_local(?ID),

    ?assertMatch(
        {error, {emqx_resource, #{reason := not_found}}},
        emqx_resource:query(?ID, get_state)
    ),
    ?assertNot(is_process_alive(Pid)).

t_do_not_start_after_created(_) ->
    {ok, _} = emqx_resource:create_local(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource},
        #{start_after_created => false}
    ),
    %% the resource should remain `disconnected` after created
    timer:sleep(200),
    ?assertMatch(
        {error, {emqx_resource, #{reason := not_connected}}},
        emqx_resource:query(?ID, get_state)
    ),
    ?assertMatch(
        {ok, _, #{status := disconnected}},
        emqx_resource:get_instance(?ID)
    ),

    %% start the resource manually..
    ok = emqx_resource:start(?ID),
    #{pid := Pid} = emqx_resource:query(?ID, get_state),
    ?assert(is_process_alive(Pid)),

    %% restart the resource
    ok = emqx_resource:restart(?ID),
    ?assertNot(is_process_alive(Pid)),
    #{pid := Pid2} = emqx_resource:query(?ID, get_state),
    ?assert(is_process_alive(Pid2)),

    ok = emqx_resource:remove_local(?ID),

    ?assertNot(is_process_alive(Pid2)).

t_query(_) ->
    {ok, _} = emqx_resource:create_local(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource}
    ),

    Pid = self(),
    Success = fun() -> Pid ! success end,
    Failure = fun() -> Pid ! failure end,

    #{pid := _} = emqx_resource:query(?ID, get_state),
    #{pid := _} = emqx_resource:query(?ID, get_state, {[{Success, []}], [{Failure, []}]}),
    #{pid := _} = emqx_resource:query(?ID, get_state, undefined),
    #{pid := _} = emqx_resource:query(?ID, get_state_failed, undefined),

    receive
        Message -> ?assertEqual(success, Message)
    after 100 ->
        ?assert(false)
    end,

    ?assertMatch(
        {error, {emqx_resource, #{reason := not_found}}},
        emqx_resource:query(<<"unknown">>, get_state)
    ),

    ok = emqx_resource:remove_local(?ID).

t_healthy_timeout(_) ->
    {ok, _} = emqx_resource:create_local(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => <<"test_resource">>},
        #{health_check_timeout => 200}
    ),
    timer:sleep(500),

    ok = emqx_resource:remove_local(?ID).

t_healthy(_) ->
    {ok, _} = emqx_resource:create_local(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => <<"test_resource">>}
    ),
    timer:sleep(400),

    #{pid := Pid} = emqx_resource:query(?ID, get_state),
    timer:sleep(300),
    emqx_resource:set_resource_status_connecting(?ID),

    {ok, connected} = emqx_resource:health_check(?ID),
    ?assertMatch(
        [#{status := connected}],
        emqx_resource:list_instances_verbose()
    ),

    erlang:exit(Pid, shutdown),

    ?assertEqual({ok, connecting}, emqx_resource:health_check(?ID)),

    ?assertMatch(
        [#{status := connecting}],
        emqx_resource:list_instances_verbose()
    ),

    ok = emqx_resource:remove_local(?ID).

t_stop_start(_) ->
    {error, _} = emqx_resource:check_and_create(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{unknown => test_resource}
    ),

    {ok, _} = emqx_resource:check_and_create(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{<<"name">> => <<"test_resource">>}
    ),

    {ok, _} = emqx_resource:check_and_recreate(
        ?ID,
        ?TEST_RESOURCE,
        #{<<"name">> => <<"test_resource">>},
        #{}
    ),

    #{pid := Pid0} = emqx_resource:query(?ID, get_state),

    ?assert(is_process_alive(Pid0)),

    ok = emqx_resource:stop(?ID),

    ?assertNot(is_process_alive(Pid0)),

    ?assertMatch(
        {error, {emqx_resource, #{reason := not_connected}}},
        emqx_resource:query(?ID, get_state)
    ),

    ok = emqx_resource:restart(?ID),
    timer:sleep(300),

    #{pid := Pid1} = emqx_resource:query(?ID, get_state),

    ?assert(is_process_alive(Pid1)).

t_stop_start_local(_) ->
    {error, _} = emqx_resource:check_and_create_local(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{unknown => test_resource}
    ),

    {ok, _} = emqx_resource:check_and_create_local(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{<<"name">> => <<"test_resource">>}
    ),

    {ok, _} = emqx_resource:check_and_recreate_local(
        ?ID,
        ?TEST_RESOURCE,
        #{<<"name">> => <<"test_resource">>},
        #{}
    ),

    #{pid := Pid0} = emqx_resource:query(?ID, get_state),

    ?assert(is_process_alive(Pid0)),

    ok = emqx_resource:stop(?ID),

    ?assertNot(is_process_alive(Pid0)),

    ?assertMatch(
        {error, {emqx_resource, #{reason := not_connected}}},
        emqx_resource:query(?ID, get_state)
    ),

    ok = emqx_resource:restart(?ID),

    #{pid := Pid1} = emqx_resource:query(?ID, get_state),

    ?assert(is_process_alive(Pid1)).

t_list_filter(_) ->
    {ok, _} = emqx_resource:create_local(
        emqx_resource:generate_id(<<"a">>),
        <<"group1">>,
        ?TEST_RESOURCE,
        #{name => a}
    ),
    {ok, _} = emqx_resource:create_local(
        emqx_resource:generate_id(<<"a">>),
        <<"group2">>,
        ?TEST_RESOURCE,
        #{name => grouped_a}
    ),

    [Id1] = emqx_resource:list_group_instances(<<"group1">>),
    ?assertMatch(
        {ok, <<"group1">>, #{config := #{name := a}}},
        emqx_resource:get_instance(Id1)
    ),

    [Id2] = emqx_resource:list_group_instances(<<"group2">>),
    ?assertMatch(
        {ok, <<"group2">>, #{config := #{name := grouped_a}}},
        emqx_resource:get_instance(Id2)
    ).

t_create_dry_run_local(_) ->
    ets:match_delete(emqx_resource_manager, {{owner, '$1'}, '_'}),
    lists:foreach(
        fun(_) ->
            create_dry_run_local_succ()
        end,
        lists:seq(1, 10)
    ),
    [] = ets:match(emqx_resource_manager, {{owner, '$1'}, '_'}).

create_dry_run_local_succ() ->
    ?assertEqual(
        ok,
        emqx_resource:create_dry_run_local(
            ?TEST_RESOURCE,
            #{name => test_resource, register => true}
        )
    ),
    ?assertEqual(undefined, whereis(test_resource)).

t_create_dry_run_local_failed(_) ->
    {Res1, _} = emqx_resource:create_dry_run_local(
        ?TEST_RESOURCE,
        #{cteate_error => true}
    ),
    ?assertEqual(error, Res1),

    {Res2, _} = emqx_resource:create_dry_run_local(
        ?TEST_RESOURCE,
        #{name => test_resource, health_check_error => true}
    ),
    ?assertEqual(error, Res2),

    Res3 = emqx_resource:create_dry_run_local(
        ?TEST_RESOURCE,
        #{name => test_resource, stop_error => true}
    ),
    ?assertEqual(ok, Res3).

t_test_func(_) ->
    ?assertEqual(ok, erlang:apply(emqx_resource_validator:not_empty("not_empty"), [<<"someval">>])),
    ?assertEqual(ok, erlang:apply(emqx_resource_validator:min(int, 3), [4])),
    ?assertEqual(ok, erlang:apply(emqx_resource_validator:max(array, 10), [[a, b, c, d]])),
    ?assertEqual(ok, erlang:apply(emqx_resource_validator:max(string, 10), ["less10"])).

t_reset_metrics(_) ->
    {ok, _} = emqx_resource:create(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource}
    ),

    #{pid := Pid} = emqx_resource:query(?ID, get_state),
    emqx_resource:reset_metrics(?ID),
    ?assert(is_process_alive(Pid)),
    ok = emqx_resource:remove(?ID),
    ?assertNot(is_process_alive(Pid)).

t_auto_retry(_) ->
    {Res, _} = emqx_resource:create_local(
        ?ID,
        ?DEFAULT_RESOURCE_GROUP,
        ?TEST_RESOURCE,
        #{name => test_resource, create_error => true},
        #{auto_retry_interval => 100}
    ),
    ?assertEqual(ok, Res).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

bin_config() ->
    <<"\"name\": \"test_resource\"">>.

config() ->
    {ok, Config} = hocon:binary(bin_config()),
    Config.
