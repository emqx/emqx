%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include("emqx_authn.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(TEST_RESOURCE, emqx_test_resource).
-define(ID, <<"id">>).

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_testcase(_, Config) ->
    Config.

init_per_suite(Config) ->
    code:ensure_loaded(?TEST_RESOURCE),
    ok = emqx_common_test_helpers:start_apps([]),
    {ok, _} = application:ensure_all_started(emqx_resource),
    Config.

end_per_suite(_Config) ->
    ok = emqx_common_test_helpers:stop_apps([emqx_resource]).

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
                   ?TEST_RESOURCE,
                   #{unknown => <<"test_resource">>}),

    {ok, _} = emqx_resource:create_local(
                ?ID,
                ?TEST_RESOURCE,
                #{name => <<"test_resource">>}),

    #{pid := Pid} = emqx_resource:query(?ID, get_state),

    ?assert(is_process_alive(Pid)),

    ok = emqx_resource:remove_local(?ID),
    {error, _} = emqx_resource:remove_local(?ID),

    ?assertNot(is_process_alive(Pid)).

t_query(_) ->
    {ok, _} = emqx_resource:create_local(
                ?ID,
                ?TEST_RESOURCE,
                #{name => <<"test_resource">>}),

    Pid = self(),
    Success = fun() -> Pid ! success end,
    Failure = fun() -> Pid ! failure end,

    #{pid := _} = emqx_resource:query(?ID, get_state),
    #{pid := _} = emqx_resource:query(?ID, get_state, {[{Success, []}], [{Failure, []}]}),

    receive
        Message -> ?assertEqual(success, Message)
    after 100 ->
        ?assert(false)
    end,

    ?assertException(
       error,
       {get_instance, _Reason},
       emqx_resource:query(<<"unknown">>, get_state)),

    ok = emqx_resource:remove_local(?ID).

t_healthy(_) ->
    {ok, _} = emqx_resource:create_local(
                ?ID,
                ?TEST_RESOURCE,
                #{name => <<"test_resource">>}),

    #{pid := Pid} = emqx_resource:query(?ID, get_state),

    ok = emqx_resource:health_check(?ID),

    [#{status := started}] = emqx_resource:list_instances_verbose(),

    erlang:exit(Pid, shutdown),

    {error, dead} = emqx_resource:health_check(?ID),

    [#{status := stopped}] = emqx_resource:list_instances_verbose(),

    ok = emqx_resource:remove_local(?ID).

t_stop_start(_) ->
    {error, _} = emqx_resource:check_and_create_local(
                   ?ID,
                   ?TEST_RESOURCE,
                   #{unknown => <<"test_resource">>}),

    {ok, _} = emqx_resource:create_local(
                ?ID,
                ?TEST_RESOURCE,
                #{name => <<"test_resource">>}),

    #{pid := Pid0} = emqx_resource:query(?ID, get_state),

    ?assert(is_process_alive(Pid0)),

    ok = emqx_resource:stop(?ID),

    ?assertNot(is_process_alive(Pid0)),

    ?assertException(
       error,
       {?ID, stopped},
       emqx_resource:query(?ID, get_state)),

    ok = emqx_resource:restart(?ID),

    #{pid := Pid1} = emqx_resource:query(?ID, get_state),

    ?assert(is_process_alive(Pid1)).

t_list_filter(_) ->
    {ok, _} = emqx_resource:create_local(
                emqx_resource:generate_id(<<"a">>),
                ?TEST_RESOURCE,
                #{name => a}),
    {ok, _} = emqx_resource:create_local(
                emqx_resource:generate_id(<<"group">>, <<"a">>),
                ?TEST_RESOURCE,
                #{name => grouped_a}),

    [Id1] = emqx_resource:list_group_instances(<<"default">>),
    {ok, #{config := #{name := a}}} = emqx_resource:get_instance(Id1),

    [Id2] = emqx_resource:list_group_instances(<<"group">>),
    {ok, #{config := #{name := grouped_a}}} = emqx_resource:get_instance(Id2).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

bin_config() ->
    <<"\"name\": \"test_resource\"">>.

config() ->
    {ok, Config} = hocon:binary(bin_config()),
    Config.
