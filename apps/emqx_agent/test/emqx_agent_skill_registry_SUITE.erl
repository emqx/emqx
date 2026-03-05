%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_skill_registry_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx_agent], #{work_dir => emqx_cth_suite:work_dir(Config)}),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_TestCase, _Config) ->
    %% Ensure registry is clean before each test
    lists:foreach(
        fun(#{skill_id := Id}) ->
            emqx_agent_skill_registry:unregister(Id)
        end,
        emqx_agent_skill_registry:list()
    ),
    [].

end_per_testcase(_TestCase, _Config) ->
    ok.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_register_and_lookup(_Config) ->
    Skill = sample_skill(<<"clickhouse.history">>, <<"1">>),
    ok = emqx_agent_skill_registry:register(Skill),
    {ok, Got} = emqx_agent_skill_registry:lookup(<<"clickhouse.history">>),
    ?assertEqual(Skill, Got).

t_lookup_not_found(_Config) ->
    ?assertEqual({error, not_found}, emqx_agent_skill_registry:lookup(<<"no.such.skill">>)).

t_register_missing_skill_id(_Config) ->
    ?assertEqual(
        {error, missing_skill_id},
        emqx_agent_skill_registry:register(#{version => <<"1">>})
    ).

t_register_overwrites(_Config) ->
    SkillV1 = sample_skill(<<"s.overwrite">>, <<"1">>),
    SkillV2 = sample_skill(<<"s.overwrite">>, <<"2">>),
    ok = emqx_agent_skill_registry:register(SkillV1),
    ok = emqx_agent_skill_registry:register(SkillV2),
    {ok, Got} = emqx_agent_skill_registry:lookup(<<"s.overwrite">>),
    ?assertEqual(<<"2">>, maps:get(version, Got)).

t_unregister(_Config) ->
    Skill = sample_skill(<<"s.delete">>, <<"1">>),
    ok = emqx_agent_skill_registry:register(Skill),
    ok = emqx_agent_skill_registry:unregister(<<"s.delete">>),
    ?assertEqual({error, not_found}, emqx_agent_skill_registry:lookup(<<"s.delete">>)).

t_unregister_nonexistent(_Config) ->
    %% Deleting a non-existent key must not crash
    ?assertEqual(ok, emqx_agent_skill_registry:unregister(<<"ghost">>)).

t_list(_Config) ->
    SkillA = sample_skill(<<"list.a">>, <<"1">>),
    SkillB = sample_skill(<<"list.b">>, <<"1">>),
    ok = emqx_agent_skill_registry:register(SkillA),
    ok = emqx_agent_skill_registry:register(SkillB),
    Listed = emqx_agent_skill_registry:list(),
    Ids = [maps:get(skill_id, S) || S <- Listed],
    ?assert(lists:member(<<"list.a">>, Ids)),
    ?assert(lists:member(<<"list.b">>, Ids)).

t_list_empty(_Config) ->
    ?assertEqual([], emqx_agent_skill_registry:list()).

t_skill_with_schemas(_Config) ->
    Skill = #{
        skill_id => <<"clickhouse.history">>,
        version => <<"1">>,
        display_name => <<"ClickHouse Time-series History">>,
        description => <<"Query historical telemetry for a device and metric.">>,
        context => {my_module, some_opaque_ref},
        input_schema => #{
            <<"type">> => <<"object">>,
            <<"properties">> => #{
                <<"device_id">> => #{<<"type">> => <<"string">>},
                <<"metric">> => #{<<"type">> => <<"string">>},
                <<"window_min">> => #{<<"type">> => <<"integer">>}
            },
            <<"required">> => [<<"device_id">>, <<"metric">>, <<"window_min">>]
        },
        output_schema => #{
            <<"type">> => <<"object">>,
            <<"properties">> => #{
                <<"series">> => #{<<"type">> => <<"array">>},
                <<"stats">> => #{<<"type">> => <<"object">>}
            },
            <<"required">> => [<<"stats">>]
        }
    },
    ok = emqx_agent_skill_registry:register(Skill),
    {ok, Got} = emqx_agent_skill_registry:lookup(<<"clickhouse.history">>),
    ?assertEqual(Skill, Got).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

sample_skill(Id, Version) ->
    #{
        skill_id => Id,
        version => Version,
        display_name => <<"Sample Skill">>,
        description => <<"A sample skill for testing.">>
    }.
