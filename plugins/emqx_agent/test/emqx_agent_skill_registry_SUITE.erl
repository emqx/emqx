%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_skill_registry_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(TYPE, <<"test.skill">>).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx, emqx_conf, emqx_agent], #{
        work_dir => emqx_cth_suite:work_dir(Config)
    }),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_TestCase, _Config) ->
    emqx_agent_skill_registry:unregister_type(?TYPE),
    emqx_agent_skill_registry:unregister_type(<<"other.type">>),
    lists:foreach(
        fun(#{skill_id := Id, type := Type}) ->
            emqx_agent_skill_registry:unregister(Type, Id)
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
    Skill = sample_skill(<<"s1">>, <<"1">>),
    ok = emqx_agent_skill_registry:register(Skill),
    {ok, Got} = emqx_agent_skill_registry:lookup(?TYPE, <<"s1">>),
    ?assertEqual(Skill, Got).

t_lookup_not_found(_Config) ->
    ?assertEqual({error, not_found}, emqx_agent_skill_registry:lookup(?TYPE, <<"no.such">>)).

t_register_missing_skill_id(_Config) ->
    ?assertEqual(
        {error, missing_skill_id},
        emqx_agent_skill_registry:register(#{version => <<"1">>})
    ).

t_register_missing_type(_Config) ->
    ?assertEqual(
        {error, missing_type},
        emqx_agent_skill_registry:register(#{skill_id => <<"x">>, version => <<"1">>})
    ).

t_register_missing_module(_Config) ->
    ?assertEqual(
        {error, missing_module},
        emqx_agent_skill_registry:register(#{skill_id => <<"x">>, type => ?TYPE})
    ).

t_register_overwrites(_Config) ->
    SkillV1 = sample_skill(<<"s.overwrite">>, <<"1">>),
    SkillV2 = sample_skill(<<"s.overwrite">>, <<"2">>),
    ok = emqx_agent_skill_registry:register(SkillV1),
    ok = emqx_agent_skill_registry:register(SkillV2),
    {ok, Got} = emqx_agent_skill_registry:lookup(?TYPE, <<"s.overwrite">>),
    ?assertEqual(<<"2">>, maps:get(version, Got)).

t_same_id_different_types(_Config) ->
    SkillA = sample_skill(<<"shared-id">>, <<"1">>),
    SkillB = (sample_skill(<<"shared-id">>, <<"1">>))#{type => <<"other.type">>},
    ok = emqx_agent_skill_registry:register(SkillA),
    ok = emqx_agent_skill_registry:register(SkillB),
    {ok, A} = emqx_agent_skill_registry:lookup(?TYPE, <<"shared-id">>),
    {ok, B} = emqx_agent_skill_registry:lookup(<<"other.type">>, <<"shared-id">>),
    ?assertEqual(?TYPE, maps:get(type, A)),
    ?assertEqual(<<"other.type">>, maps:get(type, B)).

t_unregister(_Config) ->
    Skill = sample_skill(<<"s.delete">>, <<"1">>),
    ok = emqx_agent_skill_registry:register(Skill),
    ok = emqx_agent_skill_registry:unregister(?TYPE, <<"s.delete">>),
    ?assertEqual({error, not_found}, emqx_agent_skill_registry:lookup(?TYPE, <<"s.delete">>)).

t_unregister_nonexistent(_Config) ->
    ?assertEqual(ok, emqx_agent_skill_registry:unregister(?TYPE, <<"ghost">>)).

t_list(_Config) ->
    SkillA = sample_skill(<<"list.a">>, <<"1">>),
    SkillB = sample_skill(<<"list.b">>, <<"1">>),
    ok = emqx_agent_skill_registry:register(SkillA),
    ok = emqx_agent_skill_registry:register(SkillB),
    Listed = emqx_agent_skill_registry:list(),
    Ids = [maps:get(skill_id, S) || S <- Listed],
    ?assert(lists:member(<<"list.a">>, Ids)),
    ?assert(lists:member(<<"list.b">>, Ids)).

t_list_by_type(_Config) ->
    SkillA = sample_skill(<<"a">>, <<"1">>),
    SkillB = (sample_skill(<<"b">>, <<"1">>))#{type => <<"other.type">>},
    ok = emqx_agent_skill_registry:register(SkillA),
    ok = emqx_agent_skill_registry:register(SkillB),
    OnlyTest = emqx_agent_skill_registry:list(?TYPE),
    ?assert(lists:any(fun(#{skill_id := Id}) -> Id =:= <<"a">> end, OnlyTest)),
    ?assertNot(lists:any(fun(#{skill_id := Id}) -> Id =:= <<"b">> end, OnlyTest)).

t_list_empty(_Config) ->
    ?assertEqual([], emqx_agent_skill_registry:list()).

t_register_and_resolve_type(_Config) ->
    ok = emqx_agent_skill_registry:register_type(?TYPE, ?MODULE),
    ?assertEqual(?MODULE, emqx_agent_skill_registry:resolve_type(?TYPE)).

t_register_type_overwrites(_Config) ->
    ok = emqx_agent_skill_registry:register_type(?TYPE, emqx_agent_skill_registry),
    ok = emqx_agent_skill_registry:register_type(?TYPE, ?MODULE),
    ?assertEqual(?MODULE, emqx_agent_skill_registry:resolve_type(?TYPE)).

t_unregister_type(_Config) ->
    ok = emqx_agent_skill_registry:register_type(?TYPE, ?MODULE),
    ok = emqx_agent_skill_registry:unregister_type(?TYPE),
    ?assertThrow(unknown_type, emqx_agent_skill_registry:resolve_type(?TYPE)).

t_resolve_unknown_type(_Config) ->
    ?assertThrow(unknown_type, emqx_agent_skill_registry:resolve_type(?TYPE)).

t_skill_with_schemas(_Config) ->
    Skill = #{
        skill_id => <<"postgresql.query">>,
        type => <<"postgresql.query">>,
        module => emqx_agent_skill_postgresql,
        version => <<"1">>,
        display_name => <<"PostgreSQL Query">>,
        description => <<"Query PostgreSQL for historical telemetry.">>,
        context => {my_module, some_opaque_ref},
        input_schema => #{
            <<"type">> => <<"object">>,
            <<"properties">> => #{
                <<"device_id">> => #{<<"type">> => <<"string">>},
                <<"metric">> => #{<<"type">> => <<"string">>},
                <<"window_min">> => #{<<"type">> => <<"integer">>}
            },
            <<"required">> => [<<"device_id">>, <<"metric">>, <<"window_min">>]
        }
    },
    ok = emqx_agent_skill_registry:register(Skill),
    {ok, Got} = emqx_agent_skill_registry:lookup(
        <<"postgresql.query">>, <<"postgresql.query">>
    ),
    ?assertEqual(Skill, Got).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

sample_skill(Id, Version) ->
    #{
        skill_id => Id,
        type => ?TYPE,
        module => ?MODULE,
        version => Version,
        display_name => <<"Sample Skill">>,
        description => <<"A sample skill for testing.">>
    }.
