%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_tool_registry_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(TYPE, <<"test.tool">>).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx, emqx_conf, emqx_resource, emqx_agent], #{
        work_dir => emqx_cth_suite:work_dir(Config)
    }),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_TestCase, Config) ->
    emqx_agent_tool_registry:unregister_type(?TYPE),
    emqx_agent_tool_registry:unregister_type(<<"other.type">>),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_lookup_not_found(_Config) ->
    ?assertEqual({error, not_found}, emqx_agent_tool_registry:lookup(?TYPE, <<"no.such">>)).

t_register_and_resolve_type(_Config) ->
    ok = emqx_agent_tool_registry:register_type(?TYPE, ?MODULE),
    ?assertEqual(?MODULE, emqx_agent_tool_registry:resolve_type(?TYPE)).

t_register_type_overwrites(_Config) ->
    ok = emqx_agent_tool_registry:register_type(?TYPE, emqx_agent_tool_registry),
    ok = emqx_agent_tool_registry:register_type(?TYPE, ?MODULE),
    ?assertEqual(?MODULE, emqx_agent_tool_registry:resolve_type(?TYPE)).

t_unregister_type(_Config) ->
    ok = emqx_agent_tool_registry:register_type(?TYPE, ?MODULE),
    ok = emqx_agent_tool_registry:unregister_type(?TYPE),
    ?assertThrow(unknown_type, emqx_agent_tool_registry:resolve_type(?TYPE)).

t_resolve_unknown_type(_Config) ->
    ?assertThrow(unknown_type, emqx_agent_tool_registry:resolve_type(?TYPE)).

t_builtin_tool_types_registered_after_start(_Config) ->
    ?assertEqual(emqx_agent_tool_http, emqx_agent_tool_registry:resolve_type(<<"http">>)),
    ?assertEqual(
        emqx_agent_tool_publish,
        emqx_agent_tool_registry:resolve_type(<<"message__publish">>)
    ),
    ?assert(lists:member(emqx_agent_tool_http, emqx_agent_tool:discover_tool_modules())),
    ?assert(lists:member(emqx_agent_tool_publish, emqx_agent_tool:discover_tool_modules())).

-doc "Check that builder tool types are not registered with default settings.".
t_builder_tools_not_registered_by_default(_Config) ->
    Types = [
        <<"agent__create_pipeline">>,
        <<"agent__create_tool">>,
        <<"agent__delete_pipeline">>,
        <<"agent__delete_pipeline_step">>,
        <<"agent__delete_tool">>,
        <<"agent__insert_pipeline_step">>,
        <<"agent__query_connections">>,
        <<"agent__query_pipelines">>,
        <<"agent__query_providers">>,
        <<"agent__query_tools">>,
        <<"agent__update_pipeline">>,
        <<"agent__update_pipeline_step">>,
        <<"agent__update_tool">>
    ],
    lists:foreach(
        fun(Type) -> ?assertThrow(unknown_type, emqx_agent_tool_registry:resolve_type(Type)) end,
        Types
    ).

-doc "Check that the plugin configuration enables and disables builder tools.".
t_builder_tool_registration_follows_plugin_config(_Config) ->
    Type = <<"agent__query_tools">>,
    Id = <<"query-tools">>,
    Setting = <<"enable_builder_tools">>,
    ?assertThrow(unknown_type, emqx_agent_tool_registry:resolve_type(Type)),
    ok = emqx_agent_plugin_config_fixture:setup(#{Setting => true}),
    try
        ?assertEqual(emqx_agent_tool_query_tools, emqx_agent_tool_registry:resolve_type(Type)),
        Tool = #{<<"type">> => Type, <<"id">> => Id},
        ok = emqx_agent_service:tool_create(Tool),
        ?assertMatch({ok, _}, emqx_agent_tool_registry:lookup(Type, Id)),
        ok = emqx_agent_config:init_config(),
        ?assertEqual(true, emqx_agent_config:parsed_config([enable_builder_tools])),
        NameVsn = emqx_agent_plugin_config_fixture:name_vsn(),
        ok = emqx_agent_plugin_config_fixture:put_plugin_config(NameVsn, #{
            Setting => false,
            <<"tools">> => [#{<<"Agent_QueryTools">> => Tool}]
        }),
        ?assertThrow(unknown_type, emqx_agent_tool_registry:resolve_type(Type)),
        ?assertEqual({error, not_found}, emqx_agent_tool_registry:lookup(Type, Id))
    after
        ok = emqx_agent_plugin_config_fixture:teardown()
    end.
