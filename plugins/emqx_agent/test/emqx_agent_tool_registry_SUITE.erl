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

-doc "Check that stored builder tools are not started with default settings.".
t_builder_tools_not_started_by_default(_Config) ->
    ok = emqx_agent_plugin_config_fixture:setup(),
    try
        lists:foreach(
            fun(Type) ->
                Id = <<"builder">>,
                ok = emqx_agent_service:tool_create(#{<<"type">> => Type, <<"id">> => Id}),
                ?assertEqual({error, not_found}, emqx_agent_tool_registry:lookup(Type, Id)),
                ?assertMatch(
                    #{<<"status">> := <<"disabled">>},
                    maps:get(<<Type/binary, "@", Id/binary>>, emqx_agent_tool_registry:statuses())
                )
            end,
            builder_tool_types()
        )
    after
        ok = emqx_agent_plugin_config_fixture:teardown()
    end.

-doc "Check that the plugin configuration starts and stops builder tools.".
t_builder_tools_follow_plugin_config(_Config) ->
    Type = <<"agent__query_tools">>,
    Id = <<"query-tools">>,
    Setting = <<"enable_builder_tools">>,
    ok = emqx_agent_plugin_config_fixture:setup(#{Setting => true}),
    try
        Tool = #{<<"type">> => Type, <<"id">> => Id},
        ok = emqx_agent_service:tool_create(Tool),
        ?assertMatch({ok, _}, emqx_agent_tool_registry:lookup(Type, Id)),
        NameVsn = emqx_agent_plugin_config_fixture:name_vsn(),
        ok = emqx_agent_plugin_config_fixture:put_plugin_config(NameVsn, #{
            Setting => false,
            <<"tools">> => [#{<<"Agent_QueryTools">> => Tool}]
        }),
        ?assertEqual({error, not_found}, emqx_agent_tool_registry:lookup(Type, Id)),
        ok = emqx_agent_plugin_config_fixture:put_plugin_config(NameVsn, #{
            Setting => true,
            <<"tools">> => [#{<<"Agent_QueryTools">> => Tool}]
        }),
        ?assertMatch({ok, _}, emqx_agent_tool_registry:lookup(Type, Id))
    after
        ok = emqx_agent_plugin_config_fixture:teardown()
    end.

-doc "Check that a stored disabled builder tool does not block other config edits.".
t_disabled_builder_tool_allows_other_edits(_Config) ->
    ok = emqx_agent_plugin_config_fixture:setup(#{
        <<"tools">> => [
            #{
                <<"Agent_QueryTools">> => #{
                    <<"type">> => <<"agent__query_tools">>, <<"id">> => <<"q">>
                }
            }
        ]
    }),
    try
        ?assertEqual(
            ok,
            emqx_agent_service:tool_create(#{
                <<"type">> => <<"stream__del">>,
                <<"id">> => <<"del">>,
                <<"desc">> => <<"test">>,
                <<"stream">> => <<"s">>
            })
        ),
        ?assertMatch(
            {ok, #{<<"desc">> := <<"updated">>}},
            emqx_agent_service:tool_update(<<"stream__del">>, <<"del">>, #{
                <<"desc">> => <<"updated">>, <<"stream">> => <<"s">>
            })
        )
    after
        ok = emqx_agent_plugin_config_fixture:teardown()
    end.

-doc "Check that a tool whose runtime creation crashes does not stop the registry.".
t_create_crash_keeps_registry(_Config) ->
    ok = emqx_agent_plugin_config_fixture:setup(),
    try
        Registry = whereis(emqx_agent_tool_registry),
        ok = emqx_agent_service:tool_create(#{
            <<"type">> => <<"image__fetch">>,
            <<"id">> => <<"bad-url">>,
            <<"desc">> => <<"test">>,
            <<"url">> => <<"http://exa mple.com/a">>
        }),
        ?assertEqual(Registry, whereis(emqx_agent_tool_registry)),
        ?assertMatch(
            #{<<"status">> := <<"failed">>},
            maps:get(<<"image__fetch@bad-url">>, emqx_agent_tool_registry:statuses())
        ),
        ?assertEqual(
            emqx_agent_tool_http, emqx_agent_tool_registry:resolve_type(<<"http">>)
        )
    after
        ok = emqx_agent_plugin_config_fixture:teardown()
    end.

-doc """
Check that a stopped and still loaded plugin accepts a config change,
and that it runs the changed tools when it starts again.
""".
t_config_change_while_stopped(_Config) ->
    Type = <<"stream__del">>,
    Id = <<"stopped-change">>,
    Tool = #{
        <<"type">> => Type,
        <<"id">> => Id,
        <<"desc">> => <<"test">>,
        <<"stream">> => <<"s">>
    },
    NewConfig = #{<<"tools">> => [#{<<"Stream_Del">> => Tool}]},
    ok = emqx_agent_plugin_config_fixture:setup(),
    try
        ok = application:stop(emqx_agent),
        try
            ?assertEqual(undefined, whereis(emqx_agent_sup)),
            ?assertEqual(ok, emqx_agent_app:on_config_changed(#{}, NewConfig)),
            NameVsn = emqx_agent_plugin_config_fixture:name_vsn(),
            ok = emqx_agent_plugin_config_fixture:put_plugin_config(NameVsn, NewConfig)
        after
            {ok, _} = application:ensure_all_started(emqx_agent)
        end,
        ?assertMatch({ok, _}, emqx_agent_tool_registry:lookup(Type, Id))
    after
        ok = emqx_agent_plugin_config_fixture:teardown()
    end.

-doc """
Check that a PostgreSQL tool starts when one config change adds the tool and its
connection. The stored plugin config still holds the old config while the change is
applied, as on a node that did not handle the API request.
""".
t_postgresql_tool_with_new_connection(_Config) ->
    Type = <<"postgresql__query">>,
    Id = <<"pg-tool">>,
    ConnectionId = <<"pg-conn">>,
    Connection = #{
        <<"type">> => <<"postgresql">>,
        <<"id">> => ConnectionId,
        <<"enable">> => false,
        <<"config">> => #{
            <<"server">> => <<"pgsql:5432">>,
            <<"database">> => <<"mqtt">>,
            <<"username">> => <<"root">>,
            <<"password">> => <<"public">>
        }
    },
    Tool = #{
        <<"type">> => Type,
        <<"id">> => Id,
        <<"desc">> => <<"test">>,
        <<"resource">> => ConnectionId,
        <<"query">> => <<"SELECT 1">>
    },
    NewConfig = #{
        <<"tools">> => [#{<<"Postgresql_Query">> => Tool}],
        <<"connections">> => [#{<<"ConnectionPostgresql">> => Connection}]
    },
    ok = emqx_agent_plugin_config_fixture:setup(),
    try
        ?assertEqual(ok, emqx_agent_app:on_config_changed(#{}, NewConfig)),
        ?assertMatch({ok, _}, emqx_agent_tool_registry:lookup(Type, Id))
    after
        ok = emqx_agent_plugin_config_fixture:teardown()
    end.

-doc "Check that the tool cleanup finishes when the registry process is not running.".
t_deinit_without_registry(_Config) ->
    Registry = emqx_agent_tool_registry,
    Pid = whereis(Registry),
    true = erlang:unregister(Registry),
    try
        ?assertEqual(ok, Registry:unregister_type(?TYPE)),
        ?assertEqual(ok, emqx_agent_tool_stream:deinit()),
        ?assertEqual(ok, emqx_agent_tool:deinit())
    after
        true = erlang:register(Registry, Pid),
        ok = emqx_agent_tool:init()
    end,
    ?assertEqual(emqx_agent_tool_http, Registry:resolve_type(<<"http">>)).

builder_tool_types() ->
    [
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
    ].
