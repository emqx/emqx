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
