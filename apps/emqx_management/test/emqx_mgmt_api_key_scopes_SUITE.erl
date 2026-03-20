%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_mgmt_api_key_scopes_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx_dashboard/include/emqx_dashboard_rbac.hrl").

-if(?EMQX_RELEASE_EDITION == ee).

%%--------------------------------------------------------------------
%% CT boilerplate
%%--------------------------------------------------------------------

all() ->
    [
        {group, unit_tests},
        {group, integration_tests},
        {group, api_tests}
    ].

suite() -> [{timetrap, {minutes, 1}}].

groups() ->
    [
        {unit_tests, [], [
            t_init_cache,
            t_available_scopes,
            t_available_scopes_excludes_denied,
            t_denied_scopes,
            t_path_to_scopes,
            t_path_to_scopes_pattern_match,
            t_path_to_scopes_no_cache,
            t_validate_scopes,
            t_validate_scopes_denied,
            t_validate_scopes_bad_input,
            t_preset_groups,
            t_preset_groups_all_scopes,
            t_expand_groups,
            t_all_tags_in_preset_groups
        ]},
        {integration_tests, [parallel], [
            t_authorize_with_scopes,
            t_authorize_no_scopes,
            t_authorize_empty_scopes,
            t_authorize_denied_path,
            t_check_scopes_unmapped_path
        ]},
        {api_tests, [parallel], [
            t_api_list_scopes,
            t_api_list_scopes_with_groups,
            t_api_create_with_scopes,
            t_api_update_scopes
        ]}
    ].

init_per_suite(Config) ->
    application:ensure_all_started(hackney),
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)),
    application:stop(hackney).

init_per_group(unit_tests, Config) ->
    %% Ensure cache is fresh for each unit test group
    emqx_mgmt_api_key_scopes:clear_cache(),
    Config;
init_per_group(_Group, Config) ->
    %% Ensure scope cache is initialized for integration/api tests
    emqx_mgmt_api_key_scopes:init_cache(),
    Config.

end_per_group(_Group, Config) ->
    Config.

%%--------------------------------------------------------------------
%% Unit tests for emqx_mgmt_api_key_scopes
%%--------------------------------------------------------------------

t_init_cache(_Config) ->
    %% Clear first to ensure clean state
    emqx_mgmt_api_key_scopes:clear_cache(),
    ?assertEqual(
        undefined, persistent_term:get({emqx_mgmt_api_key_scopes, scope_cache}, undefined)
    ),

    %% Init cache
    ?assertEqual(ok, emqx_mgmt_api_key_scopes:init_cache()),
    Cache = persistent_term:get({emqx_mgmt_api_key_scopes, scope_cache}, undefined),
    ?assertNotEqual(undefined, Cache),
    ?assertMatch(#{scopes := _, path_to_scopes := _}, Cache),

    %% Clear cache
    ?assertEqual(ok, emqx_mgmt_api_key_scopes:clear_cache()),
    ?assertEqual(
        undefined, persistent_term:get({emqx_mgmt_api_key_scopes, scope_cache}, undefined)
    ).

t_available_scopes(_Config) ->
    emqx_mgmt_api_key_scopes:init_cache(),
    Scopes = emqx_mgmt_api_key_scopes:available_scopes(),
    ?assert(is_list(Scopes)),
    ?assert(length(Scopes) > 0),
    %% Each scope should be a map with name and paths
    lists:foreach(
        fun(Scope) ->
            ?assertMatch(#{name := _, paths := _}, Scope),
            #{name := Name, paths := Paths} = Scope,
            ?assert(is_binary(Name)),
            ?assert(is_list(Paths)),
            ?assert(length(Paths) > 0),
            %% Name should be lowercase
            ?assertEqual(Name, string:lowercase(Name))
        end,
        Scopes
    ),
    emqx_mgmt_api_key_scopes:clear_cache().

t_path_to_scopes(_Config) ->
    emqx_mgmt_api_key_scopes:init_cache(),
    %% Registered template paths should resolve to scopes
    %% "/clients" is registered under the "Clients" tag → "clients" scope
    ClientsScopes = emqx_mgmt_api_key_scopes:path_to_scopes(<<"/clients">>),
    ?assert(is_list(ClientsScopes)),
    ?assert(length(ClientsScopes) > 0),
    ?assert(lists:member(<<"clients">>, ClientsScopes)),

    %% "/clients/:clientid" should also be in the clients scope
    ClientByIdScopes = emqx_mgmt_api_key_scopes:path_to_scopes(<<"/clients/:clientid">>),
    ?assert(is_list(ClientByIdScopes)),
    ?assert(lists:member(<<"clients">>, ClientByIdScopes)),
    emqx_mgmt_api_key_scopes:clear_cache().

t_path_to_scopes_pattern_match(_Config) ->
    emqx_mgmt_api_key_scopes:init_cache(),
    %% Actual runtime path with real clientid should match against
    %% the registered pattern "/clients/:clientid"
    ActualPathScopes = emqx_mgmt_api_key_scopes:path_to_scopes(<<"/clients/myclient">>),
    ?assert(is_list(ActualPathScopes)),
    ?assert(lists:member(<<"clients">>, ActualPathScopes)),

    %% Another pattern: "/clients/myclient/subscriptions"
    %% should match "/clients/:clientid/subscriptions" if it exists
    SubScopes = emqx_mgmt_api_key_scopes:path_to_scopes(<<"/clients/myclient/subscriptions">>),
    ?assert(is_list(SubScopes)),
    %% It should find scopes (clients or subscriptions tag)
    %% At minimum, it should not crash
    emqx_mgmt_api_key_scopes:clear_cache().

t_path_to_scopes_no_cache(_Config) ->
    %% When cache is not initialized, should return empty list
    emqx_mgmt_api_key_scopes:clear_cache(),
    ?assertEqual([], emqx_mgmt_api_key_scopes:path_to_scopes(<<"/clients">>)).

t_validate_scopes(_Config) ->
    emqx_mgmt_api_key_scopes:init_cache(),
    Scopes = emqx_mgmt_api_key_scopes:available_scopes(),
    ?assert(length(Scopes) > 0),
    %% Valid: use a known scope name
    [#{name := ValidScope} | _] = Scopes,
    ?assertEqual(ok, emqx_mgmt_api_key_scopes:validate_scopes([ValidScope])),
    %% Valid: empty list
    ?assertEqual(ok, emqx_mgmt_api_key_scopes:validate_scopes([])),

    %% Invalid: unknown scope
    ?assertMatch(
        {error, <<"Unknown scopes: ", _/binary>>},
        emqx_mgmt_api_key_scopes:validate_scopes([<<"nonexistent_scope_xyz">>])
    ),

    %% Mixed valid and invalid
    ?assertMatch(
        {error, <<"Unknown scopes: ", _/binary>>},
        emqx_mgmt_api_key_scopes:validate_scopes([ValidScope, <<"nonexistent_scope_xyz">>])
    ),
    emqx_mgmt_api_key_scopes:clear_cache().

t_validate_scopes_bad_input(_Config) ->
    %% Non-list input should return error
    ?assertMatch(
        {error, <<"scopes must be a list of strings">>},
        emqx_mgmt_api_key_scopes:validate_scopes(<<"not_a_list">>)
    ),
    ?assertMatch(
        {error, <<"scopes must be a list of strings">>},
        emqx_mgmt_api_key_scopes:validate_scopes(42)
    ),
    ?assertMatch(
        {error, <<"scopes must be a list of strings">>},
        emqx_mgmt_api_key_scopes:validate_scopes(#{})
    ).

t_available_scopes_excludes_denied(_Config) ->
    emqx_mgmt_api_key_scopes:init_cache(),
    Scopes = emqx_mgmt_api_key_scopes:available_scopes(),
    ScopeNames = [Name || #{name := Name} <- Scopes],
    Denied = emqx_mgmt_api_key_scopes:denied_scopes(),
    %% No denied scope should appear in available_scopes
    DeniedPresent = [S || S <- ScopeNames, lists:member(S, Denied)],
    ?assertEqual([], DeniedPresent),
    emqx_mgmt_api_key_scopes:clear_cache().

t_denied_scopes(_Config) ->
    Denied = emqx_mgmt_api_key_scopes:denied_scopes(),
    ?assert(is_list(Denied)),
    ?assert(length(Denied) >= 3),
    ?assert(lists:member(<<"dashboard">>, Denied)),
    ?assert(lists:member(<<"dashboard single sign-on">>, Denied)),
    ?assert(lists:member(<<"api keys">>, Denied)),
    %% is_denied_scope works
    ?assert(emqx_mgmt_api_key_scopes:is_denied_scope(<<"dashboard">>)),
    ?assert(emqx_mgmt_api_key_scopes:is_denied_scope(<<"api keys">>)),
    ?assertNot(emqx_mgmt_api_key_scopes:is_denied_scope(<<"clients">>)),
    ?assertNot(emqx_mgmt_api_key_scopes:is_denied_scope(<<"rules">>)).

t_validate_scopes_denied(_Config) ->
    emqx_mgmt_api_key_scopes:init_cache(),
    %% Trying to set a denied scope should fail
    ?assertMatch(
        {error, <<"Denied scopes", _/binary>>},
        emqx_mgmt_api_key_scopes:validate_scopes([<<"dashboard">>])
    ),
    ?assertMatch(
        {error, <<"Denied scopes", _/binary>>},
        emqx_mgmt_api_key_scopes:validate_scopes([<<"api keys">>])
    ),
    ?assertMatch(
        {error, <<"Denied scopes", _/binary>>},
        emqx_mgmt_api_key_scopes:validate_scopes([<<"dashboard single sign-on">>])
    ),
    %% Mixing valid + denied: denied takes priority
    ?assertMatch(
        {error, <<"Denied scopes", _/binary>>},
        emqx_mgmt_api_key_scopes:validate_scopes([<<"clients">>, <<"dashboard">>])
    ),
    emqx_mgmt_api_key_scopes:clear_cache().

t_preset_groups(_Config) ->
    Groups = emqx_mgmt_api_key_scopes:preset_groups(),
    ?assert(is_list(Groups)),
    %% 9 static groups + 1 dynamic (all_scopes) = 10
    ?assert(length(Groups) >= 10),
    %% Each group has name, desc, scopes
    lists:foreach(
        fun(Group) ->
            ?assertMatch(#{name := _, desc := _, scopes := _}, Group),
            #{name := Name, desc := Desc, scopes := Scopes} = Group,
            ?assert(is_binary(Name)),
            ?assert(is_binary(Desc)),
            ?assert(is_list(Scopes)),
            ?assert(length(Scopes) > 0),
            %% All scope names should be binaries
            lists:foreach(fun(S) -> ?assert(is_binary(S)) end, Scopes)
        end,
        Groups
    ),
    %% Group names should be unique
    Names = [N || #{name := N} <- Groups],
    ?assertEqual(length(Names), length(lists:usort(Names))),
    %% No duplicate scopes across groups, EXCLUDING all_scopes
    %% (all_scopes intentionally overlaps with other groups)
    NonAllGroups = [G || G = #{name := N} <- Groups, N =/= <<"all_scopes">>],
    AllScopes = lists:flatmap(fun(#{scopes := S}) -> S end, NonAllGroups),
    ?assertEqual(length(AllScopes), length(lists:usort(AllScopes))),
    %% Denied scopes must not appear in any group
    Denied = emqx_mgmt_api_key_scopes:denied_scopes(),
    AllGroupScopes = lists:flatmap(fun(#{scopes := S}) -> S end, Groups),
    DeniedInGroups = [S || S <- AllGroupScopes, lists:member(S, Denied)],
    ?assertEqual([], DeniedInGroups).

t_preset_groups_all_scopes(_Config) ->
    emqx_mgmt_api_key_scopes:init_cache(),
    Groups = emqx_mgmt_api_key_scopes:preset_groups(),
    %% Find the all_scopes group
    AllScopesGroups = [G || G = #{name := <<"all_scopes">>} <- Groups],
    ?assertEqual(1, length(AllScopesGroups)),
    [#{scopes := AllScopes}] = AllScopesGroups,
    %% all_scopes should contain all available scopes
    Available = [Name || #{name := Name} <- emqx_mgmt_api_key_scopes:available_scopes()],
    ?assertEqual(lists:sort(Available), lists:sort(AllScopes)),
    %% all_scopes should NOT contain any denied scopes
    Denied = emqx_mgmt_api_key_scopes:denied_scopes(),
    DeniedInAll = [S || S <- AllScopes, lists:member(S, Denied)],
    ?assertEqual([], DeniedInAll),
    %% all_scopes should be a superset of every other group's scopes
    OtherGroups = [G || G = #{name := N} <- Groups, N =/= <<"all_scopes">>],
    lists:foreach(
        fun(#{name := GName, scopes := GScopes}) ->
            NotInAll = [S || S <- GScopes, not lists:member(S, AllScopes)],
            ?assertEqual(
                [],
                NotInAll,
                lists:flatten(
                    io_lib:format("Group ~s has scopes not in all_scopes: ~p", [GName, NotInAll])
                )
            )
        end,
        OtherGroups
    ),
    emqx_mgmt_api_key_scopes:clear_cache().

t_expand_groups(_Config) ->
    %% Expand a group name → individual scopes
    Expanded = emqx_mgmt_api_key_scopes:expand_groups([<<"connections">>]),
    ?assert(lists:member(<<"clients">>, Expanded)),
    ?assert(lists:member(<<"subscriptions">>, Expanded)),
    ?assert(lists:member(<<"topics">>, Expanded)),
    ?assert(lists:member(<<"publish">>, Expanded)),
    ?assert(lists:member(<<"banned">>, Expanded)),

    %% Expand a mix of group name + individual scope
    Mixed = emqx_mgmt_api_key_scopes:expand_groups([<<"connections">>, <<"rules">>]),
    ?assert(lists:member(<<"clients">>, Mixed)),
    ?assert(lists:member(<<"rules">>, Mixed)),

    %% Unknown name passes through as individual scope
    PassThru = emqx_mgmt_api_key_scopes:expand_groups([<<"some_custom_scope">>]),
    ?assertEqual([<<"some_custom_scope">>], PassThru),

    %% Empty list
    ?assertEqual([], emqx_mgmt_api_key_scopes:expand_groups([])).

t_all_tags_in_preset_groups(_Config) ->
    %% This is the critical coverage test:
    %% Every tag returned by available_scopes() must exist in at least one preset group.
    %% If this test fails, a new API module was added with an OpenAPI tag that is not
    %% covered by any preset group — update preset_groups/0 in emqx_mgmt_api_key_scopes.
    emqx_mgmt_api_key_scopes:init_cache(),
    AllAvailable = [Name || #{name := Name} <- emqx_mgmt_api_key_scopes:available_scopes()],
    AllPreset = emqx_mgmt_api_key_scopes:all_preset_tags(),
    Uncovered = [Tag || Tag <- AllAvailable, not lists:member(Tag, AllPreset)],
    case Uncovered of
        [] ->
            ok;
        _ ->
            ct:fail(
                "The following OpenAPI tags are NOT covered by any preset group "
                "in emqx_mgmt_api_key_scopes:preset_groups/0. "
                "Please add them to an existing group or create a new one: ~p",
                [Uncovered]
            )
    end,
    emqx_mgmt_api_key_scopes:clear_cache().

%%--------------------------------------------------------------------
%% Integration tests for scope-based authorization in emqx_mgmt_auth
%%--------------------------------------------------------------------

t_authorize_with_scopes(_Config) ->
    %% Create an API key with scopes restricted to "clients" only
    Name = <<"SCOPES-TEST-WITH">>,
    {ok, #{<<"api_key">> := ApiKey, <<"api_secret">> := ApiSecret}} =
        create_app(Name, #{scopes => [<<"clients">>]}),

    %% Access to /clients should succeed
    ?assertEqual(ok, auth_authorize(<<"/clients">>, ApiKey, ApiSecret)),

    %% Access to /clients/:clientid should also succeed (same scope, pattern match)
    ?assertEqual(ok, auth_authorize(<<"/clients/myclient">>, ApiKey, ApiSecret)),

    %% Access to a path in a different scope (e.g., /banned) should be denied
    ?assertMatch({error, _}, auth_authorize(<<"/banned">>, ApiKey, ApiSecret)),
    delete_app(Name).

t_authorize_no_scopes(_Config) ->
    %% Create an API key without scopes — should have full access (backward compat)
    Name = <<"SCOPES-TEST-NONE">>,
    {ok, #{<<"api_key">> := ApiKey, <<"api_secret">> := ApiSecret}} =
        create_app(Name),

    %% Should be able to access any path
    ?assertEqual(ok, auth_authorize(<<"/clients">>, ApiKey, ApiSecret)),
    ?assertEqual(ok, auth_authorize(<<"/banned">>, ApiKey, ApiSecret)),
    ?assertEqual(ok, auth_authorize(<<"/alarms">>, ApiKey, ApiSecret)),
    delete_app(Name).

t_authorize_empty_scopes(_Config) ->
    %% Create an API key with empty scopes — all API paths should be denied
    Name = <<"SCOPES-TEST-EMPTY">>,
    {ok, #{<<"api_key">> := ApiKey, <<"api_secret">> := ApiSecret}} =
        create_app(Name, #{scopes => []}),

    %% All paths that are mapped to scopes should be denied
    ?assertMatch({error, _}, auth_authorize(<<"/clients">>, ApiKey, ApiSecret)),
    ?assertMatch({error, _}, auth_authorize(<<"/banned">>, ApiKey, ApiSecret)),
    delete_app(Name).

t_authorize_denied_path(_Config) ->
    %% Denied paths should be blocked even for API keys without scopes (backward compat keys).
    %% The "dashboard" tag covers /users, /users/:username, etc.
    %% The "api keys" tag covers /api_key, /api_key/:name (already hardcoded, but also via deny list).
    Name = <<"SCOPES-TEST-DENIED">>,
    {ok, #{<<"api_key">> := ApiKey, <<"api_secret">> := ApiSecret}} =
        create_app(Name),

    %% Normal paths should still work (no scopes = full access to non-denied)
    ?assertEqual(ok, auth_authorize(<<"/clients">>, ApiKey, ApiSecret)),
    ?assertEqual(ok, auth_authorize(<<"/alarms">>, ApiKey, ApiSecret)),

    %% Denied paths should be blocked via check_scopes deny list.
    %% Note: /users and /api_key are ALSO blocked by the hardcoded authorize/4 clauses
    %% (defense-in-depth), but we test check_scopes/3 directly to verify the deny list.
    Extra = #{role => ?ROLE_API_SUPERUSER},
    %% Dashboard paths — denied
    ?assertMatch(
        {error, unauthorized_role},
        emqx_mgmt_auth:check_scopes(Extra, <<"/users">>, <<"GET">>)
    ),
    ?assertMatch(
        {error, unauthorized_role},
        emqx_mgmt_auth:check_scopes(Extra, <<"/users/admin/change_pwd">>, <<"POST">>)
    ),
    %% API key paths — denied
    ?assertMatch(
        {error, unauthorized_role},
        emqx_mgmt_auth:check_scopes(Extra, <<"/api_key">>, <<"GET">>)
    ),

    %% Even with explicit scopes (e.g., [<<"clients">>]), denied paths are still blocked
    ExtraWithScopes = #{role => ?ROLE_API_SUPERUSER, scopes => [<<"clients">>]},
    ?assertMatch(
        {error, unauthorized_role},
        emqx_mgmt_auth:check_scopes(ExtraWithScopes, <<"/users">>, <<"GET">>)
    ),

    delete_app(Name).

t_check_scopes_unmapped_path(_Config) ->
    %% Paths not mapped to any scope should be allowed even with restricted scopes
    %% Use check_scopes/3 directly for this test
    Extra = #{role => ?ROLE_API_SUPERUSER, scopes => [<<"clients">>]},

    %% A path not in any scope mapping should be allowed
    %% Use a fake path that no API module registers
    ?assertEqual(ok, emqx_mgmt_auth:check_scopes(Extra, <<"/nonexistent/path">>, <<"GET">>)),

    %% But a path that IS mapped to a different scope should be denied
    ?assertMatch(
        {error, unauthorized_role},
        emqx_mgmt_auth:check_scopes(Extra, <<"/banned">>, <<"GET">>)
    ),

    %% Without scopes (undefined), all paths allowed
    ExtraNoScopes = #{role => ?ROLE_API_SUPERUSER},
    ?assertEqual(ok, emqx_mgmt_auth:check_scopes(ExtraNoScopes, <<"/banned">>, <<"GET">>)),
    ?assertEqual(ok, emqx_mgmt_auth:check_scopes(ExtraNoScopes, <<"/clients">>, <<"GET">>)).

%%--------------------------------------------------------------------
%% API endpoint tests
%%--------------------------------------------------------------------

t_api_list_scopes(_Config) ->
    AuthHeader = emqx_dashboard_SUITE:auth_header_(),
    Path = emqx_mgmt_api_test_util:api_path(["api_key", "scopes"]),
    {ok, Res} = emqx_mgmt_api_test_util:request_api(get, Path, AuthHeader),
    Body = emqx_utils_json:decode(Res),
    %% New response format: #{groups => [...], scopes => [...]}
    ?assertMatch(#{<<"groups">> := _, <<"scopes">> := _}, Body),
    Scopes = maps:get(<<"scopes">>, Body),
    ?assert(is_list(Scopes)),
    ?assert(length(Scopes) > 0),
    %% Each scope entry should have name and paths
    lists:foreach(
        fun(Scope) ->
            ?assertMatch(#{<<"name">> := _, <<"paths">> := _}, Scope),
            ?assert(is_binary(maps:get(<<"name">>, Scope))),
            ?assert(is_list(maps:get(<<"paths">>, Scope)))
        end,
        Scopes
    ).

t_api_list_scopes_with_groups(_Config) ->
    AuthHeader = emqx_dashboard_SUITE:auth_header_(),
    Path = emqx_mgmt_api_test_util:api_path(["api_key", "scopes"]),
    {ok, Res} = emqx_mgmt_api_test_util:request_api(get, Path, AuthHeader),
    Body = emqx_utils_json:decode(Res),
    Groups = maps:get(<<"groups">>, Body),
    ?assert(is_list(Groups)),
    ?assert(length(Groups) >= 10),
    %% Each group should have name, desc, scopes
    lists:foreach(
        fun(Group) ->
            ?assertMatch(#{<<"name">> := _, <<"desc">> := _, <<"scopes">> := _}, Group),
            ?assert(is_binary(maps:get(<<"name">>, Group))),
            ?assert(is_binary(maps:get(<<"desc">>, Group))),
            GroupScopes = maps:get(<<"scopes">>, Group),
            ?assert(is_list(GroupScopes)),
            ?assert(length(GroupScopes) > 0)
        end,
        Groups
    ).

t_api_create_with_scopes(_Config) ->
    Name = <<"SCOPES-API-CREATE">>,
    %% Get a valid scope name from the API
    AuthHeader = emqx_dashboard_SUITE:auth_header_(),
    ScopesPath = emqx_mgmt_api_test_util:api_path(["api_key", "scopes"]),
    {ok, ScopesRes} = emqx_mgmt_api_test_util:request_api(get, ScopesPath, AuthHeader),
    #{<<"scopes">> := ScopesList} = emqx_utils_json:decode(ScopesRes),
    [#{<<"name">> := ScopeName} | _] = ScopesList,

    %% Create API key with scopes
    {ok, Created} = create_app(Name, #{scopes => [ScopeName]}),
    ?assertMatch(#{<<"name">> := Name, <<"scopes">> := [ScopeName]}, Created),

    %% Read back and verify scopes are persisted
    {ok, ReadBack} = read_app(Name),
    ?assertMatch(#{<<"scopes">> := [ScopeName]}, ReadBack),
    delete_app(Name).

t_api_update_scopes(_Config) ->
    Name = <<"SCOPES-API-UPDATE">>,
    %% Create without scopes first
    {ok, Created} = create_app(Name),
    %% Should not have scopes field (or it's absent)
    ?assertEqual(false, maps:is_key(<<"scopes">>, Created)),

    %% Get a valid scope name
    AuthHeader = emqx_dashboard_SUITE:auth_header_(),
    ScopesPath = emqx_mgmt_api_test_util:api_path(["api_key", "scopes"]),
    {ok, ScopesRes} = emqx_mgmt_api_test_util:request_api(get, ScopesPath, AuthHeader),
    #{<<"scopes">> := AllScopesList} = emqx_utils_json:decode(ScopesRes),
    [#{<<"name">> := Scope1} | Rest] = AllScopesList,

    %% Update with scopes
    {ok, Updated1} = update_app(Name, #{scopes => [Scope1]}),
    ?assertMatch(#{<<"scopes">> := [Scope1]}, Updated1),

    %% Update to multiple scopes if more than one available
    case Rest of
        [#{<<"name">> := Scope2} | _] ->
            {ok, Updated2} = update_app(Name, #{scopes => [Scope1, Scope2]}),
            ?assertEqual(
                lists:sort([Scope1, Scope2]), lists:sort(maps:get(<<"scopes">>, Updated2))
            );
        [] ->
            %% Only one scope available, just verify single scope works
            ok
    end,

    %% Update to empty scopes
    {ok, Updated3} = update_app(Name, #{scopes => []}),
    ?assertMatch(#{<<"scopes">> := []}, Updated3),

    delete_app(Name).

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

auth_authorize(RelPath, Key, Secret) ->
    %% Build a fake cowboy-compatible request map.
    %% The authorize/4 flow:
    %%   1. First arg (_HandlerInfo) — use a dummy value that doesn't match any blocked pattern
    %%   2. check_rbac uses cowboy_req:method(Req) and cowboy_req:path(Req)
    %%   3. check_scopes uses cowboy_req:path(Req) -> get_relative_uri -> check against scopes
    %% So we need the Req path to be the *absolute* API path (with /api/v5 prefix)
    RelPathStr = binary_to_list(RelPath),
    AbsPath = erlang:list_to_binary(emqx_dashboard_swagger:relative_uri(RelPathStr)),
    FakeReq = #{method => <<"GET">>, path => AbsPath},
    emqx_mgmt_auth:authorize(dummy_handler, FakeReq, Key, Secret).

create_app(Name) ->
    create_app(Name, #{}).

create_app(Name, Extra) ->
    AuthHeader = emqx_dashboard_SUITE:auth_header_(),
    Path = emqx_mgmt_api_test_util:api_path(["api_key"]),
    ExpiredAt = to_rfc3339(erlang:system_time(second) + 1000),
    App = Extra#{
        name => Name,
        expired_at => ExpiredAt,
        desc => <<"Test scopes"/utf8>>,
        enable => true
    },
    case emqx_mgmt_api_test_util:request_api(post, Path, "", AuthHeader, App) of
        {ok, Res} -> {ok, emqx_utils_json:decode(Res)};
        Error -> Error
    end.

read_app(Name) ->
    AuthHeader = emqx_dashboard_SUITE:auth_header_(),
    Path = emqx_mgmt_api_test_util:api_path(["api_key", Name]),
    case emqx_mgmt_api_test_util:request_api(get, Path, AuthHeader) of
        {ok, Res} -> {ok, emqx_utils_json:decode(Res)};
        Error -> Error
    end.

delete_app(Name) ->
    AuthHeader = emqx_dashboard_SUITE:auth_header_(),
    DeletePath = emqx_mgmt_api_test_util:api_path(["api_key", Name]),
    emqx_mgmt_api_test_util:request_api(delete, DeletePath, AuthHeader).

update_app(Name, Change) ->
    AuthHeader = emqx_dashboard_SUITE:auth_header_(),
    UpdatePath = emqx_mgmt_api_test_util:api_path(["api_key", Name]),
    case emqx_mgmt_api_test_util:request_api(put, UpdatePath, "", AuthHeader, Change) of
        {ok, Update} -> {ok, emqx_utils_json:decode(Update)};
        Error -> Error
    end.

to_rfc3339(Sec) ->
    list_to_binary(calendar:system_time_to_rfc3339(Sec)).

-else.
%% CE edition: scopes are an EE-only feature, no tests needed
all() -> [].
-endif.
