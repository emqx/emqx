%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_mgmt_api_api_keys_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx_dashboard/include/emqx_dashboard_rbac.hrl").
-include_lib("emqx_utils/include/emqx_api_key_scopes.hrl").

-if(?EMQX_RELEASE_EDITION == ee).
-define(EE_CASES, [
    t_ee_create,
    t_ee_update,
    t_ee_authorize_viewer,
    t_ee_authorize_admin,
    t_ee_authorize_admin_cannot_manage_mfa,
    t_ee_authorize_admin_cannot_manage_mfa_module_level,
    t_ee_authorize_publisher,
    %% Schema validation: publisher role can only hold publish scope
    t_ee_publisher_only_publish_scope,
    t_ee_publisher_with_undefined_scopes,
    t_ee_publisher_with_empty_scopes,
    t_ee_publisher_rejects_connections_scope,
    t_ee_publisher_rejects_login_only_scope,
    t_ee_publisher_rejects_publish_plus_other,
    %% Schema validation: API keys cannot hold login-only scopes
    t_ee_api_key_rejects_login_only_via_post,
    t_ee_api_key_rejects_login_only_via_put,
    t_ee_admin_role_with_publish_scope_allowed,
    t_ee_viewer_role_with_publish_scope_allowed,
    %% Regression: API key auth must not leak into dashboard
    %% login-user scope check when ApiKey name == dashboard username.
    t_ee_api_key_unaffected_by_colliding_username_scopes
]).
-else.
-define(EE_CASES, []).
-endif.

-define(APP, emqx_app).

-record(?APP, {
    name = <<>> :: binary() | '_',
    api_key = <<>> :: binary() | '_',
    api_secret_hash = <<>> :: binary() | '_',
    enable = true :: boolean() | '_',
    desc = <<>> :: binary() | '_',
    expired_at = 0 :: integer() | undefined | infinity | '_',
    created_at = 0 :: integer() | '_'
}).

all() -> [{group, parallel}, {group, sequence}].
suite() -> [{timetrap, {minutes, 1}}].
groups() ->
    [
        {parallel, [parallel], [t_create, t_update, t_delete, t_authorize, t_create_unexpired_app]},
        {parallel, [parallel], ?EE_CASES},
        {sequence, [], [
            t_bootstrap_file,
            t_bootstrap_file_with_role,
            t_bootstrap_file_with_scopes,
            t_bootstrap_file_with_scopes_invalid,
            t_bootstrap_file_with_empty_scopes,
            t_bootstrap_file_with_mixed_case_scopes,
            t_bootstrap_file_with_whitespace_scopes,
            t_bootstrap_file_with_duplicate_scopes,
            t_bootstrap_file_lenient_order_independence,
            t_bootstrap_file_scope_runtime_check,
            t_bootstrap_file_publisher_only_publish_scope,
            t_create_failed
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

%% Bootstrap-file tests insert records into the api_key Mnesia table
%% without cleanup; the historical sequence group relies on disjoint key
%% prefixes to avoid leaks. Wipe every `from_bootstrap_file_*' record and
%% reset the configured `bootstrap_file' before and after each
%% `t_bootstrap_*' case so a future testcase rename or reorder cannot
%% silently inherit state from the previous one.
init_per_testcase(Case, Config) ->
    case is_bootstrap_case(Case) of
        true -> reset_bootstrap_state();
        false -> ok
    end,
    Config.

end_per_testcase(Case, _Config) ->
    case is_bootstrap_case(Case) of
        true -> reset_bootstrap_state();
        false -> ok
    end,
    ok.

is_bootstrap_case(Case) ->
    case atom_to_list(Case) of
        "t_bootstrap_" ++ _ -> true;
        _ -> false
    end.

reset_bootstrap_state() ->
    %% Drop the configured bootstrap path so we are not racing a stale
    %% post_config_update reload during cleanup.
    _ = emqx:update_config([<<"api_key">>], #{<<"bootstrap_file">> => <<>>}),
    %% Delete every key that was bootstrap-loaded; HTTP-API-created keys
    %% (used by t_create / t_authorize / t_ee_*) live under different
    %% names and stay untouched.
    Apps = emqx_mgmt_auth:list(),
    lists:foreach(
        fun(#{name := Name}) ->
            case Name of
                <<"from_bootstrap_file_", _/binary>> ->
                    _ = emqx_mgmt_auth:delete(Name);
                _ ->
                    ok
            end
        end,
        Apps
    ),
    ok.

t_bootstrap_file(_) ->
    TestPath = <<"/api/v5/status">>,
    Bin = <<"test-1:secret-1\ntest-2:secret-2">>,
    File = "./bootstrap_api_keys.txt",
    ok = file:write_file(File, Bin),
    update_file(File),
    ?assertEqual(ok, auth_authorize(TestPath, <<"test-1">>, <<"secret-1">>)),
    ?assertEqual(ok, auth_authorize(TestPath, <<"test-2">>, <<"secret-2">>)),
    ?assertMatch({error, _}, auth_authorize(TestPath, <<"test-2">>, <<"secret-1">>)),

    %% relaunch to check if the table is changed.
    Bin1 = <<"test-1:new-secret-1\ntest-2:new-secret-2">>,
    ok = file:write_file(File, Bin1),
    update_file(File),
    ?assertMatch({error, _}, auth_authorize(TestPath, <<"test-1">>, <<"secret-1">>)),
    ?assertMatch({error, _}, auth_authorize(TestPath, <<"test-2">>, <<"secret-2">>)),
    ?assertEqual(ok, auth_authorize(TestPath, <<"test-1">>, <<"new-secret-1">>)),
    ?assertEqual(ok, auth_authorize(TestPath, <<"test-2">>, <<"new-secret-2">>)),

    %% not error when bootstrap_file is empty
    update_file(<<>>),
    update_file("./bootstrap_apps_not_exist.txt"),
    ?assertMatch({error, _}, auth_authorize(TestPath, <<"test-1">>, <<"secret-1">>)),
    ?assertMatch({error, _}, auth_authorize(TestPath, <<"test-2">>, <<"secret-2">>)),
    ?assertEqual(ok, auth_authorize(TestPath, <<"test-1">>, <<"new-secret-1">>)),
    ?assertEqual(ok, auth_authorize(TestPath, <<"test-2">>, <<"new-secret-2">>)),

    %% bad format
    BadBin = <<"test-1:secret-11\ntest-2 secret-12">>,
    ok = file:write_file(File, BadBin),
    update_file(File),
    ?assertMatch({error, #{reason := "invalid_format"}}, emqx_mgmt_auth:init_bootstrap_file(File)),
    ?assertEqual(ok, auth_authorize(TestPath, <<"test-1">>, <<"secret-11">>)),
    ?assertMatch({error, _}, auth_authorize(TestPath, <<"test-2">>, <<"secret-12">>)),
    update_file(<<>>),

    %% skip the empty line
    Bin2 = <<"test-3:new-secret-1\n\n\n   \ntest-4:new-secret-2">>,
    ok = file:write_file(File, Bin2),
    update_file(File),
    ?assertMatch({error, _}, auth_authorize(TestPath, <<"test-3">>, <<"secret-1">>)),
    ?assertMatch({error, _}, auth_authorize(TestPath, <<"test-4">>, <<"secret-2">>)),
    ?assertEqual(ok, auth_authorize(TestPath, <<"test-3">>, <<"new-secret-1">>)),
    ?assertEqual(ok, auth_authorize(TestPath, <<"test-4">>, <<"new-secret-2">>)),
    ok.

t_bootstrap_file_override(_) ->
    TestPath = <<"/api/v5/status">>,
    Bin =
        <<"test-1:secret-1\ntest-1:duplicated-secret-1\ntest-2:secret-2\ntest-2:duplicated-secret-2">>,
    File = "./bootstrap_api_keys.txt",
    ok = file:write_file(File, Bin),
    update_file(File),

    ?assertEqual(ok, emqx_mgmt_auth:init_bootstrap_file(File)),

    MatchFun = fun(ApiKey) -> mnesia:match_object(#?APP{api_key = ApiKey, _ = '_'}) end,
    ?assertMatch(
        {ok, [
            #?APP{
                name = <<"from_bootstrap_file_18926f94712af04e">>,
                api_key = <<"test-1">>
            }
        ]},
        emqx_mgmt_auth:trans(MatchFun, [<<"test-1">>])
    ),
    ?assertEqual(ok, emqx_mgmt_auth:authorize(TestPath, <<"test-1">>, <<"duplicated-secret-1">>)),

    ?assertMatch(
        {ok, [
            #?APP{
                name = <<"from_bootstrap_file_de1c28a2e610e734">>,
                api_key = <<"test-2">>
            }
        ]},
        emqx_mgmt_auth:trans(MatchFun, [<<"test-2">>])
    ),
    ?assertEqual(ok, emqx_mgmt_auth:authorize(TestPath, <<"test-2">>, <<"duplicated-secret-2">>)),
    ok.

t_bootstrap_file_dup_override(_) ->
    TestPath = <<"/api/v5/status">>,
    TestApiKey = <<"test-1">>,
    Bin = <<"test-1:secret-1">>,
    File = "./bootstrap_api_keys.txt",
    ok = file:write_file(File, Bin),
    update_file(File),
    ?assertEqual(ok, emqx_mgmt_auth:init_bootstrap_file(File)),

    SameAppWithDiffName = #?APP{
        name = <<"name-1">>,
        api_key = <<"test-1">>,
        api_secret_hash = emqx_dashboard_admin:hash(<<"duplicated-secret-1">>),
        enable = true,
        desc = <<"dup api key">>,
        created_at = erlang:system_time(second),
        expired_at = infinity
    },
    WriteFun = fun(App) -> mnesia:write(App) end,
    MatchFun = fun(ApiKey) -> mnesia:match_object(#?APP{api_key = ApiKey, _ = '_'}) end,

    ?assertEqual({ok, ok}, emqx_mgmt_auth:trans(WriteFun, [SameAppWithDiffName])),
    %% as erlang term order
    ?assertMatch(
        {ok, [
            #?APP{
                name = <<"name-1">>,
                api_key = <<"test-1">>
            },
            #?APP{
                name = <<"from_bootstrap_file_18926f94712af04e">>,
                api_key = <<"test-1">>
            }
        ]},
        emqx_mgmt_auth:trans(MatchFun, [TestApiKey])
    ),

    update_file(File),

    %% Similar to loading bootstrap file at node startup
    %% the duplicated apikey in mnesia will be cleaned up
    ?assertEqual(ok, emqx_mgmt_auth:init_bootstrap_file(File)),
    ?assertMatch(
        {ok, [
            #?APP{
                name = <<"from_bootstrap_file_18926f94712af04e">>,
                api_key = <<"test-1">>
            }
        ]},
        emqx_mgmt_auth:trans(MatchFun, [<<"test-1">>])
    ),

    %% the last apikey in bootstrap file will override the all in mnesia and the previous one(s) in bootstrap file
    ?assertEqual(ok, emqx_mgmt_auth:authorize(TestPath, <<"test-1">>, <<"secret-1">>)),

    ok.

-if(?EMQX_RELEASE_EDITION == ee).
t_bootstrap_file_with_role(_) ->
    Search = fun(Name) ->
        lists:search(
            fun(#{api_key := AppName}) ->
                AppName =:= Name
            end,
            emqx_mgmt_auth:list()
        )
    end,

    Bin = <<"role-1:role-1:viewer\nrole-2:role-2:administrator\nrole-3:role-3">>,
    File = "./bootstrap_api_keys.txt",
    ok = file:write_file(File, Bin),
    update_file(File),

    ?assertMatch(
        {value, #{api_key := <<"role-1">>, role := <<"viewer">>}},
        Search(<<"role-1">>)
    ),

    ?assertMatch(
        {value, #{api_key := <<"role-2">>, role := <<"administrator">>}},
        Search(<<"role-2">>)
    ),

    ?assertMatch(
        {value, #{api_key := <<"role-3">>, role := <<"administrator">>}},
        Search(<<"role-3">>)
    ),

    %% bad role
    BadBin = <<"role-4:secret-11:bad\n">>,
    ok = file:write_file(File, BadBin),
    update_file(File),
    ?assertEqual(
        false,
        Search(<<"role-4">>)
    ),
    ok.
-else.
t_bootstrap_file_with_role(_) ->
    Search = fun(Name) ->
        lists:search(
            fun(#{api_key := AppName}) ->
                AppName =:= Name
            end,
            emqx_mgmt_auth:list()
        )
    end,

    Bin = <<"role-1:role-1:administrator\nrole-2:role-2">>,
    File = "./bootstrap_api_keys.txt",
    ok = file:write_file(File, Bin),
    update_file(File),

    ?assertMatch(
        {value, #{api_key := <<"role-1">>, role := <<"administrator">>}},
        Search(<<"role-1">>)
    ),

    ?assertMatch(
        {value, #{api_key := <<"role-2">>, role := <<"administrator">>}},
        Search(<<"role-2">>)
    ),

    %% only administrator
    OtherRoleBin = <<"role-3:role-3:viewer\n">>,
    ok = file:write_file(File, OtherRoleBin),
    update_file(File),
    ?assertEqual(
        false,
        Search(<<"role-3">>)
    ),

    %% bad role
    BadBin = <<"role-4:secret-11:bad\n">>,
    ok = file:write_file(File, BadBin),
    update_file(File),
    ?assertEqual(
        false,
        Search(<<"role-4">>)
    ),
    ok.
-endif.

%%--------------------------------------------------------------------
%% Bootstrap file: 4-segment scope format `key:secret:role:scope1,scope2`
%%--------------------------------------------------------------------

%% Read the `scopes` value stored on a bootstrapped api key.
%% Returns:
%%   `not_found' — no record for this api key
%%   `not_set'   — record exists but `scopes' is absent (back-compat:
%%                  behaves as "all non-denied paths allowed")
%%   `[binary()]' — explicit scope list
read_bootstrap_scopes(ApiKey) ->
    case
        lists:search(
            fun(#{api_key := K}) -> K =:= ApiKey end,
            emqx_mgmt_auth:list()
        )
    of
        false ->
            not_found;
        {value, #{scopes := Scopes}} when is_list(Scopes) ->
            Scopes;
        {value, _AppMap} ->
            not_set
    end.

t_bootstrap_file_with_scopes(_) ->
    File = "./bootstrap_api_keys.txt",
    %% Single scope, multiple scopes, and the special "all-allow" 3-segment
    %% baseline mixed in to ensure they coexist.
    Bin = iolist_to_binary([
        "scope-single:secret-1:administrator:",
        ?SCOPE_CONNECTIONS,
        "\n",
        "scope-multi:secret-2:administrator:",
        ?SCOPE_CONNECTIONS,
        ",",
        ?SCOPE_PUBLISH,
        ",",
        ?SCOPE_MONITORING,
        "\n",
        "scope-no-field:secret-3:administrator\n"
    ]),
    ok = file:write_file(File, Bin),
    update_file(File),

    ?assertEqual([?SCOPE_CONNECTIONS], read_bootstrap_scopes(<<"scope-single">>)),
    ?assertEqual(
        [?SCOPE_CONNECTIONS, ?SCOPE_PUBLISH, ?SCOPE_MONITORING],
        read_bootstrap_scopes(<<"scope-multi">>)
    ),
    %% 3-segment line: extra has no `scopes` key → backward-compatible all-allow.
    ?assertEqual(not_set, read_bootstrap_scopes(<<"scope-no-field">>)),
    ok.

t_bootstrap_file_with_scopes_invalid(_) ->
    File = "./bootstrap_api_keys.txt",

    %% Lenient policy: an unknown scope name is dropped with a warning log,
    %% but the entry IS created (with whatever valid scopes remain). This
    %% lets ops fix typos via the UI later instead of bouncing the whole
    %% bootstrap load.

    %% Mix of one unknown and two valid scopes — only the valid pair should
    %% remain on the api_key record.
    MixedBin = iolist_to_binary([
        "scope-mixed:secret-1:administrator:bogus_scope,",
        ?SCOPE_CONNECTIONS,
        ",",
        ?SCOPE_PUBLISH,
        "\n"
    ]),
    ok = file:write_file(File, MixedBin),
    update_file(File),
    ?assertEqual(
        [?SCOPE_CONNECTIONS, ?SCOPE_PUBLISH],
        read_bootstrap_scopes(<<"scope-mixed">>)
    ),

    %% The internal `$denied` scope is NOT in scope_catalog/0; if a user
    %% writes it the loader must drop it like any other unknown name.
    DeniedBin = iolist_to_binary([
        "scope-denied:secret-2:administrator:$denied,", ?SCOPE_AUDIT, "\n"
    ]),
    ok = file:write_file(File, DeniedBin),
    update_file(File),
    ?assertEqual([?SCOPE_AUDIT], read_bootstrap_scopes(<<"scope-denied">>)),

    %% Bad line followed by a good line — both must be loaded. The bad line
    %% becomes a deny-all key (scopes=[]), the good line stays intact.
    GoodAfterBadBin = iolist_to_binary([
        "scope-all-bad:secret-3:administrator:bogus_scope,foo\n",
        "scope-good-after:secret-4:administrator:",
        ?SCOPE_CONNECTIONS,
        "\n"
    ]),
    ok = file:write_file(File, GoodAfterBadBin),
    update_file(File),
    ?assertEqual([], read_bootstrap_scopes(<<"scope-all-bad">>)),
    ?assertEqual(
        [?SCOPE_CONNECTIONS],
        read_bootstrap_scopes(<<"scope-good-after">>)
    ),
    ok.

t_bootstrap_file_with_empty_scopes(_) ->
    File = "./bootstrap_api_keys.txt",
    %% Trailing colon with empty/whitespace-only 4th field is the explicit
    %% "no scopes" form: the loader stores `scopes => []' on the record.
    %% At runtime this denies every mapped path but still allows unmapped
    %% public endpoints, so ops can reach the node and reconfigure the key
    %% via the UI.
    Bin = iolist_to_binary([
        "scope-trailing:secret-1:administrator:\n",
        %% Whitespace-only payload — the line-level `string:trim/1' inside
        %% `read_line/1' strips the trailing spaces before the regex sees
        %% the line, so this is equivalent to the trailing-colon form.
        "scope-spaces:secret-2:administrator:   \n"
    ]),
    ok = file:write_file(File, Bin),
    update_file(File),

    ?assertEqual([], read_bootstrap_scopes(<<"scope-trailing">>)),
    ?assertEqual([], read_bootstrap_scopes(<<"scope-spaces">>)),
    ok.

%% Users will write capitalised scope names ("Connections") because typed
%% lists tend to look "more proper". `parse_scopes_str/1' lowercases each
%% token so the catalog lookup matches; without this normalization the
%% scope would be silently dropped by `filter_valid_scopes/1' (the entry
%% would still be created with no usable scopes — the worst kind of
%% confusion). Pin the lowercase contract here so it cannot regress.
t_bootstrap_file_with_mixed_case_scopes(_) ->
    File = "./bootstrap_api_keys.txt",
    Bin = iolist_to_binary([
        %% Mixed case + leading capital + ALL CAPS — every variant must
        %% normalise to the lowercase catalog name.
        "scope-mixed-case:secret-1:administrator:Connections,PUBLISH,Monitoring\n"
    ]),
    ok = file:write_file(File, Bin),
    update_file(File),
    ?assertEqual(
        [?SCOPE_CONNECTIONS, ?SCOPE_PUBLISH, ?SCOPE_MONITORING],
        read_bootstrap_scopes(<<"scope-mixed-case">>)
    ),
    ok.

%% Tokens are split on `,' with `trim_all', and each surviving element is
%% then `string:trim/1'-ed individually. Heredocs and copy-pasted YAML
%% routinely sneak whitespace into the scopes column; verify it lands
%% cleanly in the stored list.
t_bootstrap_file_with_whitespace_scopes(_) ->
    File = "./bootstrap_api_keys.txt",
    Bin = iolist_to_binary([
        "scope-ws:secret-1:administrator:  ",
        ?SCOPE_CONNECTIONS,
        "  ,   ",
        ?SCOPE_PUBLISH,
        "   \n"
    ]),
    ok = file:write_file(File, Bin),
    update_file(File),
    ?assertEqual(
        [?SCOPE_CONNECTIONS, ?SCOPE_PUBLISH],
        read_bootstrap_scopes(<<"scope-ws">>)
    ),
    ok.

%% Duplicate scope tokens are NOT collapsed by the loader. `lists:member'
%% in `check_path_in_scopes/2' tolerates duplicates at runtime, so the
%% loader keeps the user-supplied list verbatim (matching the HTTP API
%% contract). Pin this so a future "helpful" dedup does not silently
%% change list ordering or stored shape.
t_bootstrap_file_with_duplicate_scopes(_) ->
    File = "./bootstrap_api_keys.txt",
    Bin = iolist_to_binary([
        "scope-dup:secret-1:administrator:",
        ?SCOPE_CONNECTIONS,
        ",",
        ?SCOPE_CONNECTIONS,
        ",",
        ?SCOPE_PUBLISH,
        "\n"
    ]),
    ok = file:write_file(File, Bin),
    update_file(File),
    ?assertEqual(
        [?SCOPE_CONNECTIONS, ?SCOPE_CONNECTIONS, ?SCOPE_PUBLISH],
        read_bootstrap_scopes(<<"scope-dup">>)
    ),
    ok.

%% Lenient loading is order-independent: a line with bad-and-good scopes
%% before a fully-valid line, OR a fully-valid line before a bad line,
%% MUST both result in every entry being stored. The pre-lenient
%% implementation aborted the whole file on the first bad line, leaving
%% any subsequent good lines unloaded. Cover both orderings explicitly.
t_bootstrap_file_lenient_order_independence(_) ->
    File = "./bootstrap_api_keys.txt",

    BadFirst = iolist_to_binary([
        "scope-bad-1:secret-1:administrator:bogus,",
        ?SCOPE_CONNECTIONS,
        "\n",
        "scope-good-1:secret-2:administrator:",
        ?SCOPE_PUBLISH,
        "\n"
    ]),
    ok = file:write_file(File, BadFirst),
    update_file(File),
    ?assertEqual(
        [?SCOPE_CONNECTIONS],
        read_bootstrap_scopes(<<"scope-bad-1">>)
    ),
    ?assertEqual(
        [?SCOPE_PUBLISH],
        read_bootstrap_scopes(<<"scope-good-1">>)
    ),

    %% Reset between sub-cases so the second mixed-file write is its own
    %% fresh load (init_per_testcase only fires between cases, not
    %% sub-blocks).
    update_file(<<>>),

    GoodFirst = iolist_to_binary([
        "scope-good-2:secret-3:administrator:",
        ?SCOPE_AUDIT,
        "\n",
        "scope-bad-2:secret-4:administrator:bogus,",
        ?SCOPE_MONITORING,
        "\n"
    ]),
    ok = file:write_file(File, GoodFirst),
    update_file(File),
    ?assertEqual(
        [?SCOPE_AUDIT],
        read_bootstrap_scopes(<<"scope-good-2">>)
    ),
    ?assertEqual(
        [?SCOPE_MONITORING],
        read_bootstrap_scopes(<<"scope-bad-2">>)
    ),
    ok.

t_bootstrap_file_scope_runtime_check(_) ->
    File = "./bootstrap_api_keys.txt",
    %% Sanity-check that the path-to-scope cache is populated for the
    %% endpoints we exercise below — otherwise `path_to_scope/1' returns
    %% `undefined' and `check_path_in_scopes/2' would silently allow
    %% access regardless of the scope list, turning this test green for
    %% the wrong reason.
    ?assertEqual(
        ?SCOPE_CONNECTIONS,
        emqx_mgmt_api_key_scopes:path_to_scope(<<"/banned">>)
    ),
    ?assertEqual(
        ?SCOPE_PUBLISH,
        emqx_mgmt_api_key_scopes:path_to_scope(<<"/publish">>)
    ),
    ?assertEqual(
        ?SCOPE_SYSTEM,
        emqx_mgmt_api_key_scopes:path_to_scope(<<"/status">>)
    ),

    %% A single key scoped to `connections`. Endpoints that map to OTHER scopes
    %% (publish, monitoring, ...) must be denied for this key, while
    %% `connections` endpoints stay allowed.
    Bin = iolist_to_binary([
        "scope-conn-only:secret-1:administrator:", ?SCOPE_CONNECTIONS, "\n"
    ]),
    ok = file:write_file(File, Bin),
    update_file(File),

    %% connections
    BannedPath = <<"/api/v5/banned">>,
    %% publish
    PublishPath = <<"/api/v5/publish">>,
    %% system
    StatusPath = <<"/api/v5/status">>,

    ?assertEqual(
        ok,
        auth_authorize(BannedPath, <<"scope-conn-only">>, <<"secret-1">>)
    ),
    ?assertEqual(
        {error, unauthorized_role},
        auth_authorize(PublishPath, <<"scope-conn-only">>, <<"secret-1">>)
    ),
    ?assertEqual(
        {error, unauthorized_role},
        auth_authorize(StatusPath, <<"scope-conn-only">>, <<"secret-1">>)
    ),

    %% A key with no scopes field (3-segment entry) should reach all the
    %% non-denied paths, confirming the back-compat path.
    Bin2 = <<"scope-allow-all:secret-2:administrator\n">>,
    ok = file:write_file(File, Bin2),
    update_file(File),

    ?assertEqual(
        ok,
        auth_authorize(BannedPath, <<"scope-allow-all">>, <<"secret-2">>)
    ),
    ?assertEqual(
        ok,
        auth_authorize(PublishPath, <<"scope-allow-all">>, <<"secret-2">>)
    ),
    ?assertEqual(
        ok,
        auth_authorize(StatusPath, <<"scope-allow-all">>, <<"secret-2">>)
    ),

    %% A 4-segment entry with empty scopes (`scopes=[]'): mapped paths
    %% must be denied, but unmapped paths should still be reachable so an
    %% operator can fix the key via the UI.
    Bin3 = <<"scope-empty:secret-3:administrator:\n">>,
    ok = file:write_file(File, Bin3),
    update_file(File),

    %% Mapped paths under `connections' / `publish' scopes — denied.
    ?assertEqual(
        {error, unauthorized_role},
        auth_authorize(BannedPath, <<"scope-empty">>, <<"secret-3">>)
    ),
    ?assertEqual(
        {error, unauthorized_role},
        auth_authorize(PublishPath, <<"scope-empty">>, <<"secret-3">>)
    ),
    %% `/status' is mapped to `system' scope — also denied.
    ?assertEqual(
        {error, unauthorized_role},
        auth_authorize(StatusPath, <<"scope-empty">>, <<"secret-3">>)
    ),
    %% Unmapped path: there should not be any in the management app at the
    %% time this suite starts, but if `path_to_scope/1' returns `undefined'
    %% for a path the key with `scopes=[]' is allowed to reach it. We test
    %% that contract directly:
    ?assertEqual(
        ok,
        emqx_mgmt_auth:check_scopes(
            #{role => ?ROLE_API_SUPERUSER, scopes => []},
            <<"/an_unmapped_path_for_test">>,
            <<"GET">>
        )
    ),
    ok.

auth_authorize(Path, Key, Secret) ->
    %% Path is an absolute path like <<"/api/v5/status">>.
    %% Build a HandlerInfo map with relative path (strip base path prefix).
    RelPath =
        case emqx_dashboard_swagger:get_relative_uri(Path) of
            {ok, Rel} -> binary_to_list(Rel);
            _ -> binary_to_list(Path)
        end,
    %% FakeReq needs both method and path for check_rbac (which calls cowboy_req:path/method)
    FakeReq = #{method => <<"GET">>, path => Path},
    HandlerInfo = #{method => get, module => dummy_module, function => dummy_func, path => RelPath},
    emqx_mgmt_auth:authorize(HandlerInfo, FakeReq, Key, Secret).

update_file(File) ->
    ?assertMatch({ok, _}, emqx:update_config([<<"api_key">>], #{<<"bootstrap_file">> => File})).

t_create(_Config) ->
    Name = <<"EMQX-API-KEY-1">>,
    {ok, Create} = create_app(Name),
    ?assertMatch(
        #{
            <<"api_key">> := _,
            <<"api_secret">> := _,
            <<"created_at">> := _,
            <<"desc">> := _,
            <<"enable">> := true,
            <<"expired_at">> := _,
            <<"name">> := Name
        },
        Create
    ),
    {ok, List} = list_app(),
    [App] = lists:filter(fun(#{<<"name">> := NameA}) -> NameA =:= Name end, List),
    ?assertEqual(false, maps:is_key(<<"api_secret">>, App)),
    {ok, App1} = read_app(Name),
    ?assertEqual(Name, maps:get(<<"name">>, App1)),
    ?assertEqual(true, maps:get(<<"enable">>, App1)),
    ?assertEqual(false, maps:is_key(<<"api_secret">>, App1)),
    ?assertEqual({error, {"HTTP/1.1", 404, "Not Found"}}, read_app(<<"EMQX-API-KEY-NO-EXIST">>)),
    ok.

%% Bootstrap file: publisher role lenient policy — non-publish
%% scopes are dropped with a warning log; the entry IS created with
%% only the publish scope retained (or empty if no publish present).
%% This mirrors the API-layer schema validation but is non-fatal for
%% the whole bootstrap load.
t_bootstrap_file_publisher_only_publish_scope(_) ->
    File = "./bootstrap_api_keys.txt",
    Bin = iolist_to_binary([
        %% publisher with mixed scopes — only publish should survive.
        "from_bootstrap_file_pub_mixed:secret-1:publisher:",
        ?SCOPE_CONNECTIONS,
        ",",
        ?SCOPE_PUBLISH,
        ",",
        ?SCOPE_MONITORING,
        "\n",
        %% publisher with only non-publish scope — all dropped, ends
        %% up with empty scopes (which the runtime treats like the
        %% absent case for publisher: RBAC restricts to /publish).
        "from_bootstrap_file_pub_drop_all:secret-2:publisher:",
        ?SCOPE_CONNECTIONS,
        ",",
        ?SCOPE_AUDIT,
        "\n",
        %% publisher with publish only — passes through unchanged.
        "from_bootstrap_file_pub_publish_only:secret-3:publisher:",
        ?SCOPE_PUBLISH,
        "\n",
        %% Non-publisher (administrator) is NOT subject to the
        %% publisher restriction — all valid scopes retained.
        "from_bootstrap_file_admin_full:secret-4:administrator:",
        ?SCOPE_CONNECTIONS,
        ",",
        ?SCOPE_PUBLISH,
        ",",
        ?SCOPE_MONITORING,
        "\n"
    ]),
    ok = file:write_file(File, Bin),
    update_file(File),

    ?assertEqual(
        [?SCOPE_PUBLISH],
        read_bootstrap_scopes(<<"from_bootstrap_file_pub_mixed">>)
    ),
    ?assertEqual(
        [],
        read_bootstrap_scopes(<<"from_bootstrap_file_pub_drop_all">>)
    ),
    ?assertEqual(
        [?SCOPE_PUBLISH],
        read_bootstrap_scopes(<<"from_bootstrap_file_pub_publish_only">>)
    ),
    ?assertEqual(
        [?SCOPE_CONNECTIONS, ?SCOPE_PUBLISH, ?SCOPE_MONITORING],
        read_bootstrap_scopes(<<"from_bootstrap_file_admin_full">>)
    ),
    ok.

t_create_failed(_Config) ->
    BadRequest = {error, {"HTTP/1.1", 400, "Bad Request"}},

    ?assertEqual(BadRequest, create_app(<<" error format name">>)),
    LongName = iolist_to_binary(lists:duplicate(257, "A")),
    ?assertEqual(BadRequest, create_app(<<" error format name">>)),
    ?assertEqual(BadRequest, create_app(LongName)),

    {ok, List} = list_app(),
    CreateNum = 100 - erlang:length(List),
    Names = lists:map(
        fun(Seq) ->
            <<"EMQX-API-FAILED-KEY-", (integer_to_binary(Seq))/binary>>
        end,
        lists:seq(1, CreateNum)
    ),
    lists:foreach(fun(N) -> {ok, _} = create_app(N) end, Names),
    ?assertEqual(BadRequest, create_app(<<"EMQX-API-KEY-MAXIMUM">>)),

    lists:foreach(fun(N) -> {ok, _} = delete_app(N) end, Names),
    Name = <<"EMQX-API-FAILED-KEY-1">>,
    ?assertMatch({ok, _}, create_app(Name)),
    ?assertEqual(BadRequest, create_app(Name)),
    {ok, _} = delete_app(Name),
    ?assertMatch({ok, #{<<"name">> := Name}}, create_app(Name)),
    {ok, _} = delete_app(Name),
    ok.

t_update(_Config) ->
    Name = <<"EMQX-API-UPDATE-KEY">>,
    {ok, _} = create_app(Name),

    ExpiredAt = to_rfc3339(erlang:system_time(second) + 10000),
    Change = #{
        expired_at => ExpiredAt,
        desc => <<"NoteVersion1"/utf8>>,
        enable => false
    },
    {ok, Update1} = update_app(Name, Change),
    ?assertEqual(Name, maps:get(<<"name">>, Update1)),
    ?assertEqual(false, maps:get(<<"enable">>, Update1)),
    ?assertEqual(<<"NoteVersion1"/utf8>>, maps:get(<<"desc">>, Update1)),
    ?assertEqual(
        calendar:rfc3339_to_system_time(binary_to_list(ExpiredAt)),
        calendar:rfc3339_to_system_time(binary_to_list(maps:get(<<"expired_at">>, Update1)))
    ),
    Unexpired1 = maps:without([expired_at], Change),
    {ok, Update2} = update_app(Name, Unexpired1),
    ?assertEqual(<<"infinity">>, maps:get(<<"expired_at">>, Update2)),
    Unexpired2 = Change#{expired_at => <<"infinity">>},
    {ok, Update3} = update_app(Name, Unexpired2),
    ?assertEqual(<<"infinity">>, maps:get(<<"expired_at">>, Update3)),

    ?assertEqual({error, {"HTTP/1.1", 404, "Not Found"}}, update_app(<<"Not-Exist">>, Change)),
    ok.

t_delete(_Config) ->
    Name = <<"EMQX-API-DELETE-KEY">>,
    {ok, _Create} = create_app(Name),
    {ok, Delete} = delete_app(Name),
    ?assertEqual([], Delete),
    ?assertEqual({error, {"HTTP/1.1", 404, "Not Found"}}, delete_app(Name)),
    ok.

t_authorize(_Config) ->
    Name = <<"EMQX-API-AUTHORIZE-KEY">>,
    {ok, #{<<"api_key">> := ApiKey, <<"api_secret">> := ApiSecret}} = create_app(Name),
    BasicHeader = emqx_common_test_http:auth_header(
        binary_to_list(ApiKey),
        binary_to_list(ApiSecret)
    ),
    SecretError = emqx_common_test_http:auth_header(
        binary_to_list(ApiKey),
        binary_to_list(ApiKey)
    ),
    KeyError = emqx_common_test_http:auth_header("not_found_key", binary_to_list(ApiSecret)),
    Unauthorized = {error, {"HTTP/1.1", 401, "Unauthorized"}},

    BanPath = emqx_mgmt_api_test_util:api_path(["banned"]),
    ApiKeyPath = emqx_mgmt_api_test_util:api_path(["api_key"]),
    UserPath = emqx_mgmt_api_test_util:api_path(["users"]),
    DeleteUserPath1 = emqx_mgmt_api_test_util:api_path(["users", "some_user"]),
    DeleteUserPath2 = [emqx_mgmt_api_test_util:api_path([""]), "./users/some_user"],

    {ok, _Status} = emqx_mgmt_api_test_util:request_api(get, BanPath, BasicHeader),
    ?assertEqual(Unauthorized, emqx_mgmt_api_test_util:request_api(get, BanPath, KeyError)),
    ?assertEqual(Unauthorized, emqx_mgmt_api_test_util:request_api(get, BanPath, SecretError)),
    ?assertEqual(Unauthorized, emqx_mgmt_api_test_util:request_api(get, UserPath, BasicHeader)),

    ?assertEqual(
        Unauthorized, emqx_mgmt_api_test_util:request_api(delete, DeleteUserPath1, BasicHeader)
    ),
    %% We make request with hackney to avoid path normalization made by httpc.
    {ok, Code, _Headers0, _Body0} = hackney:request(delete, DeleteUserPath2, [BasicHeader], <<>>),
    ?assertEqual(401, Code),

    {error, {{"HTTP/1.1", 401, "Unauthorized"}, _Headers1, Body1}} =
        emqx_mgmt_api_test_util:request_api(
            get,
            ApiKeyPath,
            [],
            BasicHeader,
            [],
            #{return_all => true}
        ),
    ?assertMatch(
        #{
            <<"code">> := <<"API_KEY_NOT_ALLOW">>,
            <<"message">> := _
        },
        emqx_utils_json:decode(Body1, [return_maps])
    ),

    ?assertMatch(
        {ok, #{<<"api_key">> := _, <<"enable">> := false}},
        update_app(Name, #{enable => false})
    ),
    ?assertEqual(Unauthorized, emqx_mgmt_api_test_util:request_api(get, BanPath, BasicHeader)),

    Expired = #{
        expired_at => to_rfc3339(erlang:system_time(second) - 1),
        enable => true
    },
    ?assertMatch({ok, #{<<"api_key">> := _, <<"enable">> := true}}, update_app(Name, Expired)),
    ?assertEqual(Unauthorized, emqx_mgmt_api_test_util:request_api(get, BanPath, BasicHeader)),
    UnExpired = #{expired_at => infinity},
    ?assertMatch(
        {ok, #{<<"api_key">> := _, <<"expired_at">> := <<"infinity">>}},
        update_app(Name, UnExpired)
    ),
    {ok, _Status1} = emqx_mgmt_api_test_util:request_api(get, BanPath, BasicHeader),
    ok.

t_create_unexpired_app(_Config) ->
    Name1 = <<"EMQX-UNEXPIRED-API-KEY-1">>,
    Name2 = <<"EMQX-UNEXPIRED-API-KEY-2">>,
    {ok, Create1} = create_unexpired_app(Name1, #{}),
    ?assertMatch(#{<<"expired_at">> := <<"infinity">>}, Create1),
    {ok, Create2} = create_unexpired_app(Name2, #{expired_at => <<"infinity">>}),
    ?assertMatch(#{<<"expired_at">> := <<"infinity">>}, Create2),
    ok.

t_ee_create(_Config) ->
    Name = <<"EMQX-EE-API-KEY-1">>,
    {ok, Create} = create_app(Name, #{role => ?ROLE_API_VIEWER}),
    ?assertMatch(
        #{
            <<"api_key">> := _,
            <<"api_secret">> := _,
            <<"created_at">> := _,
            <<"desc">> := _,
            <<"enable">> := true,
            <<"expired_at">> := _,
            <<"name">> := Name,
            <<"role">> := ?ROLE_API_VIEWER
        },
        Create
    ),

    {ok, App} = read_app(Name),
    ?assertMatch(#{<<"name">> := Name, <<"role">> := ?ROLE_API_VIEWER}, App).

t_ee_update(_Config) ->
    Name = <<"EMQX-EE-API-UPDATE-KEY">>,
    {ok, _} = create_app(Name, #{role => ?ROLE_API_VIEWER}),

    Change = #{
        desc => <<"NoteVersion1"/utf8>>,
        enable => false,
        role => ?ROLE_API_SUPERUSER
    },
    {ok, Update1} = update_app(Name, Change),
    ?assertEqual(?ROLE_API_SUPERUSER, maps:get(<<"role">>, Update1)),

    {ok, App} = read_app(Name),
    ?assertMatch(#{<<"name">> := Name, <<"role">> := ?ROLE_API_SUPERUSER}, App).

t_ee_authorize_viewer(_Config) ->
    Name = <<"EMQX-EE-API-AUTHORIZE-KEY-VIEWER">>,
    {ok, #{<<"api_key">> := ApiKey, <<"api_secret">> := ApiSecret}} = create_app(Name, #{
        role => ?ROLE_API_VIEWER
    }),
    BasicHeader = emqx_common_test_http:auth_header(
        binary_to_list(ApiKey),
        binary_to_list(ApiSecret)
    ),

    BanPath = emqx_mgmt_api_test_util:api_path(["banned"]),
    ?assertMatch({ok, _}, emqx_mgmt_api_test_util:request_api(get, BanPath, BasicHeader)),
    ?assertMatch(
        {error, {_, 403, _}}, emqx_mgmt_api_test_util:request_api(delete, BanPath, BasicHeader)
    ).

t_ee_authorize_admin(_Config) ->
    Name = <<"EMQX-EE-API-AUTHORIZE-KEY-ADMIN">>,
    {ok, #{<<"api_key">> := ApiKey, <<"api_secret">> := ApiSecret}} = create_app(Name, #{
        role => ?ROLE_API_SUPERUSER
    }),
    BasicHeader = emqx_common_test_http:auth_header(
        binary_to_list(ApiKey),
        binary_to_list(ApiSecret)
    ),

    BanPath = emqx_mgmt_api_test_util:api_path(["banned"]),
    ?assertMatch({ok, _}, emqx_mgmt_api_test_util:request_api(get, BanPath, BasicHeader)),
    ?assertMatch(
        {ok, _}, emqx_mgmt_api_test_util:request_api(delete, BanPath, BasicHeader)
    ).

-doc """
An admin-role API key must NOT be able to reach the dashboard user-account
management endpoints change_mfa and change_pwd via HTTP Basic auth. These
endpoints belong to the human-facing dashboard surface and are intended
only for bearer-token (JWT) callers.

Before the fix, DELETE /api/v5/users/:username/mfa via API key Basic auth
returned HTTP 204 and silently disabled the target user's MFA. After the
fix it must return HTTP 401 with body code API_KEY_NOT_ALLOW, matching
the policy already enforced on /users and /users/:username.
""".
t_ee_authorize_admin_cannot_manage_mfa(_Config) ->
    Name = <<"EMQX-EE-API-AUTHORIZE-KEY-ADMIN-MFA">>,
    {ok, #{<<"api_key">> := ApiKey, <<"api_secret">> := ApiSecret}} = create_app(Name, #{
        role => ?ROLE_API_SUPERUSER
    }),
    BasicHeader = emqx_common_test_http:auth_header(
        binary_to_list(ApiKey),
        binary_to_list(ApiSecret)
    ),
    Victim = <<"mfa_victim_user">>,
    ok = ensure_victim_user(Victim),
    DeleteMfa = emqx_mgmt_api_test_util:api_path(["users", Victim, "mfa"]),
    PostMfa = DeleteMfa,
    ChangePwd = emqx_mgmt_api_test_util:api_path(["users", Victim, "change_pwd"]),

    ok = assert_api_key_not_allow(
        delete, DeleteMfa, [], BasicHeader, []
    ),
    ok = assert_api_key_not_allow(
        post, PostMfa, [], BasicHeader, #{mechanism => totp}
    ),
    ok = assert_api_key_not_allow(
        post,
        ChangePwd,
        [],
        BasicHeader,
        #{old_pwd => <<"mfa_victim_pass">>, new_pwd => <<"new_pass_123">>}
    ),

    %% Even the generic /users and /users/:username stay blocked — baseline
    %% sanity that this test is not special-casing change_mfa alone.
    UsersPath = emqx_mgmt_api_test_util:api_path(["users"]),
    UserPath = emqx_mgmt_api_test_util:api_path(["users", Victim]),
    ok = assert_api_key_not_allow(get, UsersPath, [], BasicHeader, []),
    ok = assert_api_key_not_allow(delete, UserPath, [], BasicHeader, []),
    ok.

-doc """
Lower-level companion to t_ee_authorize_admin_cannot_manage_mfa:
call emqx_mgmt_auth:authorize/4 directly with a HandlerInfo map that
names the dashboard change_mfa / change_pwd handlers. This pins the
contract at the exact clause being added in the fix, independent of
any HTTP / minirest plumbing.
""".
t_ee_authorize_admin_cannot_manage_mfa_module_level(_Config) ->
    Name = <<"EMQX-EE-API-AUTH-MFA-MODULE">>,
    {ok, #{<<"api_key">> := ApiKey, <<"api_secret">> := ApiSecret}} = create_app(Name, #{
        role => ?ROLE_API_SUPERUSER
    }),
    %% FakeReq is only consulted by check_rbac — the denylist clauses we
    %% add return before check_rbac runs, so it just needs to be a map
    %% with method/path keys to satisfy cowboy_req helpers if called.
    FakeReq = #{method => <<"DELETE">>, path => <<"/api/v5/users/someuser/mfa">>},
    DeleteMfaHandler = #{
        method => delete,
        module => emqx_dashboard_api,
        function => change_mfa,
        path => "/users/:username/mfa"
    },
    PostMfaHandler = DeleteMfaHandler#{method => post},
    ChangePwdHandler = #{
        method => post,
        module => emqx_dashboard_api,
        function => change_pwd,
        path => "/users/:username/change_pwd"
    },
    ?assertEqual(
        {error, <<"not_allowed">>, <<"users">>},
        emqx_mgmt_auth:authorize(DeleteMfaHandler, FakeReq, ApiKey, ApiSecret)
    ),
    ?assertEqual(
        {error, <<"not_allowed">>, <<"users">>},
        emqx_mgmt_auth:authorize(PostMfaHandler, FakeReq, ApiKey, ApiSecret)
    ),
    ?assertEqual(
        {error, <<"not_allowed">>, <<"users">>},
        emqx_mgmt_auth:authorize(ChangePwdHandler, FakeReq, ApiKey, ApiSecret)
    ),
    ok.

ensure_victim_user(Username) ->
    case emqx_dashboard_admin:add_user(Username, <<"mfa_victim_pass">>, ?ROLE_SUPERUSER, <<>>) of
        {ok, _} -> ok;
        {error, _AlreadyExists} -> ok
    end.

assert_api_key_not_allow(Method, Path, QueryParams, AuthHeader, Body) ->
    Result = emqx_mgmt_api_test_util:request_api(
        Method, Path, QueryParams, AuthHeader, Body, #{return_all => true}
    ),
    ?assertMatch(
        {error, {{"HTTP/1.1", 401, _}, _Headers, _Body}},
        Result
    ),
    {error, {_, _, ResponseBody}} = Result,
    ?assertMatch(
        #{<<"code">> := <<"API_KEY_NOT_ALLOW">>, <<"message">> := _},
        emqx_utils_json:decode(ResponseBody, [return_maps])
    ),
    ok.

t_ee_authorize_publisher(_Config) ->
    Name = <<"EMQX-EE-API-AUTHORIZE-KEY-PUBLISHER">>,
    {ok, #{<<"api_key">> := ApiKey, <<"api_secret">> := ApiSecret}} = create_app(Name, #{
        role => ?ROLE_API_PUBLISHER
    }),
    BasicHeader = emqx_common_test_http:auth_header(
        binary_to_list(ApiKey),
        binary_to_list(ApiSecret)
    ),

    BanPath = emqx_mgmt_api_test_util:api_path(["banned"]),
    Publish = emqx_mgmt_api_test_util:api_path(["publish"]),
    ?assertMatch(
        {error, {_, 403, _}}, emqx_mgmt_api_test_util:request_api(get, BanPath, BasicHeader)
    ),
    ?assertMatch(
        {error, {_, 403, _}}, emqx_mgmt_api_test_util:request_api(delete, BanPath, BasicHeader)
    ),
    ?_assertMatch(
        {ok, _},
        emqx_mgmt_api_test_util:request_api(
            post,
            Publish,
            [],
            BasicHeader,
            #{topic => <<"t/t_ee_authorize_publisher">>, payload => <<"hello">>}
        )
    ).

%%--------------------------------------------------------------------
%% Schema validation tests for API key scopes:
%%   * publisher role is restricted to the publish scope only;
%%   * API keys reject any login-only scope regardless of role.
%%--------------------------------------------------------------------

%% publisher role + scopes=[<<"publish">>] — accepted (the only
%% non-empty list a publisher can hold).
t_ee_publisher_only_publish_scope(_Config) ->
    Name = <<"EE-PUB-PUBLISH-ONLY">>,
    ?assertMatch(
        {ok, #{<<"name">> := Name}},
        create_app(Name, #{role => ?ROLE_API_PUBLISHER, scopes => [?SCOPE_PUBLISH]})
    ),
    delete_app(Name).

%% publisher role + scopes absent — accepted (falls back to RBAC
%% hardcoded path matching at runtime).
t_ee_publisher_with_undefined_scopes(_Config) ->
    Name = <<"EE-PUB-UNDEFINED-SCOPES">>,
    ?assertMatch(
        {ok, #{<<"name">> := Name}},
        create_app(Name, #{role => ?ROLE_API_PUBLISHER})
    ),
    delete_app(Name).

%% publisher role + scopes=[] — accepted (empty list is equivalent
%% to absent; runtime path check still applies).
t_ee_publisher_with_empty_scopes(_Config) ->
    Name = <<"EE-PUB-EMPTY-SCOPES">>,
    ?assertMatch(
        {ok, #{<<"name">> := Name}},
        create_app(Name, #{role => ?ROLE_API_PUBLISHER, scopes => []})
    ),
    delete_app(Name).

%% publisher role + scopes containing a non-publish scope — rejected
%% by validate_publisher_scopes/2 with HTTP 400.
t_ee_publisher_rejects_connections_scope(_Config) ->
    Name = <<"EE-PUB-CONN-REJECT">>,
    Result = create_app(Name, #{
        role => ?ROLE_API_PUBLISHER,
        scopes => [?SCOPE_CONNECTIONS]
    }),
    assert_400_publisher_only(Result).

%% publisher role + scopes containing a login-only scope — rejected.
%% This case exercises BOTH the publisher restriction (layer A) and
%% the login-only rejection (layer B); validate_scopes/2 returns the
%% first matching error, which is layer A here.
t_ee_publisher_rejects_login_only_scope(_Config) ->
    Name = <<"EE-PUB-LOGIN-ONLY-REJECT">>,
    Result = create_app(Name, #{
        role => ?ROLE_API_PUBLISHER,
        scopes => [?SCOPE_USER_MGMT]
    }),
    assert_400_publisher_only(Result).

%% publisher role + [publish, connections] — rejected. The publisher
%% scope check requires scopes to be exactly [<<"publish">>] or
%% empty/absent.
t_ee_publisher_rejects_publish_plus_other(_Config) ->
    Name = <<"EE-PUB-PUBLISH-PLUS-OTHER">>,
    Result = create_app(Name, #{
        role => ?ROLE_API_PUBLISHER,
        scopes => [?SCOPE_PUBLISH, ?SCOPE_CONNECTIONS]
    }),
    assert_400_publisher_only(Result).

%% Non-publisher API key (administrator) creation with login-only
%% scope — rejected with HTTP 400 from validate_no_login_only_scopes/1.
t_ee_api_key_rejects_login_only_via_post(_Config) ->
    Name = <<"EE-API-KEY-LOGIN-ONLY-POST">>,
    Result = create_app(Name, #{
        role => ?ROLE_API_SUPERUSER,
        scopes => [?SCOPE_CONNECTIONS, ?SCOPE_MFA_MGMT]
    }),
    assert_400_login_only(Result).

%% PUT /api_key/:name with login-only scope on an existing key —
%% rejected by the same validator.
t_ee_api_key_rejects_login_only_via_put(_Config) ->
    Name = <<"EE-API-KEY-LOGIN-ONLY-PUT">>,
    {ok, _} = create_app(Name, #{
        role => ?ROLE_API_SUPERUSER,
        scopes => [?SCOPE_CONNECTIONS]
    }),
    Result = update_app(Name, #{
        scopes => [?SCOPE_CONNECTIONS, ?SCOPE_USER_MGMT]
    }),
    assert_400_login_only(Result),
    delete_app(Name).

%% Administrator API key + scopes containing publish — accepted.
%% Administrator and viewer roles are NOT restricted on the publish
%% scope; only the publisher role is.
t_ee_admin_role_with_publish_scope_allowed(_Config) ->
    Name = <<"EE-ADMIN-WITH-PUBLISH">>,
    ?assertMatch(
        {ok, #{<<"name">> := Name}},
        create_app(Name, #{
            role => ?ROLE_API_SUPERUSER,
            scopes => [?SCOPE_PUBLISH, ?SCOPE_CONNECTIONS, ?SCOPE_MONITORING]
        })
    ),
    delete_app(Name).

%% Viewer role + publish scope — accepted at schema layer (RBAC will
%% reject the actual POST /publish at runtime, but the scope is
%% formally legal).
t_ee_viewer_role_with_publish_scope_allowed(_Config) ->
    Name = <<"EE-VIEWER-WITH-PUBLISH">>,
    ?assertMatch(
        {ok, #{<<"name">> := Name}},
        create_app(Name, #{
            role => ?ROLE_API_VIEWER,
            scopes => [?SCOPE_PUBLISH, ?SCOPE_MONITORING]
        })
    ),
    delete_app(Name).

%% H7: A partial-update PUT that changes the role to `publisher'
%% without supplying a `scopes' field must validate against the
%% persisted scopes, not against `undefined'. Otherwise an admin key
%% with non-`publish' scopes could be silently demoted to publisher
%% while keeping forbidden scopes — RBAC blocks the runtime path, but
%% the stored config and the API response would violate the
%% publisher-only-`publish' invariant.
t_ee_publisher_role_change_with_persisted_admin_scopes_is_rejected(_Config) ->
    Name = <<"EE-PUBLISHER-PERSISTED">>,
    {ok, _} = create_app(Name, #{
        role => ?ROLE_API_SUPERUSER,
        scopes => [?SCOPE_CONNECTIONS, ?SCOPE_MONITORING]
    }),
    %% Partial update: drop to publisher; omit `scopes' on the wire.
    Change = #{role => ?ROLE_API_PUBLISHER},
    ok = assert_400_publisher_only(update_app(Name, Change)),
    %% The persisted state must remain unchanged after the rejection.
    {ok, App} = read_app(Name),
    ?assertEqual(?ROLE_API_SUPERUSER, maps:get(<<"role">>, App)),
    ?assertEqual(
        [?SCOPE_CONNECTIONS, ?SCOPE_MONITORING], maps:get(<<"scopes">>, App)
    ),
    delete_app(Name).

%% Counterpart of H7 rejection: when persisted scopes are already
%% publisher-compatible (`[publish]'), a role change to publisher
%% without a body `scopes' field must succeed.
t_ee_publisher_role_change_with_compatible_persisted_scopes_succeeds(_Config) ->
    Name = <<"EE-PUBLISHER-COMPATIBLE">>,
    {ok, _} = create_app(Name, #{
        role => ?ROLE_API_SUPERUSER,
        scopes => [?SCOPE_PUBLISH]
    }),
    Change = #{role => ?ROLE_API_PUBLISHER},
    {ok, Updated} = update_app(Name, Change),
    ?assertEqual(?ROLE_API_PUBLISHER, maps:get(<<"role">>, Updated)),
    ?assertEqual([?SCOPE_PUBLISH], maps:get(<<"scopes">>, Updated)),
    delete_app(Name).

%% Regression: API key auth must NOT consult dashboard login-user
%% scopes, even when the API key's generated `api_key' string happens
%% to collide with a dashboard username. Prior to the fix,
%% emqx_dashboard_rbac:check_rbac/3 unconditionally invoked
%% check_login_user_scopes/2 after the role check, so a colliding
%% admin record's empty extra.scopes would falsely deny the API key.
%%
%% Construction: create the API key first, then back-add a dashboard
%% user whose username equals the generated api_key string. (Going
%% the other way is harder because api_key is server-generated.)
t_ee_api_key_unaffected_by_colliding_username_scopes(_Config) ->
    Name = <<"EE-APIKEY-COLLIDE">>,
    {ok, #{<<"api_key">> := ApiKey, <<"api_secret">> := ApiSecret}} =
        create_app(Name, #{
            role => ?ROLE_API_VIEWER,
            scopes => [?SCOPE_CONNECTIONS]
        }),
    %% Now create a dashboard user whose username equals the
    %% generated ApiKey string and assign it explicit empty scopes
    %% (which, if leaked into the API key auth path, would deny every
    %% mapped endpoint).
    {ok, _} = emqx_dashboard_admin:add_user(
        ApiKey, <<"P@ssw0rd">>, ?ROLE_VIEWER, <<>>
    ),
    {ok, ok} = emqx_dashboard_admin:set_user_scopes(ApiKey, []),
    %% The API key request must succeed: /clients maps to the
    %% connections scope, which the key holds. With the pre-fix bug,
    %% check_login_user_scopes(ApiKey, <<"/clients">>) would have
    %% resolved against the dashboard user's [] and returned false.
    Basic = emqx_common_test_http:auth_header(
        binary_to_list(ApiKey), binary_to_list(ApiSecret)
    ),
    Path = emqx_mgmt_api_test_util:api_path(["clients"]),
    ?assertMatch({ok, _}, emqx_mgmt_api_test_util:request_api(get, Path, Basic)),
    %% Cleanup
    delete_app(Name),
    _ = emqx_dashboard_admin:remove_user(ApiKey),
    ok.

assert_400_publisher_only(Result) ->
    %% request_api/5 returns {error, {"HTTP/1.1", 400, "Bad Request"}}
    %% on 4xx responses; the body is dropped unless return_all=true is
    %% set in opts. We only assert the status code here — the precise
    %% error message is covered by unit tests of validate_publisher_scopes.
    case Result of
        {error, {"HTTP/1.1", 400, _}} ->
            ok;
        Other ->
            ct:fail("expected 400 BAD_REQUEST publisher-only error, got: ~p", [Other])
    end.

assert_400_login_only(Result) ->
    %% Same shape — the body assertion is covered by the unit test
    %% of validate_no_login_only_scopes.
    case Result of
        {error, {"HTTP/1.1", 400, _}} ->
            ok;
        Other ->
            ct:fail("expected 400 BAD_REQUEST login-only error, got: ~p", [Other])
    end.

list_app() ->
    AuthHeader = emqx_dashboard_SUITE:auth_header_(),
    Path = emqx_mgmt_api_test_util:api_path(["api_key"]),
    case emqx_mgmt_api_test_util:request_api(get, Path, AuthHeader) of
        {ok, Apps} -> {ok, emqx_utils_json:decode(Apps)};
        Error -> Error
    end.

read_app(Name) ->
    AuthHeader = emqx_dashboard_SUITE:auth_header_(),
    Path = emqx_mgmt_api_test_util:api_path(["api_key", Name]),
    case emqx_mgmt_api_test_util:request_api(get, Path, AuthHeader) of
        {ok, Res} -> {ok, emqx_utils_json:decode(Res)};
        Error -> Error
    end.

create_app(Name) ->
    create_app(Name, #{}).

create_app(Name, Extra) ->
    AuthHeader = emqx_dashboard_SUITE:auth_header_(),
    Path = emqx_mgmt_api_test_util:api_path(["api_key"]),
    ExpiredAt = to_rfc3339(erlang:system_time(second) + 1000),
    App = Extra#{
        name => Name,
        expired_at => ExpiredAt,
        desc => <<"Note"/utf8>>,
        enable => true
    },
    case emqx_mgmt_api_test_util:request_api(post, Path, "", AuthHeader, App) of
        {ok, Res} -> {ok, emqx_utils_json:decode(Res)};
        Error -> Error
    end.

create_unexpired_app(Name, Params) ->
    AuthHeader = emqx_dashboard_SUITE:auth_header_(),
    Path = emqx_mgmt_api_test_util:api_path(["api_key"]),
    App = maps:merge(#{name => Name, desc => <<"Note"/utf8>>, enable => true}, Params),
    case emqx_mgmt_api_test_util:request_api(post, Path, "", AuthHeader, App) of
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
