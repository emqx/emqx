%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mgmt_api_data_backup_ns_SUITE).

-moduledoc """
Data backup REST API with namespaces: each namespaced administrator has an
isolated backup space, a global administrator can opt into a namespace, and
a global backup carries the configuration of every namespace.
Each case starts its own 3-node cluster.
""".

-compile(export_all).
-compile(nowarn_export_all).

-import(emqx_mgmt_api_data_backup_test_helpers, [
    do_init_per_testcase/2,
    download_backup/3,
    download_backup/4,
    import_backup_full/3,
    import_backup_ns/4,
    upload_backup_ns/4,
    local_ns_backup_copy/5,
    plant_ns_backup_file/4,
    forge_backup/3,
    upload_backup_full/4,
    export_backup2/3,
    upload_backup/3,
    wait_for_audit_entries/6,
    namespace_of/1,
    ns_admin_auth_header/4,
    ns_api_key_auth_header/1,
    list_filenames/2,
    list_filenames/3,
    delete_backup/3,
    delete_backup/4,
    backup_path/1
]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include("emqx_mgmt_api_data_backup_test.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Config.

end_per_suite(_) ->
    ok.

init_per_testcase(TC, Config) ->
    do_init_per_testcase(TC, Config).

end_per_testcase(_TC, Config) ->
    case ?config(cluster, Config) of
        undefined -> ok;
        Cluster -> emqx_cth_cluster:stop(Cluster)
    end.

%% Namespaced administrators get an isolated backup space under
%% `<backup>/ns/<Namespace>/'. Export, list, download and delete only ever act
%% on that space: a namespaced administrator cannot see or touch global (or
%% legacy) backups, nor another namespace's backups.
t_namespaced_backup_isolation(Config) ->
    [Core1, _Core2, _Repl] = ?config(cluster, Config),
    ApiAuth = ?config(auth, Config),
    DashboardAuth = ?config(dashboard_auth, Config),
    Ns1Auth = ?config(ns_admin_auth, Config),
    Ns2Auth = ns_admin_auth_header(Core1, <<"ns2">>, <<"ns2_admin_for_test">>, ?NS_ADMIN_PASS),

    %% Global export lands in the flat/global space; each namespace exports into
    %% its own space.
    {200, #{<<"filename">> := GFile}} = export_backup2(?NODE1_PORT, ApiAuth, #{}),
    {200, #{<<"filename">> := N1File}} = export_backup2(?NODE1_PORT, Ns1Auth, #{}),
    {200, #{<<"filename">> := N2File}} = export_backup2(?NODE1_PORT, Ns2Auth, #{}),

    %% Listing is scoped: each namespace sees only its own file.
    ?assertEqual([N1File], list_filenames(?NODE1_PORT, Ns1Auth)),
    ?assertEqual([N2File], list_filenames(?NODE1_PORT, Ns2Auth)),
    %% A namespaced API key resolves to the same isolated space as the
    %% namespaced dashboard administrator.
    Ns1ApiKeyAuth = ns_api_key_auth_header(Core1),
    ?assertEqual([N1File], list_filenames(?NODE1_PORT, Ns1ApiKeyAuth)),
    %% Global listing sees the global file, never the namespaced ones.
    GlobalFiles = list_filenames(?NODE1_PORT, DashboardAuth),
    ?assert(lists:member(GFile, GlobalFiles)),
    ?assertNot(lists:member(N1File, GlobalFiles)),
    ?assertNot(lists:member(N2File, GlobalFiles)),

    %% ns1 can download its own file, but not the global one nor ns2's --
    %% those are invisible in its space and resolve to 404.
    ?assertMatch({200, _}, download_backup(?NODE1_PORT, Ns1Auth, N1File)),
    ?assertMatch({404, _}, download_backup(?NODE1_PORT, Ns1Auth, GFile)),
    ?assertMatch({404, _}, download_backup(?NODE1_PORT, Ns1Auth, N2File)),

    %% ns1 cannot delete another space's file, but can delete its own.
    ?assertMatch({404, _}, delete_backup(?NODE1_PORT, Ns1Auth, GFile)),
    ?assertMatch({404, _}, delete_backup(?NODE1_PORT, Ns1Auth, N2File)),
    ?assertMatch({204, _}, delete_backup(?NODE1_PORT, Ns1Auth, N1File)),
    ?assertEqual([], list_filenames(?NODE1_PORT, Ns1Auth)),

    %% Global download of the global file still works (regression).
    ?assertMatch({200, _}, download_backup(?NODE1_PORT, DashboardAuth, GFile)),
    ok.

%% A namespaced administrator can round-trip its own configuration: export then
%% re-import, scoped entirely to its namespace.
t_namespaced_export_import(Config) ->
    Ns1Auth = ?config(ns_admin_auth, Config),
    {200, #{<<"filename">> := N1File}} = export_backup2(?NODE1_PORT, Ns1Auth, #{}),
    ?assertMatch({204, _}, import_backup_full(?NODE1_PORT, Ns1Auth, N1File)),
    ok.

%% A namespaced administrator uploading its own namespace's backup lands it in
%% its own space (the global administrator never sees it), and can then import
%% it back.
t_namespaced_upload_isolation(Config) ->
    [Core1 | _] = ?config(cluster, Config),
    Ns1Auth = ?config(ns_admin_auth, Config),
    DashboardAuth = ?config(dashboard_auth, Config),
    {LocalPath, Base} = local_ns_backup_copy(Config, Core1, ?NODE1_PORT, Ns1Auth, <<"ns1">>),
    %% Remove the exported original so the upload is what lands the file.
    ?assertMatch({204, _}, delete_backup(?NODE1_PORT, Ns1Auth, Base)),
    ?assertEqual(ok, upload_backup(?NODE1_PORT, Ns1Auth, LocalPath)),
    ?assert(lists:member(Base, list_filenames(?NODE1_PORT, Ns1Auth))),
    ?assertNot(lists:member(Base, list_filenames(?NODE1_PORT, DashboardAuth))),
    %% Round trip: importing the uploaded own-namespace backup succeeds.
    ?assertMatch({204, _}, import_backup_full(?NODE1_PORT, Ns1Auth, Base)),
    ok.

%% A global administrator may opt in to a specific namespace's backup space with
%% the `namespace' query parameter -- to inspect or clean up after a tenant --
%% while the default (no parameter) stays on the global backups.
t_global_admin_scoped_namespace(Config) ->
    DashboardAuth = ?config(dashboard_auth, Config),
    Ns1Auth = ?config(ns_admin_auth, Config),
    Ns1 = #{<<"namespace">> => <<"ns1">>},

    {200, #{<<"filename">> := N1File}} = export_backup2(?NODE1_PORT, Ns1Auth, #{}),

    %% Default global listing does not see the namespaced backup...
    ?assertNot(lists:member(N1File, list_filenames(?NODE1_PORT, DashboardAuth))),
    %% ...but opting into ns1 does.
    ?assertEqual([N1File], list_filenames(?NODE1_PORT, DashboardAuth, Ns1)),

    %% Global admin can download and delete within the opted-in namespace.
    ?assertMatch({200, _}, download_backup(?NODE1_PORT, DashboardAuth, N1File, Ns1)),
    ?assertMatch({204, _}, delete_backup(?NODE1_PORT, DashboardAuth, N1File, Ns1)),
    ?assertEqual([], list_filenames(?NODE1_PORT, DashboardAuth, Ns1)),
    ok.

%% Audit records for a data-backup operation must identify the target
%% namespace, whether it is the actor's own (implicit, no query param, as for
%% the namespaced admin's export below) or opted into via `?namespace=' (as
%% for the global admin's namespaced import/upload/delete elsewhere in this
%% suite). Same class of gap as emqx/emqx#18653 / #18664: `?namespace=' is a
%% query parameter, and namespace resolution for POST /data/export has no
%% query override at all -- it never appeared anywhere in the audit log
%% before this fix.
t_export_import_audit_records_namespace(Config) ->
    DashboardAuth = ?config(dashboard_auth, Config),
    Ns1Auth = ?config(ns_admin_auth, Config),
    StartAt = erlang:system_time(microsecond),

    {200, _} = export_backup2(?NODE1_PORT, DashboardAuth, #{}),
    {200, _} = export_backup2(?NODE1_PORT, Ns1Auth, #{}),

    Entries = wait_for_audit_entries(
        ?NODE1_PORT, DashboardAuth, <<"/data/export">>, StartAt, 2, 2000
    ),
    Namespaces = lists:sort([namespace_of(E) || E <- Entries]),
    ?assertEqual(lists:sort([<<"global">>, <<"ns1">>]), Namespaces),
    ok.

%% A namespaced administrator cannot escape its namespace by supplying the
%% `namespace' query parameter -- it is ignored for namespaced callers.
t_namespaced_admin_cannot_override_namespace(Config) ->
    [Core1, _Core2, _Repl] = ?config(cluster, Config),
    Ns1Auth = ?config(ns_admin_auth, Config),
    Ns2Auth = ns_admin_auth_header(Core1, <<"ns2">>, <<"ns2_admin_for_test">>, ?NS_ADMIN_PASS),

    {200, #{<<"filename">> := N1File}} = export_backup2(?NODE1_PORT, Ns1Auth, #{}),
    {200, #{<<"filename">> := N2File}} = export_backup2(?NODE1_PORT, Ns2Auth, #{}),

    %% ns1 asking for ns2 still only sees (and can only act on) its own space.
    ?assertEqual([N1File], list_filenames(?NODE1_PORT, Ns1Auth, #{<<"namespace">> => <<"ns2">>})),
    ?assertMatch(
        {404, _}, download_backup(?NODE1_PORT, Ns1Auth, N2File, #{<<"namespace">> => <<"ns2">>})
    ),
    ?assertMatch(
        {404, _}, delete_backup(?NODE1_PORT, Ns1Auth, N2File, #{<<"namespace">> => <<"ns2">>})
    ),
    ok.

%% A global administrator can import a namespaced backup directly by opting into
%% the namespace with the `namespace' query parameter -- consistent with
%% listing / downloading. Without the parameter the file is not in the global
%% scope, so the import fails.
t_global_admin_import_scoped_namespace(Config) ->
    DashboardAuth = ?config(dashboard_auth, Config),
    Ns1Auth = ?config(ns_admin_auth, Config),
    Ns1 = #{<<"namespace">> => <<"ns1">>},

    %% ns1 admin exports its own backup; it lands under `ns/ns1'.
    {200, #{<<"filename">> := N1File}} = export_backup2(?NODE1_PORT, Ns1Auth, #{}),

    %% Global admin without the parameter looks in the global scope and cannot
    %% find the namespaced file -> 400.
    ?assertMatch({400, _}, import_backup_ns(?NODE1_PORT, DashboardAuth, N1File, #{})),

    %% Opting into ns1 finds and imports it -> 204.
    ?assertMatch({204, _}, import_backup_ns(?NODE1_PORT, DashboardAuth, N1File, Ns1)),
    ok.

%% A global administrator uploading with `namespace=ns1' lands the file in the
%% namespace's space (not the global one), and can then import it scoped to that
%% namespace. Uploading without the parameter stays on the global scope.
t_global_admin_upload_scoped_namespace(Config) ->
    [Core1 | _] = ?config(cluster, Config),
    DashboardAuth = ?config(dashboard_auth, Config),
    Ns1Auth = ?config(ns_admin_auth, Config),
    Ns1 = #{<<"namespace">> => <<"ns1">>},
    {LocalPath, Base} = local_ns_backup_copy(Config, Core1, ?NODE1_PORT, Ns1Auth, <<"ns1">>),
    ?assertMatch({204, _}, delete_backup(?NODE1_PORT, Ns1Auth, Base)),

    %% Upload into ns1's space.
    ?assertEqual(ok, upload_backup_ns(?NODE1_PORT, DashboardAuth, LocalPath, Ns1)),
    %% The file is visible in ns1's space, but not in the global listing.
    ?assert(lists:member(Base, list_filenames(?NODE1_PORT, DashboardAuth, Ns1))),
    ?assertNot(lists:member(Base, list_filenames(?NODE1_PORT, DashboardAuth))),
    %% And a scoped import of the uploaded file succeeds.
    ?assertMatch({204, _}, import_backup_ns(?NODE1_PORT, DashboardAuth, Base, Ns1)),

    %% Uploading without the parameter stays on the global scope (regression).
    UploadFile = backup_path(?UPLOAD_CE_BACKUP),
    GlobalBase = list_to_binary(?UPLOAD_CE_BACKUP),
    ?assertEqual(ok, upload_backup(?NODE2_PORT, DashboardAuth, UploadFile)),
    ?assert(lists:member(GlobalBase, list_filenames(?NODE2_PORT, DashboardAuth))),
    ok.

%% A namespaced administrator cannot import or upload into another namespace by
%% supplying the `namespace' query parameter -- it is ignored for namespaced
%% callers, so both operations stay confined to the caller's own namespace.
t_namespaced_admin_import_upload_confined(Config) ->
    [Core1, _Core2, _Repl] = ?config(cluster, Config),
    Ns1Auth = ?config(ns_admin_auth, Config),
    Ns2Auth = ns_admin_auth_header(Core1, <<"ns2">>, <<"ns2_admin_for_test">>, ?NS_ADMIN_PASS),
    ForceNs1 = #{<<"namespace">> => <<"ns1">>},

    %% ns1 admin exports its own backup (lands under `ns/ns1').
    {200, #{<<"filename">> := N1File}} = export_backup2(?NODE1_PORT, Ns1Auth, #{}),

    %% ns2 admin asking for ns1 stays confined to ns2, so ns1's file is not
    %% found in its own space -> 400. It cannot reach ns1's backup.
    ?assertMatch({400, _}, import_backup_ns(?NODE1_PORT, Ns2Auth, N1File, ForceNs1)),

    %% Uploading with `namespace=ns1' lands in ns2's own space, never ns1's.
    {LocalPath, Base} = local_ns_backup_copy(Config, Core1, ?NODE1_PORT, Ns2Auth, <<"ns2">>),
    ?assertMatch({204, _}, delete_backup(?NODE1_PORT, Ns2Auth, Base)),
    ?assertEqual(ok, upload_backup_ns(?NODE1_PORT, Ns2Auth, LocalPath, ForceNs1)),
    ?assertNot(lists:member(Base, list_filenames(?NODE1_PORT, Ns1Auth))),
    ?assert(lists:member(Base, list_filenames(?NODE1_PORT, Ns2Auth))),
    ok.

-doc """
A namespaced administrator must not be able to upload an archive holding
another scope's data: both a global backup (top-level cluster.hocon and mnesia
tables) and another namespace's backup are rejected with 403, no file is left
behind, and an uninspectable archive yields 400.
""".
t_namespaced_upload_foreign_content_forbidden(Config) ->
    [Core1 | _] = ?config(cluster, Config),
    Ns1Auth = ?config(ns_admin_auth, Config),
    Ns2Auth = ns_admin_auth_header(Core1, <<"ns2">>, <<"ns2_admin_for_test">>, ?NS_ADMIN_PASS),

    %% A global archive -> 403.
    GlobalFile = backup_path(?UPLOAD_CE_BACKUP),
    ?assertMatch(
        {403, #{<<"code">> := <<"FORBIDDEN">>}},
        upload_backup_full(?NODE1_PORT, Ns1Auth, GlobalFile, #{})
    ),

    %% Another namespace's archive -> 403.
    {Ns2LocalPath, _Ns2Base} = local_ns_backup_copy(
        Config, Core1, ?NODE1_PORT, Ns2Auth, <<"ns2">>
    ),
    ?assertMatch(
        {403, #{<<"code">> := <<"FORBIDDEN">>}},
        upload_backup_full(?NODE1_PORT, Ns1Auth, Ns2LocalPath, #{})
    ),

    %% Neither rejected upload left a file in ns1's space.
    ?assertEqual([], list_filenames(?NODE1_PORT, Ns1Auth)),

    %% An archive that cannot be inspected -> 400.
    GarbagePath = filename:join(?config(priv_dir, Config), "garbage.tar.gz"),
    ok = file:write_file(GarbagePath, <<"not a tar archive">>),
    ?assertMatch({400, _}, upload_backup_full(?NODE1_PORT, Ns1Auth, GarbagePath, #{})),
    ok.

-doc """
Defense-in-depth at import time: even when a foreign-content archive is
already present in the namespace's backup space (e.g. stored before the
upload-time scope check existed), a namespaced import rejects it with 403.
""".
t_namespaced_import_foreign_content_forbidden(Config) ->
    [Core1 | _] = ?config(cluster, Config),
    Ns1Auth = ?config(ns_admin_auth, Config),
    Ns2Auth = ns_admin_auth_header(Core1, <<"ns2">>, <<"ns2_admin_for_test">>, ?NS_ADMIN_PASS),

    %% Plant ns2's archive into ns1's space, bypassing the HTTP API.
    {200, #{<<"filename">> := Ns2File}} = export_backup2(?NODE1_PORT, Ns2Auth, #{}),
    {ok, Ns2Content} = ?ON(Core1, emqx_mgmt_data_backup:read_file(<<"ns2">>, Ns2File)),
    ok = plant_ns_backup_file(Core1, <<"ns1">>, Ns2File, Ns2Content),
    ?assertMatch(
        {403, #{<<"code">> := <<"FORBIDDEN">>}},
        import_backup_ns(?NODE1_PORT, Ns1Auth, Ns2File, #{})
    ),

    %% Same for a planted global archive.
    GlobalBase = list_to_binary(?UPLOAD_CE_BACKUP),
    {ok, GlobalContent} = file:read_file(backup_path(?UPLOAD_CE_BACKUP)),
    ok = plant_ns_backup_file(Core1, <<"ns1">>, GlobalBase, GlobalContent),
    ?assertMatch(
        {403, #{<<"code">> := <<"FORBIDDEN">>}},
        import_backup_ns(?NODE1_PORT, Ns1Auth, GlobalBase, #{})
    ),
    ok.

-doc """
The global scope is a full-cluster artifact: importing a global archive
restores every `ns/<NS>/cluster.hocon' entry into its own namespace, and a
subsequent global export emits every namespace's configuration back into the
archive alongside the global content.
""".
t_full_cluster_global_export_import(Config) ->
    [Core1 | _] = Nodes = ?config(cluster, Config),
    DashboardAuth = ?config(dashboard_auth, Config),
    %% `mqtt' is not in the default allowed namespaced roots.
    _ = ?ON_ALL(Nodes, emqx_config:add_allowed_namespaced_config_root(<<"mqtt">>)),

    %% Forge a full-cluster archive carrying two namespaces' configuration.
    {LocalPath, Base} = forge_backup(Config, "emqx-export-full-cluster", #{
        <<"ns1">> => <<"mqtt { max_awaiting_rel = 111 }">>,
        <<"ns2">> => <<"mqtt { max_awaiting_rel = 222 }">>
    }),
    ?assertEqual(ok, upload_backup(?NODE1_PORT, DashboardAuth, LocalPath)),
    ?assertMatch({204, _}, import_backup_full(?NODE1_PORT, DashboardAuth, Base)),

    %% Each namespace's configuration was restored into its own namespace.
    ?assertMatch(
        #{<<"mqtt">> := #{<<"max_awaiting_rel">> := 111}},
        ?ON(Core1, emqx_config:get_all_roots_from_namespace(<<"ns1">>))
    ),
    ?assertMatch(
        #{<<"mqtt">> := #{<<"max_awaiting_rel">> := 222}},
        ?ON(Core1, emqx_config:get_all_roots_from_namespace(<<"ns2">>))
    ),

    %% A global export now carries the global scope plus both namespaces.
    {200, #{<<"filename">> := ExportFile}} = export_backup2(?NODE1_PORT, DashboardAuth, #{}),
    {ok, ExportContent} = ?ON(Core1, emqx_mgmt_data_backup:read_file(ExportFile)),
    ?assertMatch(
        {ok, #{global := true, namespaces := [<<"ns1">>, <<"ns2">>]}},
        emqx_mgmt_data_backup:peek_backup_scope_of_content(ExportContent)
    ),
    ok.

-doc """
Single-tenant regression: a cluster with no namespaces exports a global
archive with no `ns/' entries, and importing it back succeeds as before.
""".
t_global_export_import_no_namespaces(Config) ->
    [Core1 | _] = ?config(cluster, Config),
    DashboardAuth = ?config(dashboard_auth, Config),
    {200, #{<<"filename">> := File}} = export_backup2(?NODE1_PORT, DashboardAuth, #{}),
    {ok, Content} = ?ON(Core1, emqx_mgmt_data_backup:read_file(File)),
    ?assertMatch(
        {ok, #{global := true, namespaces := []}},
        emqx_mgmt_data_backup:peek_backup_scope_of_content(Content)
    ),
    ?assertMatch({204, _}, import_backup_full(?NODE1_PORT, DashboardAuth, File)),
    ok.
