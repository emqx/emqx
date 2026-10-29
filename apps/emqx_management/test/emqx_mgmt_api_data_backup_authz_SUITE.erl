%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mgmt_api_data_backup_authz_SUITE).

-moduledoc """
Data backup REST API: which callers may download, import and export
backups. Covers API keys, viewers and scoped dashboard tokens, and the
sensitive table sets (dashboard users and API keys) in an archive.
Each case starts its own 3-node cluster.
""".

-compile(export_all).
-compile(nowarn_export_all).

-import(emqx_mgmt_api_data_backup_test_helpers, [
    do_init_per_testcase/2,
    download_backup/3,
    download_backup/4,
    write_tmp_tar/1,
    import_backup_full/3,
    export_backup/2,
    export_backup2/3,
    import_backup/3,
    list_backups/4,
    upload_backup/3,
    dashboard_token_auth/4,
    scoped_dashboard_token_auth/4,
    ns_api_key_auth_header/1,
    api_key_auth_header/3,
    assert_download_denied/1,
    list_filenames/2,
    list_filenames/3,
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

init_per_testcase(TC, Config) when
    TC =:= t_download_api_key_inspection_error_fails_closed
->
    Config;
init_per_testcase(TC, Config) ->
    do_init_per_testcase(TC, Config).

end_per_testcase(_TC, Config) ->
    case ?config(cluster, Config) of
        undefined -> ok;
        Cluster -> emqx_cth_cluster:stop(Cluster)
    end.

%% API keys must not be able to export tables that hold dashboard accounts or
%% API keys themselves.
t_export_api_key_omits_sensitive_tables(Config) ->
    ApiAuth = ?config(auth, Config),
    {200, #{<<"filename">> := Filename, <<"node">> := NodeBin}} =
        export_backup2(?NODE1_PORT, ApiAuth, #{}),
    Node = binary_to_atom(NodeBin, utf8),
    {ok, BinContents} = ?ON(
        Node, emqx_mgmt_data_backup:read_file(unicode:characters_to_binary(Filename))
    ),
    {ok, TmpFile} = write_tmp_tar(BinContents),
    try
        {ok, Entries} = erl_tar:table(TmpFile, [compressed]),
        SensitiveTabs = ["mnesia/emqx_admin", "mnesia/emqx_app"],
        FoundSensitive = [
            E
         || E <- Entries,
            S <- SensitiveTabs,
            string:find(E, S) =/= nomatch
        ],
        ?assertEqual(
            [],
            FoundSensitive,
            "API-key export must not contain dashboard_users / api_keys mnesia tables"
        )
    after
        file:delete(TmpFile)
    end,
    ok.

%% API keys must be rejected with 403 when importing a backup that contains
%% sensitive mnesia tables.
t_import_api_key_blocks_sensitive_tables(Config) ->
    ApiAuth = ?config(auth, Config),
    UploadFile = backup_path(?UPLOAD_CE_BACKUP),
    ?assertEqual(ok, upload_backup(?NODE1_PORT, ApiAuth, UploadFile)),
    {Status, Body} = import_backup_full(?NODE1_PORT, ApiAuth, ?UPLOAD_CE_BACKUP),
    ?assertEqual(403, Status),
    ?assertMatch(#{<<"code">> := <<"FORBIDDEN">>}, Body),
    ok.

%% Dashboard bearer-token (JWT) callers can import backups that include the
%% sensitive mnesia tables. The API-key restriction must not regress them.
t_import_dashboard_token_allows_sensitive_tables(Config) ->
    ApiAuth = ?config(auth, Config),
    DashboardAuth = ?config(dashboard_auth, Config),
    UploadFile = backup_path(?UPLOAD_CE_BACKUP),
    ?assertEqual(ok, upload_backup(?NODE1_PORT, ApiAuth, UploadFile)),
    ?assertMatch({ok, _}, import_backup(?NODE1_PORT, DashboardAuth, ?UPLOAD_CE_BACKUP)),
    ok.

%% A Dashboard administrator whose effective scope set does not include both
%% `user_management' and `api_key_management' must be rejected with 403 when
%% importing a backup that contains the dashboard_users / api_keys tables,
%% the same as an API-key caller.
t_import_restricted_dashboard_token_blocks_sensitive_tables(Config) ->
    ApiAuth = ?config(auth, Config),
    [Core1 | _] = ?config(cluster, Config),
    %% `system' is what lets the caller reach the data-backup endpoint at
    %% all; it does NOT include the credential-management scopes that govern
    %% the sensitive tables.
    RestrictedAuth = scoped_dashboard_token_auth(
        Core1, <<"restricted_admin_for_test">>, ?DASHBOARD_PASS, [<<"system">>]
    ),
    UploadFile = backup_path(?UPLOAD_CE_BACKUP),
    ?assertEqual(ok, upload_backup(?NODE1_PORT, ApiAuth, UploadFile)),
    {Status, Body} = import_backup_full(?NODE1_PORT, RestrictedAuth, ?UPLOAD_CE_BACKUP),
    ?assertEqual(403, Status),
    ?assertMatch(#{<<"code">> := <<"FORBIDDEN">>}, Body),
    ok.

%% A Dashboard administrator whose effective scope set includes both
%% `user_management' and `api_key_management' may import a backup that
%% contains the dashboard_users / api_keys tables. (`system' is also required,
%% otherwise the login-user scope check blocks the data-backup endpoint
%% before the sensitive-table check runs.)
t_import_dashboard_token_with_credential_scopes_allows_sensitive_tables(Config) ->
    ApiAuth = ?config(auth, Config),
    [Core1 | _] = ?config(cluster, Config),
    PrivilegedAuth = scoped_dashboard_token_auth(
        Core1,
        <<"privileged_admin_for_test">>,
        ?DASHBOARD_PASS,
        [<<"system">>, <<"user_management">>, <<"api_key_management">>]
    ),
    UploadFile = backup_path(?UPLOAD_CE_BACKUP),
    ?assertEqual(ok, upload_backup(?NODE1_PORT, ApiAuth, UploadFile)),
    ?assertMatch({ok, _}, import_backup(?NODE1_PORT, PrivilegedAuth, ?UPLOAD_CE_BACKUP)),
    ok.

%% A restricted Dashboard administrator's export must omit the sensitive table
%% sets, mirroring the API-key export behaviour.
t_export_restricted_dashboard_token_omits_sensitive_tables(Config) ->
    [Core1 | _] = ?config(cluster, Config),
    RestrictedAuth = scoped_dashboard_token_auth(
        Core1, <<"restricted_exporter_for_test">>, ?DASHBOARD_PASS, [<<"system">>]
    ),
    {200, #{<<"filename">> := Filename, <<"node">> := NodeBin}} =
        export_backup2(?NODE1_PORT, RestrictedAuth, #{}),
    Node = binary_to_atom(NodeBin, utf8),
    {ok, BinContents} = ?ON(
        Node, emqx_mgmt_data_backup:read_file(unicode:characters_to_binary(Filename))
    ),
    {ok, TmpFile} = write_tmp_tar(BinContents),
    try
        {ok, Entries} = erl_tar:table(TmpFile, [compressed]),
        SensitiveTabs = ["mnesia/emqx_admin", "mnesia/emqx_app"],
        FoundSensitive = [
            E
         || E <- Entries,
            S <- SensitiveTabs,
            string:find(E, S) =/= nomatch
        ],
        ?assertEqual(
            [],
            FoundSensitive,
            "restricted dashboard export must not contain dashboard_users / api_keys mnesia tables"
        )
    after
        file:delete(TmpFile)
    end,
    ok.

%% API-key callers may download archives that do not contain dashboard users or
%% API-key records. This keeps backup-sync working: its API-key export path
%% already filters those sensitive table sets.
t_download_api_key_without_sensitive_tables_allowed(Config) ->
    ApiAuth = ?config(auth, Config),
    {200, #{<<"filename">> := Filename}} =
        export_backup2(?NODE1_PORT, ApiAuth, #{}),
    {Status, _Body} = download_backup(?NODE1_PORT, ApiAuth, Filename),
    ?assertEqual(200, Status),
    ok.

%% API-key callers must still be rejected with 403 when the archive contains
%% dashboard accounts or API-key records.
t_download_api_key_with_sensitive_tables_forbidden(Config) ->
    ApiAuth = ?config(auth, Config),
    DashboardAuth = ?config(dashboard_auth, Config),
    {200, #{<<"filename">> := Filename}} =
        export_backup2(?NODE1_PORT, DashboardAuth, #{}),
    {Status, Body} = download_backup(?NODE1_PORT, ApiAuth, Filename),
    ?assertEqual(403, Status),
    ?assertMatch(#{<<"code">> := <<"FORBIDDEN">>}, Body),
    ok.

%% If the archive cannot be inspected, API-key download must fail closed.
t_download_api_key_inspection_error_fails_closed(_Config) ->
    Filename = <<"emqx-export-inspection-error.tar.gz">>,
    ok = meck:new(emqx_mgmt_data_backup_proto_v4, [passthrough, no_link, no_history]),
    try
        meck:expect(
            emqx_mgmt_data_backup_proto_v4,
            peek_sensitive_table_sets,
            fun(Node, Filename0, infinity) ->
                ?assertEqual(node(), Node),
                ?assertEqual(Filename, Filename0),
                {error, not_found}
            end
        ),
        {Status, Body} = emqx_mgmt_api_data_backup:data_file_by_name(get, #{
            bindings => #{filename => Filename},
            query_string => #{},
            auth_meta => #{auth_type => api_key}
        }),
        ?assertEqual(404, Status),
        ?assertMatch(#{code := 'NOT_FOUND'}, Body)
    after
        meck:unload(emqx_mgmt_data_backup_proto_v4)
    end.

%% Dashboard viewers (read-only role) must be rejected with 403 when
%% downloading a backup file.
t_download_viewer_forbidden(Config) ->
    ApiAuth = ?config(auth, Config),
    ViewerAuth = ?config(viewer_auth, Config),
    {200, #{<<"filename">> := Filename}} =
        export_backup2(?NODE1_PORT, ApiAuth, #{}),
    assert_download_denied(download_backup(?NODE1_PORT, ViewerAuth, Filename)),
    ok.

-doc """
A global viewer API key and a publisher API key get 403 on a global backup that
has no dashboard-user or API-key table sets. An administrator API key still
downloads it, and the viewer API key can still list it.
""".
t_download_viewer_api_key_forbidden(Config) ->
    [Core1 | _] = ?config(cluster, Config),
    ApiAuth = ?config(auth, Config),
    ViewerKeyAuth = api_key_auth_header(Core1, <<"viewer_api_key_for_test">>, <<"viewer">>),
    PublisherKeyAuth =
        api_key_auth_header(Core1, <<"publisher_api_key_for_test">>, <<"publisher">>),
    {200, #{<<"filename">> := Filename}} = export_backup2(?NODE1_PORT, ApiAuth, #{}),
    assert_download_denied(download_backup(?NODE1_PORT, ViewerKeyAuth, Filename)),
    assert_download_denied(download_backup(?NODE1_PORT, PublisherKeyAuth, Filename)),
    ?assert(lists:member(Filename, list_filenames(?NODE1_PORT, ViewerKeyAuth))),
    ?assertMatch({200, _}, download_backup(?NODE1_PORT, ApiAuth, Filename)),
    ok.

-doc """
A namespaced viewer (login user or API key) gets 403 when it downloads a backup of
its own namespace, and can still list it. The namespaced administrator (login user
or API key) still downloads it.
""".
t_download_ns_viewer_forbidden(Config) ->
    [Core1 | _] = ?config(cluster, Config),
    Ns1Auth = ?config(ns_admin_auth, Config),
    NsViewerRole = <<"ns:ns1::viewer">>,
    NsViewerAuth =
        dashboard_token_auth(Core1, <<"ns_viewer_for_test">>, ?VIEWER_PASS, NsViewerRole),
    NsViewerKeyAuth = api_key_auth_header(Core1, <<"ns_viewer_api_key_for_test">>, NsViewerRole),
    Ns1ApiKeyAuth = ns_api_key_auth_header(Core1),
    {200, #{<<"filename">> := N1File}} = export_backup2(?NODE1_PORT, Ns1Auth, #{}),
    lists:foreach(
        fun(Auth) ->
            ?assertEqual([N1File], list_filenames(?NODE1_PORT, Auth)),
            assert_download_denied(download_backup(?NODE1_PORT, Auth, N1File))
        end,
        [NsViewerAuth, NsViewerKeyAuth]
    ),
    ?assertMatch({200, _}, download_backup(?NODE1_PORT, Ns1Auth, N1File)),
    ?assertMatch({200, _}, download_backup(?NODE1_PORT, Ns1ApiKeyAuth, N1File)),
    ok.

-doc """
A global viewer (login user or API key) gets 403 when it downloads a namespaced
backup with the `namespace` query parameter, and can still list it. The global
administrator (login user or API key) still downloads it.
""".
t_download_global_viewer_scoped_namespace_forbidden(Config) ->
    [Core1 | _] = ?config(cluster, Config),
    ApiAuth = ?config(auth, Config),
    DashboardAuth = ?config(dashboard_auth, Config),
    ViewerAuth = ?config(viewer_auth, Config),
    Ns1Auth = ?config(ns_admin_auth, Config),
    ViewerKeyAuth = api_key_auth_header(Core1, <<"viewer_api_key_for_test">>, <<"viewer">>),
    Ns1 = #{<<"namespace">> => <<"ns1">>},
    {200, #{<<"filename">> := N1File}} = export_backup2(?NODE1_PORT, Ns1Auth, #{}),
    lists:foreach(
        fun(Auth) ->
            ?assertEqual([N1File], list_filenames(?NODE1_PORT, Auth, Ns1)),
            assert_download_denied(download_backup(?NODE1_PORT, Auth, N1File, Ns1))
        end,
        [ViewerAuth, ViewerKeyAuth]
    ),
    ?assertMatch({200, _}, download_backup(?NODE1_PORT, DashboardAuth, N1File, Ns1)),
    ?assertMatch({200, _}, download_backup(?NODE1_PORT, ApiAuth, N1File, Ns1)),
    ok.

%% Global dashboard administrators must still be able to download a backup
%% file. Without this regression the download path would be unreachable.
t_download_global_admin_allowed(Config) ->
    ApiAuth = ?config(auth, Config),
    DashboardAuth = ?config(dashboard_auth, Config),
    {200, #{<<"filename">> := Filename}} =
        export_backup2(?NODE1_PORT, ApiAuth, #{}),
    {Status, _Body} = download_backup(?NODE1_PORT, DashboardAuth, Filename),
    ?assertEqual(200, Status),
    ok.

%% Listing the backup directory remains open to global dashboard viewers: the
%% listing only exposes filenames / sizes / timestamps, not the archive
%% contents, and the download is gated separately above. Namespaced callers see
%% only their own namespace's backups (see t_namespaced_backup_isolation).
t_list_files_viewer_allowed(Config) ->
    ApiAuth = ?config(auth, Config),
    ViewerAuth = ?config(viewer_auth, Config),
    {ok, _} = export_backup(?NODE1_PORT, ApiAuth),
    {ok, _} = list_backups(?NODE1_PORT, ViewerAuth, <<"1">>, <<"100">>),
    ok.
