%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mgmt_api_data_backup_test_helpers).

-moduledoc """
Cluster setup, authentication and REST request helpers shared by the
`emqx_mgmt_api_data_backup*_SUITE` suites.
""".

-export([
    do_init_per_testcase/2,
    download_backup/3,
    download_backup/4,
    to_list/1,
    write_tmp_tar/1,
    import_backup_full/3,
    import_backup_ns/4,
    upload_backup_ns/4,
    local_ns_backup_copy/5,
    plant_ns_backup_file/4,
    forge_backup/3,
    upload_backup_full/4,
    export_backup/2,
    export_backup2/3,
    import_backup/3,
    import_backup/4,
    list_backups/4,
    backup_file_op/5,
    backup_file_op/6,
    upload_backup/3,
    wait_for_audit_entries/6,
    audit_entries_since/4,
    namespace_of/1,
    request/4,
    request/5,
    request/6,
    request/7,
    cluster/2,
    node_name/3,
    auth_header/1,
    dashboard_auth_header/1,
    viewer_auth_header/1,
    ns_admin_auth_header/1,
    ns_admin_auth_header/4,
    dashboard_token_auth/4,
    scoped_dashboard_token_auth/4,
    ns_api_key_auth_header/1,
    api_key_auth_header/3,
    assert_download_denied/1,
    list_filenames/2,
    list_filenames/3,
    delete_backup/3,
    delete_backup/4,
    data_backup_simple_request/5,
    data_backup_simple_request/6,
    fresh_dashboard_auth/1,
    wait_for_auth_replication/1,
    wait_for_auth_replication/2,
    wait_for_dashboard_auth/2,
    wait_for_dashboard_auth/3,
    apps_spec/2,
    common_apps_spec/1,
    app_spec_dashboard/1,
    test_case_specific_apps_spec/1,
    backup_path/1
]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include("emqx_mgmt_api_data_backup_test.hrl").

do_init_per_testcase(TC, Config) ->
    Cluster = [Core1, _Core2, Repl] = cluster(TC, Config),
    Auth = auth_header(Core1),
    DashboardAuth = dashboard_auth_header(Core1),
    ViewerAuth = viewer_auth_header(Core1),
    NsAdminAuth = ns_admin_auth_header(Core1),
    ok = wait_for_auth_replication(Repl),
    [
        {auth, Auth},
        {dashboard_auth, DashboardAuth},
        {viewer_auth, ViewerAuth},
        {ns_admin_auth, NsAdminAuth},
        {cluster, Cluster}
        | Config
    ].

download_backup(NodeApiPort, Auth, BackupName) ->
    download_backup(NodeApiPort, Auth, BackupName, #{}).

download_backup(NodeApiPort, Auth, BackupName, QueryParams) ->
    data_backup_simple_request(
        get, NodeApiPort, ["data", "files", to_list(BackupName)], [], Auth, QueryParams
    ).

to_list(B) when is_binary(B) -> unicode:characters_to_list(B);
to_list(L) when is_list(L) -> L.

write_tmp_tar(Bin) ->
    Path = filename:join([
        "/tmp",
        "emqx-test-export-" ++ integer_to_list(erlang:unique_integer([positive])) ++ ".tar.gz"
    ]),
    ok = file:write_file(Path, Bin),
    {ok, Path}.

import_backup_full(NodeApiPort, Auth, BackupName) ->
    Path = emqx_mgmt_api_test_util:api_path(?api_base_url(NodeApiPort), ["data", "import"]),
    Body = #{
        <<"filename">> => unicode:characters_to_binary(BackupName),
        <<"allow_security_profile_mismatch">> => true
    },
    emqx_mgmt_api_test_util:simple_request(post, Path, Body, Auth).

%% Import a backup, optionally scoped to a namespace via the `namespace' query
%% parameter. Returns `{Status, Body}'.
import_backup_ns(NodeApiPort, Auth, BackupName, QueryParams) ->
    Path = emqx_mgmt_api_test_util:api_path(?api_base_url(NodeApiPort), ["data", "import"]),
    Body = #{<<"filename">> => unicode:characters_to_binary(BackupName)},
    emqx_mgmt_api_test_util:simple_request(#{
        method => post,
        url => Path,
        body => Body,
        auth_header => Auth,
        query_params => QueryParams
    }).

%% Upload a backup file, optionally scoped to a namespace via the `namespace'
%% query parameter.
upload_backup_ns(NodeApiPort, Auth, BackupFilePath, QueryParams) ->
    Path0 = emqx_mgmt_api_test_util:api_path(?api_base_url(NodeApiPort), ["data", "files"]),
    Path =
        case maps:to_list(QueryParams) of
            [] ->
                Path0;
            QueryList ->
                Query = unicode:characters_to_list(uri_string:compose_query(QueryList)),
                Path0 ++ "?" ++ Query
        end,
    Res = emqx_mgmt_api_test_util:upload_request(
        Path,
        BackupFilePath,
        "filename",
        <<"application/octet-stream">>,
        [],
        Auth
    ),
    case Res of
        {ok, {{"HTTP/1.1", 204, _}, _Headers, _}} ->
            ok;
        {ok, {{"HTTP/1.1", 400, _}, _Headers, _} = Resp} ->
            ct:pal("Backup upload failed: ~p", [Resp]),
            {error, bad_request};
        Err ->
            Err
    end.

%% Export a backup scoped to `Namespace' (using `Auth'), copy the archive from
%% `Node' to the test-runner's disk, and return `{LocalPath, Basename}'.
local_ns_backup_copy(Config, Node, Port, Auth, Namespace) ->
    {200, #{<<"filename">> := File}} = export_backup2(Port, Auth, #{}),
    {ok, Content} = ?ON(Node, emqx_mgmt_data_backup:read_file(Namespace, File)),
    LocalPath = filename:join(?config(priv_dir, Config), to_list(File)),
    ok = file:write_file(LocalPath, Content),
    {LocalPath, File}.

%% Write an archive straight into `Namespace''s backup directory on `Node',
%% bypassing the HTTP upload (and thus its content checks).
plant_ns_backup_file(Node, Namespace, Filename, Content) ->
    ?ON(Node, begin
        Path = filename:join([emqx:data_dir(), "backup", "ns", Namespace, Filename]),
        ok = filelib:ensure_dir(Path),
        file:write_file(Path, Content)
    end).

%% Forge a backup archive holding one `ns/<NS>/cluster.hocon' entry per key of
%% `NsConfigs' (`#{Namespace => HoconBin}') plus a valid META file, mirroring
%% the layout of a global export of a cluster that has namespaces but no
%% global configuration. Returns `{LocalPath, Basename}'.
forge_backup(Config, BaseName, NsConfigs) ->
    Filename = BaseName ++ ".tar.gz",
    LocalPath = filename:join(?config(priv_dir, Config), Filename),
    {ok, Tar} = erl_tar:open(LocalPath, [write, compressed]),
    Meta = #{version => emqx_release:version(), edition => emqx_release:edition()},
    MetaBin = iolist_to_binary(hocon_pp:do(Meta, #{})),
    ok = erl_tar:add(Tar, MetaBin, filename:join(BaseName, "META.hocon"), []),
    maps:foreach(
        fun(Namespace, HoconBin) ->
            NameInArchive = filename:join([BaseName, "ns", to_list(Namespace), "cluster.hocon"]),
            ok = erl_tar:add(Tar, HoconBin, NameInArchive, [])
        end,
        NsConfigs
    ),
    ok = erl_tar:close(Tar),
    {LocalPath, list_to_binary(Filename)}.

%% Upload a backup file and return `{Status, DecodedBody}'.
upload_backup_full(NodeApiPort, Auth, BackupFilePath, QueryParams) ->
    Path0 = emqx_mgmt_api_test_util:api_path(?api_base_url(NodeApiPort), ["data", "files"]),
    Path =
        case maps:to_list(QueryParams) of
            [] ->
                Path0;
            QueryList ->
                Query = unicode:characters_to_list(uri_string:compose_query(QueryList)),
                Path0 ++ "?" ++ Query
        end,
    {ok, {{"HTTP/1.1", Status, _}, _Headers, Body}} =
        emqx_mgmt_api_test_util:upload_request(
            Path,
            BackupFilePath,
            "filename",
            <<"application/octet-stream">>,
            [],
            Auth
        ),
    DecodedBody =
        case iolist_to_binary(Body) of
            <<>> -> #{};
            BodyBin -> emqx_utils_json:decode(BodyBin)
        end,
    {Status, DecodedBody}.

export_backup(NodeApiPort, Auth) ->
    Path = ["data", "export"],
    request(post, NodeApiPort, Path, _Body = #{}, Auth).

export_backup2(NodeApiPort, Auth, Body) ->
    Path = emqx_mgmt_api_test_util:api_path(?api_base_url(NodeApiPort), ["data", "export"]),
    emqx_mgmt_api_test_util:simple_request(post, Path, Body, Auth).

import_backup(NodeApiPort, Auth, BackupName) ->
    import_backup(NodeApiPort, Auth, BackupName, undefined).

import_backup(NodeApiPort, Auth, BackupName, Node) ->
    Path = ["data", "import"],
    Body = #{
        <<"filename">> => unicode:characters_to_binary(BackupName),
        <<"allow_security_profile_mismatch">> => true
    },
    Body1 =
        case Node of
            undefined -> Body;
            _ -> Body#{<<"node">> => Node}
        end,
    request(post, NodeApiPort, Path, Body1, Auth).

list_backups(NodeApiPort, Auth, Page, Limit) ->
    Path = ["data", "files"],
    request(get, NodeApiPort, Path, [{<<"page">>, Page}, {<<"limit">>, Limit}], [], Auth).

backup_file_op(Method, NodeApiPort, Auth, BackupName, QueryList) ->
    backup_file_op(Method, NodeApiPort, Auth, BackupName, QueryList, _Opts = #{}).

backup_file_op(Method, NodeApiPort, Auth, BackupName, QueryList, Opts) ->
    Path = ["data", "files", BackupName],
    request(Method, NodeApiPort, Path, QueryList, [], Auth, Opts).

upload_backup(NodeApiPort, Auth, BackupFilePath) ->
    Path = emqx_mgmt_api_test_util:api_path(?api_base_url(NodeApiPort), ["data", "files"]),
    Res = emqx_mgmt_api_test_util:upload_request(
        Path,
        BackupFilePath,
        "filename",
        <<"application/octet-stream">>,
        [],
        Auth
    ),
    case Res of
        {ok, {{"HTTP/1.1", 204, _}, _Headers, _}} ->
            ok;
        {ok, {{"HTTP/1.1", 400, _}, _Headers, _} = Resp} ->
            ct:pal("Backup upload failed: ~p", [Resp]),
            {error, bad_request};
        Err ->
            Err
    end.

%% The audit write happens after the HTTP response is already sent (see
%% minirest_handler:init/2), so poll for a bit rather than assume the record
%% is there the instant the request returns.
wait_for_audit_entries(_NodeApiPort, _Auth, _OperationId, _StartAt, _ExpectedCount, RemainMs) when
    RemainMs =< 0
->
    ct:fail(audit_entries_not_found_in_time);
wait_for_audit_entries(NodeApiPort, Auth, OperationId, StartAt, ExpectedCount, RemainMs) ->
    Entries = audit_entries_since(NodeApiPort, Auth, OperationId, StartAt),
    case length(Entries) >= ExpectedCount of
        true ->
            Entries;
        false ->
            SleepMs = 100,
            ct:sleep(SleepMs),
            wait_for_audit_entries(
                NodeApiPort, Auth, OperationId, StartAt, ExpectedCount, RemainMs - SleepMs
            )
    end.

audit_entries_since(NodeApiPort, Auth, OperationId, StartAt) ->
    QueryList = [
        {<<"operation_id">>, OperationId},
        {<<"gte_created_at">>, integer_to_binary(StartAt)},
        {<<"limit">>, <<"100">>}
    ],
    {ok, Res} = request(get, NodeApiPort, ["audit"], QueryList, [], Auth),
    #{<<"data">> := Data} = emqx_utils_json:decode(Res),
    Data.

namespace_of(#{<<"http_request">> := #{<<"namespace">> := Ns}}) -> Ns.

request(Method, NodePort, PathParts, Auth) ->
    request(Method, NodePort, PathParts, [], [], Auth).

request(Method, NodePort, PathParts, Body, Auth) ->
    request(Method, NodePort, PathParts, [], Body, Auth).

request(Method, NodePort, PathParts, QueryList, Body, Auth) ->
    request(Method, NodePort, PathParts, QueryList, Body, Auth, _Opts = #{}).

request(Method, NodePort, PathParts, QueryList, Body, Auth, Opts) ->
    Path = emqx_mgmt_api_test_util:api_path(?api_base_url(NodePort), PathParts),
    Query = unicode:characters_to_list(uri_string:compose_query(QueryList)),
    emqx_mgmt_api_test_util:request_api(Method, Path, Query, Auth, Body, Opts).

cluster(TC, Config) ->
    Nodes = emqx_cth_cluster:start(
        [
            {node_name(TC, core, 1), #{role => core, apps => apps_spec(18085, TC)}},
            {node_name(TC, core, 2), #{role => core, apps => apps_spec(18086, TC)}},
            {node_name(TC, replicant, 1), #{role => replicant, apps => apps_spec(18087, TC)}}
        ],
        #{
            work_dir => emqx_cth_suite:work_dir(TC, Config),
            start_apps_timeout => 60_000
        }
    ),
    Nodes.

node_name(TC, Role, N) ->
    NameBin = io_lib:format("api_data_backup_~s_~s~b", [TC, Role, N]),
    binary_to_atom(iolist_to_binary(NameBin)).

auth_header(Node) ->
    {ok, API} = erpc:call(Node, emqx_common_test_http, create_default_app, []),
    emqx_common_test_http:auth_header(API).

dashboard_auth_header(Node) ->
    dashboard_token_auth(Node, ?DASHBOARD_USER, ?DASHBOARD_PASS, <<"administrator">>).

viewer_auth_header(Node) ->
    dashboard_token_auth(Node, ?VIEWER_USER, ?VIEWER_PASS, <<"viewer">>).

ns_admin_auth_header(Node) ->
    ns_admin_auth_header(Node, <<"ns1">>, ?NS_ADMIN_USER, ?NS_ADMIN_PASS).

ns_admin_auth_header(Node, Namespace, User, Pass) ->
    Role = <<"ns:", Namespace/binary, "::administrator">>,
    dashboard_token_auth(Node, User, Pass, Role).

dashboard_token_auth(Node, User, Pass, Role) ->
    _ = erpc:call(Node, emqx_dashboard_admin, add_user, [
        User, Pass, Role, <<"data backup test user">>
    ]),
    {ok, #{token := Token}} = erpc:call(Node, emqx_dashboard_admin, sign_token, [User, Pass]),
    {"Authorization", "Bearer " ++ binary_to_list(Token)}.

%% Create a global dashboard administrator with an explicit scope set and
%% return its bearer-token auth header.
scoped_dashboard_token_auth(Node, User, Pass, Scopes) ->
    _ = erpc:call(Node, emqx_dashboard_admin, add_user, [
        User, Pass, <<"administrator">>, <<"data backup test user">>
    ]),
    {ok, ok} = erpc:call(Node, emqx_dashboard_admin, set_user_scopes, [User, Scopes]),
    {ok, #{token := Token}} = erpc:call(Node, emqx_dashboard_admin, sign_token, [User, Pass]),
    {"Authorization", "Bearer " ++ binary_to_list(Token)}.

ns_api_key_auth_header(Node) ->
    api_key_auth_header(Node, <<"ns_api_key_for_test">>, <<"ns:ns1::administrator">>).

api_key_auth_header(Node, Name, Role) ->
    {ok, #{api_key := Key, api_secret := Secret}} = erpc:call(Node, emqx_mgmt_auth, create, [
        Name,
        true,
        _NeverExpire = undefined,
        <<"data backup test api key">>,
        Role
    ]),
    emqx_common_test_http:auth_header(binary_to_list(Key), binary_to_list(Secret)).

assert_download_denied(Response) ->
    ?assertMatch({403, #{<<"code">> := <<"UNAUTHORIZED_ROLE">>}}, Response).

%% Return the basenames of the backups visible to `Auth'.
list_filenames(Port, Auth) ->
    list_filenames(Port, Auth, #{}).

list_filenames(Port, Auth, QueryParams) ->
    {200, #{<<"data">> := Data}} =
        data_backup_simple_request(get, Port, ["data", "files"], [], Auth, QueryParams),
    [maps:get(<<"filename">>, D) || D <- Data].

delete_backup(Port, Auth, BackupName) ->
    delete_backup(Port, Auth, BackupName, #{}).

delete_backup(Port, Auth, BackupName, QueryParams) ->
    data_backup_simple_request(
        delete, Port, ["data", "files", to_list(BackupName)], [], Auth, QueryParams
    ).

data_backup_simple_request(Method, Port, PathParts, Body, Auth) ->
    data_backup_simple_request(Method, Port, PathParts, Body, Auth, #{}).

data_backup_simple_request(Method, Port, PathParts, Body, Auth, QueryParams) ->
    Path = emqx_mgmt_api_test_util:api_path(?api_base_url(Port), PathParts),
    emqx_mgmt_api_test_util:simple_request(#{
        method => Method,
        url => Path,
        body => Body,
        auth_header => Auth,
        query_params => QueryParams
    }).

fresh_dashboard_auth(Config) ->
    [Core1, _Core2, Repl] = ?config(cluster, Config),
    Auth = dashboard_auth_header(Core1),
    ok = wait_for_dashboard_auth(Repl, Auth),
    Auth.

wait_for_auth_replication(ReplNode) ->
    wait_for_auth_replication(ReplNode, 100).

wait_for_auth_replication(ReplNode, 0) ->
    {error, {ReplNode, auth_not_ready}};
wait_for_auth_replication(ReplNode, Retries) ->
    try
        {_Header, _Val} = erpc:call(ReplNode, emqx_common_test_http, default_auth_header, []),
        ok
    catch
        _:_ ->
            timer:sleep(1),
            wait_for_auth_replication(ReplNode, Retries - 1)
    end.

wait_for_dashboard_auth(ReplNode, {"Authorization", "Bearer " ++ Token}) ->
    wait_for_dashboard_auth(ReplNode, unicode:characters_to_binary(Token), 100).

wait_for_dashboard_auth(ReplNode, _Token, 0) ->
    {error, {ReplNode, dashboard_auth_not_ready}};
wait_for_dashboard_auth(ReplNode, Token, Retries) ->
    User = ?DASHBOARD_USER,
    try
        [_] = erpc:call(ReplNode, emqx_dashboard_admin, lookup_user, [User]),
        {ok, _} = erpc:call(ReplNode, emqx_dashboard_token, lookup, [Token]),
        ok
    catch
        _:_ ->
            timer:sleep(10),
            wait_for_dashboard_auth(ReplNode, Token, Retries - 1)
    end.

apps_spec(APIPort, TC) ->
    common_apps_spec(TC) ++
        app_spec_dashboard(APIPort) ++
        test_case_specific_apps_spec(TC).

common_apps_spec(t_export_import_audit_records_namespace) ->
    [
        emqx,
        {emqx_conf, #{
            config => #{log => #{audit => #{enable => true, level => info}}}
        }},
        emqx_management
    ];
common_apps_spec(_TC) ->
    [
        emqx,
        emqx_conf,
        emqx_management
    ].

app_spec_dashboard(APIPort) ->
    [
        {emqx_dashboard, #{
            config =>
                #{
                    dashboard =>
                        #{
                            listeners =>
                                #{
                                    http =>
                                        #{bind => APIPort}
                                },
                            default_username => "",
                            default_password => ""
                        }
                }
        }}
    ].

test_case_specific_apps_spec(TC) when
    TC =:= t_export_import_audit_records_namespace
->
    [
        emqx_audit
    ];
test_case_specific_apps_spec(TC) when
    TC =:= t_upload_ee_backup;
    TC =:= t_import_ee_backup;
    TC =:= t_upload_ce_backup;
    TC =:= t_import_ce_backup;
    TC =:= t_import_api_key_blocks_sensitive_tables;
    TC =:= t_import_dashboard_token_allows_sensitive_tables;
    TC =:= t_import_restricted_dashboard_token_blocks_sensitive_tables;
    TC =:= t_import_dashboard_token_with_credential_scopes_allows_sensitive_tables;
    TC =:= t_global_admin_upload_scoped_namespace
->
    [
        emqx_auth,
        emqx_auth_http,
        emqx_auth_jwt,
        emqx_auth_mnesia,
        emqx_rule_engine,
        emqx_modules,
        emqx_bridge
    ];
test_case_specific_apps_spec(TestCase) when
    TestCase =:= t_export_cloud;
    TestCase =:= t_export_cloud_ctl;
    TestCase =:= t_import_checks_config
->
    [
        emqx_auth,
        emqx_auth_mnesia,
        emqx_schema_registry
    ];
test_case_specific_apps_spec(TestCase) when
    TestCase =:= t_schema_registry_import_order
->
    [
        emqx_schema_registry,
        emqx_schema_validation,
        emqx_message_transformation
    ];
test_case_specific_apps_spec(TestCase) when
    TestCase =:= t_exhook_backup
->
    [
        emqx_exhook
    ];
test_case_specific_apps_spec(_TC) ->
    [].

-doc """
Return the path of an upload fixture in `emqx_mgmt_api_data_backup_SUITE_data`.
The path is resolved in the source tree, because CT links a `*_SUITE_data`
directory into the build directory only for the suites it runs.
""".
backup_path(BackupName) ->
    Source = proplists:get_value(source, ?MODULE:module_info(compile)),
    filename:join([
        filename:dirname(Source), "emqx_mgmt_api_data_backup_SUITE_data", BackupName
    ]).
