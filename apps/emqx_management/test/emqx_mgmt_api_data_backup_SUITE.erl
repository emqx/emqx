%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mgmt_api_data_backup_SUITE).

-moduledoc """
Data backup REST API: export, list, get, delete, upload and import of
backups, and the config root keys and table sets an export or import covers.

Namespace isolation is in `emqx_mgmt_api_data_backup_ns_SUITE`. Role and
sensitive-table checks are in `emqx_mgmt_api_data_backup_authz_SUITE`.
Each case starts its own 3-node cluster.
""".

-compile(export_all).
-compile(nowarn_export_all).

-import(emqx_mgmt_api_data_backup_test_helpers, [
    do_init_per_testcase/2,
    import_backup_full/3,
    export_backup/2,
    export_backup2/3,
    import_backup/3,
    import_backup/4,
    list_backups/4,
    backup_file_op/5,
    backup_file_op/6,
    upload_backup/3,
    fresh_dashboard_auth/1,
    backup_path/1
]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("typerefl/include/types.hrl").
-include("emqx_mgmt_api_data_backup_test.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Config.

end_per_suite(_) ->
    ok.

init_per_testcase(TC, Config) when
    TC =:= t_import_refused_on_mixed_version_cluster
->
    Config;
init_per_testcase(TC, Config) when
    TC =:= t_upload_ee_backup;
    TC =:= t_import_ee_backup
->
    case emqx_release:edition() of
        ee -> do_init_per_testcase(TC, Config);
        ce -> Config
    end;
init_per_testcase(TC, Config) ->
    do_init_per_testcase(TC, Config).

end_per_testcase(_TC, Config) ->
    case ?config(cluster, Config) of
        undefined -> ok;
        Cluster -> emqx_cth_cluster:stop(Cluster)
    end.

t_export_backup(Config) ->
    Auth = ?config(auth, Config),
    export_test(?NODE1_PORT, Auth),
    export_test(?NODE2_PORT, Auth),
    export_test(?NODE3_PORT, Auth).

t_delete_backup(Config) ->
    test_file_op(delete, Config).

t_get_backup(Config) ->
    test_file_op(get, Config).

t_list_backups(Config) ->
    Auth = ?config(auth, Config),

    [{ok, _} = export_backup(?NODE1_PORT, Auth) || _ <- lists:seq(1, 10)],
    [{ok, _} = export_backup(?NODE2_PORT, Auth) || _ <- lists:seq(1, 10)],

    {ok, RespBody} = list_backups(?NODE1_PORT, Auth, <<"1">>, <<"100">>),
    #{<<"data">> := Data, <<"meta">> := #{<<"count">> := 20, <<"hasnext">> := false}} = emqx_utils_json:decode(
        RespBody
    ),
    ?assertEqual(20, length(Data)),

    {ok, EmptyRespBody} = list_backups(?NODE2_PORT, Auth, <<"2">>, <<"100">>),
    #{<<"data">> := EmptyData, <<"meta">> := #{<<"count">> := 20, <<"hasnext">> := false}} = emqx_utils_json:decode(
        EmptyRespBody
    ),
    ?assertEqual(0, length(EmptyData)),

    {ok, RespBodyP1} = list_backups(?NODE3_PORT, Auth, <<"1">>, <<"10">>),
    {ok, RespBodyP2} = list_backups(?NODE3_PORT, Auth, <<"2">>, <<"10">>),
    {ok, RespBodyP3} = list_backups(?NODE3_PORT, Auth, <<"3">>, <<"10">>),

    #{<<"data">> := DataP1, <<"meta">> := #{<<"count">> := 20, <<"hasnext">> := true}} = emqx_utils_json:decode(
        RespBodyP1
    ),
    ?assertEqual(10, length(DataP1)),
    #{<<"data">> := DataP2, <<"meta">> := #{<<"count">> := 20, <<"hasnext">> := false}} = emqx_utils_json:decode(
        RespBodyP2
    ),
    ?assertEqual(10, length(DataP2)),
    #{<<"data">> := DataP3, <<"meta">> := #{<<"count">> := 20, <<"hasnext">> := false}} = emqx_utils_json:decode(
        RespBodyP3
    ),
    ?assertEqual(0, length(DataP3)),

    ?assertEqual(Data, DataP1 ++ DataP2).

t_upload_ce_backup(Config) ->
    upload_backup_test(Config, ?UPLOAD_CE_BACKUP).

t_upload_ee_backup(Config) ->
    case emqx_release:edition() of
        ee -> upload_backup_test(Config, ?UPLOAD_EE_BACKUP);
        ce -> ok
    end.

t_import_ce_backup(Config) ->
    import_backup_test(Config, ?UPLOAD_CE_BACKUP).

t_import_ee_backup(Config) ->
    case emqx_release:edition() of
        ee -> import_backup_test(Config, ?UPLOAD_EE_BACKUP);
        ce -> ok
    end.

%% An RPC failure while importing a backup yields the structured 500
%% SERVICE_UNAVAILABLE response instead of an unhandled exception (erpc raises,
%% it does not return `{badrpc, _}').
t_import_rpc_failure(Config) ->
    [N1 | _] = ?config(cluster, Config),
    Auth = ?config(auth, Config),
    {ok, RespBody} = export_backup(?NODE1_PORT, Auth),
    #{<<"filename">> := FileName} = emqx_utils_json:decode(RespBody),
    ok = ?ON(N1, begin
        ok = meck:new(emqx_mgmt_data_backup_proto_v2, [passthrough, no_link, no_history]),
        meck:expect(emqx_mgmt_data_backup_proto_v2, import_file, fun(_, _, _, _, _) ->
            erlang:error({erpc, noconnection})
        end),
        ok
    end),
    try
        {Status, Body} = import_backup_full(?NODE1_PORT, Auth, FileName),
        ?assertEqual(500, Status),
        ?assertMatch(#{<<"code">> := <<"SERVICE_UNAVAILABLE">>}, Body)
    after
        ?ON(N1, meck:unload(emqx_mgmt_data_backup_proto_v2))
    end.

%% Verifies that we not only check, but also use the checked **and converted**
%% configuration being imported, so that we don't store non-converted raw configs in PT.
t_import_checks_config(TCConfig) ->
    [N1 | _] = Nodes = ?config(cluster, TCConfig),
    %% First, in the emulated "old version" node, we have a configuration value which is
    %% just a binary.  In the emulated "new version" node, this raw value shall be
    %% transformed to a map, and that is what's stored in the raw config persistent term.
    ?ON_ALL(
        Nodes,
        begin
            Mod = emqx_conf:schema_module(),
            ok = meck:new(Mod, [passthrough, no_link, no_history]),
            meck:expect(Mod, roots, fun() ->
                roots() ++ meck:passthrough([])
            end),
            ok = meck:new(emqx_mgmt_data_backup, [passthrough, no_link, no_history]),
            meck:expect(emqx_mgmt_data_backup, conf_keys, fun() ->
                [[<<"dummy">>] | meck:passthrough([])]
            end),
            ok = emqx_config:init_load(?MODULE, <<"dummy = \"not converted\" ">>)
        end
    ),
    {ok, _} = ?ON(N1, emqx_conf:update([dummy], <<"not converted">>, #{override_to => cluster})),
    %% Sanity check
    ?assertEqual(<<"not converted">>, ?ON(N1, emqx_config:get_raw([dummy]))),

    Auth = ?config(auth, TCConfig),
    Body = #{
        <<"table_sets">> => [],
        <<"root_keys">> => [<<"dummy">>]
    },
    Resp = export_backup2(?NODE1_PORT, Auth, Body),
    {200, #{<<"filename">> := Filepath}} = Resp,

    %% Now, we emulate the behavior of a new node version in which there is a schema
    %% converter for a field present in the old config.  This field shall be converted
    %% when importing, and the converted value shall be stored in the raw config
    %% persistent term.
    ?ON_ALL(Nodes, persistent_term:put(dummy_converter_type, map)),
    {ok, _} = import_backup(?NODE1_PORT, Auth, Filepath),

    %% The imported config should have been checked **and converted** by the schema.
    Expected = lists:duplicate(length(Nodes), {ok, #{<<"converted">> => true}}),
    ?assertEqual(
        Expected,
        ?ON_ALL(Nodes, emqx_config:get_raw([dummy]))
    ),
    ok.

%% Simple smoke test for cloud export API (export with scoped table set names and root
%% keys).
t_export_cloud(Config) ->
    Auth = ?config(auth, Config),
    Resp = export_cloud_backup(?NODE1_PORT, Auth),
    {200, #{<<"filename">> := Filepath}} = Resp,
    {ok, _} = import_backup(?NODE1_PORT, Auth, Filepath),
    ok.

%% Simple smoke test for exporting a subset of config root keys and tables via the CLI.
t_export_cloud_ctl(Config) ->
    [N1 | _] = ?config(cluster, Config),
    %% Need to explicitly load the commands because they are loaded by `emqx_machine'...
    ?ON(N1, emqx_mgmt_cli:load()),
    ok = ?ON(N1, meck:new(emqx_ctl, [no_link, passthrough])),
    RootKeys = [
        <<"connectors">>,
        <<"actions">>,
        <<"sources">>,
        <<"rule_engine">>,
        <<"schema_registry">>
    ],
    TableSets = [
        <<"banned">>,
        <<"builtin_authn">>,
        <<"builtin_authz">>
    ],
    RootKeysArg = lists:join($,, RootKeys),
    TableSetsArg = lists:join($,, TableSets),
    ?ON(
        N1,
        emqx_ctl:run_command([
            "data", "export", "--root-keys", RootKeysArg, "--table-sets", TableSetsArg
        ])
    ),
    {ok, [BackupFile]} = ?ON(N1, file:list_dir(filename:join([emqx:data_dir(), "backup"]))),
    ?ON(N1, emqx_ctl:run_command(["data", "import", BackupFile])),
    History = ?ON(N1, meck:history(emqx_ctl)),
    ?ON(N1, meck:unload(emqx_ctl)),
    OutputMsgs = [Fmt || {_Pid, {emqx_ctl, print, [Fmt | _]}, _Res} <- History],
    ?assertMatch(
        [_],
        [1 || "Data has been successfully exported" ++ _ <- OutputMsgs],
        #{output => OutputMsgs}
    ),
    ?assertMatch(
        [_],
        [1 || "Data has been imported successfully" ++ _ <- OutputMsgs],
        #{output => OutputMsgs}
    ),
    ok.

%% Checks returned error when one or more invalid table set names are given to the export
%% request.
t_export_bad_table_sets(Config) ->
    Auth = ?config(auth, Config),
    Body = #{<<"table_sets">> => [<<"foo">>, <<"bar">>, <<"foo">>]},
    ?assertMatch(
        {400, #{<<"message">> := <<"Invalid table sets: bar, foo">>}},
        export_backup2(?NODE1_PORT, Auth, Body)
    ),
    ok.

%% Checks returned error when one or more invalid root config keys are given to the export
%% request.
t_export_bad_root_keys(Config) ->
    Auth = ?config(auth, Config),
    Body = #{<<"root_keys">> => [<<"foo">>, <<"bar">>, <<"foo">>]},
    ?assertMatch(
        {400, #{<<"message">> := <<"Invalid root keys: bar, foo">>}},
        export_backup2(?NODE1_PORT, Auth, Body)
    ),
    ok.

%% Checks that we import schema registry serdes before schema validations / message
%% transformations.
%% Note: this test cannot reproduce the issue reliably even before the fix that introduced
%% it.  It serves just as a canary for regressions, if it ever manages to catch it.
t_schema_registry_import_order(Config) ->
    [N1 | _] = ?config(cluster, Config),
    Auth = ?config(auth, Config),
    SerdeName = <<"test">>,
    CreateParams = emqx_schema_registry_SUITE:schema_params(avro),
    MTName = <<"mt">>,
    PayloadSerde = #{
        <<"type">> => <<"avro">>,
        <<"schema">> => SerdeName
    },
    Operation = emqx_message_transformation_http_api_SUITE:operation(
        <<"payload.name">>, <<"concat(['hello'])">>
    ),
    Transformation = emqx_message_transformation_http_api_SUITE:transformation(
        MTName, [Operation], #{
            <<"payload_decoder">> => PayloadSerde,
            <<"payload_encoder">> => PayloadSerde
        }
    ),
    SVName = <<"sv">>,
    Check = emqx_schema_validation_http_api_SUITE:schema_check(json, SerdeName),
    Validation = emqx_schema_validation_http_api_SUITE:validation(SVName, [Check]),

    ?ON(N1, begin
        ok = emqx_schema_registry:add_schema(SerdeName, CreateParams),
        {ok, _} = emqx_message_transformation:insert(Transformation),
        {ok, _} = emqx_schema_validation:insert(Validation),
        ok
    end),
    ExportBody = #{},
    {200, #{<<"filename">> := Filepath}} = export_backup2(?NODE1_PORT, Auth, ExportBody),
    %% Remove schema so it's absent when importing stuff back
    ?ON(N1, begin
        {ok, _} = emqx_message_transformation:delete(MTName),
        {ok, _} = emqx_schema_validation:delete(SVName),
        ok = emqx_schema_registry:delete_schema(SerdeName),
        ok
    end),
    {ok, _} = import_backup(?NODE1_PORT, Auth, Filepath),
    ok.

t_exhook_backup(Config) ->
    [N1 | _] = ?config(cluster, Config),
    Auth = ?config(auth, Config),
    Name = <<"myhook">>,
    ExhookConf = #{
        <<"name">> => Name,
        <<"url">> => <<"http://127.0.0.1">>
    },
    {ok, _} = ?ON(N1, emqx_exhook_mgr:update_config([exhook, servers], {add, ExhookConf})),
    ExportBody = #{},
    {200, #{<<"filename">> := Filepath}} = export_backup2(?NODE1_PORT, Auth, ExportBody),
    {ok, _} = ?ON(N1, emqx_exhook_mgr:update_config([exhook, servers], {delete, Name})),
    %% Need to explicitly load the commands because they are loaded by `emqx_machine'...
    ?ON(N1, emqx_mgmt_cli:load()),
    ?ON(N1, emqx_ctl:run_command(["data", "import", Filepath])),
    ok.

%% Import is refused while the running nodes report different major.minor
%% versions: the backplane contract is frozen per minor release.
t_import_refused_on_mixed_version_cluster(_Config) ->
    ok = meck:new(emqx_management_proto_v5, [passthrough, no_link, no_history]),
    ok = meck:new(emqx, [passthrough, no_link, no_history]),
    try
        meck:expect(emqx, running_nodes, fun() -> ['a@127.0.0.1', 'b@127.0.0.1'] end),
        meck:expect(emqx_management_proto_v5, node_info, fun(['a@127.0.0.1', 'b@127.0.0.1']) ->
            [{ok, #{version => <<"6.0.4">>}}, {ok, #{version => <<"6.1.5">>}}]
        end),
        {Status, Body} = emqx_mgmt_api_data_backup:data_import(post, #{
            body => #{<<"filename">> => <<"whatever.tar.gz">>},
            query_string => #{},
            auth_meta => #{auth_type => jwt_token}
        }),
        ?assertEqual(400, Status),
        ?assertMatch(#{code := 'BAD_REQUEST'}, Body),
        #{message := Msg} = Body,
        ?assertNotEqual(nomatch, binary:match(Msg, <<"6.0">>)),
        ?assertNotEqual(nomatch, binary:match(Msg, <<"6.1">>))
    after
        meck:unload(emqx_management_proto_v5),
        meck:unload(emqx)
    end.

test_file_op(Method, Config) ->
    %% GET is restricted to the dashboard global administrator (backups can
    %% contain dashboard / api-key records). DELETE has no role restriction
    %% and continues to use the API key.
    Auth =
        case Method of
            get -> ?config(dashboard_auth, Config);
            delete -> ?config(auth, Config)
        end,
    %% Exports go through the API-key auth on every node so the archives
    %% to operate on exist regardless of the method-specific auth above.
    ApiAuth = ?config(auth, Config),

    {ok, Node1Resp} = export_backup(?NODE1_PORT, ApiAuth),
    {ok, Node2Resp} = export_backup(?NODE2_PORT, ApiAuth),
    {ok, Node3Resp} = export_backup(?NODE3_PORT, ApiAuth),

    ParsedResps = [emqx_utils_json:decode(R) || R <- [Node1Resp, Node2Resp, Node3Resp]],

    [Node1Parsed, Node2Parsed, Node3Parsed] = ParsedResps,

    %% node param is not set in Query, expect get/delete the backup on the local node
    F1 = fun() ->
        backup_file_op(Method, ?NODE1_PORT, Auth, maps:get(<<"filename">>, Node1Parsed), [])
    end,
    ?assertMatch({ok, _}, F1()),
    assert_second_call(Method, F1()),

    %% Node 2 must get/delete the backup on Node 3 via rpc
    F2 = fun() ->
        backup_file_op(
            Method,
            ?NODE2_PORT,
            Auth,
            maps:get(<<"filename">>, Node3Parsed),
            [{<<"node">>, maps:get(<<"node">>, Node3Parsed)}],
            #{return_all => true}
        )
    end,
    Res2 = F2(),
    Code2 =
        case Method of
            get -> 200;
            delete -> 204
        end,
    ?assertMatch({ok, {{_, Code2, _}, _, _}}, Res2),
    {ok, {{_, Code2, _}, Headers2List, _}} = Res2,
    case Method of
        get ->
            ?assertMatch(
                #{"content-type" := "application/octet-stream"}, maps:from_list(Headers2List)
            );
        _ ->
            ok
    end,
    assert_second_call(Method, F2()),

    %% The same as above but nodes are switched
    F3 = fun() ->
        backup_file_op(
            Method,
            ?NODE3_PORT,
            Auth,
            maps:get(<<"filename">>, Node2Parsed),
            [{<<"node">>, maps:get(<<"node">>, Node2Parsed)}]
        )
    end,
    ?assertMatch({ok, _}, F3()),
    assert_second_call(Method, F3()).

export_test(NodeApiPort, Auth) ->
    {ok, RespBody} = export_backup(NodeApiPort, Auth),
    #{
        <<"created_at">> := _,
        <<"created_at_sec">> := CreatedSec,
        <<"filename">> := _,
        <<"node">> := _,
        <<"size">> := Size
    } = emqx_utils_json:decode(RespBody),
    ?assert(is_integer(Size)),
    ?assert(is_integer(CreatedSec) andalso CreatedSec > 0).

upload_backup_test(Config, BackupName) ->
    Auth = ?config(auth, Config),
    %% GET is restricted to the dashboard global administrator (backups can
    %% contain dashboard / api-key records). The "did the bad upload leave a
    %% file behind?" probe needs admin auth to reach the 404 path.
    DashboardAuth = ?config(dashboard_auth, Config),
    UploadFile = backup_path(BackupName),
    BadImportFile = backup_path(?BAD_IMPORT_BACKUP),
    BadUploadFile = backup_path(?BAD_UPLOAD_BACKUP),

    ?assertEqual(ok, upload_backup(?NODE3_PORT, Auth, UploadFile)),
    %% This file was specially forged to pass upload validation bat fail on import
    ?assertEqual(ok, upload_backup(?NODE2_PORT, Auth, BadImportFile)),
    ?assertEqual({error, bad_request}, upload_backup(?NODE1_PORT, Auth, BadUploadFile)),
    %% Invalid file must not be kept
    ?assertMatch(
        {error, {_, 404, _}},
        backup_file_op(get, ?NODE1_PORT, DashboardAuth, ?BAD_UPLOAD_BACKUP, [])
    ).

import_backup_test(Config, BackupName) ->
    Auth = ?config(auth, Config),
    %% Fixtures contain sensitive mnesia tables (emqx_admin, emqx_app);
    %% importing them is only allowed via dashboard bearer-token auth, not API key.
    DashboardAuth = ?config(dashboard_auth, Config),
    UploadFile = backup_path(BackupName),
    BadImportFile = backup_path(?BAD_IMPORT_BACKUP),

    ?assertEqual(ok, upload_backup(?NODE3_PORT, Auth, UploadFile)),

    %% This file was specially forged to pass upload validation bat fail on import
    ?assertEqual(ok, upload_backup(?NODE2_PORT, Auth, BadImportFile)),

    %% Replicant node must be able to import the file by doing rpc to a core node
    ?assertMatch({ok, _}, import_backup(?NODE3_PORT, DashboardAuth, BackupName)),

    [N1, N2, N3] = ?config(cluster, Config),

    DashboardAuth1 = fresh_dashboard_auth(Config),
    ?assertMatch({ok, _}, import_backup(?NODE3_PORT, DashboardAuth1, BackupName)),

    DashboardAuth2 = fresh_dashboard_auth(Config),
    ?assertMatch({ok, _}, import_backup(?NODE1_PORT, DashboardAuth2, BackupName, N3)),
    %% Now this node must also have the file locally
    DashboardAuth3 = fresh_dashboard_auth(Config),
    ?assertMatch({ok, _}, import_backup(?NODE1_PORT, DashboardAuth3, BackupName, N1)),

    DashboardAuth4 = fresh_dashboard_auth(Config),
    ?assertMatch(
        {error, {_, 400, _}}, import_backup(?NODE2_PORT, DashboardAuth4, ?BAD_IMPORT_BACKUP, N2)
    ).

assert_second_call(get, Res) ->
    ?assertMatch({ok, _}, Res);
assert_second_call(delete, Res) ->
    case Res of
        {error, {_, 404, _}} ->
            ok;
        {error, {{_, 404, _}, _, _}} ->
            ok;
        _ ->
            ct:fail("unexpected result: ~p", [Res])
    end.

export_cloud_backup(NodeApiPort, Auth) ->
    Body = #{
        <<"table_sets">> => [
            <<"banned">>,
            <<"builtin_authn">>,
            <<"builtin_authz">>
        ],
        <<"root_keys">> => [
            <<"connectors">>,
            <<"actions">>,
            <<"sources">>,
            <<"rule_engine">>,
            <<"schema_registry">>
        ]
    },
    export_backup2(NodeApiPort, Auth, Body).

roots() ->
    [
        {dummy,
            hoconsc:mk(hoconsc:union([map(), binary()]), #{
                converter => fun dummy_converter/2
            })}
    ].

dummy_converter(X, _Opts) ->
    case persistent_term:get(dummy_converter_type, binary) of
        binary ->
            X;
        map ->
            #{<<"converted">> => true}
    end.
