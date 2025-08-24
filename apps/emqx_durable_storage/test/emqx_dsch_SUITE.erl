%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_dsch_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("../src/emqx_dsch.hrl").

%% This testcase verifies basic functions of the schema server:
%%
%% 1. Backend registration
%%
%% 2. Creation and persistence of the site ID
t_010_initialization(_Config) ->
    %% Test gen_server boilerplate code for handling unknown calls, casts, and infos:
    ?assertMatch({error, unknown_call}, gen_server:call(emqx_dsch, garbage)),
    gen_server:cast(emqx_dsch, garbage),
    emqx_dsch ! garbage,
    %% Verify initialization of the site ID:
    Site = emqx_dsch:this_site(),
    ?assert(is_binary(Site)),
    %% Restart application. Previous site ID should be restored:
    application:stop(emqx_durable_storage),
    application:start(emqx_durable_storage),
    ?assertMatch(Site, emqx_dsch:this_site()),
    %%    Verity `get_schema' API:
    #{
        site := Site,
        dbs := DBs
    } = emqx_dsch:get_site_schema(),
    ?assertMatch(0, maps:size(DBs)),

    %% Verify registration of backends:
    ?assertMatch(
        {error, {no_such_backend, foo}},
        emqx_dsch:get_backend_cbm(foo)
    ),
    ?assertMatch(ok, emqx_dsch:register_backend(foo, ?MODULE)),
    ?assertMatch(
        {ok, ?MODULE},
        emqx_dsch:get_backend_cbm(foo)
    ),
    %%    Check idempotency:
    ?assertMatch(ok, emqx_dsch:register_backend(foo, ?MODULE)),
    %%    Conflicts should be detected:
    ?assertMatch(
        {error, {conflict, _}},
        emqx_dsch:register_backend(foo, bar)
    ),
    %% Verify clean-up of persistent terms.
    %%
    %% Test self-check: to later verify that persistent terms are
    %% erased, we make sure they're present in the first place:
    ?assertMatch(#{ver := _, site := Site}, persistent_term:get(?dsch_pt_schema)),
    ?assertMatch(#{foo := _}, persistent_term:get(?dsch_pt_backends)),
    %%    Stop the application. Verify that persistent terms are gone:
    application:stop(emqx_durable_storage),
    ?assertMatch(undefined, persistent_term:get(?dsch_pt_schema, undefined)),
    ?assertMatch(undefined, persistent_term:get(?dsch_pt_backends, undefined)).

%% This testcase verifies creation and persistence of the DB schemas.
t_020_ensure_schema(_Config) ->
    emqx_dsch:register_backend(test, ?MODULE),
    emqx_dsch:register_backend(test2, ?MODULE),
    ?assertMatch(
        {ok, true, #{backend := test, foo := bar}},
        emqx_dsch:ensure_db_schema(test_db, #{backend => test, foo => bar})
    ),
    ?assertMatch(
        #{
            ver := 1,
            site := _,
            dbs := #{test_db := #{backend := test, foo := bar}}
        },
        emqx_dsch:get_site_schema()
    ),
    %% Attempt to override schema with the same backend and different
    %% perameters (original schema should be preserved):
    ?assertMatch(
        {ok, false, #{backend := test, foo := bar}},
        emqx_dsch:ensure_db_schema(test_db, #{backend => test, foo => 1})
    ),
    ?assertMatch(
        #{
            ver := 1,
            site := _,
            dbs := #{test_db := #{backend := test, foo := bar}}
        },
        emqx_dsch:get_site_schema()
    ),
    %% Attempt to re-create DB with a different backend should fail:
    ?assertMatch(
        {error, {backend_mismatch, test, test2}},
        emqx_dsch:ensure_db_schema(test_db, #{backend => test2, foo => 1})
    ),
    %% Create a new DB with different backend:
    ?assertMatch(
        {ok, true, #{backend := test2, bar := baz}},
        emqx_dsch:ensure_db_schema(test_db2, #{backend => test2, bar => baz})
    ),
    %% Restart application. DB schemas should be restored:
    SchemaBeforeRestart = emqx_dsch:get_site_schema(),
    ?assertMatch(
        #{
            ver := 1,
            site := _,
            dbs := #{
                test_db := _,
                test_db2 := #{backend := test2, bar := baz}
            }
        },
        SchemaBeforeRestart
    ),
    application:stop(emqx_durable_storage),
    application:start(emqx_durable_storage),
    ?assertEqual(
        SchemaBeforeRestart,
        emqx_dsch:get_site_schema()
    ).

%% This testcase verifies functionality related to database runtime states.
%%
%% 1. Database can be opened only when its schema exists and it isn't
%% open already.
%%
%% 2. Proper cleanup when application is stopped.
%%
%% 3. Proper cleanup when DB is stopped.
t_030_open_close_db(_Config) ->
    RTconf = #{bar => baz},
    emqx_dsch:register_backend(test, ?MODULE),
    %% Try to open DB without a schema, it should fail:
    ?assertMatch(
        {error, no_db_schema},
        emqx_dsch:open_db(test_db, RTconf)
    ),
    %% Create schema and open DB normally. Its global state should be initialized:
    {ok, true, DBSchema} = emqx_dsch:ensure_db_schema(test_db, #{backend => test, foo => bar}),
    ?assertMatch(ok, emqx_dsch:open_db(test_db, RTconf)),
    %% Try to re-open (it should fail, the original config should remain):
    ?assertMatch(
        {error, already_open},
        emqx_dsch:open_db(test_db, #{foo => bar})
    ),
    #{
        cbm := ?MODULE,
        gvars := GVars1,
        runtime := RTconf
    } = Consts1 = emqx_dsch:get_db_runtime(test_db),
    %% Test self-check: to later verify that persistent term is
    %% erased, we make sure it's present in the first place:
    ?assertEqual(
        Consts1,
        persistent_term:get(?dsch_pt_db_runtime(test_db))
    ),
    %% Global variables table should be valid:
    ?assertMatch(
        [_ | _],
        ets:info(GVars1)
    ),
    ?assertEqual(
        GVars1,
        emqx_dsch:db_gvars(test_db)
    ),
    %% Restart the application. Make sure DB's persistent term is
    %% cleaned up and gvars are gone:
    application:stop(emqx_durable_storage),
    ?assertEqual(
        undefined,
        persistent_term:get(?dsch_pt_db_runtime(test_db), undefined)
    ),
    ?assertEqual(
        undefined,
        ets:info(GVars1)
    ),
    application:start(emqx_durable_storage),
    emqx_dsch:register_backend(test, ?MODULE),
    %% This time openining the DB should succeed immediately since the
    %% schema already exists:
    ?assertMatch(ok, emqx_dsch:open_db(test_db, #{baz => quux})),
    ?assertMatch(
        #{
            cbm := ?MODULE,
            gvars := _,
            runtime := #{baz := quux}
        },
        emqx_dsch:get_db_runtime(test_db)
    ),
    %% Verify the same using ?with_dsch macro:
    ?assertMatch(
        ?MODULE,
        ?with_dsch(
            test_db,
            #{cbm := CBM},
            begin
                CBM
            end
        )
    ),
    %% Close the DB and verify proper cleanup:
    ?assertMatch(ok, emqx_dsch:close_db(test_db)),
    ?assertMatch(ok, emqx_dsch:close_db(test_db)),
    ?assertMatch(
        undefined,
        emqx_dsch:get_db_runtime(test_db)
    ),
    %% Runtime is gone, but schema should remain:
    ?assertEqual(
        DBSchema,
        emqx_dsch:get_db_schema(test_db)
    ),
    %% Verify the same using ?with_dsch macro:
    ?assertMatch(
        {error, recoverable, {database_is_not_open, test_db}},
        ?with_dsch(
            test_db,
            #{cbm := CBM},
            begin
                CBM
            end
        )
    ).

%% This testcase verifies `drop_db_schema' function
t_040_drop_db_schema(_Config) ->
    Site = emqx_dsch:this_site(),
    ok = emqx_dsch:register_backend(test, ?MODULE),
    %% It's impossible to drop a DB that doesn't exist:
    ?assertMatch(
        {error, no_db_schema},
        emqx_dsch:drop_db_schema(test_db)
    ),
    %% Create and open two DBs:
    ?assertMatch(
        {ok, _, _},
        emqx_dsch:ensure_db_schema(test_db, #{backend => test})
    ),
    ?assertMatch(
        {ok, _, _},
        emqx_dsch:ensure_db_schema(test_db2, #{backend => test})
    ),
    ?assertMatch(
        ok,
        emqx_dsch:open_db(test_db, #{})
    ),
    %% It's impossible to drop schema of an open DB:
    ?assertMatch(
        {error, database_is_open},
        emqx_dsch:drop_db_schema(test_db)
    ),
    %% Close DB. Now its schema can be dropped:
    ok = emqx_dsch:close_db(test_db),
    ?assertMatch(
        ok,
        emqx_dsch:drop_db_schema(test_db)
    ),
    %% Verify that the DB is gone from the global schema:
    #{site := Site, dbs := DBSchemas} = SchemaBeforeRestart = emqx_dsch:get_site_schema(),
    ?assertMatch(
        [{test_db2, _}],
        maps:to_list(DBSchemas)
    ),
    %% Restart application and verify that the change was persisted:
    application:stop(emqx_durable_storage),
    application:start(emqx_durable_storage),
    ?assertMatch(
        SchemaBeforeRestart,
        emqx_dsch:get_site_schema()
    ).

%% This testcase verfies updating of database runtime config.
%%
%% 1. It's impossible to update runtime configuration of a DB that isn't open.
%%
%% 2. Runtime config should be merged using `deep_merge'
%%
%% 3. Other constants (Schema, CBM, gvars table) shouldn't change
t_050_update_db_config(_Config) ->
    RTconf1 = #{foo => #{1 => 2, 2 => 3}, bar => baz},
    Patch1 = #{foo => #{1 => 3}, baz => quux},
    ok = emqx_dsch:register_backend(test, ?MODULE),
    {ok, true, DBSchema} = emqx_dsch:ensure_db_schema(test_db, #{backend => test, foo => bar}),
    %% It should be impossible to update state of a closed db:
    ?assertMatch(
        {error, {database_is_not_open, test_db}},
        emqx_dsch:update_db_config(test_db, Patch1)
    ),
    %% Open DB:
    ?assertMatch(ok, emqx_dsch:open_db(test_db, RTconf1)),
    #{
        cbm := ?MODULE,
        gvars := GVars,
        runtime := RTconf1
    } = emqx_dsch:get_db_runtime(test_db),
    %% Patch its config:
    ?assertMatch(ok, emqx_dsch:update_db_config(test_db, Patch1)),
    #{
        cbm := ?MODULE,
        gvars := GVars,
        runtime := #{foo := #{1 := 3, 2 := 3}, bar := baz, baz := quux}
    } = emqx_dsch:get_db_runtime(test_db),
    %% Patch it again (without actual changes):
    ?assertMatch(ok, emqx_dsch:update_db_config(test_db, #{})),
    #{
        cbm := ?MODULE,
        gvars := GVars,
        runtime := #{foo := #{1 := 3, 2 := 3}, bar := baz, baz := quux}
    } = emqx_dsch:get_db_runtime(test_db),
    %% DB schema is not affected by any of this:
    ?assertEqual(
        DBSchema,
        emqx_dsch:get_db_schema(test_db)
    ).

%% This testcase verifies operations with the cluster and peers.
%%
%% Peers cannot join to a singleton cluster; cluster cannot become
%% singleton while having peers.
t_060_set_cluster(_Config) ->
    Sch0 = emqx_dsch:get_site_schema(),
    %% Try invalid cluster ID:
    ?assertMatch(
        {error, badarg},
        emqx_dsch:set_cluster("invalid")
    ),
    ?assertMatch(
        {error, badarg},
        emqx_dsch:set_cluster(invalid)
    ),
    %% Verify that peers cannot be added while in singleton mode:
    ?assertMatch(
        {error, cannot_add_peers_while_in_singleton_mode},
        emqx_dsch:set_peer(<<"peer1">>, active)
    ),
    %% Deleting a peer that doesn't exist should succeed though:
    ?assertMatch(
        ok,
        emqx_dsch:delete_peer(<<"peer1">>)
    ),
    %% Set cluster to a correct value:
    CID = <<"my_cluster">>,
    ok = emqx_dsch:set_cluster(CID),
    ?assertEqual(
        Sch0#{cluster => CID},
        emqx_dsch:get_site_schema()
    ),
    %% Add some peers:
    ok = emqx_dsch:set_peer(<<"peer1">>, active),
    ok = emqx_dsch:set_peer(<<"peer2">>, banned),
    %% Restart the application and make sure the changes are persisted:
    application:stop(emqx_durable_storage),
    application:start(emqx_durable_storage),
    ?assertEqual(
        Sch0#{cluster => CID, peers => #{<<"peer1">> => active, <<"peer2">> => banned}},
        emqx_dsch:get_site_schema()
    ),
    %% Try to enter singleton mode while having peers. This should fail:
    ?assertMatch(
        {error, has_peers},
        emqx_dsch:set_cluster(singleton)
    ),
    %% Delete peers and try again:
    ok = emqx_dsch:delete_peer(<<"peer1">>),
    ok = emqx_dsch:delete_peer(<<"peer2">>),
    ?assertMatch(
        ok,
        emqx_dsch:set_cluster(singleton)
    ),
    %% Schema should return to the original state:
    ?assertEqual(
        Sch0,
        emqx_dsch:get_site_schema()
    ),
    %% Restart the application and make sure the changes are persisted:
    application:stop(emqx_durable_storage),
    application:start(emqx_durable_storage),
    ?assertEqual(
        Sch0,
        emqx_dsch:get_site_schema()
    ).

%% This testcase verifies various scenarios where schema migration is interrupted:
t_070_aborted_schema_migrations(_Config) ->
    ListBackups = fun() ->
        {ok, Files} = file:list_dir(filename:dirname(emqx_dsch:schema_file())),
        [FN || FN <- Files, string:find(FN, "BAK") =/= nomatch]
    end,
    ?check_trace(
        #{timetrap => 15_000},
        begin
            %% No backup files should originally exist:
            ?assertMatch(
                [],
                ListBackups()
            ),
            New = emqx_dsch:schema_file() ++ ".NEW",
            %% Fill schema with data:
            ok = emqx_dsch:register_backend(test, ?MODULE),
            ok = emqx_dsch:set_cluster(<<"my_cluster">>),
            ok = emqx_dsch:set_peer(<<"peer1">>, active),
            ok = emqx_dsch:set_peer(<<"peer2">>, banned),
            {ok, true, _} = emqx_dsch:ensure_db_schema(test_db1, #{backend => test, foo => bar}),
            {ok, true, _} = emqx_dsch:ensure_db_schema(test_db2, #{backend => test, bar => baz}),
            Schema = emqx_dsch:get_site_schema(),
            %% Stop application and imitate aborted attempt to migrate
            %% the site schema by creating a "NEW" file filled with
            %% junk. `emqx_dsch' server should discard it without
            %% trying to read it.
            application:stop(emqx_durable_storage),
            ok = file:write_file(New, <<"garbage">>),
            ?assertMatch(
                {_, {ok, _}},
                ?wait_async_action(
                    application:start(emqx_durable_storage),
                    #{?snk_kind := emqx_dsch_discard_new_schema, file := New}
                )
            ),
            ?assertNot(filelib:is_file(New)),
            ?assertEqual(
                Schema,
                emqx_dsch:get_site_schema()
            ),
            %% Since this was unusual, we leave a backup:
            ?assertMatch(
                [_],
                ListBackups()
            ),
            %% Stop application and move current schema to NEW. This
            %% is unusual, but should be handled too.
            application:stop(emqx_durable_storage),
            ok = file:rename(emqx_dsch:schema_file(), New),
            ?assertMatch(
                {_, {ok, _}},
                ?wait_async_action(
                    application:start(emqx_durable_storage),
                    #{?snk_kind := "Restoring schema from NEW file", file := New}
                )
            ),
            ?assertEqual(
                Schema,
                emqx_dsch:get_site_schema()
            ),
            %% Since this was very unusual, we leave another backup:
            ?assertMatch(
                [_, _],
                ListBackups()
            ),
            %% Restart application normally and verify that this
            %% didn't leave new BAK files:
            application:stop(emqx_durable_storage),
            application:start(emqx_durable_storage),
            ?assertMatch(
                [_, _],
                ListBackups()
            ),
            ?assertEqual(
                Schema,
                emqx_dsch:get_site_schema()
            )
        end,
        []
    ).

%%

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_testcase(TCName, Config) ->
    WorkDir = emqx_cth_suite:work_dir(TCName, Config),
    Apps = emqx_cth_suite:start(
        [{emqx_durable_storage, #{override_env => [{db_data_dir, WorkDir}]}}],
        #{work_dir => WorkDir}
    ),
    [{apps, Apps} | Config].

end_per_testcase(_TCName, Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)).
