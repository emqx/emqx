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
    ?assertMatch(#{site := Site}, persistent_term:get(?dsch_pt_schema)),
    ?assertMatch(#{foo := _}, persistent_term:get(?dsch_pt_backends)),
    %%    Stop the application. Verify that persistent terms are gone:
    application:stop(emqx_durable_storage),
    ?assertMatch(undefined, persistent_term:get(?dsch_pt_schema, undefined)),
    ?assertMatch(undefined, persistent_term:get(?dsch_pt_backends, undefined)).

%% This testcase verifies creation and persistence of the DB schemas.
t_020_ensure_schema(_Config) ->
    emqx_dsch:register_backend(test, ?MODULE),
    emqx_dsch:register_backend(test2, ?MODULE),
    %% Try to create a DB using nonexistent backend (it should fail):
    ?assertMatch(
        {error, {no_such_backend, bad}},
        emqx_dsch:ensure_db_schema(test_db, #{backend => bad, foo => bar})
    ),
    %% Successful creation:
    ?assertMatch(
        {ok, true, #{backend := test, foo := bar}},
        emqx_dsch:ensure_db_schema(test_db, #{backend => test, foo => bar})
    ),
    ?assertMatch(
        #{
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
        {error, database_is_currently_open},
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
%% 2. Backend is notified about the changes
%%
%% 3. Other constants (Schema, CBM, gvars table) shouldn't change
t_050_update_db_config(_Config) ->
    RTconf1 = #{foo => #{1 => 2, 2 => 3}, bar => baz},
    Patch1 = #{foo => #{1 => 3}, baz => quux},
    ?check_trace(
        begin
            ok = emqx_dsch:register_backend(test, ?MODULE),
            {ok, true, DBSchema} = emqx_dsch:ensure_db_schema(test_db, #{
                backend => test, foo => bar
            }),
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
                runtime := Patch1
            } = emqx_dsch:get_db_runtime(test_db),
            %% Patch it again (without actual changes):
            ?assertMatch(ok, emqx_dsch:update_db_config(test_db, #{})),
            #{
                cbm := ?MODULE,
                gvars := GVars,
                runtime := #{}
            } = emqx_dsch:get_db_runtime(test_db),
            %% DB schema is not affected by any of this:
            ?assertEqual(
                DBSchema,
                emqx_dsch:get_db_schema(test_db)
            )
        end,
        fun(Trace) ->
            %% Verify that callbacks have been executed:
            ?assertEqual(
                [
                    {test_db, Patch1},
                    {test_db, #{}}
                ],
                ?projection([db, conf], ?of_kind(test_config_change, Trace))
            )
        end
    ).

%% This testcase verifies that DB schema can be updated while DB is
%% offline, and the backend is notified about the changes.
t_051_update_db_schema(_Config) ->
    DB = test_db,
    ?check_trace(
        begin
            ok = emqx_dsch:register_backend(test, ?MODULE),
            {ok, true, _} = emqx_dsch:ensure_db_schema(DB, #{
                backend => test, foo => bar
            }),
            ?assertMatch(
                {error, backend_cannot_be_changed},
                emqx_dsch:update_db_schema(DB, #{backend => other})
            ),
            ?assertMatch(
                ok,
                emqx_dsch:update_db_schema(DB, #{backend => test, foo => baz})
            ),
            ?assertMatch(
                #{backend := test, foo := baz},
                emqx_dsch:get_db_schema(DB)
            ),
            ?assertMatch(
                [
                    {_, #{
                        command := change_schema,
                        old := #{backend := test, foo := bar},
                        new := #{backend := test, foo := baz}
                    }}
                ],
                maps:to_list(emqx_dsch:list_pending({db, DB}))
            )
        end,
        []
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
    %% Now it can't be overwritten:
    ?assertMatch(
        {error, cannot_change_cluster_existing_id},
        emqx_dsch:set_cluster(<<"other_cluster">>)
    ),
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

%% This testcase verifies that node owning the site ID can be located
%% via global.
t_070_global_registration(_Config) ->
    ?check_trace(
        begin
            #{site := Site} = Schema = emqx_dsch:get_site_schema(),
            ?assertEqual(
                node(),
                emqx_dsch:whereis_site(Site)
            ),
            ?assertEqual(
                undefined,
                emqx_dsch:whereis_site(<<"bad">>)
            ),
            %% Try to access schema via RPC:
            ?assertEqual(
                {ok, Schema},
                emqx_dsch:get_site_schema(Site)
            ),
            ?assertEqual(
                {error, down},
                emqx_dsch:get_site_schema(<<"bad">>)
            ),
            %% Server should detect name conflicts:
            ?assertMatch(
                {_, {ok, _}},
                ?wait_async_action(
                    begin
                        Pid = global:whereis_name(?global_name(Site)),
                        Pid ! {global_name_conflict, ?global_name(Site)}
                    end,
                    #{?snk_kind := global_site_conflict, site := Site}
                )
            )
        end,
        []
    ).

%% This testcase verifies various scenarios where schema migration is interrupted:
t_080_aborted_schema_migrations(_Config) ->
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

%% This testcase verifies that schema server can manage list of
%% pending actions.
%%
%% 1. Each new action is assigned a unique ID.
%%
%% 2. There's basic validation of actions
%%
%% 3. Actions can be deleted by their ID.
%%
%% 4. Actions can be listed and filtered by scope
%%
%% 5. They survive node restart
t_090_add_pending(_Config) ->
    DB = test_db,
    ?check_trace(
        begin
            %% Invalid scope should be rejected:
            ?assertMatch(
                {error, badarg},
                emqx_dsch:add_pending(bad_scope, cmd, #{})
            ),
            %% Invalid commands with `site` scope should be rejected:
            ?assertMatch(
                {error, unknown_site_command},
                emqx_dsch:add_pending(site, cmd, #{})
            ),
            %% Cannot submit commands to a non-existent DB:
            ?assertMatch(
                {error, no_db},
                emqx_dsch:add_pending({db, DB}, cmd, #{})
            ),
            %% Create a DB:
            emqx_dsch:register_backend(test, ?MODULE),
            ?assertMatch(
                {ok, _, _},
                emqx_dsch:ensure_db_schema(DB, #{backend => test})
            ),
            %% Now command should succeed:
            ?assertMatch(
                ok,
                emqx_dsch:add_pending({db, DB}, cmd, #{})
            ),
            %% Verify filtering of results by scope:
            ?assertMatch(
                [{1, #{scope := {db, DB}, command := cmd, start_time := _}}],
                maps:to_list(emqx_dsch:list_pending())
            ),
            ?assertMatch(
                [{1, #{scope := {db, DB}, command := cmd, start_time := _}}],
                maps:to_list(emqx_dsch:list_pending({db, DB}))
            ),
            ?assertMatch(
                [],
                maps:to_list(emqx_dsch:list_pending({db, other}))
            ),
            ?assertMatch(
                [],
                maps:to_list(emqx_dsch:list_pending(site))
            ),
            %% Add two more action, their ids should be 2 and 3:
            ok = emqx_dsch:add_pending({db, DB}, cmd, #{foo => bar}),
            ok = emqx_dsch:add_pending({db, DB}, cmd2, #{}),
            ?assertMatch(
                [{1, #{}}, {2, #{command := cmd}}, {3, #{command := cmd2}}],
                maps:to_list(emqx_dsch:list_pending())
            ),
            %% Delete 1 and 3 and restart the node:
            ok = emqx_dsch:del_pending(1),
            ok = emqx_dsch:del_pending(3),
            ?assertMatch(
                [{2, #{scope := {db, DB}, command := cmd, foo := bar}}],
                maps:to_list(emqx_dsch:list_pending())
            ),
            application:stop(emqx_durable_storage),
            application:start(emqx_durable_storage),
            %% Old action should be preserved together with the IDs counter:
            ok = emqx_dsch:add_pending({db, DB}, cmd3, #{}),
            ?assertMatch(
                [{2, #{command := cmd, foo := bar}}, {4, #{command := cmd3}}],
                maps:to_list(emqx_dsch:list_pending())
            )
        end,
        []
    ).

%% The following testcase verifies execution of the pending tasks.
%%
%% Pending actions with "DB" scope should only be executed when the
%% corresponding DB is open.
t_100_action_execution(_Config) ->
    DB = test_db,
    ?check_trace(
        begin
            %% Create a DB without opening it:
            emqx_dsch:register_backend(test, ?MODULE),
            ?assertMatch(
                {ok, _, _},
                emqx_dsch:ensure_db_schema(DB, #{backend => test})
            ),
            %% Add cluster, then add and remove a peer, it should
            %% create pending actions for the DB:
            ok = emqx_dsch:set_cluster(<<"my_cluster">>),
            ok = emqx_dsch:set_peer(<<"peer">>, active),
            ok = emqx_dsch:delete_peer(<<"peer">>),
            %% Start the DB. It should now receive and process all
            %% events:
            {ok, SRef1} = snabbkaffe:subscribe(
                ?match_event(#{?snk_kind := test_schema_change}),
                3,
                5_000
            ),
            ?tp(test_open_db, #{}),
            ok = emqx_dsch:open_db(DB, #{}),
            {ok, Changes} = snabbkaffe:receive_events(SRef1),
            ?assertMatch(
                [
                    {DB, #{command := set_cluster, cluster := <<"my_cluster">>}},
                    {DB, #{command := set_peer, site := <<"peer">>, state := active}},
                    {DB, #{command := delete_peer, site := <<"peer">>}}
                ],
                ?projection([db, task], Changes)
            ),
            %% Test closing of a DB with pending tasks:
            ?force_ordering(
                #{?snk_kind := test_close_db},
                #{?snk_kind := test_schema_change}
            ),
            ?assertMatch(
                {ok, {ok, _}},
                ?wait_async_action(
                    emqx_dsch:close_db(DB),
                    #{?snk_kind := "Completed schema change"}
                )
            )
        end,
        [
            {no_failed_start, fun(Trace) ->
                ?assertMatch(
                    [],
                    ?of_kind(?tp_pending_spawn_fail, Trace)
                )
            end}
        ]
    ).

%% This testcase verifies operations with the DB global variables.
t_200_gvars(_Config) ->
    %% Setup:
    DB = test_db,
    Scope = scope,
    Shard = <<"1">>,
    OtherShard = <<"2">>,
    ok = emqx_dsch:register_backend(test, ?MODULE),
    ?assertMatch({ok, _, _}, emqx_dsch:ensure_db_schema(DB, #{backend => test})),
    ?assertMatch(ok, emqx_dsch:open_db(DB, #{})),
    %% DB gvars:
    ?assertEqual(undefined, emqx_dsch:gvar_get(DB, Scope, foo)),
    ?assertEqual(ok, emqx_dsch:gvar_set(DB, Scope, foo, bar)),
    ?assertEqual({ok, bar}, emqx_dsch:gvar_get(DB, Scope, foo)),
    ?assertEqual(ok, emqx_dsch:gvar_unset(DB, Scope, foo)),
    ?assertEqual(undefined, emqx_dsch:gvar_get(DB, Scope, foo)),
    %% Shard gvars:
    %%   Test simple set and unset:
    ?assertEqual(undefined, emqx_dsch:gvar_get(DB, Shard, Scope, foo)),
    ?assertEqual(ok, emqx_dsch:gvar_set(DB, Shard, Scope, foo, foo)),
    ?assertEqual({ok, foo}, emqx_dsch:gvar_get(DB, Shard, Scope, foo)),
    ?assertEqual(ok, emqx_dsch:gvar_unset(DB, Shard, Scope, foo)),
    ?assertEqual(undefined, emqx_dsch:gvar_get(DB, Shard, Scope, foo)),
    %%   Now verify batch deletion of gvars of the shard.
    %%   This data should be deleted:
    ?assertEqual(ok, emqx_dsch:gvar_set(DB, Shard, Scope, bar, bar)),
    ?assertEqual(ok, emqx_dsch:gvar_set(DB, Shard, Scope, baz, baz)),
    %%   This data should be preserved:
    ?assertEqual(ok, emqx_dsch:gvar_set(DB, Shard, other_scope, foo, foo)),
    ?assertEqual(ok, emqx_dsch:gvar_set(DB, OtherShard, Scope, bar, bar)),
    ?assertEqual(ok, emqx_dsch:gvar_set(DB, OtherShard, Scope, baz, baz)),
    ?assertEqual(ok, emqx_dsch:gvar_set(DB, Scope, foo, foo)),
    %%   Delete all gvars that belong to `Shard':
    ?assertEqual(ok, emqx_dsch:gvar_unset_all(DB, Shard, Scope)),
    %%   Verify what's left:
    L = lists:sort(ets:tab2list(emqx_dsch:db_gvars(DB))),
    ?assertEqual(
        [
            {{db, Scope, foo}, foo},
            {{shard, Shard, other_scope, foo}, foo},
            {{shard, OtherShard, Scope, bar}, bar},
            {{shard, OtherShard, Scope, baz}, baz}
        ],
        L
    ).

handle_schema_event(DB, ChangeId, Task) ->
    ?tp(info, test_schema_change, #{db => DB, id => ChangeId, task => Task}),
    emqx_dsch:del_pending(ChangeId).

handle_db_config_change(DB, NewConf) ->
    ?tp(info, test_config_change, #{db => DB, conf => NewConf}).

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
