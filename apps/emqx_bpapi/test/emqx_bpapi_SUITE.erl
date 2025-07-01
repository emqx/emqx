%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bpapi_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("../include/emqx_bpapi.hrl").

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx], #{work_dir => emqx_cth_suite:work_dir(Config)}),
    [mnesia:dirty_write(Rec) || Rec <- fake_records()],
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    meck:unload(),
    emqx_cth_suite:stop(?config(apps, Config)).

t_max_supported_version(_Config) ->
    ?assertMatch(3, emqx_bpapi:supported_version('fake-node2@localhost', api2)),
    ?assertMatch(2, emqx_bpapi:supported_version(api2)),
    ?assertMatch(undefined, emqx_bpapi:supported_version('fake-node2@localhost', nonexistent_api)),
    ?assertError(_, emqx_bpapi:supported_version(nonexistent_api)).

t_announce(Config) ->
    meck:new(emqx_bpapi, [passthrough, no_history]),
    Filename = filename:join(?config(data_dir, Config), "test.versions"),
    meck:expect(emqx_bpapi, versions_file, fun(_) -> Filename end),
    FakeNode = 'fake-node@127.0.0.1',
    ?assertMatch(ok, emqx_bpapi:announce(FakeNode, emqx)),
    timer:sleep(100),
    ?assertMatch(4, emqx_bpapi:supported_version(FakeNode, api2)),
    ?assertMatch(2, emqx_bpapi:supported_version(FakeNode, api1)),
    ?assertMatch(2, emqx_bpapi:supported_version(api2)),
    ?assertMatch(2, emqx_bpapi:supported_version(api1)).

%% Assert that, if a replicant is initially part of a cluster, but then re-connects to a
%% fresh core node with no memory of the replicant, it correctly re-announces its BPAPIs.
t_replicant_joins_fresh_core(Config) ->
    BPAPI = emqx_broker,
    AppSpecs = [emqx],
    ClusterSpec = [
        {bpapi_core, #{apps => AppSpecs, role => core}},
        {bpapi_replicant, #{apps => AppSpecs, role => replicant}}
    ],
    [CoreSpec, ReplicantSpec] =
        emqx_cth_cluster:mk_nodespecs(
            ClusterSpec,
            #{work_dir => emqx_cth_suite:work_dir(?FUNCTION_NAME, Config)}
        ),
    [Core, Replicant] = Nodes = emqx_cth_cluster:start([CoreSpec, ReplicantSpec]),
    try
        snabbkaffe:start_trace(),
        %% 1) Replicant has its BPAPIs correctly announced.
        ?assertNotEqual(undefined, ?ON(Replicant, emqx_bpapi:supported_version(Replicant, BPAPI))),
        ?assertNotEqual([], ?ON(Replicant, emqx_bpapi:supported_apis(Replicant))),
        %% 2) Core node is stopped.  This will make the replicant disconnected from any core.
        ct:pal("stopping core"),
        {ok, {ok, _}} =
            ?wait_async_action(
                emqx_cth_cluster:stop([Core]),
                #{
                    ?snk_kind := cm_registry_node_down,
                    ?snk_meta := #{node := Replicant}
                }
            ),
        %% 3) A _fresh_ core node is (re)started and replicant connects to it, taking all
        %% table contents from this fresh core node.  This core node does not know about
        %% the replicant's BPAPI in its newly created BPAPI table.  The replicant should
        %% re-announce its BPAPIs.
        ct:pal("cleaning core"),
        #{work_dir := CoreWorkDir} = CoreSpec,
        ok = file:del_dir_r(CoreWorkDir),
        ct:pal("restarting core"),
        [Core] = emqx_cth_cluster:restart([CoreSpec]),
        ct:pal("waiting to stabilize"),
        ?block_until(#{?snk_kind := "bpapi_replicant_checker_reannounced"}, 5_000),
        %% Retry is needed because, even after the transaction, ETS needs a moment to make
        %% the APIs available.
        ?retry(500, 10, ?assertNotEqual([], ?ON(Replicant, emqx_bpapi:supported_apis(Replicant)))),
        ?assertNotEqual(undefined, ?ON(Replicant, emqx_bpapi:supported_version(Replicant, BPAPI))),
        ok
    after
        emqx_cth_cluster:stop(Nodes),
        snabbkaffe:stop()
    end.

fake_records() ->
    [
        #?TAB{key = {'fake-node@localhost', api1}, version = 2},
        #?TAB{key = {'fake-node2@localhost', api1}, version = 2},
        #?TAB{key = {?multicall, api1}, version = 2},

        #?TAB{key = {'fake-node@localhost', api2}, version = 2},
        #?TAB{key = {'fake-node2@localhost', api2}, version = 3},
        #?TAB{key = {?multicall, api2}, version = 2}
    ].
