%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_auth_mnesia_bookkeeper_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_config.hrl").

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-import(emqx_common_test_helpers, [on_exit/1]).

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all_with_matrix(?MODULE).

groups() ->
    emqx_common_test_helpers:groups_with_matrix(?MODULE).

init_per_suite(TCConfig) ->
    TCConfig.

end_per_suite(_TCConfig) ->
    ok.

init_per_testcase(_TestCase, TCConfig) ->
    TCConfig.

end_per_testcase(_TestCase, _TCConfig) ->
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

mk_cluster(TestCase, #{n := NumNodes} = _Opts, TCConfig) ->
    AppSpecs = [
        emqx,
        {emqx_conf, "authentication = [{mechanism = password_based, backend = built_in_database}]"},
        emqx_auth,
        emqx_auth_mnesia
    ],
    NodeSpecs0 = lists:map(
        fun(N) ->
            Name = mk_node_name(TestCase, N),
            {Name, #{apps => AppSpecs}}
        end,
        lists:seq(1, NumNodes)
    ),
    NodeSpecs = emqx_cth_cluster:mk_nodespecs(NodeSpecs0, #{
        work_dir => emqx_cth_suite:work_dir(TestCase, TCConfig),
        shutdown => 15_000
    }),
    Nodes = emqx_cth_cluster:start(NodeSpecs),
    on_exit(fun() -> ok = emqx_cth_cluster:stop(Nodes) end),
    {Nodes, NodeSpecs}.

mk_node_name(TestCase, N) ->
    Name0 = iolist_to_binary([atom_to_binary(TestCase), "_", integer_to_binary(N)]),
    binary_to_atom(Name0).

store_rules(Namespace, Who, Rules) ->
    emqx_authz_mnesia:store_rules(Namespace, Who, Rules).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

-doc """
Simple smoke test that verifies that bookkeeper tallies the namespaced authn and authz
record counts when it (re)starts.
""".
t_smoke_restart(TCConfig) ->
    {[N], [NSpec]} = mk_cluster(?FUNCTION_NAME, #{n => 1}, TCConfig),
    ct:timetrap({seconds, 30}),

    ct:pal("Setup some namespaced (and global) authn users and authz rules"),
    Ns1 = <<"ns1">>,
    Ns2 = <<"ns2">>,

    Users = [
        #{namespace => Ns, user_id => UId, password => <<"p">>}
     || Ns <- [?global_ns, Ns1, Ns2],
        UId <- [<<"u1">>, <<"u2">>]
    ],
    ?ON(
        N,
        lists:foreach(
            fun(U) ->
                {ok, _} = emqx_authn_chains:add_user(
                    'mqtt:global',
                    <<"password_based:built_in_database">>,
                    U
                )
            end,
            Users
        )
    ),

    ACLs = [
        {Ns, Who, [
            #{
                <<"permission">> => <<"allow">>,
                <<"action">> => <<"publish">>,
                <<"topic">> => <<"t">>,
                <<"listener_re">> => <<"^tcp:">>
            }
        ]}
     || Ns <- [?global_ns, Ns1, Ns2],
        Who <- [{username, <<"u1">>}, {clientid, <<"cid">>}, all]
    ],
    ?ON(
        N,
        lists:foreach(
            fun({Ns, Who, Rules}) ->
                ok = store_rules(Ns, Who, Rules)
            end,
            ACLs
        )
    ),

    ct:pal("Sanity checks"),
    ?assertEqual(2, ?ON(N, emqx_authn_mnesia:record_count(?global_ns))),
    ?assertEqual(2, ?ON(N, emqx_authn_mnesia:record_count(Ns1))),
    ?assertEqual(2, ?ON(N, emqx_authn_mnesia:record_count(Ns2))),

    ?assertEqual(3, ?ON(N, emqx_authz_mnesia:record_count(?global_ns))),
    ?assertEqual(3, ?ON(N, emqx_authz_mnesia:record_count(Ns1))),
    ?assertEqual(3, ?ON(N, emqx_authz_mnesia:record_count(Ns2))),

    ct:pal("Manual ops"),
    ?assertEqual(ok, ?ON(N, emqx_auth_mnesia_bookkeeper:tally_authn_now())),
    ?assertEqual(ok, ?ON(N, emqx_auth_mnesia_bookkeeper:tally_authz_now())),

    ?assertEqual(2, ?ON(N, emqx_authn_mnesia:record_count(?global_ns))),
    ?assertEqual(2, ?ON(N, emqx_authn_mnesia:record_count(Ns1))),
    ?assertEqual(2, ?ON(N, emqx_authn_mnesia:record_count(Ns2))),

    ?assertEqual(3, ?ON(N, emqx_authz_mnesia:record_count(?global_ns))),
    ?assertEqual(3, ?ON(N, emqx_authz_mnesia:record_count(Ns1))),
    ?assertEqual(3, ?ON(N, emqx_authz_mnesia:record_count(Ns2))),

    ct:pal("Restart node; should tally after startup"),
    ok = snabbkaffe:start_trace(),
    {[N], {ok, _}} =
        ?wait_async_action(
            emqx_cth_cluster:restart([NSpec]),
            #{?snk_kind := "auth_bookkeeper_finished_startup"}
        ),
    ok = snabbkaffe:stop(),

    ct:pal("Node restarted"),
    ?assertEqual(2, ?ON(N, emqx_authn_mnesia:record_count(?global_ns))),
    ?assertEqual(2, ?ON(N, emqx_authn_mnesia:record_count(Ns1))),
    ?assertEqual(2, ?ON(N, emqx_authn_mnesia:record_count(Ns2))),

    ?assertEqual(3, ?ON(N, emqx_authz_mnesia:record_count(?global_ns))),
    ?assertEqual(3, ?ON(N, emqx_authz_mnesia:record_count(Ns1))),
    ?assertEqual(3, ?ON(N, emqx_authz_mnesia:record_count(Ns2))),

    ok.
