%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authn_mnesia_bpapi_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx_config.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("emqx_auth_mnesia_internal.hrl").

-define(AUTHN_ID, <<"mechanism:backend">>).
-define(USER_GROUP, 'global:mqtt').
-define(USER_ID, <<"legacy">>).
-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    Apps = [
        emqx,
        {emqx_conf, emqx_authn_test_lib:emqx_appspec()},
        emqx_auth,
        emqx_auth_mnesia
    ],
    NodeSpecs = emqx_cth_cluster:mk_nodespecs(
        [
            {authn_bpapi_old, #{apps => Apps}},
            {authn_bpapi_new, #{apps => Apps}}
        ],
        #{
            work_dir => emqx_cth_suite:work_dir(TestCase, Config),
            shutdown => 15_000
        }
    ),
    Nodes = emqx_cth_cluster:start(NodeSpecs),
    [{nodes, Nodes} | Config].

end_per_testcase(_TestCase, Config) ->
    ok = emqx_cth_cluster:stop(?config(nodes, Config)).

t_global_storage_during_rolling_upgrade(Config) ->
    [OldNode, NewNode] = Nodes = ?config(nodes, Config),
    ok = announce_authn_version(OldNode, 2),
    wait_authn_version(Nodes, 2),

    %% Create authenticator
    State = ?ON(NewNode, begin
        {ok, S} = emqx_authn_mnesia:create(?AUTHN_ID, authn_config()),
        S
    end),

    %% Assert old records are used on user create/update
    ?assertMatch(
        {ok, _},
        ?ON(
            NewNode,
            emqx_authn_mnesia:add_user(
                #{user_id => ?USER_ID, password => <<"old">>}, State
            )
        )
    ),
    assert_legacy_record(Nodes),
    ?assertMatch(
        {ok, _},
        ?ON(
            NewNode,
            emqx_authn_mnesia:update_user(
                ?global_ns,
                ?USER_ID,
                #{password => <<"new">>, is_superuser => true},
                State
            )
        )
    ),
    assert_legacy_record(Nodes),
    ?assertMatch(
        {ok, #{is_superuser := true}},
        ?ON(
            OldNode,
            emqx_authn_mnesia:authenticate(
                #{username => ?USER_ID, password => <<"new">>}, State
            )
        )
    ),

    %% Update announced versions
    ok = announce_authn_version(OldNode, 3),
    wait_authn_version(Nodes, 3),

    %% Update user
    ?assertMatch(
        {ok, _},
        ?ON(
            NewNode,
            emqx_authn_mnesia:update_user(
                ?global_ns, ?USER_ID, #{is_superuser => false}, State
            )
        )
    ),

    %% Verify that now the user moved to the namespaced table
    assert_namespaced_record(Nodes).

announce_authn_version(Node, Version) ->
    APIs0 = ?ON(Node, emqx_bpapi:supported_apis(Node)),
    APIs = [{emqx_authn, Version} | lists:keydelete(emqx_authn, 1, APIs0)],
    {atomic, ok} = ?ON(
        Node,
        mria:transaction(emqx_common_shard, fun emqx_bpapi:announce_fun/2, [Node, APIs])
    ),
    ok.

wait_authn_version(Nodes, Version) ->
    lists:foreach(
        fun(Node) ->
            ?retry(
                100,
                20,
                ?assertEqual(Version, ?ON(Node, emqx_bpapi:supported_version(emqx_authn)))
            )
        end,
        Nodes
    ).

assert_legacy_record(Nodes) ->
    lists:foreach(
        fun(Node) ->
            ?assertMatch(
                [_],
                ?ON(Node, ets:lookup(emqx_authn_mnesia, {?USER_GROUP, ?USER_ID}))
            ),
            ?assertEqual(
                [],
                ?ON(
                    Node,
                    ets:lookup(
                        emqx_authn_mnesia_ns,
                        ?AUTHN_NS_KEY(?global_ns, ?USER_GROUP, ?USER_ID)
                    )
                )
            )
        end,
        Nodes
    ).

assert_namespaced_record(Nodes) ->
    lists:foreach(
        fun(Node) ->
            ?assertEqual(
                [], ?ON(Node, ets:lookup(emqx_authn_mnesia, {?USER_GROUP, ?USER_ID}))
            ),
            ?assertMatch(
                [_],
                ?ON(
                    Node,
                    ets:lookup(
                        emqx_authn_mnesia_ns,
                        ?AUTHN_NS_KEY(?global_ns, ?USER_GROUP, ?USER_ID)
                    )
                )
            )
        end,
        Nodes
    ).

authn_config() ->
    #{
        user_id_type => username,
        password_hash_algorithm => #{name => sha256, salt_position => suffix},
        user_group => ?USER_GROUP,
        autogenerate_password => false
    }.
