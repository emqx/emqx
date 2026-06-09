%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Exercises the REST handler callbacks directly (no minirest /
%% dashboard listener), so this suite can run on hosts where the
%% dashboard port 18083 is already in use.  HTTP-wire integration
%% lives in CI's full-dashboard runs of the dashboard-helper-based
%% suites.

-module(emqx_topic_metrics2_api_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_config.hrl").
-include("../include/emqx_topic_metrics.hrl").

suite() -> [{timetrap, {seconds, 30}}].

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            {emqx, #{override_env => [{boot_modules, [broker]}]}},
            emqx_topic_metrics
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(_Case, Config) ->
    ok = emqx_topic_metrics2:deregister_all(),
    Config.

end_per_testcase(_Case, _Config) ->
    ok = emqx_topic_metrics2:deregister_all(),
    ok.

%%------------------------------------------------------------------------------
%% Cases
%%------------------------------------------------------------------------------

t_crud(_Config) ->
    %% empty list
    ?assertEqual({200, []}, list(?global_ns)),

    %% create
    {Status, Body} = create(?global_ns, <<"alpha">>, <<"alpha/#">>),
    ?assertEqual(201, Status),
    ?assertMatch(
        #{
            name := <<"alpha">>,
            topic_filter := <<"alpha/#">>,
            namespace := null,
            metrics := #{'messages.in.count' := 0}
        },
        Body
    ),

    %% read one
    {200, One} = get_one(?global_ns, <<"alpha">>),
    ?assertMatch(#{name := <<"alpha">>}, One),

    %% list one
    {200, Many} = list(?global_ns),
    ?assertMatch([_], Many),

    %% delete one
    {204} = delete_one(?global_ns, <<"alpha">>),
    ?assertMatch({404, _}, get_one(?global_ns, <<"alpha">>)).

t_create_duplicate(_Config) ->
    {201, _} = create(?global_ns, <<"a">>, <<"a/#">>),
    {409, #{code := 'ALREADY_EXISTS'}} = create(?global_ns, <<"a">>, <<"a/#">>).

t_create_bad_name(_Config) ->
    {400, #{code := 'BAD_NAME'}} = create(?global_ns, <<"bad name!">>, <<"a/#">>),
    {400, #{code := 'BAD_NAME'}} = create(?global_ns, <<>>, <<"a/#">>).

t_create_bad_topic_filter(_Config) ->
    {400, #{code := 'BAD_TOPIC_FILTER'}} = create(?global_ns, <<"a">>, <<"a/#/x">>),
    {400, #{code := 'BAD_TOPIC_FILTER'}} = create(?global_ns, <<"a">>, <<>>).

t_wildcard_filter_ok(_Config) ->
    {201, _} = create(?global_ns, <<"wild">>, <<"sensor/+/temp">>).

t_delete_all(_Config) ->
    [
        {201, _} = create(?global_ns, integer_to_binary(I), <<"t/", (integer_to_binary(I))/binary>>)
     || I <- lists:seq(1, 3)
    ],
    {200, L1} = list(?global_ns),
    ?assertEqual(3, length(L1)),
    {204} = delete_all(?global_ns),
    {200, []} = list(?global_ns).

t_reset_counter(_Config) ->
    {201, _} = create(?global_ns, <<"r">>, <<"r/#">>),
    {ok, #{counter_ref := CRef}} =
        emqx_topic_metrics_registry:lookup({?global_ns, <<"r">>}),
    counters:add(CRef, 1, 5),
    {200, Before} = get_one(?global_ns, <<"r">>),
    ?assertMatch(#{metrics := #{'messages.in.count' := 5}}, Before),
    {204} = reset_one(?global_ns, <<"r">>),
    {200, After} = get_one(?global_ns, <<"r">>),
    ?assertMatch(#{metrics := #{'messages.in.count' := 0}}, After).

t_cap(_Config) ->
    %% Drive the public facade so the cap is enforced against the
    %% same code path the API uses (mria-backed cluster table).
    [
        ok = emqx_topic_metrics2:register(
            integer_to_binary(I),
            <<"t/", (integer_to_binary(I))/binary, "/#">>,
            ?global_ns
        )
     || I <- lists:seq(1, ?MAX_COLLECTIONS)
    ],
    {409, #{code := 'EXCEED_LIMIT'}} = create(?global_ns, <<"overflow">>, <<"t/x/#">>).

t_namespace_isolation(_Config) ->
    %% A global admin creates two global-namespace collections.
    {201, _} = create(?global_ns, <<"g">>, <<"g/#">>),
    {201, _} = create(?global_ns, <<"a">>, <<"a/#">>),
    %% Now act as a namespaced admin "acme".
    %% Acme can create their own collections; their name space is
    %% isolated from global by the ns-keyed mnesia table.
    {201, _} = create(<<"acme">>, <<"acme_a">>, <<"acme/#">>),
    {200, AllForAcme} = list(<<"acme">>),
    ?assertEqual([<<"acme_a">>], [maps:get(name, R) || R <- AllForAcme]),
    %% Acme cannot see global-owned collections by name — the lookup
    %% builds key {<<"acme">>, <<"g">>}, which doesn't exist, so the
    %% response is 404 (no information about the global row's existence).
    ?assertMatch({404, #{code := 'NAME_NOT_FOUND'}}, get_one(<<"acme">>, <<"g">>)),
    ?assertMatch({404, #{code := 'NAME_NOT_FOUND'}}, delete_one(<<"acme">>, <<"g">>)),
    %% Same bin-name reused across namespaces does not clash.
    {201, _} = create(<<"acme">>, <<"a">>, <<"acme/a/#">>),
    {200, AcmeA} = get_one(<<"acme">>, <<"a">>),
    ?assertMatch(#{namespace := <<"acme">>, topic_filter := <<"acme/a/#">>}, AcmeA),
    %% The global "a" is untouched.
    {200, GlobalA} = get_one(?global_ns, <<"a">>),
    ?assertMatch(#{namespace := null, topic_filter := <<"a/#">>}, GlobalA),
    %% Global can list everything across namespaces.
    {200, AllForGlobal} = list(?global_ns),
    ?assertEqual(4, length(AllForGlobal)).

%%------------------------------------------------------------------------------
%% Helpers — drive emqx_topic_metrics2_api handler callbacks directly
%%------------------------------------------------------------------------------

list(OwnerNs) ->
    out(emqx_topic_metrics2_api:collections(get, req(OwnerNs))).

create(OwnerNs, Name, Topic) ->
    Body = #{<<"name">> => Name, <<"topic_filter">> => Topic},
    out(emqx_topic_metrics2_api:collections(post, (req(OwnerNs))#{body => Body})).

delete_all(OwnerNs) ->
    out(emqx_topic_metrics2_api:collections(delete, req(OwnerNs))).

get_one(OwnerNs, Name) ->
    out(emqx_topic_metrics2_api:collection(get, (req(OwnerNs))#{bindings => #{name => Name}})).

delete_one(OwnerNs, Name) ->
    out(emqx_topic_metrics2_api:collection(delete, (req(OwnerNs))#{bindings => #{name => Name}})).

reset_one(OwnerNs, Name) ->
    out(emqx_topic_metrics2_api:reset(put, (req(OwnerNs))#{bindings => #{name => Name}})).

%% Build a minimal Req that emqx_dashboard:get_namespace/1 understands.
%% `?global_ns' should produce a request with no `auth_meta'.
%% The auth_meta key `namespace' is the same atom used in
%% emqx_dashboard_rbac.hrl `-define(namespace, namespace).' — we
%% inline it here to avoid a dashboard-header dep from this test.
req(?global_ns) -> #{};
req(NS) when is_binary(NS) -> #{auth_meta => #{namespace => NS}}.

out({Status}) -> {Status};
out({Status, Body}) -> {Status, Body}.
