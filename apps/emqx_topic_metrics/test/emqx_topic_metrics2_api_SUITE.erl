%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Exercises the v2 topic-metrics REST surface over real HTTP via the
%% dashboard listener — middleware (auth, body parsing, swagger
%% validation, RBAC) is in the loop, so handler / Req contract
%% changes can't slip past us.

-module(emqx_topic_metrics2_api_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_config.hrl").
-include("../include/emqx_topic_metrics.hrl").

-define(NS_ACME, <<"acme">>).
-define(NS_BRAVO, <<"bravo">>).
-define(NS_ACME_ADMIN, <<"acme_admin">>).
-define(NS_BRAVO_ADMIN, <<"bravo_admin">>).
-define(NS_ADMIN_PASS, <<"public123!">>).
-define(RESET_OP_ID, <<"/mqtt/topic_metrics2/:name/reset">>).

suite() -> [{timetrap, {seconds, 60}}].

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            {emqx_conf, #{config => #{log => #{audit => #{enable => true, level => info}}}}},
            emqx_audit,
            emqx_topic_metrics,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    %% Create one namespaced superuser per tenant we'll exercise; the
    %% dashboard then translates their token into Req `auth_meta' with
    %% the right `namespace' atom.
    {ok, _} = emqx_dashboard_admin:add_user(
        ?NS_ACME_ADMIN,
        ?NS_ADMIN_PASS,
        <<"ns:", ?NS_ACME/binary, "::administrator">>,
        <<"">>
    ),
    {ok, _} = emqx_dashboard_admin:add_user(
        ?NS_BRAVO_ADMIN,
        ?NS_ADMIN_PASS,
        <<"ns:", ?NS_BRAVO/binary, "::administrator">>,
        <<"">>
    ),
    {ok, #{token := AcmeToken}} =
        emqx_dashboard_admin:sign_token(?NS_ACME_ADMIN, ?NS_ADMIN_PASS),
    {ok, #{token := BravoToken}} =
        emqx_dashboard_admin:sign_token(?NS_BRAVO_ADMIN, ?NS_ADMIN_PASS),
    [
        {apps, Apps},
        {acme_token, AcmeToken},
        {bravo_token, BravoToken}
        | Config
    ].

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

t_crud(Config) ->
    %% empty list
    ?assertEqual({200, []}, list(Config, ?global_ns)),

    %% create
    {Status, Body} = create(Config, ?global_ns, <<"alpha">>, <<"alpha/#">>),
    ?assertEqual(201, Status),
    ?assertMatch(
        #{
            <<"name">> := <<"alpha">>,
            <<"topic_filter">> := <<"alpha/#">>,
            <<"namespace">> := null,
            <<"metrics">> := #{<<"messages.in.count">> := 0}
        },
        Body
    ),

    %% read one
    {200, One} = get_one(Config, ?global_ns, <<"alpha">>),
    ?assertMatch(#{<<"name">> := <<"alpha">>, <<"namespace">> := null}, One),

    %% list one
    {200, Many} = list(Config, ?global_ns),
    ?assertMatch([_], Many),

    %% delete one
    {204, _} = delete_one(Config, ?global_ns, <<"alpha">>),
    ?assertMatch({404, _}, get_one(Config, ?global_ns, <<"alpha">>)).

t_create_duplicate(Config) ->
    {201, _} = create(Config, ?global_ns, <<"a">>, <<"a/#">>),
    {409, #{<<"code">> := <<"ALREADY_EXISTS">>}} =
        create(Config, ?global_ns, <<"a">>, <<"a/#">>).

t_create_bad_name(Config) ->
    {400, #{<<"code">> := <<"BAD_NAME">>}} =
        create(Config, ?global_ns, <<"bad name!">>, <<"a/#">>),
    {400, _} = create(Config, ?global_ns, <<>>, <<"a/#">>).

t_create_bad_topic_filter(Config) ->
    {400, #{<<"code">> := <<"BAD_TOPIC_FILTER">>}} =
        create(Config, ?global_ns, <<"a">>, <<"a/#/x">>),
    {400, _} = create(Config, ?global_ns, <<"a">>, <<>>).

t_wildcard_filter_ok(Config) ->
    {201, _} = create(Config, ?global_ns, <<"wild">>, <<"sensor/+/temp">>).

-doc "Ordinary and wildcard topic filters are accepted (no regression).".
t_ordinary_filters_ok(Config) ->
    {201, _} = create(Config, ?global_ns, <<"exact">>, <<"sensor/1">>),
    {201, _} = create(Config, ?global_ns, <<"hash">>, <<"sensor/#">>),
    {201, _} = create(Config, ?global_ns, <<"plus">>, <<"sensor/+/temp">>).

-doc """
Creating a collection with a shared-subscription topic filter
($share/, $queue/) is rejected with 400 BAD_TOPIC_FILTER.
""".
t_create_shared_filter_rejected(Config) ->
    ?assertMatch(
        {400, #{<<"code">> := <<"BAD_TOPIC_FILTER">>}},
        create(Config, ?global_ns, <<"s1">>, <<"$share/group/sensor/1">>)
    ),
    ?assertMatch(
        {400, #{<<"code">> := <<"BAD_TOPIC_FILTER">>}},
        create(Config, ?global_ns, <<"s2">>, <<"$queue/sensor/1">>)
    ),
    %% Rejected shared filters must not leave a collection behind.
    ?assertEqual({200, []}, list(Config, ?global_ns)).

-doc """
The facade register/3 path also rejects shared-subscription topic
filters (config/import/rehydrate share this validator).
""".
t_register_shared_filter_rejected(_Config) ->
    ?assertMatch(
        {error, #{cause := shared_topic_filter_not_allowed}},
        emqx_topic_metrics2:register(<<"s">>, <<"$share/g/sensor/1">>, ?global_ns)
    ),
    ?assertMatch(
        {error, #{cause := shared_topic_filter_not_allowed}},
        emqx_topic_metrics2:register(<<"q">>, <<"$queue/sensor/1">>, ?global_ns)
    ).

t_delete_all(Config) ->
    [
        {201, _} =
            create(
                Config,
                ?global_ns,
                integer_to_binary(I),
                <<"t/", (integer_to_binary(I))/binary>>
            )
     || I <- lists:seq(1, 3)
    ],
    {200, L1} = list(Config, ?global_ns),
    ?assertEqual(3, length(L1)),
    {204, _} = delete_all(Config, ?global_ns),
    {200, []} = list(Config, ?global_ns).

t_reset_counter(Config) ->
    {201, _} = create(Config, ?global_ns, <<"r">>, <<"r/#">>),
    {ok, #{counter_ref := CRef}} =
        emqx_topic_metrics_registry:lookup({?global_ns, <<"r">>}),
    counters:add(CRef, 1, 5),
    {200, Before} = get_one(Config, ?global_ns, <<"r">>),
    ?assertMatch(#{<<"metrics">> := #{<<"messages.in.count">> := 5}}, Before),
    {204, _} = reset_one(Config, ?global_ns, <<"r">>),
    {200, After} = get_one(Config, ?global_ns, <<"r">>),
    ?assertMatch(#{<<"metrics">> := #{<<"messages.in.count">> := 0}}, After).

-doc """
Regression test for #18534: with audit logging enabled (as it is for
this whole suite), resetting over REST must commit the cluster_rpc
transaction.
""".
t_reset_with_audit_enabled(Config) ->
    {201, _} = create(Config, ?global_ns, <<"au">>, <<"au/#">>),
    TnxId0 = emqx_cluster_rpc:latest_tnx_id(),
    {204, _} = reset_one(Config, ?global_ns, <<"au">>),
    %% The reset transaction committed.
    ?assert(emqx_cluster_rpc:latest_tnx_id() > TnxId0).

t_cap(Config) ->
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
    {409, #{<<"code">> := <<"EXCEED_LIMIT">>}} =
        create(Config, ?global_ns, <<"overflow">>, <<"t/x/#">>).

t_namespace_isolation(Config) ->
    %% A global admin creates two global-namespace collections.
    {201, _} = create(Config, ?global_ns, <<"g">>, <<"g/#">>),
    {201, _} = create(Config, ?global_ns, <<"a">>, <<"a/#">>),
    %% Now act as a namespaced admin "acme".
    %% Acme can create their own collections; their name space is
    %% isolated from global by the ns-keyed mnesia table.
    {201, _} = create(Config, ?NS_ACME, <<"acme_a">>, <<"acme/#">>),
    {200, AllForAcme} = list(Config, ?NS_ACME),
    ?assertEqual([<<"acme_a">>], [maps:get(<<"name">>, R) || R <- AllForAcme]),
    %% Acme cannot see global-owned collections by name — the lookup
    %% builds key {<<"acme">>, <<"g">>}, which doesn't exist, so the
    %% response is 404 (no information about the global row's existence).
    ?assertMatch({404, #{<<"code">> := <<"NAME_NOT_FOUND">>}}, get_one(Config, ?NS_ACME, <<"g">>)),
    ?assertMatch(
        {404, #{<<"code">> := <<"NAME_NOT_FOUND">>}},
        delete_one(Config, ?NS_ACME, <<"g">>)
    ),
    %% Same bin-name reused across namespaces does not clash; the
    %% {namespace, name} pair is the unique row identity.
    {201, _} = create(Config, ?NS_ACME, <<"a">>, <<"acme/a/#">>),
    {200, AcmeA} = get_one(Config, ?NS_ACME, <<"a">>),
    ?assertMatch(
        #{
            <<"name">> := <<"a">>,
            <<"namespace">> := ?NS_ACME,
            <<"topic_filter">> := <<"acme/a/#">>
        },
        AcmeA
    ),
    %% The global "a" is untouched.
    {200, GlobalA} = get_one(Config, ?global_ns, <<"a">>),
    ?assertMatch(
        #{
            <<"name">> := <<"a">>,
            <<"namespace">> := null,
            <<"topic_filter">> := <<"a/#">>
        },
        GlobalA
    ),
    %% Global can list everything across namespaces; (namespace, name)
    %% pairs must be unique even when `name' alone is not.
    {200, AllForGlobal} = list(Config, ?global_ns),
    Keys = [{maps:get(<<"namespace">>, R), maps:get(<<"name">>, R)} || R <- AllForGlobal],
    ?assertEqual(lists:sort(Keys), lists:usort(Keys)),
    ?assertEqual(4, length(AllForGlobal)).

-doc "Global admin GETs/resets/deletes a namespaced collection via `?ns='.".
t_global_targets_namespace_via_ns_param(Config) ->
    %% acme owns a collection.
    {201, _} = create(Config, ?NS_ACME, <<"n">>, <<"acme/n/#">>),
    %% Global admin, no ns -> its own (global) namespace, which has no `n'.
    ?assertMatch({404, _}, get_one(Config, ?global_ns, <<"n">>)),
    %% Global admin with ?ns=acme -> reaches acme's collection.
    {200, One} = get_one_ns(Config, ?global_ns, <<"n">>, ?NS_ACME),
    ?assertMatch(#{<<"name">> := <<"n">>, <<"namespace">> := ?NS_ACME}, One),
    %% Reset it (204), then delete it (204), then it's gone.
    {204, _} = reset_one_ns(Config, ?global_ns, <<"n">>, ?NS_ACME),
    {204, _} = delete_one_ns(Config, ?global_ns, <<"n">>, ?NS_ACME),
    ?assertMatch({404, _}, get_one_ns(Config, ?global_ns, <<"n">>, ?NS_ACME)).

-doc "Namespaced admin naming a FOREIGN ns is rejected with 403 and mutates nothing.".
t_namespaced_admin_foreign_ns_forbidden(Config) ->
    %% acme owns a collection.
    {201, _} = create(Config, ?NS_ACME, <<"n">>, <<"acme/n/#">>),
    %% bravo tries to reach acme's collection by passing ?ns=acme.
    ?assertMatch(
        {403, #{<<"code">> := <<"FORBIDDEN">>}},
        get_one_ns(Config, ?NS_BRAVO, <<"n">>, ?NS_ACME)
    ),
    ?assertMatch(
        {403, #{<<"code">> := <<"FORBIDDEN">>}},
        reset_one_ns(Config, ?NS_BRAVO, <<"n">>, ?NS_ACME)
    ),
    ?assertMatch(
        {403, #{<<"code">> := <<"FORBIDDEN">>}},
        delete_one_ns(Config, ?NS_BRAVO, <<"n">>, ?NS_ACME)
    ),
    %% acme's collection is untouched.
    {200, Still} = get_one(Config, ?NS_ACME, <<"n">>),
    ?assertMatch(#{<<"name">> := <<"n">>, <<"namespace">> := ?NS_ACME}, Still).

-doc "Namespaced admin naming their OWN ns behaves exactly like no param.".
t_namespaced_admin_own_ns_ok(Config) ->
    {201, _} = create(Config, ?NS_ACME, <<"n">>, <<"acme/n/#">>),
    {200, ViaParam} = get_one_ns(Config, ?NS_ACME, <<"n">>, ?NS_ACME),
    {200, ViaNoParam} = get_one(Config, ?NS_ACME, <<"n">>),
    ?assertEqual(ViaNoParam, ViaParam),
    {204, _} = reset_one_ns(Config, ?NS_ACME, <<"n">>, ?NS_ACME),
    {204, _} = delete_one_ns(Config, ?NS_ACME, <<"n">>, ?NS_ACME),
    ?assertMatch({404, _}, get_one(Config, ?NS_ACME, <<"n">>)).

-doc """
Regression test for #18653, using the issue's exact scenario:
resetting `global/test' and `acme/test' must produce two audit
records that can be told apart. Before the fix, both records only
carried the collection name `test' — the `ns' query parameter used
to reach `acme/test' was dropped before it reached the audit log.
""".
t_reset_audit_records_namespace(Config) ->
    StartAt = erlang:system_time(microsecond),
    {201, _} = create(Config, ?global_ns, <<"test">>, <<"test/#">>),
    {201, _} = create(Config, ?NS_ACME, <<"test">>, <<"test/#">>),
    {204, _} = reset_one(Config, ?global_ns, <<"test">>),
    {204, _} = reset_one_ns(Config, ?global_ns, <<"test">>, ?NS_ACME),
    Entries = wait_for_audit_entries(?RESET_OP_ID, StartAt, 2, 2000),
    Namespaces = lists:sort([namespace_of(E) || E <- Entries]),
    ?assertEqual(lists:sort([<<"global">>, ?NS_ACME]), Namespaces).

-doc """
A namespaced admin resetting their own collection with no `ns' query
parameter still gets their namespace recorded on the audit entry —
not `global', and not absent. This is the case a fix that only
records the raw `ns' query parameter would miss, since a namespaced
admin's target namespace comes from their dashboard token, not from
the request.
""".
t_reset_audit_no_ns_records_actor_namespace(Config) ->
    StartAt = erlang:system_time(microsecond),
    {201, _} = create(Config, ?NS_ACME, <<"n2">>, <<"n2/#">>),
    {204, _} = reset_one(Config, ?NS_ACME, <<"n2">>),
    [Entry] = wait_for_audit_entries(?RESET_OP_ID, StartAt, 1, 2000),
    ?assertEqual(?NS_ACME, namespace_of(Entry)).

%%------------------------------------------------------------------------------
%% Helpers — real HTTP via dashboard listener
%%------------------------------------------------------------------------------

list(Config, Ns) ->
    request(Config, Ns, get, ["mqtt", "topic_metrics2"], []).

create(Config, Ns, Name, Topic) ->
    Body = #{<<"name">> => Name, <<"topic_filter">> => Topic},
    request(Config, Ns, post, ["mqtt", "topic_metrics2"], Body).

delete_all(Config, Ns) ->
    request(Config, Ns, delete, ["mqtt", "topic_metrics2"], []).

get_one(Config, Ns, Name) ->
    request(Config, Ns, get, ["mqtt", "topic_metrics2", b2l(Name)], []).

delete_one(Config, Ns, Name) ->
    request(Config, Ns, delete, ["mqtt", "topic_metrics2", b2l(Name)], []).

reset_one(Config, Ns, Name) ->
    %% PUT with no body — `[]' tells the helper to take the body-less branch.
    request(Config, Ns, put, ["mqtt", "topic_metrics2", b2l(Name), "reset"], []).

%% Same as get_one/delete_one/reset_one, but keep the actor's token
%% (`Ns') while addressing `TargetNs' via the `?ns=' query parameter.
get_one_ns(Config, Ns, Name, TargetNs) ->
    request_ns(Config, Ns, get, ["mqtt", "topic_metrics2", b2l(Name)], TargetNs, []).

delete_one_ns(Config, Ns, Name, TargetNs) ->
    request_ns(Config, Ns, delete, ["mqtt", "topic_metrics2", b2l(Name)], TargetNs, []).

reset_one_ns(Config, Ns, Name, TargetNs) ->
    request_ns(Config, Ns, put, ["mqtt", "topic_metrics2", b2l(Name), "reset"], TargetNs, []).

%% Real HTTP request through the dashboard listener. Returns
%% `{StatusCode, DecodedBody}'.
request(Config, Ns, Method, PathParts, Body) ->
    emqx_mgmt_api_test_util:simple_request(#{
        method => Method,
        url => emqx_mgmt_api_test_util:api_path(PathParts),
        body => Body,
        auth_header => auth_header(Config, Ns)
    }).

%% Like request/5 but adds an `ns=<TargetNs>' query parameter, keeping
%% the actor's own token from `Ns'.
request_ns(Config, Ns, Method, PathParts, TargetNs, Body) ->
    emqx_mgmt_api_test_util:simple_request(#{
        method => Method,
        url => emqx_mgmt_api_test_util:api_path(PathParts),
        query_params => #{<<"ns">> => TargetNs},
        body => Body,
        auth_header => auth_header(Config, Ns)
    }).

auth_header(_Config, ?global_ns) ->
    emqx_mgmt_api_test_util:auth_header_();
auth_header(Config, ?NS_ACME) ->
    bearer(?config(acme_token, Config));
auth_header(Config, ?NS_BRAVO) ->
    bearer(?config(bravo_token, Config)).

bearer(Token) ->
    {"Authorization", "Bearer " ++ binary_to_list(Token)}.

b2l(B) when is_binary(B) -> binary_to_list(B);
b2l(L) when is_list(L) -> L.

%%------------------------------------------------------------------------------
%% Audit helpers
%%------------------------------------------------------------------------------

%% The audit write happens after the HTTP response is already sent
%% (see minirest_handler:init/2), so poll for a bit rather than
%% assume the record is there the instant the request returns.
wait_for_audit_entries(_OperationId, _StartAt, _ExpectedCount, RemainMs) when RemainMs =< 0 ->
    ct:fail(audit_entries_not_found_in_time);
wait_for_audit_entries(OperationId, StartAt, ExpectedCount, RemainMs) ->
    Entries = audit_entries_since(OperationId, StartAt),
    case length(Entries) >= ExpectedCount of
        true ->
            Entries;
        false ->
            SleepMs = 100,
            ct:sleep(SleepMs),
            wait_for_audit_entries(OperationId, StartAt, ExpectedCount, RemainMs - SleepMs)
    end.

audit_entries_since(OperationId, StartAt) ->
    AuditPath = emqx_mgmt_api_test_util:api_path(["audit"]),
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    Query = lists:flatten(
        io_lib:format("operation_id=~ts&gte_created_at=~B&limit=100", [OperationId, StartAt])
    ),
    {ok, Res} = emqx_mgmt_api_test_util:request_api(get, AuditPath, Query, AuthHeader),
    #{<<"data">> := Data} = emqx_utils_json:decode(Res),
    Data.

namespace_of(#{<<"http_request">> := #{<<"namespace">> := Ns}}) -> Ns.
