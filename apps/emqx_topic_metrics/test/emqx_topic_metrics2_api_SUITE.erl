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

suite() -> [{timetrap, {seconds, 60}}].

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
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
            <<"id">> := <<"$global:alpha">>,
            <<"name">> := <<"alpha">>,
            <<"topic_filter">> := <<"alpha/#">>,
            <<"namespace">> := null,
            <<"metrics">> := #{<<"messages.in.count">> := 0}
        },
        Body
    ),

    %% read one
    {200, One} = get_one(Config, ?global_ns, <<"alpha">>),
    ?assertMatch(#{<<"name">> := <<"alpha">>, <<"id">> := <<"$global:alpha">>}, One),

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
    %% Same bin-name reused across namespaces does not clash. The
    %% composite id is what differentiates the two rows in any
    %% client-side keying.
    {201, _} = create(Config, ?NS_ACME, <<"a">>, <<"acme/a/#">>),
    {200, AcmeA} = get_one(Config, ?NS_ACME, <<"a">>),
    ?assertMatch(
        #{
            <<"id">> := <<"acme:a">>,
            <<"namespace">> := ?NS_ACME,
            <<"topic_filter">> := <<"acme/a/#">>
        },
        AcmeA
    ),
    %% The global "a" is untouched.
    {200, GlobalA} = get_one(Config, ?global_ns, <<"a">>),
    ?assertMatch(
        #{
            <<"id">> := <<"$global:a">>,
            <<"namespace">> := null,
            <<"topic_filter">> := <<"a/#">>
        },
        GlobalA
    ),
    %% Global can list everything across namespaces; (namespace, name)
    %% must be unique even when `name' alone is not.
    {200, AllForGlobal} = list(Config, ?global_ns),
    Ids = [maps:get(<<"id">>, R) || R <- AllForGlobal],
    ?assertEqual(lists:sort(Ids), lists:usort(Ids)),
    ?assertEqual(4, length(AllForGlobal)).

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

%% Real HTTP request through the dashboard listener. Returns
%% `{StatusCode, DecodedBody}'.
request(Config, Ns, Method, PathParts, Body) ->
    Path = emqx_mgmt_api_test_util:api_path(PathParts),
    Headers = auth_header(Config, Ns),
    case
        emqx_mgmt_api_test_util:request_api(
            Method, Path, _Qs = "", Headers, Body, #{return_all => true}
        )
    of
        {ok, {{_HttpVer, Status, _Reason}, _RespHdrs, RespBody}} ->
            {Status, decode(RespBody)};
        {error, {{_HttpVer, Status, _Reason}, _RespHdrs, RespBody}} ->
            {Status, decode(RespBody)}
    end.

auth_header(_Config, ?global_ns) ->
    emqx_mgmt_api_test_util:auth_header_();
auth_header(Config, ?NS_ACME) ->
    bearer(?config(acme_token, Config));
auth_header(Config, ?NS_BRAVO) ->
    bearer(?config(bravo_token, Config)).

bearer(Token) ->
    {"Authorization", "Bearer " ++ binary_to_list(Token)}.

decode(<<>>) ->
    <<>>;
decode(Bin) ->
    try
        emqx_utils_json:decode(Bin)
    catch
        _:_ -> Bin
    end.

b2l(B) when is_binary(B) -> binary_to_list(B);
b2l(L) when is_list(L) -> L.
