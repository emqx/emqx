%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_mgmt_api_alarms_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-define(ACT_ALARM, test_act_alarm).
-define(DE_ACT_ALARM, test_de_act_alarm).

-define(local, local).
-define(cluster, cluster).

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).
-define(ON_ALL(NODES, BODY), erpc:multicall(NODES, fun() -> BODY end)).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    [
        {group, ?cluster},
        {group, ?local}
    ].

groups() ->
    AllTCs0 = emqx_common_test_helpers:all_with_matrix(?MODULE),
    AllTCs = lists:filter(
        fun
            ({group, _}) -> false;
            (_) -> true
        end,
        AllTCs0
    ),
    CustomMatrix = emqx_common_test_helpers:groups_with_matrix(?MODULE),
    LocalTCs = merge_custom_groups(?local, AllTCs, CustomMatrix),
    ClusterTCs = merge_custom_groups(?cluster, cluster_testcases(), CustomMatrix),
    [
        {?cluster, ClusterTCs},
        {?local, LocalTCs}
    ].

merge_custom_groups(RootGroup, GroupTCs, CustomMatrix0) ->
    CustomMatrix =
        lists:flatmap(
            fun
                ({G, _, SubGroup}) when G == RootGroup ->
                    SubGroup;
                (_) ->
                    []
            end,
            CustomMatrix0
        ),
    CustomMatrix ++ GroupTCs.

cluster_testcases() ->
    Key = ?cluster,
    lists:filter(
        fun
            ({testcase, TestCase, _Opts}) ->
                emqx_common_test_helpers:get_tc_prop(?MODULE, TestCase, Key, false);
            (TestCase) ->
                emqx_common_test_helpers:get_tc_prop(?MODULE, TestCase, Key, false)
        end,
        emqx_common_test_helpers:all(?MODULE)
    ).

init_per_suite(TCConfig) ->
    TCConfig.

end_per_suite(_TCConfig) ->
    ok.

init_per_group(?cluster = Group, TCConfig) ->
    AppSpecs = [
        emqx,
        emqx_management
    ],
    Nodes = emqx_cth_cluster:start(
        [
            {mgmt_api_alarms1, #{
                role => core,
                apps => AppSpecs ++ [emqx_mgmt_api_test_util:emqx_dashboard()]
            }},
            {mgmt_api_alarms2, #{
                role => core,
                apps => AppSpecs
            }}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Group, TCConfig)}
    ),
    [{nodes, Nodes} | TCConfig];
init_per_group(?local, TCConfig) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(TCConfig)}
    ),
    [{apps, Apps} | TCConfig];
init_per_group(_Group, TCConfig) ->
    TCConfig.

end_per_group(?cluster, TCConfig) ->
    Nodes = get_config(nodes, TCConfig),
    ok = emqx_cth_cluster:stop(Nodes),
    ok;
end_per_group(?local, TCConfig) ->
    Apps = get_config(apps, TCConfig),
    emqx_cth_suite:stop(Apps),
    ok;
end_per_group(_Group, _TCConfig) ->
    ok.

init_per_testcase(_TestCase, TCConfig) ->
    setup_auth_header(TCConfig),
    TCConfig.

end_per_testcase(_TestCase, _TCConfig) ->
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

get_config(K, TCConfig) -> emqx_bridge_v2_testlib:get_value(K, TCConfig).
get_config(K, TCConfig, Default) -> proplists:get_value(K, TCConfig, Default).

setup_auth_header(TCConfig) ->
    case get_config(nodes, TCConfig, undefined) of
        [N1 | _] ->
            Fun = fun() -> ?ON(N1, emqx_mgmt_api_test_util:auth_header_()) end,
            emqx_bridge_v2_testlib:set_auth_header_getter(Fun);
        _ ->
            ok
    end.

get_auth_header() ->
    case emqx_bridge_v2_testlib:get_auth_header_getter() of
        Fun when is_function(Fun, 0) ->
            Fun();
        _ ->
            emqx_mgmt_api_test_util:auth_header_()
    end.

list_alarms(Opts) ->
    QueryParams = maps:get(query_params, Opts, #{}),
    emqx_mgmt_api_test_util:simple_request(#{
        method => get,
        url => emqx_mgmt_api_test_util:api_path(["alarms"]),
        auth_header => get_auth_header(),
        query_params => QueryParams
    }).

get_alarms(AssertCount, Activated) when is_atom(Activated) ->
    get_alarms(AssertCount, atom_to_list(Activated));
get_alarms(AssertCount, Activated) ->
    Path = emqx_mgmt_api_test_util:api_path(["alarms"]),
    Qs = "activated=" ++ Activated,
    Headers = get_auth_header(),
    {ok, Response} = emqx_mgmt_api_test_util:request_api(get, Path, Qs, Headers),
    Data = emqx_utils_json:decode(Response),
    Meta = maps:get(<<"meta">>, Data),
    Page = maps:get(<<"page">>, Meta),
    Limit = maps:get(<<"limit">>, Meta),
    Count = maps:get(<<"count">>, Meta),
    ?assertEqual(Page, 1),
    ?assertEqual(Limit, emqx_mgmt:default_row_limit()),
    ?assert(Count >= AssertCount).

%% Helper function to test successful force deactivate
assert_force_deactivate_success(AlarmName) ->
    Path = emqx_mgmt_api_test_util:api_path(["alarms", "force_deactivate"]),
    Body = #{<<"name">> => AlarmName},
    Headers = get_auth_header(),
    {ok, _} = emqx_mgmt_api_test_util:request_api(post, Path, "", Headers, Body).

%% Helper function to test failed force deactivate
assert_force_deactivate_fails(AlarmName, ExpectedCode, ExpectedMessage) ->
    Path = emqx_mgmt_api_test_util:api_path(["alarms", "force_deactivate"]),
    Body =
        case AlarmName of
            undefined -> #{};
            Name -> #{<<"name">> => Name}
        end,
    Headers = get_auth_header(),
    Response = emqx_mgmt_api_test_util:request_api(post, Path, "", Headers, Body, #{
        return_all => true
    }),
    case Response of
        {error, {{_, 400, _}, _, ErrorBody}} ->
            ErrorData = emqx_utils_json:decode(ErrorBody),
            ?assertEqual(ExpectedCode, maps:get(<<"code">>, ErrorData)),
            ?assertEqual(ExpectedMessage, maps:get(<<"message">>, ErrorData));
        Other ->
            ct:fail("Expected 400 error with code ~p, but got: ~p", [ExpectedCode, Other])
    end.

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_alarms_api(_) ->
    ok = emqx_alarm:activate(?ACT_ALARM),
    ok = emqx_alarm:activate(?DE_ACT_ALARM),
    ok = emqx_alarm:deactivate(?DE_ACT_ALARM),
    get_alarms(1, true),
    get_alarms(1, false).

t_delete_alarms_api(_) ->
    Path = emqx_mgmt_api_test_util:api_path(["alarms"]),
    {ok, _} = emqx_mgmt_api_test_util:request_api(delete, Path),
    get_alarms(1, true),
    get_alarms(0, false).

t_force_deactivate_alarm_api(_) ->
    AlarmName = <<"test_force_deactivate_alarm">>,

    %% Test force deactivate non-existent alarm - should fail
    assert_force_deactivate_fails(
        AlarmName, <<"NOT_FOUND">>, <<"Alarm not found or already deactivated">>
    ),

    %% Activate an alarm
    ok = emqx_alarm:activate(AlarmName, #{detail => <<"test detail">>}, <<"test message">>),

    %% Verify alarm is activated
    Alarms = emqx_alarm:get_alarms(activated),
    ?assert(lists:any(fun(#{name := Name}) -> Name =:= AlarmName end, Alarms)),

    %% Test successful force deactivate
    assert_force_deactivate_success(AlarmName),

    %% Verify alarm is deactivated
    ActivatedAlarms = emqx_alarm:get_alarms(activated),
    ?assertNot(lists:any(fun(#{name := Name}) -> Name =:= AlarmName end, ActivatedAlarms)),

    %% Test force deactivate already deactivated alarm - should fail
    assert_force_deactivate_fails(
        AlarmName, <<"NOT_FOUND">>, <<"Alarm not found or already deactivated">>
    ),

    %% Test with atom alarm name
    AtomAlarmName = test_atom_alarm,
    ok = emqx_alarm:activate(AtomAlarmName, #{}, <<"atom alarm">>),
    assert_force_deactivate_success(atom_to_binary(AtomAlarmName, utf8)),

    %% Test parameter validation
    assert_force_deactivate_fails(undefined, <<"INVALID_PARAMETER">>, <<"name is required">>),
    assert_force_deactivate_fails(<<>>, <<"INVALID_PARAMETER">>, <<"name is required">>).

t_alarm_monitor(_) ->
    AlarmName = <<"conn_congestion/test_client/test_user">>,
    TestPid = spawn(fun() ->
        AlarmDetails = #{test => details},
        AlarmMessage = <<"connection congested: test">>,
        ok = emqx_alarm:activate(AlarmName, AlarmDetails, AlarmMessage),
        %% Keep the process alive to maintain the alarm
        receive
            stop -> ok;
            _ -> ok
        end
    end),
    %% Verify alarm is activated
    timer:sleep(100),
    Alarms = emqx_alarm:get_alarms(activated),
    ?assert(lists:any(fun(#{name := Name}) -> Name =:= AlarmName end, Alarms)),

    %% Force kill the process
    exit(TestPid, kill),
    timer:sleep(100),
    ActivatedAlarms = emqx_alarm:get_alarms(activated),
    ?assertNot(lists:any(fun(#{name := Name}) -> Name =:= AlarmName end, ActivatedAlarms)),
    DeactivatedAlarms = emqx_alarm:get_alarms(deactivated),
    ?assert(lists:any(fun(#{name := Name}) -> Name =:= AlarmName end, DeactivatedAlarms)),
    ok.

t_cluster_force_deactivate() ->
    [{matrix, true}].
t_cluster_force_deactivate(matrix) ->
    [[?cluster]];
t_cluster_force_deactivate(TCConfig) when is_list(TCConfig) ->
    Nodes = get_config(nodes, TCConfig),
    Name = <<"some_alarm">>,
    ?ON_ALL(
        Nodes,
        ok = emqx_alarm:activate(
            Name,
            #{detail => <<"test detail">>},
            <<"test message">>
        )
    ),
    %% 2 alarms with the same name, one in each node.
    ?assertMatch({200, #{<<"data">> := [_, _]}}, list_alarms(#{})),
    %% Both should be deactivated
    assert_force_deactivate_success(Name),
    %% Both cleared
    ?assertMatch({200, #{<<"data">> := []}}, list_alarms(#{})),
    assert_force_deactivate_fails(
        Name, <<"NOT_FOUND">>, <<"Alarm not found or already deactivated">>
    ),
    ok.
