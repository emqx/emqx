%%--------------------------------------------------------------------
%% Copyright (c) 2022-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_rule_engine_api_rule_apply_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(CONF_DEFAULT, <<"rule_engine {rules {}}">>).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    application:load(emqx_conf),
    AppsToStart = [
        emqx,
        emqx_conf,
        emqx_connector,
        emqx_bridge,
        emqx_bridge_http,
        emqx_rule_engine
    ],
    %% I don't know why we need to stop the apps and then start them but if we
    %% don't do this and other suites run before this suite the test cases will
    %% fail as it seems like the connector silently refuses to start.
    ok = emqx_cth_suite:stop(AppsToStart),
    Apps = emqx_cth_suite:start(
        AppsToStart,
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    emqx_mgmt_api_test_util:init_suite(),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_mgmt_api_test_util:end_suite(),
    ok = emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(_Case, Config) ->
    emqx_bridge_http_test_lib:init_http_success_server(Config).

end_per_testcase(_TestCase, _Config) ->
    ok = emqx_bridge_http_connector_test_server:stop(),
    emqx_bridge_v2_testlib:delete_all_bridges(),
    emqx_bridge_v2_testlib:delete_all_connectors(),
    emqx_common_test_helpers:call_janitor(),
    ok.

t_basic_apply_rule_trace_ruleid(Config) ->
    basic_apply_rule_test_helper(Config, ruleid, false).

t_basic_apply_rule_trace_clientid(Config) ->
    basic_apply_rule_test_helper(Config, clientid, false).

t_basic_apply_rule_trace_ruleid_stop_after_render(Config) ->
    basic_apply_rule_test_helper(Config, ruleid, true).

basic_apply_rule_test_helper(Config, TraceType, StopAfterRender) ->
    HTTPServerConfig = ?config(http_server, Config),
    emqx_bridge_http_test_lib:make_bridge(HTTPServerConfig),
    #{status := connected} = emqx_bridge_v2:health_check(
        http, emqx_bridge_http_test_lib:bridge_name()
    ),
    %% Create Rule
    RuleTopic = iolist_to_binary([<<"my_rule_topic/">>, atom_to_binary(?FUNCTION_NAME)]),
    SQL = <<"SELECT payload.id as id FROM \"", RuleTopic/binary, "\"">>,
    {ok, #{<<"id">> := RuleId}} =
        emqx_bridge_testlib:create_rule_and_action_http(
            http,
            RuleTopic,
            Config,
            #{sql => SQL}
        ),
    ClientId = <<"c_emqx">>,
    %% ===================================
    %% Create trace for RuleId
    %% ===================================
    TraceName = atom_to_binary(?FUNCTION_NAME),
    TraceValue =
        case TraceType of
            ruleid ->
                RuleId;
            clientid ->
                ClientId
        end,
    create_trace(TraceName, TraceType, TraceValue),
    %% ===================================
    Context = #{
        clientid => ClientId,
        event_type => message_publish,
        payload => <<"{\"msg\": \"hello\"}">>,
        qos => 1,
        topic => RuleTopic,
        username => <<"u_emqx">>
    },
    Params = #{
        <<"context">> => Context,
        <<"stop_action_after_template_rendering">> => StopAfterRender
    },
    emqx_trace:check(),
    ok = emqx_trace_handler_SUITE:filesync(TraceName, TraceType),
    Now = erlang:system_time(second) - 10,
    {ok, _} = file:read_file(emqx_trace:log_file(TraceName, Now)),
    ?assertMatch({ok, _}, call_apply_rule_api(RuleId, Params)),
    ?retry(
        _Interval0 = 200,
        _NAttempts0 = 20,
        begin
            Bin = read_rule_trace_file(TraceName, TraceType, Now),
            io:format("THELOG:~n~s", [Bin]),
            ?assertNotEqual(nomatch, binary:match(Bin, [<<"rule_activated">>])),
            ?assertNotEqual(nomatch, binary:match(Bin, [<<"SELECT_yielded_result">>])),
            ?assertNotEqual(nomatch, binary:match(Bin, [<<"bridge_action">>])),
            ?assertNotEqual(nomatch, binary:match(Bin, [<<"action_activated">>])),
            ?assertNotEqual(nomatch, binary:match(Bin, [<<"action_template_rendered">>])),
            ?assertNotEqual(nomatch, binary:match(Bin, [<<"QUERY_ASYNC">>]))
        end
    ),
    case StopAfterRender of
        true ->
            ?retry(
                _Interval0 = 200,
                _NAttempts0 = 20,
                begin
                    Bin = read_rule_trace_file(TraceName, TraceType, Now),
                    io:format("THELOG2:~n~s", [Bin]),
                    ?assertNotEqual(nomatch, binary:match(Bin, [<<"action_failed">>]))
                end
            );
        false ->
            ?retry(
                _Interval0 = 200,
                _NAttempts0 = 20,
                begin
                    Bin = read_rule_trace_file(TraceName, TraceType, Now),
                    io:format("THELOG3:~n~s", [Bin]),
                    ?assertNotEqual(nomatch, binary:match(Bin, [<<"action_success">>]))
                end
            )
    end,
    emqx_trace:delete(TraceName),
    ok.

create_trace(TraceName, TraceType, TraceValue) ->
    Now = erlang:system_time(second) - 10,
    Start = Now,
    End = Now + 60,
    Trace = #{
        name => TraceName,
        type => TraceType,
        TraceType => TraceValue,
        start_at => Start,
        end_at => End
    },
    emqx_trace_SUITE:reload(),
    ok = emqx_trace:clear(),
    {ok, _} = emqx_trace:create(Trace).

t_apply_rule_test_batch_separation_stop_after_render(_Config) ->
    MeckOpts = [passthrough, no_link, no_history, non_strict],
    catch meck:new(emqx_connector_info, MeckOpts),
    meck:expect(
        emqx_connector_info,
        hard_coded_test_connector_info_modules,
        0,
        [emqx_rule_engine_test_connector_info]
    ),
    emqx_connector_info:clean_cache(),
    catch meck:new(emqx_action_info, MeckOpts),
    meck:expect(
        emqx_action_info,
        hard_coded_test_action_info_modules,
        0,
        [emqx_rule_engine_test_action_info]
    ),
    emqx_action_info:clean_cache(),
    {ok, _} = emqx_connector:create(rule_engine_test, ?FUNCTION_NAME, #{}),
    Name = atom_to_binary(?FUNCTION_NAME),
    ActionConf =
        #{
            <<"connector">> => Name,
            <<"parameters">> =>
                #{
                    <<"values">> =>
                        #{
                            <<"send_to_pid">> => emqx_utils:bin_to_hexstr(
                                term_to_binary(self()), upper
                            )
                        }
                },
            <<"resource_opts">> => #{
                <<"batch_size">> => 1000,
                <<"batch_time">> => 500
            }
        },
    {ok, _} = emqx_bridge_v2:create(
        rule_engine_test,
        ?FUNCTION_NAME,
        ActionConf
    ),
    SQL = <<"SELECT payload.is_stop_after_render as stop_after_render FROM \"", Name/binary, "\"">>,
    {ok, RuleID} = create_rule_with_action(
        rule_engine_test,
        ?FUNCTION_NAME,
        SQL
    ),
    create_trace(Name, ruleid, RuleID),
    emqx_trace:check(),
    ok = emqx_trace_handler_SUITE:filesync(Name, ruleid),
    Now = erlang:system_time(second) - 10,
    %% Stop
    ParmsStopAfterRender = apply_rule_parms(true, Name),
    ParmsNoStopAfterRender = apply_rule_parms(false, Name),
    %% Check that batching is working
    Count = 400,
    CountMsgFun =
        fun
            CountMsgFunRec(0 = _CurCount, GotBatchWithAtLeastTwo) ->
                GotBatchWithAtLeastTwo;
            CountMsgFunRec(CurCount, GotBatchWithAtLeastTwo) ->
                receive
                    List ->
                        Len = length(List),
                        CountMsgFunRec(CurCount - Len, GotBatchWithAtLeastTwo orelse (Len > 1))
                end
        end,
    lists:foreach(
        fun(_) ->
            {ok, _} = call_apply_rule_api(RuleID, ParmsStopAfterRender)
        end,
        lists:seq(1, Count)
    ),
    %% We should get the messages and at least one batch with more than 1
    true = CountMsgFun(Count, false),
    %% We should check that we don't get any mixed batch
    CheckBatchesFun =
        fun
            CheckBatchesFunRec(0 = _CurCount) ->
                ok;
            CheckBatchesFunRec(CurCount) ->
                receive
                    [{_, #{<<"stop_after_render">> := StopValue}} | _] = List ->
                        [
                            ?assertMatch(#{<<"stop_after_render">> := StopValue}, Msg)
                         || {_, Msg} <- List
                        ],
                        Len = length(List),
                        CheckBatchesFunRec(CurCount - Len)
                end
        end,
    lists:foreach(
        fun(_) ->
            case rand:normal() < 0 of
                true ->
                    {ok, _} = call_apply_rule_api(RuleID, ParmsStopAfterRender);
                false ->
                    {ok, _} = call_apply_rule_api(RuleID, ParmsNoStopAfterRender)
            end
        end,
        lists:seq(1, Count)
    ),
    CheckBatchesFun(Count),
    %% Just check that the log file is created as expected
    ?retry(
        _Interval0 = 200,
        _NAttempts0 = 20,
        begin
            Bin = read_rule_trace_file(Name, ruleid, Now),
            ?assertNotEqual(nomatch, binary:match(Bin, [<<"action_success">>]))
        end
    ),
    ok.

apply_rule_parms(StopAfterRender, Name) ->
    Payload = #{<<"is_stop_after_render">> => StopAfterRender},
    Context = #{
        clientid => Name,
        event_type => message_publish,
        payload => emqx_utils_json:encode(Payload),
        qos => 1,
        topic => Name,
        username => <<"u_emqx">>
    },
    #{
        <<"context">> => Context,
        <<"stop_action_after_template_rendering">> => StopAfterRender
    }.

create_rule_with_action(ActionType, ActionName, SQL) ->
    BridgeId = emqx_bridge_resource:bridge_id(ActionType, ActionName),
    Params = #{
        enable => true,
        sql => SQL,
        actions => [BridgeId]
    },
    Path = emqx_mgmt_api_test_util:api_path(["rules"]),
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    ct:pal("rule action params: ~p", [Params]),
    case emqx_mgmt_api_test_util:request_api(post, Path, "", AuthHeader, Params) of
        {ok, Res0} ->
            #{<<"id">> := RuleId} = emqx_utils_json:decode(Res0, [return_maps]),
            {ok, RuleId};
        Error ->
            Error
    end.

%% Helper Functions

call_apply_rule_api(RuleId, Params) ->
    Method = post,
    Path = emqx_mgmt_api_test_util:api_path(["rules", RuleId, "test"]),
    Res = request(Method, Path, Params),
    Res.

request(Method, Path, Params) ->
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    Opts = #{return_all => true},
    case emqx_mgmt_api_test_util:request_api(Method, Path, "", AuthHeader, Params, Opts) of
        {ok, {Status, Headers, Body0}} ->
            Body = maybe_json_decode(Body0),
            {ok, {Status, Headers, Body}};
        {error, {Status, Headers, Body0}} ->
            Body =
                case emqx_utils_json:safe_decode(Body0, [return_maps]) of
                    {ok, Decoded0 = #{<<"message">> := Msg0}} ->
                        Msg = maybe_json_decode(Msg0),
                        Decoded0#{<<"message">> := Msg};
                    {ok, Decoded0} ->
                        Decoded0;
                    {error, _} ->
                        Body0
                end,
            {error, {Status, Headers, Body}};
        Error ->
            Error
    end.

maybe_json_decode(X) ->
    case emqx_utils_json:safe_decode(X, [return_maps]) of
        {ok, Decoded} -> Decoded;
        {error, _} -> X
    end.

read_rule_trace_file(TraceName, TraceType, From) ->
    emqx_trace:check(),
    ok = emqx_trace_handler_SUITE:filesync(TraceName, TraceType),
    {ok, Bin} = file:read_file(emqx_trace:log_file(TraceName, From)),
    io_lib:format("MYTRACE:~n~s", [Bin]),
    Bin.
