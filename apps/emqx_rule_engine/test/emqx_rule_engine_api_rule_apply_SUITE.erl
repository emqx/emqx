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
    Now = erlang:system_time(second) - 10,
    Start = Now,
    End = Now + 60,
    TraceName = atom_to_binary(?FUNCTION_NAME),
    TraceValue =
        case TraceType of
            ruleid ->
                RuleId;
            clientid ->
                ClientId
        end,
    Trace = #{
        name => TraceName,
        type => TraceType,
        TraceType => TraceValue,
        start_at => Start,
        end_at => End
    },
    emqx_trace_SUITE:reload(),
    ok = emqx_trace:clear(),
    {ok, _} = emqx_trace:create(Trace),
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
        % body => #{
        <<"context">> => Context,
        <<"stop_action_after_template_rendering">> => StopAfterRender
        % }
    },
    emqx_trace:check(),
    ok = emqx_trace_handler_SUITE:filesync(TraceName, TraceType),
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

%% Helper Functions

call_apply_rule_api(RuleId, Params) ->
    Method = post,
    Path = emqx_mgmt_api_test_util:api_path(["rules", RuleId, "test"]),
    ct:pal("sql test (http):\n  ~p", [Params]),
    Res = request(Method, Path, Params),
    ct:pal("sql test (http) result:\n  ~p", [Res]),
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
