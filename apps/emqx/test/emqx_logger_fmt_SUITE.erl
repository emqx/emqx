%%--------------------------------------------------------------------
%% Copyright (c) 2019-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_logger_fmt_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include("emqx_trace.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, Config) ->
    Config.

t_text_fmt_lazy_values(_) ->
    check_fmt_lazy_values(emqx_logger_textfmt).

t_text_fmt_lazy_values_only_in_debug_level_events(_) ->
    check_fmt_lazy_values_only_in_debug_level_events(emqx_logger_textfmt).

t_text_payload(_) ->
    check_fmt_payload(emqx_logger_textfmt).

t_json_fmt_lazy_values(_) ->
    check_fmt_lazy_values(emqx_logger_jsonfmt).

t_json_fmt_lazy_values_only_in_debug_level_events(_) ->
    check_fmt_lazy_values_only_in_debug_level_events(emqx_logger_jsonfmt).

t_json_payload(_) ->
    check_fmt_payload(emqx_logger_jsonfmt).

check_fmt_lazy_values(FormatModule) ->
    LogEntryIOData = FormatModule:format(event_with_lazy_value(), conf()),
    LogEntryBin = unicode:characters_to_binary(LogEntryIOData),
    %% Result of lazy evealuation should exist
    ?assertNotEqual(nomatch, binary:match(LogEntryBin, [<<"hej">>])),
    %% The lazy value should have been evaluated
    ?assertEqual(nomatch, binary:match(LogEntryBin, [<<"emqx_trace_format_func_data">>])),
    ok.

check_fmt_lazy_values_only_in_debug_level_events(FormatModule) ->
    %% For performace reason we only search for lazy values to evaluate if log level is debug
    WarningEvent = (event_with_lazy_value())#{level => info},
    LogEntryIOData = FormatModule:format(WarningEvent, conf()),
    LogEntryBin = unicode:characters_to_binary(LogEntryIOData),
    %% The input data for the formatting should exist
    ?assertNotEqual(nomatch, binary:match(LogEntryBin, [<<"hej">>])),
    %% The lazy value should not have been evaluated
    ?assertNotEqual(nomatch, binary:match(LogEntryBin, [<<"emqx_trace_format_func_data">>])),
    ok.

check_fmt_payload(FormatModule) ->
    %% For performace reason we only search for lazy values to evaluate if log level is debug
    WarningEvent = (event_with_lazy_value())#{level => info},
    Conf = conf(),
    LogEntryIOData = FormatModule:format(WarningEvent, Conf#{payload_encode => hidden}),
    LogEntryBin = unicode:characters_to_binary(LogEntryIOData),
    %% The input data for the formatting should exist
    ?assertEqual(nomatch, binary:match(LogEntryBin, [<<"content">>])),
    %% The lazy value should not have been evaluated
    ?assertNotEqual(nomatch, binary:match(LogEntryBin, [<<"******">>])),
    ok.

conf() ->
    #{
        time_offset => [],
        chars_limit => unlimited,
        depth => 100,
        single_line => true,
        template => ["[", level, "] ", msg, "\n"],
        timestamp_format => auto
    }.

event_with_lazy_value() ->
    #{
        meta => #{
            pid => what,
            time => 1715763862274127,
            gl => what,
            report_cb => fun logger:format_otp_report/1
        },
        msg =>
            {report, #{
                reason =>
                    #emqx_trace_format_func_data{function = fun(Data) -> Data end, data = hej},
                msg => hej,
                payload => <<"content">>
            }},
        level => debug
    }.
