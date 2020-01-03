%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_logger_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-define(LOGGER, emqx_logger).
-define(a, "a").

-define(PARSE_TRANS_TEST_CODE,
        "-module(mytest).\n"
        "-logger_header(\"[MyTest]\").\n"
        "-export([run/0]).\n"
        "-compile({parse_transform, logger_header}).\n"
        "run() -> '$logger_header'().").

-define(PARSE_TRANS_TEST_CODE2,
        "-module(mytest).\n"
        "-export([run/0]).\n"
        "-compile({parse_transform, logger_header}).\n"
        "run() -> '$logger_header'().").

all() -> emqx_ct:all(?MODULE).

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, Config) ->
    Config.

t_debug(_) ->
    ?assertEqual(ok, ?LOGGER:debug("for_test")),
    ?assertEqual(ok, ?LOGGER:debug("for_test", [])),
    ?assertEqual(ok, ?LOGGER:debug(#{pid => self()}, "for_test", [])).

t_info(_) ->
    ?assertEqual(ok, ?LOGGER:info("for_test")),
    ?assertEqual(ok, ?LOGGER:info("for_test", [])),
    ?assertEqual(ok, ?LOGGER:info(#{pid => self()}, "for_test", [])).

t_warning(_) ->
    ?assertEqual(ok, ?LOGGER:warning("for_test")),
    ?assertEqual(ok, ?LOGGER:warning("for_test", [])),
    ?assertEqual(ok, ?LOGGER:warning(#{pid => self()}, "for_test", [])).

t_error(_) ->
    ?assertEqual(ok, ?LOGGER:error("for_test")),
    ?assertEqual(ok, ?LOGGER:error("for_test", [])),
    ?assertEqual(ok, ?LOGGER:error(#{pid => self()}, "for_test", [])).

t_critical(_) ->
    ?assertEqual(ok, ?LOGGER:critical("for_test")),
    ?assertEqual(ok, ?LOGGER:critical("for_test", [])),
    ?assertEqual(ok, ?LOGGER:critical(#{pid => self()}, "for_test", [])).

t_set_proc_metadata(_) ->
    ?assertEqual(ok, ?LOGGER:set_proc_metadata(#{pid => self()})).

t_primary_log_level(_) ->
    ?assertEqual(ok, ?LOGGER:set_primary_log_level(debug)),
    ?assertEqual(debug, ?LOGGER:get_primary_log_level()).

t_get_log_handlers(_) ->
    ok = logger:add_handler(logger_std_h_for_test, logger_std_h,  #{config => #{type => file, file => "logger_std_h_for_test"}}),
    ok = logger:add_handler(logger_disk_log_h_for_test, logger_disk_log_h,  #{config => #{file => "logger_disk_log_h_for_test"}}),
    ?assertMatch([_|_], ?LOGGER:get_log_handlers()).

t_get_log_handler(_) ->
    [{HandlerId, _, _} | _ ] = ?LOGGER:get_log_handlers(),
    ?assertMatch({HandlerId, _, _}, ?LOGGER:get_log_handler(HandlerId)).

t_set_log_handler_level(_) ->
    [{HandlerId, _, _} | _ ] = ?LOGGER:get_log_handlers(),
    Level = debug,
    ?LOGGER:set_log_handler_level(HandlerId, Level),
    ?assertMatch({HandlerId, Level, _}, ?LOGGER:get_log_handler(HandlerId)).

t_set_log_level(_) ->
    ?assertMatch({error, _Error}, ?LOGGER:set_log_level(for_test)),
    ?assertEqual(ok, ?LOGGER:set_log_level(debug)).

t_set_all_log_handlers_level(_) ->
    ?assertMatch({error, _Error}, ?LOGGER:set_all_log_handlers_level(for_test)).

t_parse_transform(_) ->
    {ok, Toks, _EndLine} = erl_scan:string(?PARSE_TRANS_TEST_CODE),
    FormToks = split_toks_at_dot(Toks),
    Forms = [case erl_parse:parse_form(Ts) of
                 {ok, Form} ->
                     Form;
                 {error, Reason} ->
                     erlang:error({parse_form_error, Ts, Reason})
             end
             || Ts <- FormToks],
    %ct:log("=====: ~p", [Forms]),
    AST1 = emqx_logger:parse_transform(Forms, []),
    %ct:log("=====: ~p", [AST1]),
    ?assertNotEqual(false, lists:keyfind('$logger_header', 3, AST1)).

t_parse_transform_empty_header(_) ->
    {ok, Toks, _EndLine} = erl_scan:string(?PARSE_TRANS_TEST_CODE2),
    FormToks = split_toks_at_dot(Toks),
    Forms = [case erl_parse:parse_form(Ts) of
                 {ok, Form} ->
                     Form;
                 {error, Reason} ->
                     erlang:error({parse_form_error, Ts, Reason})
             end
             || Ts <- FormToks],
    %ct:log("=====: ~p", [Forms]),
    AST2 = emqx_logger:parse_transform(Forms++[{eof, 15}], []),
    %ct:log("=====: ~p", [AST2]),
    ?assertNotEqual(false, lists:keyfind('$logger_header', 3, AST2)).


t_set_metadata_peername(_) ->
    ?assertEqual(ok, ?LOGGER:set_metadata_peername("for_test")).

t_set_metadata_clientid(_) ->
    ?assertEqual(ok, ?LOGGER:set_metadata_clientid(<<>>)),
    ?assertEqual(ok, ?LOGGER:set_metadata_clientid("for_test")).


split_toks_at_dot(AllToks) ->
    case lists:splitwith(fun is_no_dot/1, AllToks) of
        {Toks, [{dot,_}=Dot]}      -> [Toks ++ [Dot]];
        {Toks, [{dot,_}=Dot | Tl]} -> [Toks ++ [Dot] | split_toks_at_dot(Tl)]
    end.

is_no_dot({dot,_}) -> false;
is_no_dot(_)       -> true.
