%%--------------------------------------------------------------------
%% Copyright (c) 2019-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-define(SUPPORTED_LEVELS, [emergency, alert, critical, error, warning, notice, info, debug]).

all() -> emqx_common_test_helpers:all(?MODULE).

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
    ok = logger:add_handler(logger_std_h_for_test, logger_std_h, #{
        config => #{type => file, file => "logger_std_h_for_test"}
    }),
    ok = logger:add_handler(logger_disk_log_h_for_test, logger_disk_log_h, #{
        config => #{file => "logger_disk_log_h_for_test"}
    }),
    ?assertMatch([_ | _], ?LOGGER:get_log_handlers()).

t_get_log_handler(_) ->
    [
        ?assertMatch(#{id := Id}, ?LOGGER:get_log_handler(Id))
     || #{id := Id} <- ?LOGGER:get_log_handlers()
    ].

t_set_log_handler_level(_) ->
    [
        begin
            ?LOGGER:set_log_handler_level(Id, Level),
            ?assertMatch(#{id := Id, level := Level}, ?LOGGER:get_log_handler(Id))
        end
     || #{id := Id} <- ?LOGGER:get_log_handlers(),
        Level <- ?SUPPORTED_LEVELS
    ],
    ?LOGGER:set_log_level(warning).

t_set_log_level(_) ->
    ?assertMatch({error, _Error}, ?LOGGER:set_log_level(for_test)),
    ?assertEqual(ok, ?LOGGER:set_log_level(debug)),
    ?assertEqual(ok, ?LOGGER:set_log_level(warning)).

t_set_all_log_handlers_level(_) ->
    ?assertMatch({error, _Error}, ?LOGGER:set_all_log_handlers_level(for_test)).

t_start_stop_log_handler(_) ->
    io:format("====== started: ~p~n", [?LOGGER:get_log_handlers(started)]),
    io:format("====== stopped: ~p~n", [?LOGGER:get_log_handlers(stopped)]),
    StartedN = length(?LOGGER:get_log_handlers(started)),
    StoppedN = length(?LOGGER:get_log_handlers(stopped)),
    [
        begin
            io:format("------ stopping : ~p~n", [Id]),
            ok = ?LOGGER:stop_log_handler(Id),
            ?assertEqual(StartedN - 1, length(?LOGGER:get_log_handlers(started))),
            ?assertEqual(StoppedN + 1, length(?LOGGER:get_log_handlers(stopped))),
            io:format("------ starting : ~p~n", [Id]),
            ok = ?LOGGER:start_log_handler(Id),
            ?assertEqual(StartedN, length(?LOGGER:get_log_handlers(started))),
            ?assertEqual(StoppedN, length(?LOGGER:get_log_handlers(stopped)))
        end
     || #{id := Id} <- ?LOGGER:get_log_handlers(started)
    ].

t_start_stop_log_handler2(_) ->
    %% start a handler that is already started returns ok
    [
        begin
            ok = ?LOGGER:start_log_handler(Id)
        end
     || #{id := Id} <- ?LOGGER:get_log_handlers(started)
    ],
    %% stop a no exists handler returns {not_started, Id}
    ?assertMatch(
        {error, {not_started, invalid_handler_id}},
        ?LOGGER:stop_log_handler(invalid_handler_id)
    ),
    %% stop a handler that is already stopped returns {not_started, Id}
    ok = ?LOGGER:stop_log_handler(default),
    ?assertMatch(
        {error, {not_started, default}},
        ?LOGGER:stop_log_handler(default)
    ).

t_set_metadata_peername(_) ->
    ?assertEqual(ok, ?LOGGER:set_metadata_peername("for_test")).

t_set_metadata_clientid(_) ->
    ?assertEqual(ok, ?LOGGER:set_metadata_clientid(<<>>)),
    ?assertEqual(ok, ?LOGGER:set_metadata_clientid("for_test")).

split_toks_at_dot(AllToks) ->
    case lists:splitwith(fun is_no_dot/1, AllToks) of
        {Toks, [{dot, _} = Dot]} -> [Toks ++ [Dot]];
        {Toks, [{dot, _} = Dot | Tl]} -> [Toks ++ [Dot] | split_toks_at_dot(Tl)]
    end.

is_no_dot({dot, _}) -> false;
is_no_dot(_) -> true.
