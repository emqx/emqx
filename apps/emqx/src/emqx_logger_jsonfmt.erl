%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc This logger formatter tries format logs into JSON objects
%%
%% Due to the fact that the `Report' body of log entries are *NOT*
%% structured, we only try to JSON-ify `Meta',
%%
%% `Report' body format is pretty-printed and used as the `msg'
%% JSON field in the finale result.
%%
%% e.g. logger:log(info, _Data = #{foo => bar}, _Meta = #{metaf1 => 2})
%% will results in a JSON object look like below:
%%
%% {"time": 1620226963427808, "level": "info", "msg": "foo: bar", "metaf1": 2}

-module(emqx_logger_jsonfmt).

-export([format/2]).

%% For CLI HTTP API outputs
-export([best_effort_json/1, best_effort_json/2, best_effort_json_obj/1]).

%% For emqx_trace_json_formatter
-export([format_msg/3]).

-ifdef(TEST).
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([report_cb_1/1, report_cb_2/2, report_cb_crash/2]).
-endif.

-export_type([config/0]).

-elvis([{elvis_style, no_nested_try_catch, #{ignore => [emqx_logger_jsonfmt]}}]).

%% this is what used when calling logger:log(Level, Report, Meta).
-define(DEFAULT_FORMATTER, fun logger:format_otp_report/1).

-type config() :: #{
    depth => pos_integer() | unlimited,
    report_cb => logger:report_cb(),
    single_line => boolean(),
    chars_limit => unlimited | pos_integer(),
    payload_encode => text | hidden | hex
}.

-define(IS_STRING(String), (is_list(String) orelse is_binary(String))).

%% @doc Format a list() or map() to JSON object.
%% This is used for CLI result prints,
%% or HTTP API result formatting.
%% The JSON object is pretty-printed.
%% NOTE: do not use this function for logging.
best_effort_json(Input) ->
    best_effort_json(Input, [pretty, force_utf8]).
best_effort_json(Input, Opts) ->
    JsonReady = best_effort_json_obj(Input),
    emqx_utils_json:encode(JsonReady, Opts).

best_effort_json_obj(Input) ->
    Config = #{depth => unlimited, single_line => true, chars_limit => unlimited},
    best_effort_json_obj(Input, Config).

-spec format(logger:log_event(), config()) -> iodata().
format(#{level := _Level, msg := _Msg, meta := _Meta} = Entry, Config0) when is_map(Config0) ->
    #{level := Level, msg := Msg, meta := Meta} =
        emqx_logger_textfmt:evaluate_lazy_values_if_dbg_level(Entry),
    Config = add_default_config(Config0),
    [format(Msg, Meta#{level => Level}, Config), "\n"].

format(Msg, Meta, Config) ->
    Data =
        try maybe_format_msg(Msg, Meta, Config) of
            Map when is_map(Map) ->
                maps:merge(Map, Meta);
            Bin when is_binary(Bin) ->
                Meta#{msg => Bin}
        catch
            C:R:S ->
                Meta#{
                    msg => "emqx_logger_jsonfmt_format_error",
                    fmt_raw_input => Msg,
                    fmt_error => C,
                    fmt_reason => R,
                    fmt_stacktrace => S
                }
        end,
    emqx_utils_json:encode(json_obj_root(Data, Config)).

maybe_format_msg(undefined, _Meta, _Config) ->
    #{};
maybe_format_msg({report, Report0} = Msg, #{report_cb := Cb} = Meta, Config) ->
    Report = emqx_logger_textfmt:try_encode_meta(Report0, Config),
    case is_map(Report) andalso Cb =:= ?DEFAULT_FORMATTER of
        true ->
            %% reporting a map without a customised format function
            Report;
        false ->
            format_msg(Msg, Meta, Config)
    end;
maybe_format_msg(Msg, Meta, Config) ->
    format_msg(Msg, Meta, Config).

format_msg({string, Chardata}, Meta, Config) ->
    %% already formatted
    format_msg({"~ts", [Chardata]}, Meta, Config);
format_msg({report, _} = Msg, Meta, #{report_cb := Fun} = Config) when
    is_function(Fun, 1); is_function(Fun, 2)
->
    %% a format callback function in config, no idea when this happens, but leaving it
    format_msg(Msg, Meta#{report_cb => Fun}, maps:remove(report_cb, Config));
format_msg({report, Report}, #{report_cb := Fun} = Meta, Config) when is_function(Fun, 1) ->
    %% a format callback function of arity 1
    case Fun(Report) of
        {Format, Args} when is_list(Format), is_list(Args) ->
            format_msg({Format, Args}, maps:remove(report_cb, Meta), Config);
        Other ->
            #{
                msg => "report_cb_bad_return",
                report_cb_fun => Fun,
                report_cb_return => Other
            }
    end;
format_msg({report, Report}, #{report_cb := Fun}, Config) when is_function(Fun, 2) ->
    %% a format callback function of arity 2
    case Fun(Report, maps:with([depth, single_line, chars_limit], Config)) of
        Chardata when ?IS_STRING(Chardata) ->
            try
                unicode:characters_to_binary(Chardata, utf8)
            catch
                _:_ ->
                    #{
                        msg => "report_cb_bad_return",
                        report_cb_fun => Fun,
                        report_cb_return => Chardata
                    }
            end;
        Other ->
            #{
                msg => "report_cb_bad_return",
                report_cb_fun => Fun,
                report_cb_return => Other
            }
    end;
format_msg({Fmt, Args}, _Meta, Config) ->
    do_format_msg(Fmt, Args, Config).

do_format_msg(Format0, Args, #{
    depth := Depth,
    single_line := SingleLine,
    chars_limit := Limit
}) ->
    Opts = chars_limit_to_opts(Limit),
    Format1 = io_lib:scan_format(Format0, Args),
    Format = reformat(Format1, Depth, SingleLine),
    Text0 = io_lib:build_text(Format, Opts),
    Text =
        case SingleLine of
            true -> re:replace(Text0, ",?\r?\n\s*", ", ", [{return, list}, global, unicode]);
            false -> Text0
        end,
    trim(unicode:characters_to_binary(Text, utf8)).

chars_limit_to_opts(unlimited) -> [];
chars_limit_to_opts(Limit) -> [{chars_limit, Limit}].

%% Get rid of the leading spaces.
%% leave alone the trailing spaces.
trim(<<$\s, Rest/binary>>) -> trim(Rest);
trim(Bin) -> Bin.

reformat(Format, unlimited, false) ->
    Format;
reformat([#{control_char := C} = M | T], Depth, true) when C =:= $p ->
    [limit_depth(M#{width => 0}, Depth) | reformat(T, Depth, true)];
reformat([#{control_char := C} = M | T], Depth, true) when C =:= $P ->
    [M#{width => 0} | reformat(T, Depth, true)];
reformat([#{control_char := C} = M | T], Depth, Single) when C =:= $p; C =:= $w ->
    [limit_depth(M, Depth) | reformat(T, Depth, Single)];
reformat([H | T], Depth, Single) ->
    [H | reformat(T, Depth, Single)];
reformat([], _, _) ->
    [].

limit_depth(M0, unlimited) ->
    M0;
limit_depth(#{control_char := C0, args := Args} = M0, Depth) ->
    %To uppercase.
    C = C0 - ($a - $A),
    M0#{control_char := C, args := Args ++ [Depth]}.

add_default_config(Config0) ->
    Default = #{single_line => true},
    Depth = get_depth(maps:get(depth, Config0, undefined)),
    maps:merge(Default, Config0#{depth => Depth}).

get_depth(undefined) -> error_logger:get_format_depth();
get_depth(S) -> max(5, S).

best_effort_unicode(Input, Config) ->
    try unicode:characters_to_binary(Input, utf8) of
        B when is_binary(B) -> B;
        _ -> do_format_msg("~p", [Input], Config)
    catch
        _:_ ->
            do_format_msg("~p", [Input], Config)
    end.

best_effort_json_obj(List, Config) when is_list(List) ->
    try
        json_obj(convert_tuple_list_to_map(List), Config)
    catch
        _:_ ->
            [json(I, Config) || I <- List]
    end;
best_effort_json_obj(Map, Config) ->
    try
        json_obj(Map, Config)
    catch
        _:_ ->
            do_format_msg("~p", [Map], Config)
    end.

%% This function will throw if the list do not only contain tuples or if there
%% are duplicate keys.
convert_tuple_list_to_map(List) ->
    %% Crash if this is not a tuple list
    CandidateMap = maps:from_list(List),
    %% Crash if there are duplicates
    NumberOfItems = length(List),
    NumberOfItems = maps:size(CandidateMap),
    CandidateMap.

json(A, _) when is_atom(A) -> A;
json(I, _) when is_integer(I) -> I;
json(F, _) when is_float(F) -> F;
json(P, C) when is_pid(P) -> json(pid_to_list(P), C);
json(P, C) when is_port(P) -> json(port_to_list(P), C);
json(F, C) when is_function(F) -> json(erlang:fun_to_list(F), C);
json(B, Config) when is_binary(B) ->
    best_effort_unicode(B, Config);
json(M, Config) when is_list(M), is_tuple(hd(M)), tuple_size(hd(M)) =:= 2 ->
    best_effort_json_obj(M, Config);
json(L, Config) when is_list(L) ->
    case lists:all(fun erlang:is_binary/1, L) of
        true ->
            %% string array
            L;
        false ->
            try unicode:characters_to_binary(L, utf8) of
                B when is_binary(B) -> B;
                _ -> [json(I, Config) || I <- L]
            catch
                _:_ ->
                    [json(I, Config) || I <- L]
            end
    end;
json(Map, Config) when is_map(Map) ->
    best_effort_json_obj(Map, Config);
json({'$array$', List}, Config) when is_list(List) ->
    [json(I, Config) || I <- List];
json(Term, Config) ->
    do_format_msg("~p", [Term], Config).

json_obj_root(Data0, Config) ->
    Time = maps:get(time, Data0, undefined),
    Level = maps:get(level, Data0, undefined),
    Msg1 =
        case maps:get(msg, Data0, undefined) of
            undefined ->
                maps:get('$kind', Data0, undefined);
            Msg0 ->
                Msg0
        end,
    Msg =
        case Msg1 of
            undefined ->
                undefined;
            _ ->
                json(Msg1, Config)
        end,
    MFA = emqx_utils:format_mfal(Data0, Config),
    Data =
        maps:fold(
            fun(K, V, D) ->
                {K1, V1} = json_kv(K, V, Config),
                [{K1, V1} | D]
            end,
            [],
            maps:without(
                [time, gl, file, report_cb, msg, '$kind', level, mfa, is_trace], Data0
            )
        ),
    lists:filter(
        fun({_, V}) -> V =/= undefined end,
        [{time, format_ts(Time, Config)}, {level, Level}, {msg, Msg}, {mfa, MFA}]
    ) ++ Data.

format_ts(Ts, #{timestamp_format := rfc3339, time_offset := Offset}) when is_integer(Ts) ->
    iolist_to_binary(
        calendar:system_time_to_rfc3339(Ts, [
            {unit, microsecond},
            {offset, Offset},
            {time_designator, $T}
        ])
    );
format_ts(Ts, _Config) ->
    % auto | epoch
    Ts.

json_obj(Data, Config) ->
    maps:fold(
        fun(K, V, D) ->
            {K1, V1} = json_kv(K, V, Config),
            maps:put(K1, V1, D)
        end,
        maps:new(),
        Data
    ).

json_kv(K0, V, Config) ->
    K = json_key(K0),
    case is_map(V) of
        true -> {K, best_effort_json_obj(V, Config)};
        false -> {K, json(V, Config)}
    end.

json_key(A) when is_atom(A) -> json_key(atom_to_binary(A, utf8));
json_key(Term) ->
    try unicode:characters_to_binary(Term, utf8) of
        OK when is_binary(OK) andalso OK =/= <<>> ->
            OK;
        _ ->
            throw({badkey, Term})
    catch
        _:_ ->
            throw({badkey, Term})
    end.

-ifdef(TEST).

no_crash_test_() ->
    Opts = [{numtests, 1000}, {to_file, user}],
    {timeout, 30, fun() -> ?assert(proper:quickcheck(t_no_crash(), Opts)) end}.

t_no_crash() ->
    ?FORALL(
        {Level, Report, Meta, Config},
        {p_level(), p_report(), p_meta(), p_config()},
        t_no_crash_run(Level, Report, Meta, Config)
    ).

t_no_crash_run(Level, Report, {undefined, Meta}, Config) ->
    t_no_crash_run(Level, Report, maps:from_list(Meta), Config);
t_no_crash_run(Level, Report, {ReportCb, Meta}, Config) ->
    t_no_crash_run(Level, Report, maps:from_list([{report_cb, ReportCb} | Meta]), Config);
t_no_crash_run(Level, Report, Meta, Config) ->
    Input = #{
        level => Level,
        msg => {report, Report},
        meta => filter(Meta)
    },
    _ = format(Input, maps:from_list(Config)),
    true.

%% assume top level Report and Meta are sane
filter(Map) ->
    Keys = lists:filter(
        fun(K) ->
            try
                json_key(K),
                true
            catch
                throw:{badkey, _} -> false
            end
        end,
        maps:keys(Map)
    ),
    maps:with(Keys, Map).

p_report_cb() ->
    proper_types:oneof([
        fun ?MODULE:report_cb_1/1,
        fun ?MODULE:report_cb_2/2,
        fun ?MODULE:report_cb_crash/2,
        fun logger:format_otp_report/1,
        fun logger:format_report/1,
        format_report_undefined
    ]).

report_cb_1(Input) -> {"~p", [Input]}.

report_cb_2(Input, _Config) -> io_lib:format("~p", [Input]).

report_cb_crash(_Input, _Config) -> error(report_cb_crash).

p_kvlist() ->
    proper_types:list({
        proper_types:oneof([
            proper_types:atom(),
            proper_types:binary()
        ]),
        proper_types:term()
    }).

%% meta type is 2-tuple, report_cb type, and some random key value pairs
p_meta() ->
    {p_report_cb(), p_kvlist()}.

p_report() -> p_kvlist().

p_limit() -> proper_types:oneof([proper_types:pos_integer(), unlimited]).

p_level() -> proper_types:oneof([info, debug, error, warning, foobar]).

p_config() ->
    proper_types:shrink_list(
        [
            {depth, p_limit()},
            {single_line, proper_types:boolean()},
            {chars_limit, p_limit()}
        ]
    ).

%% NOTE: pretty-printing format is asserted in the test
%% This affects the CLI output format, consult the team before changing
%% the format.
best_effort_json_test() ->
    ?assertEqual(
        <<"{\n  \n}">>,
        best_effort_json([])
    ),
    ?assertEqual(
        <<"{\n  \"key\" : [\n    \n  ]\n}">>,
        best_effort_json(#{key => []})
    ),
    ?assertEqual(
        <<"[\n  {\n    \"key\" : [\n      \n    ]\n  }\n]">>,
        best_effort_json([#{key => []}])
    ),
    %% List is IO Data
    ?assertMatch(
        #{<<"what">> := <<"hej\n">>},
        emqx_utils_json:decode(emqx_logger_jsonfmt:best_effort_json(#{what => [<<"hej">>, 10]}))
    ),
    %% Force list to be interpreted as an array
    ?assertMatch(
        #{<<"what">> := [<<"hej">>, 10]},
        emqx_utils_json:decode(
            emqx_logger_jsonfmt:best_effort_json(#{what => {'$array$', [<<"hej">>, 10]}})
        )
    ),
    %% IO Data inside an array
    ?assertMatch(
        #{<<"what">> := [<<"hej">>, 10, <<"hej\n">>]},
        emqx_utils_json:decode(
            emqx_logger_jsonfmt:best_effort_json(#{
                what => {'$array$', [<<"hej">>, 10, [<<"hej">>, 10]]}
            })
        )
    ),
    %% Array inside an array
    ?assertMatch(
        #{<<"what">> := [<<"hej">>, 10, [<<"hej">>, 10]]},
        emqx_utils_json:decode(
            emqx_logger_jsonfmt:best_effort_json(#{
                what => {'$array$', [<<"hej">>, 10, {'$array$', [<<"hej">>, 10]}]}
            })
        )
    ),
    ok.

config() ->
    #{
        chars_limit => unlimited,
        depth => unlimited,
        single_line => true
    }.

make_log(Report) ->
    #{
        level => info,
        msg => Report,
        meta => #{time => 1111, report_cb => ?DEFAULT_FORMATTER}
    }.

ensure_json_output_test() ->
    JSON = format(make_log({report, #{foo => bar}}), config()),
    ?assert(is_map(emqx_utils_json:decode(JSON))),
    ok.

chars_limit_not_applied_on_raw_map_fields_test() ->
    Limit = 32,
    Len = 100,
    LongStr = lists:duplicate(Len, $a),
    Config0 = config(),
    Config = Config0#{
        chars_limit => Limit
    },
    JSON = format(make_log({report, #{foo => LongStr}}), Config),
    #{<<"foo">> := LongStr1} = emqx_utils_json:decode(JSON),
    ?assertEqual(Len, size(LongStr1)),
    ok.

chars_limit_applied_on_format_result_test() ->
    Limit = 32,
    Len = 100,
    LongStr = lists:duplicate(Len, $a),
    Config0 = config(),
    Config = Config0#{
        chars_limit => Limit
    },
    JSON = format(make_log({string, LongStr}), Config),
    #{<<"msg">> := LongStr1} = emqx_utils_json:decode(JSON),
    ?assertEqual(Limit, size(LongStr1)),
    ok.

string_array_test() ->
    Array = #{<<"arr">> => [<<"a">>, <<"b">>]},
    Encoded = emqx_utils_json:encode(json(Array, config())),
    ?assertEqual(Array, emqx_utils_json:decode(Encoded)).

iolist_test() ->
    Iolist = #{iolist => ["a", ["b"]]},
    Concat = #{<<"iolist">> => <<"ab">>},
    Encoded = emqx_utils_json:encode(json(Iolist, config())),
    ?assertEqual(Concat, emqx_utils_json:decode(Encoded)).

-endif.
