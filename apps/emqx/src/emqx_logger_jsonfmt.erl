%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    emqx_utils_log:format(Fmt, Args, Config).

add_default_config(Config0) ->
    Default = #{single_line => true},
    Depth = get_depth(maps:get(depth, Config0, undefined)),
    maps:merge(Default, Config0#{depth => Depth}).

get_depth(undefined) -> error_logger:get_format_depth();
get_depth(S) -> max(5, S).
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
                emqx_utils_json:format(Msg1, Config)
        end,
    MFA = emqx_utils:format_mfal(Data0, Config),
    Data =
        maps:fold(
            fun(K, V, D) ->
                {K1, V1} = emqx_utils_json:json_kv(K, V, Config),
                [{K1, V1} | D]
            end,
            [],
            maps:without(
                [time, gl, file, report_cb, msg, '$kind', level, mfa, is_trace], Data0
            )
        ),
    Properties =
        lists:filter(
            fun({_, V}) -> V =/= undefined end,
            [{time, format_ts(Time, Config)}, {level, Level}, {msg, Msg}, {mfa, MFA}]
        ) ++ Data,
    {Properties}.

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
                emqx_utils_json:json_key(K),
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
    Json = format(make_log({report, #{foo => bar}}), config()),
    ?assert(is_map(emqx_utils_json:decode(Json))),
    ok.

chars_limit_not_applied_on_raw_map_fields_test() ->
    Limit = 32,
    Len = 100,
    LongStr = lists:duplicate(Len, $a),
    Config0 = config(),
    Config = Config0#{
        chars_limit => Limit
    },
    Json = format(make_log({report, #{foo => LongStr}}), Config),
    #{<<"foo">> := LongStr1} = emqx_utils_json:decode(Json),
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
    Json = format(make_log({string, LongStr}), Config),
    #{<<"msg">> := LongStr1} = emqx_utils_json:decode(Json),
    ?assertEqual(Limit, size(LongStr1)),
    ok.

-endif.
