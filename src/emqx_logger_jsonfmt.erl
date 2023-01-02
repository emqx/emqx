%%--------------------------------------------------------------------
%% Copyright (c) 2021-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-ifdef(TEST).
-export([report_cb_1/1, report_cb_2/2, report_cb_crash/2]).
-endif.

-export_type([config/0]).

-type config() :: #{depth       => pos_integer() | unlimited,
                    report_cb   => logger:report_cb(),
                    single_line => boolean()}.

-define(IS_STRING(String), (is_list(String) orelse is_binary(String))).

-spec format(logger:log_event(), config()) -> iodata().
format(#{level := Level, msg := Msg, meta := Meta}, Config0) when is_map(Config0) ->
    Config = add_default_config(Config0),
    [format(Msg, Meta#{level => Level}, Config) , "\n"].

format(Msg, Meta, Config) ->
    Data0 =
        try Meta#{msg => format_msg(Msg, Meta, Config)}
        catch
            C:R:S ->
                Meta#{ msg => "emqx_logger_jsonfmt_format_error"
                     , fmt_raw_input => Msg
                     , fmt_error => C
                     , fmt_reason => R
                     , fmt_stacktrace => S
                     }
        end,
    Data = maps:without([report_cb], Data0),
    jiffy:encode(json_obj(Data, Config)).

format_msg({string, Chardata}, Meta, Config) ->
    format_msg({"~ts", [Chardata]}, Meta, Config);
format_msg({report, _} = Msg, Meta, #{report_cb := Fun} = Config)
  when is_function(Fun,1); is_function(Fun,2) ->
    format_msg(Msg, Meta#{report_cb => Fun}, maps:remove(report_cb, Config));
format_msg({report, Report}, #{report_cb := Fun} = Meta, Config) when is_function(Fun, 1) ->
    case Fun(Report) of
        {Format, Args} when is_list(Format), is_list(Args) ->
            format_msg({Format, Args}, maps:remove(report_cb, Meta), Config);
        Other ->
            #{ msg => "report_cb_bad_return"
             , report_cb_fun => Fun
             , report_cb_return => Other
             }
    end;
format_msg({report, Report}, #{report_cb := Fun}, Config) when is_function(Fun, 2) ->
    case Fun(Report, maps:with([depth, single_line], Config)) of
        Chardata when ?IS_STRING(Chardata) ->
            try
                unicode:characters_to_binary(Chardata, utf8)
            catch
                _:_ ->
                    #{ msg => "report_cb_bad_return"
                     , report_cb_fun => Fun
                     , report_cb_return => Chardata
                     }
            end;
        Other ->
            #{ msg => "report_cb_bad_return"
             , report_cb_fun => Fun
             , report_cb_return => Other
             }
    end;
format_msg({Fmt, Args}, _Meta, Config) ->
    do_format_msg(Fmt, Args, Config).

do_format_msg(Format0, Args, #{depth := Depth,
                               single_line := SingleLine
                              }) ->
    Format1 = io_lib:scan_format(Format0, Args),
    Format = reformat(Format1, Depth, SingleLine),
    Text0 = io_lib:build_text(Format, []),
    Text = case SingleLine of
               true -> re:replace(Text0, ",?\r?\n\s*",", ", [{return, list}, global, unicode]);
               false -> Text0
           end,
    trim(unicode:characters_to_binary(Text, utf8)).

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
reformat([#{control_char := C}=M | T], Depth, Single) when C =:= $p; C =:= $w ->
    [limit_depth(M, Depth) | reformat(T, Depth, Single)];
reformat([H | T], Depth, Single) ->
    [H | reformat(T, Depth, Single)];
reformat([], _, _) ->
    [].

limit_depth(M0, unlimited) -> M0;
limit_depth(#{control_char:=C0, args:=Args}=M0, Depth) ->
    C = C0 - ($a - $A), %To uppercase.
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
        _ : _ ->
            do_format_msg("~p", [Input], Config)
    end.

best_effort_json_obj(List, Config) when is_list(List) ->
    try
        json_obj(maps:from_list(List), Config)
    catch
        _ : _ ->
            [json(I, Config) || I <- List]
    end;
best_effort_json_obj(Map, Config) ->
    try
        json_obj(Map, Config)
    catch
        _ : _ ->
            do_format_msg("~p", [Map], Config)
    end.

json([], _) -> "[]";
json(<<"">>, _) -> "\"\"";
json(A, _) when is_atom(A) -> atom_to_binary(A, utf8);
json(I, _) when is_integer(I) -> I;
json(F, _) when is_float(F) -> F;
json(P, C) when is_pid(P) -> json(pid_to_list(P), C);
json(P, C) when is_port(P) -> json(port_to_list(P), C);
json(F, C) when is_function(F) -> json(erlang:fun_to_list(F), C);
json(B, Config) when is_binary(B) ->
    best_effort_unicode(B, Config);
json(L, Config) when is_list(L), is_integer(hd(L))->
    best_effort_unicode(L, Config);
json(M, Config) when is_list(M), is_tuple(hd(M)), tuple_size(hd(M)) =:= 2 ->
    best_effort_json_obj(M, Config);
json(L, Config) when is_list(L) ->
    [json(I, Config) || I <- L];
json(Map, Config) when is_map(Map) ->
    best_effort_json_obj(Map, Config);
json(Term, Config) ->
    do_format_msg("~p", [Term], Config).

json_obj(Data, Config) ->
    maps:fold(fun (K, V, D) ->
                      json_kv(K, V, D, Config)
              end, maps:new(), Data).

json_kv(mfa, {M, F, A}, Data, _Config) -> %% emqx/snabbkaffe
    maps:put(mfa, <<(atom_to_binary(M, utf8))/binary, $:,
                    (atom_to_binary(F, utf8))/binary, $/,
                    (integer_to_binary(A))/binary>>, Data);
json_kv('$kind', Kind, Data, Config) -> %% snabbkaffe
    maps:put(msg, json(Kind, Config), Data);
json_kv(K0, V, Data, Config) ->
    K = json_key(K0),
    case is_map(V) of
        true -> maps:put(json(K, Config), best_effort_json_obj(V, Config), Data);
        false -> maps:put(json(K, Config), json(V, Config), Data)
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
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

no_crash_test_() ->
    Opts = [{numtests, 1000}, {to_file, user}],
    {timeout, 30,
     fun() -> ?assert(proper:quickcheck(t_no_crash(), Opts)) end}.

t_no_crash() ->
    ?FORALL({Level, Report, Meta, Config},
            {p_level(), p_report(), p_meta(), p_config()},
            t_no_crash_run(Level, Report, Meta, Config)).

t_no_crash_run(Level, Report, {undefined, Meta}, Config) ->
    t_no_crash_run(Level, Report, maps:from_list(Meta), Config);
t_no_crash_run(Level, Report, {ReportCb, Meta}, Config) ->
    t_no_crash_run(Level, Report, maps:from_list([{report_cb, ReportCb} | Meta]), Config);
t_no_crash_run(Level, Report, Meta, Config) ->
    Input = #{ level => Level
             , msg => {report, Report}
             , meta => filter(Meta)
             },
    _ = format(Input, maps:from_list(Config)),
    true.

%% assume top level Report and Meta are sane
filter(Map) ->
    Keys = lists:filter(
             fun(K) ->
                     try json_key(K), true
                     catch throw : {badkey, _} -> false
                     end
             end, maps:keys(Map)),
    maps:with(Keys, Map).

p_report_cb() ->
    proper_types:oneof([ fun ?MODULE:report_cb_1/1
                       , fun ?MODULE:report_cb_2/2
                       , fun ?MODULE:report_cb_crash/2
                       , fun logger:format_otp_report/1
                       , fun logger:format_report/1
                       , format_report_undefined
                       ]).

report_cb_1(Input) -> {"~p", [Input]}.

report_cb_2(Input, _Config) -> io_lib:format("~p", [Input]).

report_cb_crash(_Input, _Config) -> error(report_cb_crash).

p_kvlist() ->
    proper_types:list({
        proper_types:oneof([proper_types:atom(),
                            proper_types:binary()
                           ]), proper_types:term()}).

%% meta type is 2-tuple, report_cb type, and some random key value pairs
p_meta() ->
    {p_report_cb(), p_kvlist()}.

p_report() -> p_kvlist().

p_limit() -> proper_types:oneof([proper_types:pos_integer(), unlimited]).

p_level() -> proper_types:oneof([info, debug, error, warning, foobar]).

p_config() ->
    proper_types:shrink_list(
      [ {depth, p_limit()}
      , {single_line, proper_types:boolean()}
      ]).

-endif.
