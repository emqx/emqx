%%
%% %CopyrightBegin%
%%
%% Copyright Ericsson AB 2013-2019. All Rights Reserved.
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
%%
%% %CopyrightEnd%
%%

%% This file is copied from lib/kernel/src/logger_formatter.erl, and
%% modified for a more concise time format other than the default RFC3339.

-module(emqx_logger_formatter).

-export([format/2]).

-export([check_config/1]).

-define(DEFAULT_FORMAT_TEMPLATE_SINGLE, [time," ",level,": ",msg,"\n"]).

-define(FormatP, "~0tp").

-define(IS_STRING(String),
        (is_list(String) orelse is_binary(String))).

%%%-----------------------------------------------------------------
%%% Types
-type config() :: #{chars_limit     => pos_integer() | unlimited,
                    depth           => pos_integer() | unlimited,
                    max_size        => pos_integer() | unlimited,
                    report_cb       => logger:report_cb(),
                    quit        => template()}.
-type template() :: [metakey() | {metakey(),template(),template()} | string()].
-type metakey() :: atom() | [atom()].

%%%-----------------------------------------------------------------
%%% API
-spec format(LogEvent,Config) -> unicode:chardata() when
      LogEvent :: logger:log_event(),
      Config :: config().
format(#{level:=Level,msg:=Msg0,meta:=Meta},Config0)
  when is_map(Config0) ->
    Config = add_default_config(Config0),
    Template = maps:get(template,Config),
    {BT,AT0} = lists:splitwith(fun(msg) -> false; (_) -> true end, Template),
    {DoMsg,AT} =
        case AT0 of
            [msg|Rest] -> {true,Rest};
            _ ->{false,AT0}
        end,
    B = do_format(Level,Meta,BT,Config),
    A = do_format(Level,Meta,AT,Config),
    MsgStr =
        if DoMsg ->
                Config1 =
                    case maps:get(chars_limit,Config) of
                        unlimited ->
                            Config;
                        Size0 ->
                            Size =
                                case Size0 - string:length([B,A]) of
                                    S when S>=0 -> S;
                                    _ -> 0
                                end,
                            Config#{chars_limit=>Size}
                    end,
                string:trim(format_msg(Msg0,Meta,Config1));
           true ->
                ""
        end,
    truncate([B,MsgStr,A],maps:get(max_size,Config)).

do_format(Level,Data,[level|Format],Config) ->
    [to_string(level,Level,Config)|do_format(Level,Data,Format,Config)];
do_format(Level,Data,[{Key,IfExist,Else}|Format],Config) ->
    String =
        case value(Key,Data) of
            {ok,Value} -> do_format(Level,Data#{Key=>Value},IfExist,Config);
            error -> do_format(Level,Data,Else,Config)
        end,
    [String|do_format(Level,Data,Format,Config)];
do_format(Level,Data,[Key|Format],Config)
  when is_atom(Key) orelse
       (is_list(Key) andalso is_atom(hd(Key))) ->
    String =
        case value(Key,Data) of
            {ok,Value} -> to_string(Key,Value,Config);
            error -> ""
        end,
    [String|do_format(Level,Data,Format,Config)];
do_format(Level,Data,[Str|Format],Config) ->
    [Str|do_format(Level,Data,Format,Config)];
do_format(_Level,_Data,[],_Config) ->
    [].

value(Key,Meta) when is_map_key(Key,Meta) ->
    {ok,maps:get(Key,Meta)};
value([Key|Keys],Meta) when is_map_key(Key,Meta) ->
    value(Keys,maps:get(Key,Meta));
value([],Value) ->
    {ok,Value};
value(_,_) ->
    error.

to_string(time,Time,Config) ->
    format_time(Time,Config);
to_string(mfa,MFA,Config) ->
    format_mfa(MFA,Config);
to_string(_,Value,Config) ->
    to_string(Value,Config).

to_string(X,_) when is_atom(X) ->
    atom_to_list(X);
to_string(X,_) when is_integer(X) ->
    integer_to_list(X);
to_string(X,_) when is_pid(X) ->
    pid_to_list(X);
to_string(X,_) when is_reference(X) ->
    ref_to_list(X);
to_string(X,_) when is_list(X) ->
    case printable_list(lists:flatten(X)) of
        true -> X;
        _ -> io_lib:format(?FormatP,[X])
    end;
to_string(X,_) ->
    io_lib:format("~s",[X]).

printable_list([]) ->
    false;
printable_list(X) ->
    io_lib:printable_list(X).

format_msg({string,Chardata},Meta,Config) ->
    format_msg({"~ts",[Chardata]},Meta,Config);
format_msg({report,_}=Msg,Meta,#{report_cb:=Fun}=Config)
  when is_function(Fun,1); is_function(Fun,2) ->
    format_msg(Msg,Meta#{report_cb=>Fun},maps:remove(report_cb,Config));
format_msg({report,Report},#{report_cb:=Fun}=Meta,Config) when is_function(Fun,1) ->
    try Fun(Report) of
        {Format,Args} when is_list(Format), is_list(Args) ->
            format_msg({Format,Args},maps:remove(report_cb,Meta),Config);
        Other ->
            format_msg({"REPORT_CB/1 ERROR: ~0tp; Returned: ~0tp",
                        [Report,Other]},Meta,Config)
    catch C:R:S ->
            format_msg({"REPORT_CB/1 CRASH: ~0tp; Reason: ~0tp",
                        [Report,{C,R,logger:filter_stacktrace(?MODULE,S)}]},Meta,Config)
    end;
format_msg({report,Report},#{report_cb:=Fun}=Meta,Config) when is_function(Fun,2) ->
    try Fun(Report,maps:with([depth,chars_limit,single_line],Config)) of
        Chardata when ?IS_STRING(Chardata) ->
            try chardata_to_list(Chardata) % already size limited by report_cb
            catch _:_ ->
                    format_msg({"REPORT_CB/2 ERROR: ~0tp; Returned: ~0tp",
                                [Report,Chardata]},Meta,Config)
            end;
        Other ->
            format_msg({"REPORT_CB/2 ERROR: ~0tp; Returned: ~0tp",
                        [Report,Other]},Meta,Config)
    catch C:R:S ->
            format_msg({"REPORT_CB/2 CRASH: ~0tp; Reason: ~0tp",
                        [Report,{C,R,logger:filter_stacktrace(?MODULE,S)}]},
                       Meta,Config)
    end;
format_msg({report,Report},Meta,Config) ->
    format_msg({report,Report},
               Meta#{report_cb=>fun logger:format_report/1},
               Config);
format_msg(Msg,_Meta,#{depth:=Depth,chars_limit:=CharsLimit}) ->
    Opts = chars_limit_to_opts(CharsLimit),
    do_format_msg(Msg, Depth, Opts).

chars_limit_to_opts(unlimited) -> [];
chars_limit_to_opts(CharsLimit) -> [{chars_limit,CharsLimit}].

do_format_msg({Format0,Args},Depth,Opts) ->
    try
        Format1 = io_lib:scan_format(Format0, Args),
        Format = reformat(Format1, Depth),
        io_lib:build_text(Format,Opts)
    catch C:R:S ->
            FormatError = "FORMAT ERROR: ~0tp - ~0tp",
            case Format0 of
                FormatError ->
                    %% already been here - avoid failing cyclically
                    erlang:raise(C,R,S);
                _ ->
                    format_msg({FormatError,[Format0,Args]},Depth,Opts)
            end
    end.

reformat(Format,unlimited) ->
    Format;
reformat([#{control_char:=C}=M|T], Depth) when C =:= $p ->
    [limit_depth(M#{width => 0}, Depth)|reformat(T, Depth)];
reformat([#{control_char:=C}=M|T], Depth) when C =:= $P ->
    [M#{width => 0}|reformat(T, Depth)];
reformat([#{control_char:=C}=M|T], Depth) when C =:= $p; C =:= $w ->
    [limit_depth(M, Depth)|reformat(T, Depth)];
reformat([H|T], Depth) ->
    [H|reformat(T, Depth)];
reformat([], _) ->
    [].

limit_depth(M0, unlimited) ->
    M0;
limit_depth(#{control_char:=C0, args:=Args}=M0, Depth) ->
    C = C0 - ($a - $A),				%To uppercase.
    M0#{control_char:=C,args:=Args++[Depth]}.

chardata_to_list(Chardata) ->
    case unicode:characters_to_list(Chardata,unicode) of
        List when is_list(List) ->
            List;
        Error ->
            throw(Error)
    end.

truncate(String,unlimited) ->
    String;
truncate(String,Size) ->
    Length = string:length(String),
    if Length>Size ->
            case lists:reverse(lists:flatten(String)) of
                [$\n|_] ->
                    string:slice(String,0,Size-4)++"...\n";
                _ ->
                    string:slice(String,0,Size-3)++"..."
            end;
       true ->
            String
    end.

%% Convert microseconds-timestamp into local datatime string in milliseconds
format_time(SysTime,#{})
  when is_integer(SysTime) ->
    Ms = SysTime rem 1000000 div 1000,
    {Date, _Time = {H, Mi, S}} = calendar:system_time_to_local_time(SysTime, microsecond),
    format_time({Date, {H, Mi, S, Ms}}).
format_time({{Y, M, D}, {H, Mi, S, Ms}}) ->
    io_lib:format("~b-~2..0b-~2..0b ~2..0b:~2..0b:~2..0b.~3..0b", [Y, M, D, H, Mi, S, Ms]);
format_time({{Y, M, D}, {H, Mi, S}}) ->
    io_lib:format("~b-~2..0b-~2..0b ~2..0b:~2..0b:~2..0b", [Y, M, D, H, Mi, S]).

format_mfa({M,F,A},_) when is_atom(M), is_atom(F), is_integer(A) ->
    atom_to_list(M)++":"++atom_to_list(F)++"/"++integer_to_list(A);
format_mfa({M,F,A},Config) when is_atom(M), is_atom(F), is_list(A) ->
    format_mfa({M,F,length(A)},Config);
format_mfa(MFA,Config) ->
    to_string(MFA,Config).

%% Ensure that all valid configuration parameters exist in the final
%% configuration map
add_default_config(Config0) ->
    Default =
        #{chars_limit=>unlimited,
          error_logger_notice_header=>info},
    MaxSize = get_max_size(maps:get(max_size,Config0,undefined)),
    Depth = get_depth(maps:get(depth,Config0,undefined)),
    add_default_template(maps:merge(Default,Config0#{max_size=>MaxSize,
                                                     depth=>Depth})).

add_default_template(#{template:=_}=Config) ->
    Config;
add_default_template(Config) ->
    Config#{template=>?DEFAULT_FORMAT_TEMPLATE_SINGLE}.

get_max_size(undefined) ->
    unlimited;
get_max_size(S) ->
    max(10,S).

get_depth(undefined) ->
    error_logger:get_format_depth();
get_depth(S) ->
    max(5,S).

-spec check_config(Config) -> ok | {error,term()} when
      Config :: config().
check_config(Config) when is_map(Config) ->
    do_check_config(maps:to_list(Config));
check_config(Config) ->
    {error,{invalid_formatter_config,?MODULE,Config}}.

do_check_config([{Type,L}|Config]) when Type == chars_limit;
                                        Type == depth;
                                        Type == max_size ->
    case check_limit(L) of
        ok -> do_check_config(Config);
        error -> {error,{invalid_formatter_config,?MODULE,{Type,L}}}
    end;
do_check_config([{error_logger_notice_header,ELNH}|Config]) when ELNH == info;
                                                                 ELNH == notice ->
    do_check_config(Config);
do_check_config([{report_cb,RCB}|Config]) when is_function(RCB,1);
                                               is_function(RCB,2) ->
    do_check_config(Config);
do_check_config([{template,T}|Config]) ->
    case check_template(T) of
        ok -> do_check_config(Config);
        error -> {error,{invalid_formatter_template,?MODULE,T}}
    end;

do_check_config([C|_]) ->
    {error,{invalid_formatter_config,?MODULE,C}};
do_check_config([]) ->
    ok.

check_limit(L) when is_integer(L), L>0 ->
    ok;
check_limit(unlimited) ->
    ok;
check_limit(_) ->
    error.

check_template([Key|T]) when is_atom(Key) ->
    check_template(T);
check_template([Key|T]) when is_list(Key), is_atom(hd(Key)) ->
    case lists:all(fun(X) when is_atom(X) -> true;
                      (_) -> false
                   end,
                   Key) of
        true ->
            check_template(T);
        false ->
            error
    end;
check_template([{Key,IfExist,Else}|T])
  when is_atom(Key) orelse
       (is_list(Key) andalso is_atom(hd(Key))) ->
    case check_template(IfExist) of
        ok ->
            case check_template(Else) of
                ok ->
                    check_template(T);
                error ->
                    error
            end;
        error ->
            error
    end;
check_template([Str|T]) when is_list(Str) ->
    case io_lib:printable_unicode_list(Str) of
        true -> check_template(T);
        false -> error
    end;
check_template([]) ->
    ok;
check_template(_) ->
    error.
