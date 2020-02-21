%%
%% %CopyrightBegin%
%%
%% Copyright Ericsson AB 2018. All Rights Reserved.
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
-module(emqx_logger_formatter_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("kernel/include/logger.hrl").

-define(TRY(X), my_try(fun() -> X end)).

suite() ->
    [{timetrap,{seconds,30}}].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(Case, Config) ->
    try apply(?MODULE,Case,[cleanup,Config])
    catch error:undef -> ok
    end,
    ok.

groups() ->
    [].

all() ->
    [default,
     single_line,
     template,
     format_msg,
     report_cb,
     max_size,
     depth,
     chars_limit,
     format_mfa,
     level_or_msg_in_meta,
     faulty_log,
     faulty_config,
     faulty_msg,
     check_config,
     update_config].

default(_Config) ->
    String1 = format(info,{"~p",[term]},#{},#{}),
    ct:log(String1),
    ?assertMatch([_Date, _Time,"info:","term\n"], string:lexemes(String1," ")),

    Time = timestamp(),
    ExpectedTimestamp = default_time_format(Time),
    String2 = format(info,{"~p",[term]},#{time=>Time},#{}),
    ct:log("ExpectedTimestamp: ~p, got: ~p", [ExpectedTimestamp, String2]),
    " info: term\n" = string:prefix(String2,ExpectedTimestamp),
    ok.

single_line(_Config) ->
    Time = timestamp(),
    ExpectedTimestamp = default_time_format(Time),
    String1 = format(info,{"~p",[term]},#{time=>Time},#{}),
    ct:log(String1),
    ?assertMatch(" info: term\n", string:prefix(String1,ExpectedTimestamp)),

    String2 = format(info,{"a:~n~p",[term]},#{time=>Time},#{}),
    ct:log(String2),
    ?assertMatch(" info: a:\nterm\n", string:prefix(String2,ExpectedTimestamp)),


    Prefix =
        "Some characters to fill the line ------------------------------------- ",
    %% There would actually be newlines inside the
    %% list and map.
    String4 = format(info,{"~s~p~n~s~p~n",[Prefix,
                                           lists:seq(1,10),
                                           Prefix,
                                           #{a=>map,with=>a,few=>accociations}]},
                     #{time=>Time},
                     #{}),
    ct:log(String4),
    match = re:run(String4,"\\[1,2,3,\n",[global,{capture,none}]),
    {match,Match4} = re:run(String4,"=>\n",[global,{capture,all}]),
    3 = length(Match4),

    %% Test that big metadata fields do not get line breaks
    String5 = format(info,"",
                     #{mymeta=>lists:seq(1,100)},
                     #{template=>[mymeta,"\n"]}),
    ct:log(String5),
    [_] = string:lexemes(String5,"\n"),
    ok.

template(_Config) ->
    Time = timestamp(),

    Template1 = [msg],
    String1 = format(info,{"~p",[term]},#{time=>Time},#{template=>Template1}),
    ct:log(String1),
    "term" = String1,

    Template2 = [msg,unknown],
    String2 = format(info,{"~p",[term]},#{time=>Time},#{template=>Template2}),
    ct:log(String2),
    "term" = String2,

    Template3 = ["string"],
    String3 = format(info,{"~p",[term]},#{time=>Time},#{template=>Template3}),
    ct:log(String3),
    "string" = String3,

    Template4 = ["string\nnewline"],
    String4 = format(info,{"~p",[term]},#{time=>Time},#{template=>Template4}),
    ct:log(String4),
    "string\nnewline" = String4,

    Template5 = [],
    String5 = format(info,{"~p",[term]},#{time=>Time},#{template=>Template5}),
    ct:log(String5),
    "" = String5,

    Ref6 = erlang:make_ref(),
    Meta6 = #{atom=>some_atom,
              integer=>632,
              list=>[list,"string",4321,#{},{tuple}],
              mfa=>{mod,func,0},
              pid=>self(),
              ref=>Ref6,
              string=>"some string",
              time=>Time,
              tuple=>{1,atom,"list"},
              nested=>#{subkey=>subvalue}},
    Template6 = lists:join(";",lists:sort(maps:keys(maps:remove(nested,Meta6))) ++
                               [[nested,subkey]]),
    String6 = format(info,{"~p",[term]},Meta6,#{template=>Template6}),
    ct:log(String6),
    SelfStr = pid_to_list(self()),
    RefStr6 = ref_to_list(Ref6),
    ListStr = "[list,\"string\",4321,#{},{tuple}]",
    ExpectedTime6 = default_time_format(Time),
    ["some_atom",
     "632",
     ListStr,
     "mod:func/0",
     SelfStr,
     RefStr6,
     "some string",
     ExpectedTime6,
     "{1,atom,\"list\"}",
     "subvalue"] = string:lexemes(String6,";"),

    Meta7 = #{time=>Time,
              nested=>#{key1=>#{subkey1=>value1},
                        key2=>value2}},
    Template7 = lists:join(";",[nested,
                                [nested,key1],
                                [nested,key1,subkey1],
                                [nested,key2],
                                [nested,key2,subkey2],
                                [nested,key3],
                                [nested,key3,subkey3]]),
    String7 = format(info,{"~p",[term]},Meta7,#{template=>Template7}),
    ct:log(String7),
    [MultipleKeysStr7,
     "#{subkey1 => value1}",
     "value1",
     "value2",
     "",
     "",
     ""] = string:split(String7,";",all),
    %% Order of keys is not fixed
    case MultipleKeysStr7 of
        "#{key2 => value2,key1 => #{subkey1 => value1}}" -> ok;
        "#{key1 => #{subkey1 => value1},key2 => value2}" -> ok;
        _ -> ct:fail({full_nested_map_unexpected,MultipleKeysStr7})
    end,

    Meta8 = #{time=>Time,
              nested=>#{key1=>#{subkey1=>value1},
                        key2=>value2}},
    Template8 =
        lists:join(
          ";",
          [{nested,["exist:",nested],["noexist"]},
           {[nested,key1],["exist:",[nested,key1]],["noexist"]},
           {[nested,key1,subkey1],["exist:",[nested,key1,subkey1]],["noexist"]},
           {[nested,key2],["exist:",[nested,key2]],["noexist"]},
           {[nested,key2,subkey2],["exist:",[nested,key2,subkey2]],["noexist"]},
           {[nested,key3],["exist:",[nested,key3]],["noexist"]},
           {[nested,key3,subkey3],["exist:",[nested,key3,subkey3]],["noexist"]}]),
    String8 = format(info,{"~p",[term]},Meta8,#{template=>Template8}),
    ct:log(String8),
    [MultipleKeysStr8,
     "exist:#{subkey1 => value1}",
     "exist:value1",
     "exist:value2",
     "noexist",
     "noexist",
     "noexist"] = string:split(String8,";",all),
    %% Order of keys is not fixed
    case MultipleKeysStr8 of
        "exist:#{key2 => value2,key1 => #{subkey1 => value1}}" -> ok;
        "exist:#{key1 => #{subkey1 => value1},key2 => value2}" -> ok;
        _ -> ct:fail({full_nested_map_unexpected,MultipleKeysStr8})
    end,

    ok.

format_msg(_Config) ->
    Template = [msg],

    String1 = format(info,{"~p",[term]},#{},#{template=>Template}),
    ct:log(String1),
    "term" = String1,

    String2 = format(info,{"list",[term]},#{},#{template=>Template}),
    ct:log(String2),
    "FORMAT ERROR: \"list\" - [term]" = String2,

    String3 = format(info,{report,term},#{},#{template=>Template}),
    ct:log(String3),
    "term" = String3,

    String4 = format(info,{report,term},
                     #{report_cb=>fun(_)-> {"formatted",[]} end},
                     #{template=>Template}),
    ct:log(String4),
    "formatted" = String4,

    String5 = format(info,{report,term},
                     #{report_cb=>fun(_)-> faulty_return end},
                     #{template=>Template}),
    ct:log(String5),
    "REPORT_CB/1 ERROR: term; Returned: faulty_return" = String5,

    String6 = format(info,{report,term},
                     #{report_cb=>fun(_)-> erlang:error(fun_crashed) end},
                     #{template=>Template}),
    ct:log(String6),
    "REPORT_CB/1 CRASH: term; Reason: {error,fun_crashed,"++_ = String6,

    String7 = format(info,{report,term},
                     #{report_cb=>fun(_,_)-> ['not',a,string] end},
                     #{template=>Template}),
    ct:log(String7),
    "REPORT_CB/2 ERROR: term; Returned: ['not',a,string]" = String7,

    String8 = format(info,{report,term},
                     #{report_cb=>fun(_,_)-> faulty_return end},
                     #{template=>Template}),
    ct:log(String8),
    "REPORT_CB/2 ERROR: term; Returned: faulty_return" = String8,

    String9 = format(info,{report,term},
                     #{report_cb=>fun(_,_)-> erlang:error(fun_crashed) end},
                     #{template=>Template}),
    ct:log(String9),
    "REPORT_CB/2 CRASH: term; Reason: {error,fun_crashed,"++_ = String9,

    %% strings are not formatted
    String10 = format(info,{string,"string"},
                     #{report_cb=>fun(_)-> {"formatted",[]} end},
                     #{template=>Template}),
    ct:log(String10),
    "string" = String10,

    String11 = format(info,{string,['not',printable,list]},
                     #{report_cb=>fun(_)-> {"formatted",[]} end},
                     #{template=>Template}),
    ct:log("~ts",[String11]), % avoiding ct_log crash
    "FORMAT ERROR: \"~ts\" - [['not',printable,list]]" = String11,

    String12 = format(info,{string,"string"},#{},#{template=>Template}),
    ct:log(String12),
    "string" = String12,

    ok.

report_cb(_Config) ->
    Template = [msg],
    MetaFun = fun(_) -> {"meta_rcb",[]} end,
    ConfigFun = fun(_) -> {"config_rcb",[]} end,
    "term" = format(info,{report,term},#{},#{template=>Template}),
    "meta_rcb" =
        format(info,{report,term},#{report_cb=>MetaFun},#{template=>Template}),
    "config_rcb" =
        format(info,{report,term},#{},#{template=>Template,
                                        report_cb=>ConfigFun}),
    "config_rcb" =
        format(info,{report,term},#{report_cb=>MetaFun},#{template=>Template,
                                                          report_cb=>ConfigFun}),
    ok.

max_size(_Config) ->
    Cfg = #{template=>[msg]},
    "12345678901234567890" = format(info,{"12345678901234567890",[]},#{},Cfg),
    %% application:set_env(kernel,logger_max_size,11),
    %% "12345678901234567890" = % min value is 50, so this is not limited
    %%     format(info,{"12345678901234567890",[]},#{},Cfg),
    %% "12345678901234567890123456789012345678901234567..." = % 50
    %%     format(info,
    %%            {"123456789012345678901234567890123456789012345678901234567890",
    %%             []},
    %%            #{},
    %%            Cfg),
    %% application:set_env(kernel,logger_max_size,53),
    %% "12345678901234567890123456789012345678901234567890..." = %53
    %%     format(info,
    %%            {"123456789012345678901234567890123456789012345678901234567890",
    %%             []},
    %%            #{},
    %%            Cfg),
    "123456789012..." =
        format(info,{"12345678901234567890",[]},#{},Cfg#{max_size=>15}),
    "12345678901234567890" =
        format(info,{"12345678901234567890",[]},#{},Cfg#{max_size=>unlimited}),
    %% Check that one newline at the end of the line is kept (if it exists)
    "12345678901...\n" =
        format(info,{"12345678901234567890\n",[]},#{},Cfg#{max_size=>15}),
    "12345678901...\n" =
        format(info,{"12345678901234567890",[]},#{},Cfg#{template=>[msg,"\n"],
                                                         max_size=>15}),
    ok.
max_size(cleanup,_Config) ->
    application:unset_env(kernel,logger_max_size),
    ok.

depth(_Config) ->
    Template = [msg],
    "[1,2,3,4,5,6,7,8,9,0,1,2,3,4,5,6,7,8,9,0]" =
        format(info,
               {"~p",[[1,2,3,4,5,6,7,8,9,0,1,2,3,4,5,6,7,8,9,0]]},
               #{},
               #{template=>Template}),
    application:set_env(kernel,error_logger_format_depth,11),
    "[1,2,3,4,5,6,7,8,9,0|...]" =
        format(info,
               {"~p",[[1,2,3,4,5,6,7,8,9,0,1,2,3,4,5,6,7,8,9,0]]},
               #{},
               #{template=>Template}),
    "[1,2,3,4,5,6,7,8,9,0,1,2|...]" =
        format(info,
               {"~p",[[1,2,3,4,5,6,7,8,9,0,1,2,3,4,5,6,7,8,9,0]]},
               #{},
               #{template=>Template,
                 depth=>13}),
    "[1,2,3,4,5,6,7,8,9,0,1,2,3,4,5,6,7,8,9,0]" =
        format(info,
               {"~p",[[1,2,3,4,5,6,7,8,9,0,1,2,3,4,5,6,7,8,9,0]]},
               #{},
               #{template=>Template,
                 depth=>unlimited}),
    ok.
depth(cleanup,_Config) ->
    application:unset_env(kernel,error_logger_format_depth),
    ok.

chars_limit(_Config) ->
    FA = {"LoL: ~p~nL: ~p~nMap: ~p~n",
                [lists:duplicate(10,lists:seq(1,100)),
                 lists:seq(1,100),
                 maps:from_list(lists:zip(lists:seq(1,100),
                                          lists:duplicate(100,value)))]},
    Meta = #{time=>timestamp()},
    Template = [time," - ", msg, "\n"],
    FC = #{template=>Template,
           depth=>unlimited,
           max_size=>unlimited,
           chars_limit=>unlimited},
    CL1 = 80,
    String1 = format(info,FA,Meta,FC#{chars_limit=>CL1}),
    L1 = string:length(String1),
    ct:log("String1: ~p~nLength1: ~p~n",[lists:flatten(String1),L1]),
    true = L1 > CL1,
    true = L1 < CL1 + 15,

    String2 = format(info,FA,Meta,FC#{chars_limit=>CL1,depth=>10}),
    L2 = string:length(String2),
    ct:log("String2: ~p~nLength2: ~p~n",[lists:flatten(String2),L2]),
    String2 = String1,

    CL3 = 200,
    String3 = format(info,FA,Meta,FC#{chars_limit=>CL3}),
    L3 = string:length(String3),
    ct:log("String3: ~p~nLength3: ~p~n",[lists:flatten(String3),L3]),
    true = L3 > CL3,
    true = L3 < CL3 + 15,

    String4 = format(info,FA,Meta,FC#{chars_limit=>CL3,depth=>10}),
    L4 = string:length(String4),
    ct:log("String4: ~p~nLength4: ~p~n",[lists:flatten(String4),L4]),
    true = L4 > CL3,
    true = L4 < CL3 + 15,

    %% Test that max_size truncates the string which is limited by
    %% depth and chars_limit
    MS5 = 150,
    String5 = format(info,FA,Meta,FC#{chars_limit=>CL3,depth=>10,max_size=>MS5}),
    L5 = string:length(String5),
    ct:log("String5: ~p~nLength5: ~p~n",[String5,L5]),
    L5 = MS5,
    true = lists:prefix(lists:sublist(String5,L5-4),String4),

    %% Test that chars_limit limits string also
    Str = "123456789012345678901234567890123456789012345678901234567890123456789",
    CL6 = 80,
    String6 = format(info,{string,Str},Meta,FC#{chars_limit=>CL6}),
    L6 = string:length(String6),
    ct:log("String6: ~p~nLength6: ~p~n",[String6,L6]),
    L6 = CL6,

    ok.

format_mfa(_Config) ->
    Template = [mfa],

    Meta1 = #{mfa=>{mod,func,0}},
    String1 = format(info,{"~p",[term]},Meta1,#{template=>Template}),
    ct:log(String1),
    "mod:func/0" = String1,

    Meta2 = #{mfa=>{mod,func,[]}},
    String2 = format(info,{"~p",[term]},Meta2,#{template=>Template}),
    ct:log(String2),
    "mod:func/0" = String2,

    Meta3 = #{mfa=>"mod:func/0"},
    String3 = format(info,{"~p",[term]},Meta3,#{template=>Template}),
    ct:log(String3),
    "mod:func/0" = String3,

    Meta4 = #{mfa=>othermfa},
    String4 = format(info,{"~p",[term]},Meta4,#{template=>Template}),
    ct:log(String4),
    "othermfa" = String4,

    ok.

level_or_msg_in_meta(_Config) ->
    %% The template contains atoms to pick out values from meta,
    %% or level/msg to add these from the log event. What if you have
    %% a key named 'level' or 'msg' in meta and want to display
    %% its value?
    %% For now we simply ignore Meta on this and display the
    %% actual level and msg from the log event.

    Meta = #{level=>mylevel,
                 msg=>"metamsg"},
    Template = [level,";",msg],
    String = format(info,{"~p",[term]},Meta,#{template=>Template}),
    ct:log(String),
    "info;term" = String, % so mylevel and "metamsg" are ignored
    ok.

faulty_log(_Config) ->
    %% Unexpected log (should be type logger:log_event()) - print error
    {error,
     function_clause,
     {emqx_logger_formatter,format,[_,_],_}} =
        ?TRY(emqx_logger_formatter:format(unexp_log,#{})),
    ok.

faulty_config(_Config) ->
    {error,
     function_clause,
     {emqx_logger_formatter,format,[_,_],_}} =
        ?TRY(emqx_logger_formatter:format(#{level=>info,
                                       msg=>{"~p",[term]},
                                       meta=>#{time=>timestamp()}},
                                     unexp_config)),
    ok.

faulty_msg(_Config) ->
    {error,
     function_clause,
     {emqx_logger_formatter,_,_,_}} =
        ?TRY(emqx_logger_formatter:format(#{level=>info,
                                       msg=>term,
                                       meta=>#{time=>timestamp()}},
                                     #{})),
    ok.

-define(cfgerr(X), {error,{invalid_formatter_config,emqx_logger_formatter,X}}).
check_config(_Config) ->
    ok = emqx_logger_formatter:check_config(#{}),
    ?cfgerr(bad) = emqx_logger_formatter:check_config(bad),

    C1 = #{chars_limit => 1,
           depth => 1,
           max_size => 1,
           report_cb => fun(R) -> {"~p",[R]} end,
           template => []},
    ok = emqx_logger_formatter:check_config(C1),

    ok = emqx_logger_formatter:check_config(#{chars_limit => unlimited}),
    ?cfgerr({chars_limit,bad}) =
        emqx_logger_formatter:check_config(#{chars_limit => bad}),

    ok = emqx_logger_formatter:check_config(#{depth => unlimited}),
    ?cfgerr({depth,bad}) =
        emqx_logger_formatter:check_config(#{depth => bad}),

    ok = emqx_logger_formatter:check_config(#{max_size => unlimited}),
    ?cfgerr({max_size,bad}) =
        emqx_logger_formatter:check_config(#{max_size => bad}),

    ok =
        emqx_logger_formatter:check_config(#{report_cb => fun(_,_) -> "" end}),
    ?cfgerr({report_cb,F}) =
        emqx_logger_formatter:check_config(#{report_cb => F=fun(_,_,_) -> {"",[]} end}),
    ?cfgerr({report_cb,bad}) =
        emqx_logger_formatter:check_config(#{report_cb => bad}),

    Ts = [[key],
          [[key1,key2]],
          [{key,[key],[]}],
          [{[key1,key2],[[key1,key2]],["noexist"]}],
          ["string"]],
    [begin
         ct:log("check template: ~p",[T]),
         ok = emqx_logger_formatter:check_config(#{template => T})
     end
     || T <- Ts],

    ETs = [bad,
           [{key,bad}],
           [{key,[key],bad}],
           [{key,[key],"bad"}],
           "bad",
           [[key,$a,$b,$c]],
           [[$a,$b,$c,key]]],
    [begin
         ct:log("check template: ~p",[T]),
         {error,{invalid_formatter_template,emqx_logger_formatter,T}} =
         emqx_logger_formatter:check_config(#{template => T})
     end
     || T <- ETs],
    ok.

%% Test that formatter config can be changed, and that the default
%% template is updated accordingly
update_config(_Config) ->
    #{level := OldLevel} = logger:get_primary_config(),
    logger:set_primary_config(level, debug),
    {error,{not_found,?MODULE}} = logger:update_formatter_config(?MODULE,#{}),

    logger:add_handler_filter(default,silence,{fun(_,_) -> stop end,ok}),
    ok = logger:add_handler(?MODULE,?MODULE,#{formatter => {emqx_logger_formatter, #{chars_limit => unlimited}},
                                              config => #{type => standard_io}}),
    D = lists:seq(1,1000),
    logger:notice("~p~n",[D]),
    {Lines1,C1} = check_log(),
    ct:log("lines1: ~p", [Lines1]),
    ct:log("c1: ~p",[C1]),
    [Line1 | _] = Lines1,
    [_Date, WithOutDate1] = string:split(Line1," "),
    [_, "notice: "++D1] = string:split(WithOutDate1," "),
    ?assert(length(D1)<1000),
    ?assertMatch(#{chars_limit := unlimited}, C1),

    error_logger:error_msg("~p",[D]),
    {Lines5,C5} = check_log(),
    ct:log("Lines5: ~p", [Lines5]),
    ct:log("c5: ~p",[C5]),
    [Line5 | _] = Lines5,
    [_Date, WithOutDate5] = string:split(Line5," "),
    [_, "error: "++_D5] = string:split(WithOutDate5," "),

    ?assertMatch({error,{invalid_formatter_config,bad}},
        logger:update_formatter_config(?MODULE,bad)),
    ?assertMatch({error,{invalid_formatter_config,emqx_logger_formatter,{depth,bad}}},
        logger:update_formatter_config(?MODULE,depth,bad)),

    logger:set_primary_config(level, OldLevel),
    ok.

update_config(cleanup,_Config) ->
    _ = logger:remove_handler(?MODULE),
    _ = logger:remove_handler_filter(default,silence),
    ok.

%%%-----------------------------------------------------------------
%%% Internal
format(Level,Msg,Meta,Config) ->
    format(#{level=>Level,msg=>Msg,meta=>add_time(Meta)},Config).

format(Log,Config) ->
    lists:flatten(emqx_logger_formatter:format(Log,Config)).

default_time_format(SysTime) when is_integer(SysTime) ->
    Ms = SysTime rem 1000000 div 1000,
    {Date, _Time = {H, Mi, S}} = calendar:system_time_to_local_time(SysTime, microsecond),
    default_time_format({Date, {H, Mi, S, Ms}});
default_time_format({{Y, M, D}, {H, Mi, S, Ms}}) ->
    io_lib:format("~b-~2..0b-~2..0b ~2..0b:~2..0b:~2..0b.~3..0b", [Y, M, D, H, Mi, S, Ms]);
default_time_format({{Y, M, D}, {H, Mi, S}}) ->
    io_lib:format("~b-~2..0b-~2..0b ~2..0b:~2..0b:~2..0b", [Y, M, D, H, Mi, S]).

integer(Str) ->
    is_integer(list_to_integer(Str)).
integer(Str,Max) ->
    integer(Str,0,Max).
integer(Str,Min,Max) ->
    Int = list_to_integer(Str),
    Int >= Min andalso Int =<Max.

%%%-----------------------------------------------------------------
%%% Called by macro ?TRY(X)
my_try(Fun) ->
    try Fun() catch C:R:S -> {C,R,hd(S)} end.

timestamp() ->
    erlang:system_time(microsecond).

%% necessary?
add_time(#{time:=_}=Meta) ->
    Meta;
add_time(Meta) ->
    Meta#{time=>timestamp()}.

%%%-----------------------------------------------------------------
%%% handler callback
log(Log,#{formatter:={M,C}}) ->
    put(log,{M:format(Log,C),C}),
    ok.

check_log() ->
    {S,C} = erase(log),
    {string:lexemes(S,"\n"),C}.
