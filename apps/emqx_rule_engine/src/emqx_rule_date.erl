%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_rule_date).

-export([date/3, date/4, parse_date/4]).

-export([
    is_int_char/1,
    is_symbol_char/1,
    is_m_char/1
]).

-record(result, {
    %%year()
    year = "1970" :: string(),
    %%month()
    month = "1" :: string(),
    %%day()
    day = "1" :: string(),
    %%hour()
    hour = "0" :: string(),
    %%minute() %% epoch in millisecond precision
    minute = "0" :: string(),
    %%second() %% epoch in millisecond precision
    second = "0" :: string(),
    %%integer() %% zone maybe some value
    zone = "+00:00" :: string()
}).

%% -type time_unit() :: 'microsecond'
%%                    | 'millisecond'
%%                    | 'nanosecond'
%%                    | 'second'.
%% -type offset() :: [byte()] | (Time :: integer()).
date(TimeUnit, Offset, FormatString) ->
    date(TimeUnit, Offset, FormatString, erlang:system_time(TimeUnit)).

date(TimeUnit, Offset, FormatString, TimeEpoch) ->
    [Head | Other] = string:split(FormatString, "%", all),
    R = create_tag([{st, Head}], Other),
    Res = lists:map(
        fun(Expr) ->
            eval_tag(rmap(make_time(TimeUnit, Offset, TimeEpoch)), Expr)
        end,
        R
    ),
    lists:concat(Res).

parse_date(TimeUnit, Offset, FormatString, InputString) ->
    [Head | Other] = string:split(FormatString, "%", all),
    R = create_tag([{st, Head}], Other),
    IsZ = fun(V) ->
        case V of
            {tag, $Z} -> true;
            _ -> false
        end
    end,
    R1 = lists:filter(IsZ, R),
    IfFun = fun(Con, A, B) ->
        case Con of
            [] -> A;
            _ -> B
        end
    end,
    Res = parse_input(FormatString, InputString),
    Str =
        Res#result.year ++ "-" ++
            Res#result.month ++ "-" ++
            Res#result.day ++ "T" ++
            Res#result.hour ++ ":" ++
            Res#result.minute ++ ":" ++
            Res#result.second ++
            IfFun(R1, Offset, Res#result.zone),
    calendar:rfc3339_to_system_time(Str, [{unit, TimeUnit}]).

mlist(R) ->
    %% %H	Shows hour in 24-hour format [15]
    [
        {$H, R#result.hour},
        %% %M	Displays minutes [00-59]
        {$M, R#result.minute},
        %% %S	Displays seconds [00-59]
        {$S, R#result.second},
        %% %y	Displays year YYYY [2021]
        {$y, R#result.year},
        %% %m	Displays the number of the month [01-12]
        {$m, R#result.month},
        %% %d	Displays the number of the month [01-12]
        {$d, R#result.day},
        %% %Z	Displays Time zone
        {$Z, R#result.zone}
    ].

rmap(Result) ->
    maps:from_list(mlist(Result)).

support_char() -> "HMSymdZ".

create_tag(Head, []) ->
    Head;
create_tag(Head, [Val1 | RVal]) ->
    case Val1 of
        [] ->
            create_tag(Head ++ [{st, [$%]}], RVal);
        [H | Other] ->
            case lists:member(H, support_char()) of
                true -> create_tag(Head ++ [{tag, H}, {st, Other}], RVal);
                false -> create_tag(Head ++ [{st, [$% | Val1]}], RVal)
            end
    end.

eval_tag(_, {st, Str}) ->
    Str;
eval_tag(Map, {tag, Char}) ->
    maps:get(Char, Map, "undefined").

%% make_time(TimeUnit, Offset) ->
%%     make_time(TimeUnit, Offset, erlang:system_time(TimeUnit)).
make_time(TimeUnit, Offset, TimeEpoch) ->
    Res = calendar:system_time_to_rfc3339(TimeEpoch,
                                          [{unit, TimeUnit}, {offset, Offset}]),
    [Y1, Y2, Y3, Y4, $-, Mon1, Mon2, $-, D1, D2, _T,
     H1, H2, $:, Min1, Min2, $:, S1, S2 | TimeStr] = Res,
    IsFractionChar = fun(C) -> C >= $0 andalso C =< $9 orelse C =:= $. end,
    {FractionStr, UtcOffset} = lists:splitwith(IsFractionChar, TimeStr),
    #result{
       year = [Y1, Y2, Y3, Y4]
      , month = [Mon1, Mon2]
      , day = [D1, D2]
      , hour = [H1, H2]
      , minute = [Min1, Min2]
      , second = [S1, S2] ++ FractionStr
      , zone = UtcOffset
      }.

is_int_char(C) ->
    C >= $0 andalso C =< $9.
is_symbol_char(C) ->
    C =:= $- orelse C =:= $+.
is_m_char(C) ->
    C =:= $:.

parse_char_with_fun(_, []) ->
    error(null_input);
parse_char_with_fun(ValidFun, [C | Other]) ->
    Res =
        case erlang:is_function(ValidFun) of
            true -> ValidFun(C);
            false -> erlang:apply(emqx_rule_date, ValidFun, [C])
        end,
    case Res of
        true -> {C, Other};
        false -> error({unexpected, [C | Other]})
    end.
parse_string([], Input) ->
    {[], Input};
parse_string([C | Other], Input) ->
    {C1, Input1} = parse_char_with_fun(fun(V) -> V =:= C end, Input),
    {Res, Input2} = parse_string(Other, Input1),
    {[C1 | Res], Input2}.

parse_times(0, _, Input) ->
    {[], Input};
parse_times(Times, Fun, Input) ->
    {C1, Input1} = parse_char_with_fun(Fun, Input),
    {Res, Input2} = parse_times((Times - 1), Fun, Input1),
    {[C1 | Res], Input2}.

parse_int_times(Times, Input) ->
    parse_times(Times, is_int_char, Input).

parse_fraction(Input) ->
    IsFractionChar = fun(C) -> C >= $0 andalso C =< $9 orelse C =:= $. end,
    lists:splitwith(IsFractionChar, Input).

parse_second(Input) ->
    {M, Input1} = parse_int_times(2, Input),
    {M1, Input2} = parse_fraction(Input1),
    {M ++ M1, Input2}.

parse_zone(Input) ->
    {S, Input1} = parse_char_with_fun(is_symbol_char, Input),
    {M, Input2} = parse_int_times(2, Input1),
    {C, Input3} = parse_char_with_fun(is_m_char, Input2),
    {V, Input4} = parse_int_times(2, Input3),
    {[S | M ++ [C | V]], Input4}.

mlist1() ->
    maps:from_list(
        %% %H	Shows hour in 24-hour format [15]
        [
            {$H, fun(Input) -> parse_int_times(2, Input) end},
            %% %M	Displays minutes [00-59]
            {$M, fun(Input) -> parse_int_times(2, Input) end},
            %% %S	Displays seconds [00-59]
            {$S, fun(Input) -> parse_second(Input) end},
            %% %y	Displays year YYYY [2021]
            {$y, fun(Input) -> parse_int_times(4, Input) end},
            %% %m	Displays the number of the month [01-12]
            {$m, fun(Input) -> parse_int_times(2, Input) end},
            %% %d	Displays the number of the month [01-12]
            {$d, fun(Input) -> parse_int_times(2, Input) end},
            %% %Z	Displays Time zone
            {$Z, fun(Input) -> parse_zone(Input) end}
        ]
    ).

update_result($H, Res, Str) -> Res#result{hour = Str};
update_result($M, Res, Str) -> Res#result{minute = Str};
update_result($S, Res, Str) -> Res#result{second = Str};
update_result($y, Res, Str) -> Res#result{year = Str};
update_result($m, Res, Str) -> Res#result{month = Str};
update_result($d, Res, Str) -> Res#result{day = Str};
update_result($Z, Res, Str) -> Res#result{zone = Str}.

parse_tag(Res, {st, St}, InputString) ->
    {_A, B} = parse_string(St, InputString),
    {Res, B};
parse_tag(Res, {tag, St}, InputString) ->
    Fun = maps:get(St, mlist1()),
    {A, B} = Fun(InputString),
    NRes = update_result(St, Res, A),
    {NRes, B}.

parse_tags(Res, [], _) ->
    Res;
parse_tags(Res, [Tag | Others], InputString) ->
    {NRes, B} = parse_tag(Res, Tag, InputString),
    parse_tags(NRes, Others, B).

parse_input(FormatString, InputString) ->
    [Head | Other] = string:split(FormatString, "%", all),
    R = create_tag([{st, Head}], Other),
    parse_tags(#result{}, R, InputString).
