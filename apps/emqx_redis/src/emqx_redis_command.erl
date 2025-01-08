%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc `split/1` function reimplements the one used by Redis itself for `redis-cli`.
%% See `sdssplitargs` function, https://github.com/redis/redis/blob/unstable/src/sds.c.

-module(emqx_redis_command).

-export([split/1]).

-define(CH_SPACE, 32).
-define(CH_N, 10).
-define(CH_R, 13).
-define(CH_T, 9).
-define(CH_B, 8).
-define(CH_A, 11).

-define(IS_CH_HEX_DIGIT(C),
    (((C >= $a) andalso (C =< $f)) orelse
        ((C >= $A) andalso (C =< $F)) orelse
        ((C >= $0) andalso (C =< $9)))
).
-define(IS_CH_SPACE(C),
    (C =:= ?CH_SPACE orelse
        C =:= ?CH_N orelse
        C =:= ?CH_R orelse
        C =:= ?CH_T orelse
        C =:= ?CH_B orelse
        C =:= ?CH_A)
).

split(Line) when is_binary(Line) ->
    case split(binary_to_list(Line)) of
        {ok, Args} ->
            {ok, [list_to_binary(Arg) || Arg <- Args]};
        {error, _} = Error ->
            Error
    end;
split(Line) ->
    split(Line, []).

split([], Acc) ->
    {ok, lists:reverse(Acc)};
split([C | Rest] = Line, Acc) ->
    case ?IS_CH_SPACE(C) of
        true -> split(Rest, Acc);
        false -> split_noq([], Line, Acc)
    end.

hex_digit_to_int(C) when (C >= $a) andalso (C =< $f) -> 10 + C - $a;
hex_digit_to_int(C) when (C >= $A) andalso (C =< $F) -> 10 + C - $A;
hex_digit_to_int(C) when (C >= $0) andalso (C =< $9) -> C - $0.

maybe_special_char($n) -> ?CH_N;
maybe_special_char($r) -> ?CH_R;
maybe_special_char($t) -> ?CH_T;
maybe_special_char($b) -> ?CH_B;
maybe_special_char($a) -> ?CH_A;
maybe_special_char(C) -> C.

%% Inside double quotes
split_inq(CurAcc, Line, Acc) ->
    case Line of
        [$\\, $x, HD1, HD2 | LineRest] when ?IS_CH_HEX_DIGIT(HD1) andalso ?IS_CH_HEX_DIGIT(HD2) ->
            C = hex_digit_to_int(HD1) * 16 + hex_digit_to_int(HD2),
            NewCurAcc = [C | CurAcc],
            split_inq(NewCurAcc, LineRest, Acc);
        [$\\, SC | LineRest] ->
            C = maybe_special_char(SC),
            NewCurAcc = [C | CurAcc],
            split_inq(NewCurAcc, LineRest, Acc);
        [$", C | _] when not ?IS_CH_SPACE(C) ->
            {error, trailing_after_quote};
        [$" | LineRest] ->
            split(LineRest, [lists:reverse(CurAcc) | Acc]);
        [] ->
            {error, unterminated_quote};
        [C | LineRest] ->
            NewCurAcc = [C | CurAcc],
            split_inq(NewCurAcc, LineRest, Acc)
    end.

%% Inside single quotes
split_insq(CurAcc, Line, Acc) ->
    case Line of
        [$\\, $' | LineRest] ->
            NewCurAcc = [$' | CurAcc],
            split_insq(NewCurAcc, LineRest, Acc);
        [$', C | _] when not ?IS_CH_SPACE(C) ->
            {error, trailing_after_single_quote};
        [$' | LineRest] ->
            split(LineRest, [lists:reverse(CurAcc) | Acc]);
        [] ->
            {error, unterminated_single_quote};
        [C | LineRest] ->
            NewCurAcc = [C | CurAcc],
            split_insq(NewCurAcc, LineRest, Acc)
    end.

%% Outside quotes
split_noq(CurAcc, Line, Acc) ->
    case Line of
        [C | LineRest] when
            ?IS_CH_SPACE(C); C =:= ?CH_N; C =:= ?CH_R; C =:= ?CH_T
        ->
            split(LineRest, [lists:reverse(CurAcc) | Acc]);
        [] ->
            split([], [lists:reverse(CurAcc) | Acc]);
        [$' | LineRest] ->
            split_insq(CurAcc, LineRest, Acc);
        [$" | LineRest] ->
            split_inq(CurAcc, LineRest, Acc);
        [C | LineRest] ->
            NewCurAcc = [C | CurAcc],
            split_noq(NewCurAcc, LineRest, Acc)
    end.
