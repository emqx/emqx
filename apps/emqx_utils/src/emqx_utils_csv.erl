%%--------------------------------------------------------------------
%% Copyright (c) 2017-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_utils_csv).

-export([escape_field/1]).

-type csv_field() :: binary() | string().

%% @doc Escape a field for CSV output.
%% Handles commas, quotes, and newlines by wrapping in quotes and escaping quotes.
%% Optimized to avoid unnecessary string conversion for most common cases.
-spec escape_field(Field) -> EscapedField when
    Field :: csv_field(),
    EscapedField :: binary() | string().
escape_field(ClientId) when is_binary(ClientId) ->
    case binary:match(ClientId, [<<$,>>, <<$">>, <<$\n>>, <<$\r>>]) of
        nomatch ->
            % No special characters, return as binary
            ClientId;
        _ ->
            % Contains special characters, convert to list and escape
            Field = unicode:characters_to_list(ClientId, utf8),
            unicode:characters_to_binary(escape_field(Field), utf8)
    end;
escape_field(Field) when is_list(Field) ->
    case
        lists:any(fun(C) -> C =:= $, orelse C =:= $" orelse C =:= $\n orelse C =:= $\r end, Field)
    of
        true ->
            do_escape_field(Field);
        false ->
            Field
    end.

%% @private
%% @doc Internal function to perform the actual CSV escaping.
-spec do_escape_field(Field) -> EscapedField when
    Field :: string(),
    EscapedField :: string().
do_escape_field(Field) ->
    % Escape quotes by doubling them and wrap in quotes
    Escaped = lists:foldr(
        fun
            ($", Acc) -> [$", $" | Acc];
            (C, Acc) -> [C | Acc]
        end,
        [],
        Field
    ),
    [$" | Escaped] ++ [$"].
