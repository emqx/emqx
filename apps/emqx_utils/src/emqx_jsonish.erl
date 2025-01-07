%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_jsonish).

-behaviour(emqx_template).
-export([lookup/2]).

-export_type([t/0]).

%% @doc Either a map or a JSON serial.
%% Think of it as a kind of lazily parsed and/or constructed JSON.
-type t() :: propmap() | serial().

%% @doc JSON in serialized form.
-type serial() :: binary().

-type propmap() :: #{prop() => value()}.
-type prop() :: atom() | binary().
-type value() :: scalar() | [scalar() | propmap()] | t().
-type scalar() :: atom() | unicode:chardata() | number().

%%

%% @doc Lookup a value in the JSON-ish map accessible through the given accessor.
%% If accessor implies drilling down into a binary, it will be treated as JSON serial.
%% Failure to parse the binary as JSON will result in an _invalid type_ error.
%% Nested JSON is NOT parsed recursively.
-spec lookup(emqx_template:accessor(), t()) ->
    {ok, value()}
    | {error, undefined | {_Location :: non_neg_integer(), _InvalidType :: atom()}}.
lookup(Var, Jsonish) ->
    lookup(0, _Decoded = false, Var, Jsonish).

lookup(_, _, [], Value) ->
    {ok, Value};
lookup(Loc, Decoded, [Prop | Rest], Jsonish) when is_map(Jsonish) ->
    case emqx_template:lookup(Prop, Jsonish) of
        {ok, Value} ->
            lookup(Loc + 1, Decoded, Rest, Value);
        {error, Reason} ->
            {error, Reason}
    end;
lookup(Loc, _Decoded = false, Props, Json) when is_binary(Json) ->
    try emqx_utils_json:decode(Json) of
        Value ->
            % NOTE: This is intentional, we don't want to parse nested JSON.
            lookup(Loc, true, Props, Value)
    catch
        error:_ ->
            {error, {Loc, binary}}
    end;
lookup(Loc, _, _, Invalid) ->
    {error, {Loc, type_name(Invalid)}}.

type_name(Term) when is_atom(Term) -> atom;
type_name(Term) when is_number(Term) -> number;
type_name(Term) when is_binary(Term) -> binary;
type_name(Term) when is_list(Term) -> list.
