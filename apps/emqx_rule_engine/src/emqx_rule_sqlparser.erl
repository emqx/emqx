%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_rule_sqlparser).

-include("rule_engine.hrl").

-export([parse/1]).

-export([
    select_fields/1,
    select_is_foreach/1,
    select_doeach/1,
    select_incase/1,
    select_from/1,
    select_where/1
]).

-import(proplists, [
    get_value/2,
    get_value/3
]).

-record(select, {fields, from, where, is_foreach, doeach, incase}).

-opaque select() :: #select{}.

-type const() :: {const, number() | binary()}.

-type variable() :: binary() | list(binary()).

-type alias() :: binary() | list(binary()).

-type field() ::
    const()
    | variable()
    | {as, field(), alias()}
    | {'fun', atom(), list(field())}.

-export_type([select/0]).

%% Parse one select statement.
-spec parse(string() | binary()) -> {ok, select()} | {error, term()}.
parse(Sql) ->
    try
        case rulesql:parsetree(Sql) of
            {ok, {select, Clauses}} ->
                {ok, #select{
                    is_foreach = false,
                    fields = get_value(fields, Clauses),
                    doeach = [],
                    incase = {},
                    from = get_value(from, Clauses),
                    where = get_value(where, Clauses)
                }};
            {ok, {foreach, Clauses}} ->
                {ok, #select{
                    is_foreach = true,
                    fields = get_value(fields, Clauses),
                    doeach = get_value(do, Clauses, []),
                    incase = get_value(incase, Clauses, {}),
                    from = get_value(from, Clauses),
                    where = get_value(where, Clauses)
                }};
            Error ->
                {error, Error}
        end
    catch
        _Error:Reason:StackTrace ->
            {error, {Reason, StackTrace}}
    end.

-spec select_fields(select()) -> list(field()).
select_fields(#select{fields = Fields}) ->
    Fields.

-spec select_is_foreach(select()) -> boolean().
select_is_foreach(#select{is_foreach = IsForeach}) ->
    IsForeach.

-spec select_doeach(select()) -> list(field()).
select_doeach(#select{doeach = DoEach}) ->
    DoEach.

-spec select_incase(select()) -> list(field()).
select_incase(#select{incase = InCase}) ->
    InCase.

-spec select_from(select()) -> list(binary()).
select_from(#select{from = From}) ->
    From.

-spec select_where(select()) -> tuple().
select_where(#select{where = Where}) ->
    Where.
