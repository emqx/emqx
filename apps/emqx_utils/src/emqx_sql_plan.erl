%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_sql_plan).

-export([compile/2, render/3, render_batch/3, parse_placeholder/1]).
-export_type([plan/0]).

-elvis([
    {elvis_style, invalid_dynamic_call, #{
        ignore => [
            {emqx_sql_plan, compile, 2},
            {emqx_sql_plan, render, 3},
            {emqx_sql_plan, render_batch, 3}
        ]
    }}
]).

-record(plan, {plan :: term(), cb_mod :: module()}).
-opaque plan() :: #plan{}.

-callback compile(unicode:chardata()) -> {ok, term()} | {error, term()}.
-callback render(term(), map(), map()) -> {ok, iolist()} | {error, term()}.
-callback render_batch(term(), [map()], map()) -> {ok, iolist()} | {error, term()}.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec compile(module(), unicode:chardata()) -> {ok, plan()} | {error, term()}.
compile(Module, Template) ->
    case Module:compile(Template) of
        {ok, Plan} -> {ok, #plan{plan = Plan, cb_mod = Module}};
        {error, _} = Error -> Error
    end.

-spec render(plan(), map(), map()) -> {ok, iolist()} | {error, term()}.
render(#plan{plan = Plan, cb_mod = Module}, Data, Opts) ->
    Module:render(Plan, Data, Opts).

-spec render_batch(plan(), [map()], map()) -> {ok, iolist()} | {error, term()}.
render_batch(#plan{plan = Plan, cb_mod = Module}, Data, Opts) ->
    Module:render_batch(Plan, Data, Opts).

-spec parse_placeholder(binary()) ->
    {ok, emqx_template:placeholder()} | {error, invalid_placeholder}.
parse_placeholder(Source) ->
    case valid_placeholder_source(Source) of
        true ->
            case emqx_template:parse(Source) of
                [{var, _, _} = Placeholder] -> {ok, Placeholder};
                _ -> {error, invalid_placeholder}
            end;
        false ->
            {error, invalid_placeholder}
    end.

%%------------------------------------------------------------------------------
%% Private
%%------------------------------------------------------------------------------

%% Restrict emqx_template's envelope to nonempty dotted paths.
%% Retain `${}` and `${.}`.
%% See emqx_template:parse/1 in apps/emqx_utils/src/emqx_template.erl.
valid_placeholder_source(<<"${}">>) ->
    true;
valid_placeholder_source(<<"${.}">>) ->
    true;
valid_placeholder_source(Source) ->
    re:run(
        Source,
        <<"^\\$\\{\\.?[A-Za-z0-9_]+(?:\\.[A-Za-z0-9_]+)*\\}$">>,
        [{capture, none}]
    ) =:= match.
