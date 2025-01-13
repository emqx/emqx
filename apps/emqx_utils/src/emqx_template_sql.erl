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

-module(emqx_template_sql).

-export([parse/1]).
-export([parse/2]).
-export([has_placeholder/1]).
-export([render/3]).
-export([render_strict/3]).

-export([parse_prepstmt/2]).
-export([render_prepstmt/2]).
-export([render_prepstmt_strict/2]).

-export_type([
    row_template/0,
    statement/0,
    raw_statement_template/0,
    sql_parameters/0
]).

-type template() :: emqx_template:str().
-type row_template() :: [emqx_template:placeholder()].
-type context() :: emqx_template:context().
-type statement() :: unicode:chardata().
-type raw_statement_template() :: unicode:chardata().
-type sql_parameters() :: '$n' | ':n' | '?'.

-type values() :: [emqx_utils_sql:value()].

-type parse_opts() :: #{
    parameters => sql_parameters(),
    % Inherited from `emqx_template:parse_opts()`
    strip_double_quote => boolean()
}.

-type render_opts() :: #{
    %% String escaping rules to use.
    %% Default: `sql` (generic)
    escaping => sql | mysql | cql,
    %% Value to map `undefined` to, either to NULLs or to arbitrary strings.
    %% Default: `null`
    undefined => null | unicode:chardata()
}.

-define(TEMPLATE_PARSE_OPTS, [strip_double_quote]).

%%

%% @doc Parse an SQL statement string with zero or more placeholders into a template.
-spec parse(raw_statement_template()) ->
    template().
parse(String) ->
    parse(String, #{}).

%% @doc Parse an SQL statement string with zero or more placeholders into a template.
-spec parse(raw_statement_template(), parse_opts()) ->
    template().
parse(String, Opts) ->
    emqx_template:parse(String, Opts).

-spec has_placeholder(raw_statement_template()) -> boolean().
has_placeholder(String) ->
    lists:any(
        fun
            ({var, _, _}) -> true;
            (_) -> false
        end,
        parse(String)
    ).

%% @doc Render an SQL statement template given a set of bindings.
%% Interpolation generally follows the SQL syntax, strings are escaped according to the
%% `escaping` option.
-spec render(template(), context(), render_opts()) ->
    {statement(), [_Error]}.
render(Template, Context, Opts) ->
    emqx_template:render(Template, Context, #{
        var_trans => fun(Value) -> emqx_utils_sql:to_sql_string(Value, Opts) end
    }).

%% @doc Render an SQL statement template given a set of bindings.
%% Errors are raised if any placeholders are not bound.
-spec render_strict(template(), context(), render_opts()) ->
    statement().
render_strict(Template, Context, Opts) ->
    emqx_template:render_strict(Template, Context, #{
        var_trans => fun(Value) -> emqx_utils_sql:to_sql_string(Value, Opts) end
    }).

%% @doc Parse an SQL statement string into a prepared statement and a row template.
%% The row template is a template for a row of SQL values to be inserted to a database
%% during the execution of the prepared statement.
%% Example:
%% ```
%% {Statement, RowTemplate} = emqx_template_sql:parse_prepstmt(
%%     "INSERT INTO table (id, name, age) VALUES (${id}, ${name}, 42)",
%%     #{parameters => '$n'}
%% ),
%% Statement = <<"INSERT INTO table (id, name, age) VALUES ($1, $2, 42)">>,
%% RowTemplate = [{var, "...", [...]}, ...]
%% ```
-spec parse_prepstmt(raw_statement_template(), parse_opts()) ->
    {statement(), row_template()}.
parse_prepstmt(String, Opts) ->
    Template = emqx_template:parse(String, maps:with(?TEMPLATE_PARSE_OPTS, Opts)),
    Statement = mk_prepared_statement(Template, Opts),
    Placeholders = [Placeholder || Placeholder <- Template, element(1, Placeholder) == var],
    {Statement, Placeholders}.

mk_prepared_statement(Template, Opts) ->
    ParameterFormat = maps:get(parameters, Opts, '?'),
    {Statement, _} =
        lists:mapfoldl(
            fun
                (Var, Acc) when element(1, Var) == var ->
                    mk_replace(ParameterFormat, Acc);
                (String, Acc) ->
                    {String, Acc}
            end,
            1,
            Template
        ),
    Statement.

mk_replace('?', Acc) ->
    {"?", Acc};
mk_replace('$n', N) ->
    {"$" ++ integer_to_list(N), N + 1};
mk_replace(':n', N) ->
    {":" ++ integer_to_list(N), N + 1}.

%% @doc Render a row template into a list of SQL values.
%% An _SQL value_ is a vaguely defined concept here, it is something that's considered
%% compatible with the protocol of the database being used. See the definition of
%% `emqx_utils_sql:value()` for more details.
-spec render_prepstmt(template(), context()) ->
    {values(), [_Error]}.
render_prepstmt(Template, Context) ->
    Opts = #{var_trans => fun emqx_utils_sql:to_sql_value/1},
    emqx_template:render(Template, Context, Opts).

-spec render_prepstmt_strict(template(), context()) ->
    values().
render_prepstmt_strict(Template, Context) ->
    Opts = #{var_trans => fun emqx_utils_sql:to_sql_value/1},
    emqx_template:render_strict(Template, Context, Opts).
