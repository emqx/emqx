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

-module(emqx_connector_template_sql).

-export([parse/1]).
-export([parse/2]).
-export([render/3]).
-export([render_strict/3]).

-export([parse_prepstmt/2]).
-export([render_prepstmt/2]).
-export([render_prepstmt_strict/2]).

-export_type([row_template/0]).

-type template() :: emqx_connector_template:t().
-type row_template() :: [emqx_connector_template:placeholder()].
-type bindings() :: emqx_connector_template:bindings().

-type values() :: [emqx_connector_sql:value()].

-type parse_opts() :: #{
    parameters => '$n' | '?',
    % Inherited from `emqx_connector_template:parse_opts()`
    strip_double_quote => boolean()
}.

-type render_opts() :: #{
    escaping => mysql | cql | sql
}.

-define(TEMPLATE_PARSE_OPTS, [strip_double_quote]).

%%

%% @doc Parse an SQL statement string with zero or more placeholders into a template.
-spec parse(unicode:chardata()) ->
    template().
parse(String) ->
    parse(String, #{}).

%% @doc Parse an SQL statement string with zero or more placeholders into a template.
-spec parse(unicode:chardata(), parse_opts()) ->
    template().
parse(String, Opts) ->
    emqx_connector_template:parse(String, Opts).

%% @doc Render an SQL statement template given a set of bindings.
%% Interpolation generally follows the SQL syntax, strings are escaped according to the
%% `escaping` option.
-spec render(template(), bindings(), render_opts()) ->
    {unicode:chardata(), [_Error]}.
render(Template, Bindings, Opts) ->
    emqx_connector_template:render(Template, Bindings, #{
        var_trans => fun(Value) -> emqx_connector_sql:to_sql_string(Value, Opts) end
    }).

%% @doc Render an SQL statement template given a set of bindings.
%% Errors are raised if any placeholders are not bound.
-spec render_strict(template(), bindings(), render_opts()) ->
    unicode:chardata().
render_strict(Template, Bindings, Opts) ->
    emqx_connector_template:render_strict(Template, Bindings, #{
        var_trans => fun(Value) -> emqx_connector_sql:to_sql_string(Value, Opts) end
    }).

%% @doc Parse an SQL statement string into a prepared statement and a row template.
%% The row template is a template for a row of SQL values to be inserted to a database
%% during the execution of the prepared statement.
%% Example:
%% ```
%% {Statement, RowTemplate} = emqx_connector_template_sql:parse_prepstmt(
%%     "INSERT INTO table (id, name, age) VALUES (${id}, ${name}, 42)",
%%     #{parameters => '$n'}
%% ),
%% Statement = <<"INSERT INTO table (id, name, age) VALUES ($1, $2, 42)">>,
%% RowTemplate = [{var, [...]}, ...]
%% ```
-spec parse_prepstmt(unicode:chardata(), parse_opts()) ->
    {unicode:chardata(), row_template()}.
parse_prepstmt(String, Opts) ->
    Template = emqx_connector_template:parse(String, maps:with(?TEMPLATE_PARSE_OPTS, Opts)),
    Statement = mk_prepared_statement(Template, Opts),
    Placeholders = [Placeholder || Placeholder = {var, _} <- Template],
    {Statement, Placeholders}.

mk_prepared_statement(Template, Opts) ->
    ParameterFormat = maps:get(parameters, Opts, '?'),
    {Statement, _} =
        lists:mapfoldl(
            fun
                ({var, _}, Acc) ->
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
    {"$" ++ integer_to_list(N), N + 1}.

%% @doc Render a row template into a list of SQL values.
%% An _SQL value_ is a vaguely defined concept here, it is something that's considered
%% compatible with the protocol of the database being used. See the definition of
%% `emqx_connector_sql:value()` for more details.
-spec render_prepstmt(template(), bindings()) ->
    {values(), [_Error]}.
render_prepstmt(Template, Bindings) ->
    Opts = #{var_trans => fun emqx_connector_sql:to_sql_value/1},
    emqx_connector_template:render(Template, Bindings, Opts).

-spec render_prepstmt_strict(template(), bindings()) ->
    values().
render_prepstmt_strict(Template, Bindings) ->
    Opts = #{var_trans => fun emqx_connector_sql:to_sql_value/1},
    emqx_connector_template:render_strict(Template, Bindings, Opts).
