%%--------------------------------------------------------------------
%% Copyright (c) 2020-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authz_postgresql).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_auth/include/emqx_authz.hrl").

-include_lib("epgsql/include/epgsql.hrl").

-behaviour(emqx_authz_source).

%% AuthZ Callbacks
-export([
    description/0,
    create/1,
    update/1,
    destroy/1,
    authorize/4
]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-define(ALLOWED_VARS, ?AUTHZ_DEFAULT_ALLOWED_VARS).

description() ->
    "AuthZ with PostgreSQL".

create(#{query := SQL0} = Source) ->
    {SQL, PlaceHolders} = emqx_auth_template:parse_sql(SQL0, '$n', ?ALLOWED_VARS),
    ResourceID = emqx_authz_utils:make_resource_id(emqx_postgresql),
    {ok, _Data} = emqx_authz_utils:create_resource(
        ResourceID,
        emqx_postgresql,
        Source#{prepare_statement => #{ResourceID => SQL}}
    ),
    Source#{annotations => #{id => ResourceID, placeholders => PlaceHolders}}.

update(#{query := SQL0, annotations := #{id := ResourceID}} = Source) ->
    {SQL, PlaceHolders} = emqx_auth_template:parse_sql(SQL0, '$n', ?ALLOWED_VARS),
    case
        emqx_authz_utils:update_resource(
            emqx_postgresql,
            Source#{prepare_statement => #{ResourceID => SQL}}
        )
    of
        {error, Reason} ->
            error({load_config_error, Reason});
        {ok, Id} ->
            Source#{annotations => #{id => Id, placeholders => PlaceHolders}}
    end.

destroy(#{annotations := #{id := Id}}) ->
    emqx_authz_utils:remove_resource(Id).

authorize(
    Client,
    Action,
    Topic,
    #{
        annotations := #{
            id := ResourceID,
            placeholders := Placeholders
        }
    }
) ->
    Vars = emqx_authz_utils:vars_for_rule_query(Client, Action),
    RenderedParams = emqx_auth_template:render_sql_params(Placeholders, Vars),
    case
        emqx_resource:simple_sync_query(ResourceID, {prepared_query, ResourceID, RenderedParams})
    of
        {ok, Columns, Rows} ->
            do_authorize(Client, Action, Topic, column_names(Columns), Rows);
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "query_postgresql_error",
                reason => Reason,
                params => RenderedParams,
                resource_id => ResourceID
            }),
            nomatch
    end.

do_authorize(_Client, _Action, _Topic, _ColumnNames, []) ->
    nomatch;
do_authorize(Client, Action, Topic, ColumnNames, [Row | Tail]) ->
    case emqx_authz_utils:do_authorize(postgresql, Client, Action, Topic, ColumnNames, Row) of
        nomatch ->
            do_authorize(Client, Action, Topic, ColumnNames, Tail);
        {matched, Permission} ->
            {matched, Permission}
    end.

column_names(Columns) ->
    lists:map(
        fun(#column{name = Name}) -> Name end,
        Columns
    ).
