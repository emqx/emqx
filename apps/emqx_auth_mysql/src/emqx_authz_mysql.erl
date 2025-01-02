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

-module(emqx_authz_mysql).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_auth/include/emqx_authz.hrl").

-behaviour(emqx_authz_source).

-define(PREPARE_KEY, ?MODULE).

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
    "AuthZ with Mysql".

create(#{query := SQL} = Source0) ->
    {PrepareSQL, TmplToken} = emqx_auth_template:parse_sql(SQL, '?', ?ALLOWED_VARS),
    ResourceId = emqx_authz_utils:make_resource_id(?MODULE),
    Source = Source0#{prepare_statement => #{?PREPARE_KEY => PrepareSQL}},
    {ok, _Data} = emqx_authz_utils:create_resource(ResourceId, emqx_mysql, Source),
    Source#{annotations => #{id => ResourceId, tmpl_token => TmplToken}}.

update(#{query := SQL} = Source0) ->
    {PrepareSQL, TmplToken} = emqx_auth_template:parse_sql(SQL, '?', ?ALLOWED_VARS),
    Source = Source0#{prepare_statement => #{?PREPARE_KEY => PrepareSQL}},
    case emqx_authz_utils:update_resource(emqx_mysql, Source) of
        {error, Reason} ->
            error({load_config_error, Reason});
        {ok, Id} ->
            Source#{annotations => #{id => Id, tmpl_token => TmplToken}}
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
            tmpl_token := TmplToken
        }
    }
) ->
    Vars = emqx_authz_utils:vars_for_rule_query(Client, Action),
    RenderParams = emqx_auth_template:render_sql_params(TmplToken, Vars),
    case
        emqx_resource:simple_sync_query(ResourceID, {prepared_query, ?PREPARE_KEY, RenderParams})
    of
        {ok, ColumnNames, Rows} ->
            do_authorize(Client, Action, Topic, ColumnNames, Rows);
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "query_mysql_error",
                reason => Reason,
                tmpl_token => TmplToken,
                params => RenderParams,
                resource_id => ResourceID
            }),
            nomatch
    end.

do_authorize(_Client, _Action, _Topic, _ColumnNames, []) ->
    nomatch;
do_authorize(Client, Action, Topic, ColumnNames, [Row | Tail]) ->
    case emqx_authz_utils:do_authorize(mysql, Client, Action, Topic, ColumnNames, Row) of
        nomatch ->
            do_authorize(Client, Action, Topic, ColumnNames, Tail);
        {matched, Permission} ->
            {matched, Permission}
    end.
