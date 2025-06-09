%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authz_postgresql).

-behaviour(emqx_authz_source).

%% AuthZ Callbacks
-export([
    create/1,
    update/2,
    destroy/1,
    authorize/4
]).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_auth/include/emqx_authz.hrl").
-include_lib("epgsql/include/epgsql.hrl").
-include("emqx_auth_postgresql.hrl").

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-define(ALLOWED_VARS, ?AUTHZ_DEFAULT_ALLOWED_VARS).

create(Source) ->
    ResourceId = emqx_authz_utils:make_resource_id(?AUTHZ_TYPE),
    State = new_state(ResourceId, Source),
    ok = emqx_authz_utils:create_resource(emqx_postgresql, State),
    State.

update(#{resource_id := ResourceId} = _State, Source) ->
    State = new_state(ResourceId, Source),
    ok = emqx_authz_utils:update_resource(emqx_postgresql, State),
    State.

destroy(#{resource_id := ResourceId}) ->
    emqx_authz_utils:remove_resource(ResourceId).

authorize(
    Client,
    Action,
    Topic,
    #{
        resource_id := ResourceId,
        placeholders := Placeholders,
        cache_key_template := CacheKeyTemplate
    }
) ->
    Vars = emqx_authz_utils:vars_for_rule_query(Client, Action),
    RenderedParams = emqx_auth_template:render_sql_params(Placeholders, Vars),
    CacheKey = emqx_auth_template:cache_key(Vars, CacheKeyTemplate),
    case
        emqx_authz_utils:cached_simple_sync_query(
            CacheKey, ResourceId, {prepared_query, ResourceId, RenderedParams}
        )
    of
        {ok, Columns, Rows} ->
            do_authorize(Client, Action, Topic, column_names(Columns), Rows);
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "query_postgresql_error",
                reason => Reason,
                params => RenderedParams,
                resource_id => ResourceId
            }),
            nomatch
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

new_state(ResourceId, #{query := SQL} = Source0) ->
    {Vars, SQLTemplate, Placeholders} = emqx_auth_template:parse_sql(SQL, '$n', ?ALLOWED_VARS),
    CacheKeyTemplate = emqx_auth_template:cache_key_template(Vars),
    Source = Source0#{prepare_statement => #{ResourceId => SQLTemplate}},
    ResourceConfig = emqx_authz_utils:resource_config(
        [query], Source
    ),
    emqx_authz_utils:init_state(Source, #{
        resource_config => ResourceConfig,
        resource_id => ResourceId,
        placeholders => Placeholders,
        cache_key_template => CacheKeyTemplate
    }).

do_authorize(_Client, _Action, _Topic, _ColumnNames, []) ->
    nomatch;
do_authorize(Client, Action, Topic, ColumnNames, [Row | Tail]) ->
    case emqx_authz_utils:authorize_with_row(postgresql, Client, Action, Topic, ColumnNames, Row) of
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
