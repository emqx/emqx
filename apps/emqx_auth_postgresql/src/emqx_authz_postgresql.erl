%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authz_postgresql).

-behaviour(emqx_authz_source).

%% AuthZ Callbacks
-export([
    create/1,
    update/1,
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

create(#{query := SQL0} = Source) ->
    {Vars, SQL, Placeholders} = emqx_auth_template:parse_sql(SQL0, '$n', ?ALLOWED_VARS),
    CacheKeyTemplate = emqx_auth_template:cache_key_template(Vars),
    ResourceId = emqx_authz_utils:make_resource_id(?AUTHZ_TYPE),
    {ok, _Data} = emqx_authz_utils:create_resource(
        ResourceId,
        emqx_postgresql,
        Source#{prepare_statement => #{ResourceId => SQL}},
        ?AUTHZ_TYPE
    ),
    Source#{
        annotations => #{
            id => ResourceId, placeholders => Placeholders, cache_key_template => CacheKeyTemplate
        }
    }.

update(#{query := SQL0, annotations := #{id := ResourceId}} = Source) ->
    {Vars, SQL, Placeholders} = emqx_auth_template:parse_sql(SQL0, '$n', ?ALLOWED_VARS),
    CacheKeyTemplate = emqx_auth_template:cache_key_template(Vars),
    case
        emqx_authz_utils:update_resource(
            emqx_postgresql,
            Source#{prepare_statement => #{ResourceId => SQL}},
            ?AUTHZ_TYPE
        )
    of
        {error, Reason} ->
            error({load_config_error, Reason});
        {ok, Id} ->
            Source#{
                annotations => #{
                    id => Id, placeholders => Placeholders, cache_key_template => CacheKeyTemplate
                }
            }
    end.

destroy(#{annotations := #{id := Id}}) ->
    emqx_authz_utils:remove_resource(Id).

authorize(
    Client,
    Action,
    Topic,
    #{
        annotations := #{
            id := ResourceId,
            placeholders := Placeholders,
            cache_key_template := CacheKeyTemplate
        }
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
