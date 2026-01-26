%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authz_mysql).

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
-include("emqx_auth_mysql.hrl").

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-define(PREPARE_KEY, ?MODULE).
-define(ALLOWED_VARS, ?AUTHZ_DEFAULT_ALLOWED_VARS).

create(Source) ->
    ResourceId = emqx_authz_utils:make_resource_id(?AUTHZ_TYPE),
    State = new_state(ResourceId, Source),
    ok = emqx_authz_utils:create_resource(emqx_auth_mysql_connector, State),
    State.

update(#{resource_id := ResourceId} = _State, Source) ->
    State = new_state(ResourceId, Source),
    ok = emqx_authz_utils:update_resource(emqx_auth_mysql_connector, State),
    State.

destroy(#{resource_id := ResourceId}) ->
    emqx_authz_utils:remove_resource(ResourceId).

authorize(
    Client,
    Action,
    Topic,
    #{
        resource_id := ResourceId,
        args_template := ArgsTemplate,
        query_timeout := QueryTimeout,
        cache_key_template := CacheKeyTemplate
    }
) ->
    Vars = emqx_authz_utils:vars_for_rule_query(Client, Action),
    RenderedArgs = emqx_auth_template:render_sql_params(ArgsTemplate, Vars),
    CacheKey = emqx_auth_template:cache_key(Vars, CacheKeyTemplate),
    case
        emqx_authz_utils:cached_simple_sync_query(
            CacheKey,
            ResourceId,
            {prepared_query, ?PREPARE_KEY, RenderedArgs, #{timeout => QueryTimeout}}
        )
    of
        {ok, ColumnNames, Rows} ->
            do_authorize(Client, Action, Topic, ColumnNames, Rows);
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "query_mysql_error",
                reason => Reason,
                args_template => ArgsTemplate,
                params => RenderedArgs,
                resource_id => ResourceId
            }),
            nomatch
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

new_state(
    ResourceId,
    #{
        query := SQLTemplate,
        query_timeout := QueryTimeout
    } = Source0
) ->
    {Vars, SQL, ArgsTemplate} = emqx_auth_template:parse_sql(SQLTemplate, '?', ?ALLOWED_VARS),
    CacheKeyTemplate = emqx_auth_template:cache_key_template(Vars),
    Source = Source0#{prepare_statements => #{?PREPARE_KEY => SQL}},
    ResourceConfig = emqx_authz_utils:cleanup_resource_config(
        [query, query_timeout], Source
    ),
    emqx_authz_utils:init_state(Source, #{
        resource_config => ResourceConfig,
        resource_id => ResourceId,
        args_template => ArgsTemplate,
        query_timeout => QueryTimeout,
        cache_key_template => CacheKeyTemplate
    }).

do_authorize(_Client, _Action, _Topic, _ColumnNames, []) ->
    nomatch;
do_authorize(Client, Action, Topic, ColumnNames, [Row | Tail]) ->
    case emqx_authz_utils:authorize_with_row(mysql, Client, Action, Topic, ColumnNames, Row) of
        nomatch ->
            do_authorize(Client, Action, Topic, ColumnNames, Tail);
        {matched, Permission} ->
            {matched, Permission}
    end.
